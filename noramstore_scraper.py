"""
Noram Store (noramstore.ca) CSV scraper.

Reads part numbers from a CSV, queries the Sunhammer API (same as the storefront search),
and for each row scrapes the product page only when API ``stockid`` exactly matches that
part number. Output is one CSV row per input row (filled or blank). See ``main()`` below.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, quote, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_ORIGIN = "https://www.noramstore.ca"
DEFAULT_SEARCH_PAGE = f"{DEFAULT_ORIGIN}/search.html"
SUNHAMMER_API_BASE = "https://api.sunhammer.io"

# Partslogic on noramstore.ca calls /products with these params (see browser network tab).
# Omitting them can change result sets vs the live site.
SUNHAMMER_PRODUCT_SEARCH_DEFAULTS: Dict[str, str] = {
    "Fitment-Specific or Universal": "Fitment-Specific",
    "fitment": "",
}

# Browser-like headers reduce the chance of generic WAF blocks on HTML pages.
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.85",
    "Accept-Language": "en-CA,en-US;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


def _normalize_ws(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    cleaned = " ".join(text.split())
    return cleaned or None


def _absolute_url(base: str, maybe_relative: Optional[str]) -> Optional[str]:
    if not maybe_relative:
        return None
    joined = urljoin(base if base.endswith("/") else base + "/", maybe_relative.strip())
    return joined


def _parse_product_id_from_url(url: str) -> Optional[str]:
    m = re.search(r"/i-(\d+)", url)
    return m.group(1) if m else None


def _ensure_www_noram_product_url(url: str) -> str:
    """Normalize store host to https://www.noramstore.ca and keep path/query."""
    parsed = urlparse(url.strip())
    path = parsed.path or ""
    if not path.startswith("/"):
        path = "/" + path
    netloc = "www.noramstore.ca"
    scheme = "https"
    return urlunparse((scheme, netloc, path, "", parsed.query, ""))


def _canonical_product_url_from_listing(
    listing: Dict[str, Any], query_for_url: str
) -> str:
    """Build www.noramstore.ca product URL with q= set to the search term (matches site behavior)."""
    raw_url = listing.get("url") or ""
    if not raw_url:
        raise ValueError(f"listing has no url: {listing!r}")
    product_url = _ensure_www_noram_product_url(raw_url)
    parsed = urlparse(product_url)
    qs = parse_qs(parsed.query)
    qs["q"] = [query_for_url.strip()]
    new_query = urlencode([(k, v[0]) for k, v in qs.items()], quote_via=quote)
    return urlunparse(
        (parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment)
    )


def _parse_dimensions_to_dict(text: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Parse dimension strings like 'Package Dimensions: W17" x H13" x L70"' or '70 x 17 x 13'.

    Returns a dict with float inches and unit 'in', or None if no pattern matches.
    """
    if not text:
        return None
    norm = _normalize_ws(text) or ""
    # Normalize fancy quotes / inch marks that sit next to numbers.
    norm = (
        norm.replace("\u201c", '"')
        .replace("\u201d", '"')
        .replace("\u2033", '"')
        .replace("×", "x")
    )
    w_m = re.search(r"W\s*([\d.]+)", norm, re.I)
    h_m = re.search(r"H\s*([\d.]+)", norm, re.I)
    l_m = re.search(r"L\s*([\d.]+)", norm, re.I)
    if w_m and h_m and l_m:
        return {
            "unit": "in",
            "width": float(w_m.group(1)),
            "height": float(h_m.group(1)),
            "length": float(l_m.group(1)),
        }
    triple = re.search(
        r"([\d.]+)\s*x\s*([\d.]+)\s*x\s*([\d.]+)", norm, re.I
    )
    if triple:
        # Order in summaries is typically length × width × height.
        return {
            "unit": "in",
            "length": float(triple.group(1)),
            "width": float(triple.group(2)),
            "height": float(triple.group(3)),
        }
    return None


def _parse_weight_to_lb(text: Optional[str]) -> Optional[float]:
    """Parse weight text (e.g. 'Weight: 7.0 lbs.') to pounds as float."""
    if not text:
        return None
    norm = _normalize_ws(text) or ""
    m = re.search(r"([\d.]+)\s*(?:lbs?|lb)\.?", norm, re.I)
    if m:
        return float(m.group(1))
    return None


class NoramStoreScraper:
    """
    For each part number: search the Sunhammer API for an exact ``stockid`` match,
    then fetch and parse that product page. Used by the CSV CLI in ``main()``.
    """

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        origin: str = DEFAULT_ORIGIN,
        search_page_url: str = DEFAULT_SEARCH_PAGE,
        timeout: int = 30,
        max_retries: int = 5,
        backoff_factor: float = 0.6,
        status_forcelist: Tuple[int, ...] = (429, 500, 502, 503, 504),
        headers: Optional[Dict[str, str]] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.origin = origin.rstrip("/")
        self.search_page_url = search_page_url
        self.timeout = timeout
        self._api_key_override = api_key
        self._cached_api_key: Optional[str] = api_key
        self.log = logger or logging.getLogger(__name__)
        if not self.log.handlers:
            logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

        self.session = requests.Session()
        self.session.headers.update(headers or DEFAULT_HEADERS)

        retry = Retry(
            total=max_retries,
            connect=max_retries,
            read=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            allowed_methods=frozenset(["GET", "HEAD"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    # ---------------------------------------------------------------------
    # API key (Sunhammer / Partslogic)
    # ---------------------------------------------------------------------
    def get_api_key(self, *, force_refresh: bool = False) -> Optional[str]:
        """
        Return the Sunhammer API key embedded in search.html (same as the live site).

        You can pass api_key=... to __init__ to skip discovery or when the site rotates keys.
        """
        if self._api_key_override and not force_refresh:
            return self._api_key_override
        if self._cached_api_key and not force_refresh:
            return self._cached_api_key

        self.log.info("Discovering API key from %s", self.search_page_url)
        try:
            html = self.fetch_text(self.search_page_url)
        except requests.RequestException as exc:
            self.log.error("Could not load search page for API key: %s", exc)
            return None

        m = re.search(r'API_KEY:\s*"([^"]+)"', html)
        if not m:
            self.log.error("API_KEY pattern not found in search page HTML.")
            return None
        self._cached_api_key = m.group(1).strip()
        self.log.info("API key discovered (prefix=%s…)", self._cached_api_key[:8])
        return self._cached_api_key

    # ---------------------------------------------------------------------
    # HTTP helpers
    # ---------------------------------------------------------------------
    def fetch_text(self, url: str) -> str:
        """GET URL and return decoded text. Retries are handled by the HTTPAdapter."""
        self.log.debug("GET %s", url)
        resp = self.session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.text

    def fetch_json(
        self,
        url: str,
        *,
        params: Optional[Dict[str, str]] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> Any:
        headers = dict(self.session.headers)
        headers["Accept"] = "application/json"
        if extra_headers:
            headers.update(extra_headers)
        self.log.debug("GET JSON %s params=%s", url, params)
        resp = self.session.get(url, timeout=self.timeout, headers=headers, params=params)
        resp.raise_for_status()
        return resp.json()

    # ---------------------------------------------------------------------
    # Search (Sunhammer products API)
    # ---------------------------------------------------------------------
    def search_products(
        self,
        sku_or_term: str,
        *,
        page: int = 1,
        limit: int = 50,
        api_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Query the storefront product search API used by Partslogic.

        The most useful query parameter for SKU-style lookups is ``q``.

        Default facet params mirror the storefront widget so ordering and filters match
        what users see when searching.
        """
        key = api_key or self.get_api_key()
        if not key:
            raise RuntimeError(
                "Sunhammer API key unavailable. Pass api_key=... to NoramStoreScraper(...) "
                "or ensure search.html is reachable."
            )

        params = {
            **SUNHAMMER_PRODUCT_SEARCH_DEFAULTS,
            "page": str(page),
            "limit": str(limit),
            "q": sku_or_term.strip(),
        }
        url = f"{SUNHAMMER_API_BASE}/products"
        self.log.info(
            "API search q=%r page=%s limit=%s", sku_or_term.strip(), page, limit
        )
        return self.fetch_json(
            url,
            params=params,
            extra_headers={
                "sunhammer-api-key": key,
                "Referer": f"{DEFAULT_ORIGIN}/",
            },
        )

    def find_exact_stock_listing(
        self,
        sku: str,
        *,
        limit: int = 50,
        max_pages: Optional[int] = None,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[int], int]:
        """
        Walk search API pages until a listing with ``stockid`` equal to ``sku`` (case-insensitive).

        Every row on each page is checked; a non-matching *first* result does not mean there
        is no match later in the same page or on the next page.

        Returns ``(listing | None, api_total, pages_scanned)``.
        """
        q = sku.strip()
        if not q:
            return None, None, 0
        want = q.upper()
        page = 1
        total: Optional[int] = None
        pages_scanned = 0
        cumulative = 0

        while True:
            if max_pages is not None and page > max_pages:
                break
            payload = self.search_products(q, page=page, limit=limit)
            pages_scanned += 1
            if total is None and payload.get("total") is not None:
                try:
                    total = int(payload["total"])
                except (TypeError, ValueError):
                    total = None
            batch = payload.get("list") or []
            for item in batch:
                sid = (item.get("stockid") or "").strip().upper()
                if sid == want:
                    self.log.info("Exact stockid match for %r on API page %s", q, page)
                    return item, total, pages_scanned
            if not batch:
                break
            cumulative += len(batch)
            if len(batch) < limit:
                break
            if total is not None and cumulative >= total:
                break
            page += 1

        self.log.info("No exact stockid match for %r (%s API page(s))", q, pages_scanned)
        return None, total, pages_scanned

    def scrape_exact_stock_match(
        self,
        part_number: str,
        *,
        limit: int = 50,
        max_pages: Optional[int] = None,
        sleep_s: float = 0.0,
    ) -> Dict[str, Any]:
        """
        If API ``stockid`` matches ``part_number`` (any page), fetch and parse that product.
        Returns dict with ``results`` (0 or 1 record) and ``errors``.
        """
        q = part_number.strip()
        results: List[Dict[str, Any]] = []
        errors: List[Dict[str, Any]] = []

        if not q:
            return {
                "search_query": q,
                "total_reported_by_api": None,
                "exact_match_found": False,
                "api_pages_scanned": 0,
                "listings_fetched": 0,
                "results": [],
                "errors": [],
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            }

        listing, total, pages_scanned = self.find_exact_stock_listing(
            q, limit=limit, max_pages=max_pages
        )
        if listing is None:
            return {
                "search_query": q,
                "total_reported_by_api": total,
                "exact_match_found": False,
                "api_pages_scanned": pages_scanned,
                "listings_fetched": 0,
                "results": [],
                "errors": [],
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            }

        api_fields = (
            "id",
            "stockid",
            "title",
            "price",
            "brand_name",
            "condition",
            "url",
            "dealerid",
            "availability",
            "availability_remarks",
            "image_url",
        )
        try:
            url = _canonical_product_url_from_listing(listing, q)
        except ValueError as exc:
            errors.append({"stockid": listing.get("stockid"), "error": str(exc)})
            return {
                "search_query": q,
                "total_reported_by_api": total,
                "exact_match_found": True,
                "api_pages_scanned": pages_scanned,
                "listings_fetched": 0,
                "results": [],
                "errors": errors,
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            }

        try:
            html = self.fetch_text(url)
            meta = {
                "search": {
                    "query": q,
                    "exact_stock_match": True,
                    "api_pages_scanned": pages_scanned,
                    "total_reported_by_api": total,
                    "api_hit": {k: listing.get(k) for k in api_fields},
                }
            }
            record = self.parse_product_page(
                html, page_url=url, expected_sku=q, extra_meta=meta
            )
            api_stock = listing.get("stockid")
            page_sku = record.get("sku")
            if (
                api_stock
                and page_sku
                and api_stock.strip().upper() != page_sku.strip().upper()
            ):
                record["sku_consistency_warning"] = {
                    "api_stockid": api_stock,
                    "page_sku": page_sku,
                }
            results.append(record)
        except requests.RequestException as exc:
            self.log.error("HTTP error scraping %s: %s", url, exc)
            errors.append(
                {"url": url, "stockid": listing.get("stockid"), "error": str(exc)}
            )
        except Exception as exc:
            self.log.exception("Failed scraping %s", url)
            errors.append(
                {"url": url, "stockid": listing.get("stockid"), "error": str(exc)}
            )

        if sleep_s > 0:
            time.sleep(sleep_s)

        return {
            "search_query": q,
            "total_reported_by_api": total,
            "exact_match_found": True,
            "api_pages_scanned": pages_scanned,
            "listings_fetched": 1 if results else 0,
            "results": results,
            "errors": errors,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }

    # ---------------------------------------------------------------------
    # Product page parsing
    # ---------------------------------------------------------------------
    def parse_product_page(
        self,
        html: str,
        *,
        page_url: str,
        expected_sku: Optional[str] = None,
        extra_meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        soup = BeautifulSoup(html, "html.parser")
        base_for_urls = page_url

        def sel_text(selector: str) -> Optional[str]:
            node = soup.select_one(selector)
            if not node:
                return None
            return _normalize_ws(node.get_text())

        def sel_href_text(selector: str) -> Tuple[Optional[str], Optional[str]]:
            node = soup.select_one(selector)
            if not node:
                return None, None
            href = node.get("href")
            if href:
                href = _absolute_url(self.origin + "/", href)
            return href, _normalize_ws(node.get_text())

        title = sel_text("h1.wsm-prod-title")
        sku = sel_text("span.wsm-prod-sku")
        price = sel_text("span.wsm-cat-price-price-value")
        availability = sel_text("span.wsm-cat-avail-remarks-value")

        brand_href, brand = sel_href_text("li.wsm_product_info_brand a")
        condition = sel_text("li.wsm_product_info_condition")
        dimensions_raw = sel_text("li.wsm_product_info_dimensions")
        weight_raw = sel_text("li.wsm_product_info_weight")
        dimensions = _parse_dimensions_to_dict(dimensions_raw)
        weight_lb = _parse_weight_to_lb(weight_raw)

        fitment_anchors = soup.select("li.wsm_product_details_tags2 a")
        fitment: List[Dict[str, Optional[str]]] = []
        for a in fitment_anchors:
            href = a.get("href")
            if href:
                href = _absolute_url(self.origin + "/", href)
            fitment.append({"text": _normalize_ws(a.get_text()), "url": href})

        features: List[List[str]] = []
        summary_table = soup.select_one(".wsm-prod-summary table")
        if summary_table:
            for tr in summary_table.select("tr"):
                cells = [
                    _normalize_ws(td.get_text())
                    for td in tr.select("td, th")
                    if _normalize_ws(td.get_text())
                ]
                if cells:
                    features.append(cells)

        main_image: Optional[str] = None
        img = soup.select_one("#wsm-prod-rotate-image img")
        if img and img.has_attr("src"):
            main_image = _absolute_url(self.origin + "/", img["src"])

        expected = (expected_sku or "").strip() or None
        sku_mismatch = False
        if expected and sku:
            sku_mismatch = expected.upper() != sku.upper()

        record: Dict[str, Any] = {
            "source_url": page_url,
            "product_id": _parse_product_id_from_url(page_url),
            "title": title,
            "sku": sku,
            "price": price,
            "availability": availability,
            "brand": brand,
            "brand_url": brand_href,
            "condition": condition,
            "fitment": fitment,
            "features_rows": features,
            "dimensions": dimensions,
            "weight_lb": weight_lb,
            "main_image_url": main_image,
            "expected_sku": expected,
            "sku_mismatch": sku_mismatch,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }
        if extra_meta:
            record.update(extra_meta)
        return record

    # ---------------------------------------------------------------------
    # Batch + IO
    # ---------------------------------------------------------------------
    @staticmethod
    def save_json(data: Any, path: str) -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    root = logging.getLogger()
    root.handlers.clear()
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


# --- CSV export --------------------------------------------------------------

CSV_EXPORT_FIELDNAMES = [
    "SKU",
    "product_id",
    "title",
    "price",
    "brand",
    "width",
    "height",
    "length",
    "weight_lb",
]

BLANK_CSV_ROW = {k: "" for k in CSV_EXPORT_FIELDNAMES}


def _count_csv_data_rows(path: str) -> int:
    """Number of data rows in a CSV (excludes header). Missing/empty file -> 0."""
    if not os.path.isfile(path):
        return 0
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                return 0
            return sum(1 for _ in reader)
    except OSError:
        return 0


def _validate_output_csv_header(path: str) -> None:
    """Ensure existing output uses the same columns (for --resume)."""
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader, None)
    if not header:
        raise ValueError(f"Output CSV has no header: {path}")
    got = [h.strip() for h in header]
    if got != CSV_EXPORT_FIELDNAMES:
        raise ValueError(
            f"Output CSV columns do not match expected header.\n"
            f"Expected: {CSV_EXPORT_FIELDNAMES}\nGot: {got}"
        )


def _count_filled_and_blank_rows(path: str) -> Tuple[int, int]:
    """Scan output CSV: rows with non-empty SKU vs blank."""
    filled = 0
    blank = 0
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if (row.get("SKU") or "").strip():
                filled += 1
            else:
                blank += 1
    return filled, blank


def _read_skus_from_csv(path: str, column_hint: Optional[str] = None) -> Tuple[List[str], str]:
    """
    Read SKU / part numbers from a CSV. Header row required (uses csv.DictReader).

    Auto-detect column: part_number, sku, stockid (case/spacing insensitive).
    """
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        if not fieldnames:
            return [], ""

        if column_hint:
            hint = column_hint.strip().lower()
            use_key: Optional[str] = None
            for fn in fieldnames:
                if fn and fn.strip().lower() == hint:
                    use_key = fn
                    break
            if use_key is None:
                raise ValueError(
                    f"Column {column_hint!r} not found in CSV. Columns: {fieldnames!r}"
                )
        else:
            norm_to_orig = {
                re.sub(r"[\s_]+", "_", (f or "").strip().lower()): f for f in fieldnames if f
            }
            use_key = None
            for pref in ("part_number", "sku", "stockid"):
                if pref in norm_to_orig:
                    use_key = norm_to_orig[pref]
                    break
            if use_key is None:
                use_key = fieldnames[0]

        skus: List[str] = []
        for row in reader:
            raw = (row.get(use_key) or "").strip()
            if raw:
                skus.append(raw)
    return skus, use_key


def _numeric_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return format(value, "g")
    if isinstance(value, int):
        return str(value)
    return str(value).strip()


def _price_for_csv(price: Optional[str]) -> str:
    if not price:
        return ""
    cleaned = re.sub(r"[^\d.]", "", str(price))
    if not cleaned:
        return ""
    try:
        n = float(cleaned)
        if n == int(n):
            return str(int(n))
        return format(n, "g")
    except ValueError:
        return cleaned


def record_to_csv_row(record: Dict[str, Any]) -> Dict[str, str]:
    dim = record.get("dimensions") or {}
    title = record.get("title") or ""
    title = " ".join(str(title).split())
    return {
        "SKU": (record.get("sku") or "").strip(),
        "product_id": str(record.get("product_id") or "").strip(),
        "title": title,
        "price": _price_for_csv(record.get("price")),
        "brand": (record.get("brand") or "").strip(),
        "width": _numeric_cell(dim.get("width")),
        "height": _numeric_cell(dim.get("height")),
        "length": _numeric_cell(dim.get("length")),
        "weight_lb": _numeric_cell(record.get("weight_lb")),
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Read part numbers from INPUT_CSV, search noramstore's API for an exact stockid "
            "match, scrape that product page, and write one row per input to OUTPUT_CSV "
            "(filled or blank)."
        ),
        epilog='Example: python noramstore_scraper.py "all skus.csv" results.csv',
    )
    parser.add_argument(
        "input_csv",
        help="Input CSV with a header row (uses part_number, sku, or stockid column by default).",
    )
    parser.add_argument("output_csv", help="Output CSV path.")
    parser.add_argument(
        "--sku-column",
        metavar="NAME",
        default=None,
        help="Column name for part numbers (default: auto-detect part_number, sku, stockid).",
    )
    parser.add_argument(
        "--api-key",
        dest="api_key",
        default=None,
        help="Override Sunhammer API key from search.html (optional).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="API page size per search (default: 50).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Max API pages to scan per part (default: all).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="Seconds to sleep after each product page fetch.",
    )
    parser.add_argument(
        "--sleep-between-searches",
        type=float,
        default=0.0,
        help="Extra seconds after each input row.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help=(
            "Append to OUTPUT_CSV instead of overwriting. Skips the first N input rows "
            "where N = number of data rows already in OUTPUT_CSV (same INPUT_CSV as before)."
        ),
    )
    parser.add_argument("--verbose", action="store_true", help="Debug logging.")
    args = parser.parse_args(argv)

    _configure_logging(args.verbose)

    csv_in = os.path.abspath(os.path.expanduser(args.input_csv))
    csv_out = os.path.abspath(os.path.expanduser(args.output_csv))
    if not os.path.isfile(csv_in):
        parser.error(f"Input CSV not found: {csv_in}")

    try:
        skus, col = _read_skus_from_csv(csv_in, args.sku_column)
    except ValueError as exc:
        parser.error(str(exc))
    if not skus:
        parser.error(f"No part numbers in column {col!r}: {csv_in}")

    total_input_rows = len(skus)
    err_path = os.path.splitext(csv_out)[0] + "_errors.json"
    prev_errors: List[Dict[str, Any]] = []

    if args.resume:
        if not os.path.isfile(csv_out):
            parser.error(f"--resume requires existing OUTPUT_CSV: {csv_out}")
        try:
            _validate_output_csv_header(csv_out)
        except ValueError as exc:
            parser.error(str(exc))
        already_done = _count_csv_data_rows(csv_out)
        if already_done > total_input_rows:
            parser.error(
                f"Output has {already_done} data rows but input has only {total_input_rows} SKUs."
            )
        if already_done == total_input_rows:
            print(
                f"Nothing to do: {csv_out} already has {already_done} rows "
                f"(matches input length)."
            )
            return 0
        skus_todo = skus[already_done:]
        if os.path.isfile(err_path):
            try:
                with open(err_path, encoding="utf-8") as ef:
                    prev_doc = json.load(ef)
                prev_errors = list(prev_doc.get("errors", []))
            except (OSError, json.JSONDecodeError):
                pass
        file_mode = "a"
        write_header = False
        start_log_index = already_done
    else:
        skus_todo = skus
        file_mode = "w"
        write_header = True
        start_log_index = 0

    out_dir = os.path.dirname(csv_out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    scraper = NoramStoreScraper(api_key=args.api_key)
    errors_this_run: List[Dict[str, Any]] = []
    # utf-8-sig only for a new file (BOM once). Append with plain utf-8 to avoid a second BOM.
    out_encoding = "utf-8-sig" if write_header else "utf-8"
    with open(csv_out, file_mode, newline="", encoding=out_encoding) as f:
        writer = csv.DictWriter(f, fieldnames=CSV_EXPORT_FIELDNAMES)
        if write_header:
            writer.writeheader()
        for j, term in enumerate(skus_todo):
            i = start_log_index + j
            scraper.log.info(
                "Row %s/%s: %r (column %r)",
                i + 1,
                total_input_rows,
                term,
                col,
            )
            try:
                bundle = scraper.scrape_exact_stock_match(
                    term,
                    limit=args.limit,
                    max_pages=args.max_pages,
                    sleep_s=args.sleep,
                )
                for err in bundle["errors"]:
                    errors_this_run.append({"search_term": term, **err})
                if bundle["results"]:
                    writer.writerow(record_to_csv_row(bundle["results"][0]))
                else:
                    writer.writerow(BLANK_CSV_ROW)
            except Exception as exc:
                scraper.log.exception("Failed for %r", term)
                errors_this_run.append({"search_term": term, "error": str(exc)})
                writer.writerow(BLANK_CSV_ROW)
            f.flush()
            if args.sleep_between_searches > 0:
                time.sleep(args.sleep_between_searches)

    matched_rows, blank_rows = _count_filled_and_blank_rows(csv_out)
    errors_acc = prev_errors + errors_this_run
    scraper.save_json(
        {
            "output_rows": total_input_rows,
            "matched_rows": matched_rows,
            "blank_rows": blank_rows,
            "search_terms": total_input_rows,
            "errors": errors_acc,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "resumed": bool(args.resume),
        },
        err_path,
    )
    print(
        f"Output {csv_out}: {matched_rows + blank_rows} total data rows "
        f"({matched_rows} exact SKU match, {blank_rows} blank); "
        f"{len(errors_acc)} error entries -> {err_path}"
        + (f" (appended {len(skus_todo)} rows)" if args.resume else "")
    )
    return 0 if not errors_acc else 2


if __name__ == "__main__":
    raise SystemExit(main())

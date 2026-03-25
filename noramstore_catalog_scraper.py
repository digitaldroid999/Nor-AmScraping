"""
Noram Store full-catalog crawler via Sunhammer fitment + products APIs.

Workflow (depth-first):

1. List **years** (newest first: 2025, 2024, …).
2. For each year, list **makes** (e.g. Acura, Audi, …).
3. For each make, list **models** (e.g. Integra, …).
4. For each ``Year|Make|Model``, paginate ``/products``; for each listing, if ``stockid`` is
   in the whitelist CSV, scrape HTML (same as ``noramstore_scraper``) and **flush** rows
   immediately; otherwise skip.

Checkpointing: ``--resume`` saves year/make/model indices plus product pagination state.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import random
import re
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import requests

from noramstore_scraper import (
    CSV_EXPORT_FIELDNAMES,
    NoramStoreScraper,
    _canonical_product_url_from_listing,
    _parse_product_id_from_url,
    _validate_output_csv_header,
    record_to_csv_row,
)

DEFAULT_ORIGIN = "https://www.noramstore.ca"
SUNHAMMER_API_BASE = "https://api.sunhammer.io"
FITMENT_LABELS_BASE = f"{SUNHAMMER_API_BASE}/fitment/labels"
DEFAULT_GROUP_ID = 71685
DEFAULT_API_KEY = os.environ.get(
    "SUNHAMMER_API_KEY", "85a70623-0e93-4e93-97c7-d0cdf7ab30df"
)

DEFAULT_PRODUCT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Referer": f"{DEFAULT_ORIGIN}/",
}

# ---------------------------------------------------------------------------
# CSV / whitelist
# ---------------------------------------------------------------------------


def _read_skus_from_csv(path: str, column_hint: Optional[str] = None) -> Tuple[List[str], str]:
    """Read part numbers; auto-detect part_number / sku / stockid column."""
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


def _whitelist_set(skus: Iterable[str]) -> Set[str]:
    return {(s or "").strip().upper() for s in skus if (s or "").strip()}


def _product_id_from_listing(listing: Dict[str, Any], stockid: str) -> Optional[str]:
    """Product id from canonical URL (same key as ``noramstore_scraper`` CSV) without fetching HTML."""
    try:
        url = _canonical_product_url_from_listing(listing, stockid.strip())
    except ValueError:
        return None
    return _parse_product_id_from_url(url) or None


def _load_seen_product_ids(path: str) -> Set[str]:
    """IDs already present in the master CSV (``product_id`` column; legacy ``id`` supported)."""
    if not os.path.isfile(path):
        return set()
    seen: Set[str] = set()
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        key = "product_id"
        if key not in fieldnames and "id" in fieldnames:
            key = "id"
        if key not in fieldnames:
            return seen
        for row in reader:
            pid = (row.get(key) or "").strip()
            if pid:
                seen.add(pid)
    return seen


# ---------------------------------------------------------------------------
# HTTP with sleep + exponential backoff
# ---------------------------------------------------------------------------


@dataclass
class HttpConfig:
    sleep_s: float = 0.1
    max_retries: int = 8
    backoff_base_s: float = 0.5
    backoff_max_s: float = 120.0
    timeout_s: float = 60.0


class SunhammerHttp:
    def __init__(
        self,
        api_key: str,
        *,
        config: HttpConfig,
        log: logging.Logger,
    ) -> None:
        self.api_key = api_key
        self.config = config
        self.log = log
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_PRODUCT_HEADERS)
        self.session.headers["sunhammer-api-key"] = api_key

    def _sleep(self) -> None:
        time.sleep(self.config.sleep_s)

    def get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        GET JSON with rate limiting (after each attempt) and retries on 429/5xx/transient errors.
        """
        last_exc: Optional[Exception] = None
        for attempt in range(self.config.max_retries + 1):
            self._sleep()
            try:
                resp = self.session.get(
                    url,
                    params=params,
                    timeout=self.config.timeout_s,
                )
                if resp.status_code in (429, 502, 503, 504):
                    raise requests.HTTPError(
                        f"{resp.status_code} {resp.reason}", response=resp
                    )
                resp.raise_for_status()
                return resp.json()
            except (requests.RequestException, ValueError) as exc:
                last_exc = exc
                if attempt >= self.config.max_retries:
                    break
                delay = min(
                    self.config.backoff_max_s,
                    self.config.backoff_base_s * (2**attempt),
                )
                jitter = random.uniform(0, delay * 0.1)
                wait = delay + jitter
                self.log.warning(
                    "Request failed (attempt %s/%s): %s — sleeping %.1fs",
                    attempt + 1,
                    self.config.max_retries + 1,
                    exc,
                    wait,
                )
                time.sleep(wait)
        assert last_exc is not None
        raise last_exc


# ---------------------------------------------------------------------------
# Fitment crawler
# ---------------------------------------------------------------------------


class FitmentCrawler:
    """
    Walks Sunhammer fitment label endpoints: Year → Make → Model.
    """

    def __init__(
        self,
        http: SunhammerHttp,
        *,
        group_id: int = DEFAULT_GROUP_ID,
        log: Optional[logging.Logger] = None,
    ) -> None:
        self.http = http
        self.group_id = group_id
        self.log = log or logging.getLogger(__name__)

    def _label_values(self, label: str, parents: List[str]) -> List[str]:
        url = f"{FITMENT_LABELS_BASE}/{label}"
        params: Dict[str, Any] = {"groupId": self.group_id}
        if parents:
            params["parents"] = parents
        data = self.http.get_json(url, params=params)
        if not isinstance(data, list):
            raise ValueError(f"Expected list from {label}, got {type(data)}")
        values = [str(x.get("value", "")).strip() for x in data if x.get("value")]
        return values

    def _sort_years(self, years: List[str]) -> List[str]:
        def key(y: str) -> Tuple[int, str]:
            try:
                return (int(y), y)
            except ValueError:
                return (0, y)

        return sorted(set(years), key=key, reverse=True)

    def _sort_alpha(self, xs: List[str]) -> List[str]:
        return sorted(set(xs), key=lambda s: s.lower())

    def get_years(self) -> List[str]:
        """Years (newest first)."""
        return self._sort_years(self._label_values("Year", []))

    def get_makes_for_year(self, year: str) -> List[str]:
        """Makes for one year."""
        return self._sort_alpha(self._label_values("Make", [year]))

    def get_models_for_year_make(self, year: str, make: str) -> List[str]:
        """Models for one year + make."""
        return self._sort_alpha(self._label_values("Model", [year, make]))


# ---------------------------------------------------------------------------
# Product scraper
# ---------------------------------------------------------------------------


class ProductScraper:
    """Paginates ``/products`` for a given fitment string."""

    def __init__(
        self,
        http: SunhammerHttp,
        *,
        limit: int = 100,
        log: Optional[logging.Logger] = None,
    ) -> None:
        self.http = http
        self.limit = limit
        self.log = log or logging.getLogger(__name__)

    def fetch_page(self, fitment: str, page: int) -> Dict[str, Any]:
        url = f"{SUNHAMMER_API_BASE}/products"
        params = {
            "fitment": fitment,
            "limit": self.limit,
            "page": page,
            "q": "",
        }
        return self.http.get_json(url, params=params)

    def iter_pages(
        self,
        fitment: str,
        start_page: int = 1,
        prior_item_count: int = 0,
    ) -> Iterable[Tuple[int, List[Dict[str, Any]], Optional[int]]]:
        """
        Yields (page_number, batch, total) until all products for this fitment are consumed.

        ``prior_item_count`` must be the number of items already fetched for this fitment
        on pages before ``start_page`` (required for correct ``total`` handling on resume).
        """
        page = start_page
        total: Optional[int] = None
        cumulative = prior_item_count

        while True:
            payload = self.fetch_page(fitment, page)
            if not isinstance(payload, dict):
                raise ValueError(f"Unexpected products payload: {type(payload)}")

            if total is None and payload.get("total") is not None:
                try:
                    total = int(payload["total"])
                except (TypeError, ValueError):
                    total = None

            batch = payload.get("list") or []
            if not isinstance(batch, list):
                raise ValueError("products list missing or not a list")

            yield page, batch, total

            if not batch:
                break

            cumulative += len(batch)
            if len(batch) < self.limit:
                break
            if total is not None and cumulative >= total:
                break
            page += 1


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------


@dataclass
class CrawlCheckpoint:
    """Depth-first position: year → make → model, plus product-API pagination within the leaf."""

    year_index: int = 0
    make_index: int = 0
    model_index: int = 0
    next_page: int = 1
    fitment_items_fetched: int = 0
    version: int = 2

    @classmethod
    def load(cls, path: str) -> "CrawlCheckpoint":
        with open(path, encoding="utf-8") as f:
            raw = json.load(f)
        if "next_fitment_index" in raw and "year_index" not in raw:
            raise ValueError(
                "Checkpoint format is outdated (flat fitment index). "
                "Remove the checkpoint file or use a new --checkpoint path to start depth-first mode."
            )
        return cls(
            year_index=int(raw.get("year_index", 0)),
            make_index=int(raw.get("make_index", 0)),
            model_index=int(raw.get("model_index", 0)),
            next_page=int(raw.get("next_page", 1)),
            fitment_items_fetched=int(raw.get("fitment_items_fetched", 0)),
            version=int(raw.get("version", 2)),
        )

    def save(self, path: str) -> None:
        """
        Atomic write. On Windows, ``os.replace`` can fail with WinError 32 if another process
        (sync, AV, editor) touches the file; we use a unique temp name and retry with backoff.
        """
        directory = os.path.dirname(os.path.abspath(path)) or "."
        os.makedirs(directory, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            prefix=".crawl_ckpt_",
            suffix=".json",
            dir=directory,
        )
        tmp_exists = True
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(asdict(self), f, indent=2)
            last_err: Optional[OSError] = None
            for attempt in range(20):
                try:
                    os.replace(tmp_path, path)
                    tmp_exists = False
                    return
                except OSError as exc:
                    last_err = exc
                    time.sleep(min(0.05 * (1.4**attempt), 3.0))
            if last_err is not None:
                raise last_err
        finally:
            if tmp_exists and os.path.isfile(tmp_path):
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass


def _open_csv_append(path: str, fieldnames: List[str]) -> Tuple[Any, csv.DictWriter]:
    exists = os.path.isfile(path) and os.path.getsize(path) > 0
    f = open(path, "a", newline="", encoding="utf-8-sig" if not exists else "utf-8")
    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
    if not exists:
        writer.writeheader()
    return f, writer


def run_crawl(
    *,
    whitelist: Set[str],
    crawler: FitmentCrawler,
    checkpoint: CrawlCheckpoint,
    all_products_path: str,
    fitment_products_path: str,
    checkpoint_path: str,
    product_scraper: ProductScraper,
    store_scraper: NoramStoreScraper,
    html_sleep_s: float,
    log: logging.Logger,
) -> Tuple[int, int, int]:
    """
    Returns (fitments_processed, new_unique_products, fitment_rows_written).

    Master CSV uses the same columns as ``noramstore_scraper`` (HTML-derived fields).
    """
    ck_resume = CrawlCheckpoint(
        year_index=checkpoint.year_index,
        make_index=checkpoint.make_index,
        model_index=checkpoint.model_index,
        next_page=checkpoint.next_page,
        fitment_items_fetched=checkpoint.fitment_items_fetched,
    )

    years = crawler.get_years()
    log.info("Years (newest first): %s", len(years))
    if checkpoint.year_index >= len(years):
        log.info("Checkpoint past last year — nothing to do.")
        return 0, 0, 0

    seen_product_ids = _load_seen_product_ids(all_products_path)

    all_fields = list(CSV_EXPORT_FIELDNAMES)
    fitment_fields = ["fitment", "product_id", "stockid", "title"]

    f_all, w_all = _open_csv_append(all_products_path, all_fields)
    f_fit, w_fit = _open_csv_append(fitment_products_path, fitment_fields)

    new_unique = 0
    fitment_rows = 0
    processed_fitments = 0

    try:
        yi = checkpoint.year_index
        while yi < len(years):
            year = years[yi]
            makes = crawler.get_makes_for_year(year)
            log.info(
                "Year %s (%s/%s): %s makes",
                year,
                yi + 1,
                len(years),
                len(makes),
            )

            mi = checkpoint.make_index if yi == ck_resume.year_index else 0
            if not makes:
                log.warning("No makes for year %s — advancing to next year", year)
                yi += 1
                checkpoint.year_index = yi
                checkpoint.make_index = 0
                checkpoint.model_index = 0
                checkpoint.next_page = 1
                checkpoint.fitment_items_fetched = 0
                checkpoint.save(checkpoint_path)
                continue

            while mi < len(makes):
                make = makes[mi]
                models = crawler.get_models_for_year_make(year, make)
                log.info(
                    "  Make %s (%s/%s) under %s: %s models",
                    make,
                    mi + 1,
                    len(makes),
                    year,
                    len(models),
                )

                mxi = (
                    checkpoint.model_index
                    if (yi == ck_resume.year_index and mi == ck_resume.make_index)
                    else 0
                )
                while mxi < len(models):
                    model = models[mxi]
                    fitment = f"{year}|{make}|{model}"
                    is_resume_leaf = (
                        yi == ck_resume.year_index
                        and mi == ck_resume.make_index
                        and mxi == ck_resume.model_index
                    )
                    start_page = ck_resume.next_page if is_resume_leaf else 1
                    prior_items = ck_resume.fitment_items_fetched if is_resume_leaf else 0

                    log.info(
                        "    Model %s (%s/%s) → %r  (products page >= %s, items so far %s)",
                        model,
                        mxi + 1,
                        len(models),
                        fitment,
                        start_page,
                        prior_items,
                    )

                    for page, batch, total in product_scraper.iter_pages(
                        fitment,
                        start_page=start_page,
                        prior_item_count=prior_items,
                    ):
                        wl_matches = 0
                        for item in batch:
                            if not isinstance(item, dict):
                                continue
                            sid = (item.get("stockid") or "").strip().upper()
                            if not sid or sid not in whitelist:
                                continue
                            wl_matches += 1

                            raw_stockid = (item.get("stockid") or "").strip()
                            title = (item.get("title") or "").strip()
                            preview_pid = _product_id_from_listing(item, raw_stockid)
                            api_pid = str(item.get("id") or "").strip()
                            fitment_pid = preview_pid or api_pid

                            if preview_pid and preview_pid not in seen_product_ids:
                                record, errs = store_scraper.scrape_product_page_from_listing(
                                    item,
                                    query_for_url=raw_stockid,
                                    search_meta_extra={
                                        "via": "fitment_catalog",
                                        "fitment": fitment,
                                    },
                                )
                                if record:
                                    row = record_to_csv_row(record)
                                    w_all.writerow(row)
                                    seen_product_ids.add(
                                        (row.get("product_id") or "").strip() or preview_pid
                                    )
                                    new_unique += 1
                                else:
                                    for err in errs:
                                        log.warning(
                                            "HTML scrape failed stockid=%s: %s",
                                            err.get("stockid"),
                                            err.get("error"),
                                        )
                                if html_sleep_s > 0:
                                    time.sleep(html_sleep_s)
                            elif not preview_pid:
                                log.warning(
                                    "Could not build product URL for stockid=%s (skipping HTML)",
                                    raw_stockid,
                                )

                            w_fit.writerow(
                                {
                                    "fitment": fitment,
                                    "product_id": fitment_pid,
                                    "stockid": raw_stockid,
                                    "title": title,
                                }
                            )
                            fitment_rows += 1

                        f_all.flush()
                        f_fit.flush()

                        prior_items += len(batch)
                        checkpoint.year_index = yi
                        checkpoint.make_index = mi
                        checkpoint.model_index = mxi
                        checkpoint.next_page = page + 1
                        checkpoint.fitment_items_fetched = prior_items
                        checkpoint.save(checkpoint_path)

                        log.debug(
                            "      page %s: %s items, total=%s, whitelist matches %s",
                            page,
                            len(batch),
                            total,
                            wl_matches,
                        )

                    mxi += 1
                    checkpoint.year_index = yi
                    checkpoint.make_index = mi
                    checkpoint.model_index = mxi
                    checkpoint.next_page = 1
                    checkpoint.fitment_items_fetched = 0
                    checkpoint.save(checkpoint_path)
                    processed_fitments += 1

                mi += 1
                checkpoint.make_index = mi
                checkpoint.model_index = 0
                checkpoint.next_page = 1
                checkpoint.fitment_items_fetched = 0
                checkpoint.save(checkpoint_path)

            yi += 1
            checkpoint.year_index = yi
            checkpoint.make_index = 0
            checkpoint.model_index = 0
            checkpoint.next_page = 1
            checkpoint.fitment_items_fetched = 0
            checkpoint.save(checkpoint_path)

    finally:
        f_all.close()
        f_fit.close()

    return processed_fitments, new_unique, fitment_rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def argparse_main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Crawl Noram Store via Sunhammer fitment + products APIs (whitelist by stockid).",
    )
    parser.add_argument(
        "--skus-file",
        default="all skus.csv",
        help="CSV with part numbers (part_number / sku / stockid column).",
    )
    parser.add_argument(
        "--sku-column",
        default=None,
        help="Optional explicit column name for part numbers.",
    )
    parser.add_argument(
        "--all-products",
        default="all_products.csv",
        help="Output: unique products (master list by id).",
    )
    parser.add_argument(
        "--fitment-products",
        default="fitment_products.csv",
        help="Output: one row per (fitment, product) pair.",
    )
    parser.add_argument(
        "--checkpoint",
        default="crawl_checkpoint.json",
        help="Resume state: year/make/model indices + product API page.",
    )
    parser.add_argument(
        "--group-id",
        type=int,
        default=DEFAULT_GROUP_ID,
        help="Sunhammer fitment groupId (default 71685 for noramstore.ca).",
    )
    parser.add_argument(
        "--api-key",
        default=DEFAULT_API_KEY,
        help="Sunhammer API key (or set SUNHAMMER_API_KEY).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.1,
        help="Seconds between Sunhammer API requests (default 0.1).",
    )
    parser.add_argument(
        "--html-sleep",
        type=float,
        default=0.1,
        help="Extra pause after each storefront HTML fetch (default 0.1).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Products API page size (default 100).",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint; append to CSV outputs.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="DEBUG logging.",
    )
    args = parser.parse_args(argv)

    log = logging.getLogger("noram_catalog")
    log.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    if not log.handlers:
        h = logging.StreamHandler(sys.stderr)
        h.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
        log.addHandler(h)

    if not args.api_key:
        log.error("Missing API key (--api-key or SUNHAMMER_API_KEY).")
        return 2

    if not os.path.isfile(args.skus_file):
        log.error("SKUs file not found: %s", args.skus_file)
        return 2

    skus, col = _read_skus_from_csv(args.skus_file, args.sku_column)
    whitelist = _whitelist_set(skus)
    log.info(
        "Loaded %s whitelist SKUs from %s (column %r)",
        len(whitelist),
        args.skus_file,
        col,
    )

    http_cfg = HttpConfig(sleep_s=args.sleep)
    http = SunhammerHttp(args.api_key, config=http_cfg, log=log)

    checkpoint_path = args.checkpoint
    checkpoint = CrawlCheckpoint()
    if args.resume and os.path.isfile(checkpoint_path):
        try:
            checkpoint = CrawlCheckpoint.load(checkpoint_path)
        except ValueError as exc:
            log.error("%s", exc)
            return 2
        log.info(
            "Resuming from checkpoint: year_index=%s make_index=%s model_index=%s "
            "next_page=%s fitment_items_fetched=%s",
            checkpoint.year_index,
            checkpoint.make_index,
            checkpoint.model_index,
            checkpoint.next_page,
            checkpoint.fitment_items_fetched,
        )

    crawler = FitmentCrawler(http, group_id=args.group_id, log=log)

    if args.resume and os.path.isfile(args.all_products) and os.path.getsize(args.all_products) > 0:
        try:
            _validate_output_csv_header(args.all_products)
        except ValueError as exc:
            log.error("%s", exc)
            return 2

    product_scraper = ProductScraper(http, limit=args.limit, log=log)
    store_scraper = NoramStoreScraper(api_key=args.api_key, logger=log)

    proc, new_u, frows = run_crawl(
        whitelist=whitelist,
        crawler=crawler,
        checkpoint=checkpoint,
        all_products_path=args.all_products,
        fitment_products_path=args.fitment_products,
        checkpoint_path=checkpoint_path,
        product_scraper=product_scraper,
        store_scraper=store_scraper,
        html_sleep_s=args.html_sleep,
        log=log,
    )

    log.info(
        "Done. Fitments (year|make|model) completed this run: %s "
        "(checkpoint year_index=%s make_index=%s model_index=%s)",
        proc,
        checkpoint.year_index,
        checkpoint.make_index,
        checkpoint.model_index,
    )
    log.info(
        "New unique products (master rows): %s; fitment-product rows: %s",
        new_u,
        frows,
    )
    return 0


def main() -> None:
    raise SystemExit(argparse_main())


if __name__ == "__main__":
    main()

"""
Noram Store SKU hunter: crawl Sunhammer fitments + products, match ``stockid`` against a
target set (O(1) lookups), stop when all targets are found, append each hit to JSON Lines.

Request pacing: 0.1s between Sunhammer API calls by default (see ``HttpConfig.sleep_s``).
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from noramstore_catalog_scraper import (
    DEFAULT_GROUP_ID,
    FitmentCrawler,
    HttpConfig,
    ProductScraper,
    SunhammerHttp,
)

DEFAULT_API_KEY = os.environ.get(
    "SUNHAMMER_API_KEY", "85a70623-0e93-4e93-97c7-d0cdf7ab30df"
)

_HEADER_NAMES = frozenset(
    {
        "sku",
        "stockid",
        "stock_id",
        "part",
        "part_number",
        "partnumber",
        "part no",
        "part_no",
    }
)


def _looks_like_header(first_cell: str) -> bool:
    t = first_cell.strip().lower().replace(" ", "_").replace("-", "_")
    return t in _HEADER_NAMES


def load_target_skus_csv(path: str) -> Set[str]:
    """
    Load SKUs from the first column into a set (uppercased for API ``stockid`` matching).
    Skips a header row when the first cell looks like a column name.
    """
    targets: Set[str] = set()
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if not row:
                continue
            cell = (row[0] or "").strip()
            if not cell:
                continue
            if i == 0 and _looks_like_header(cell):
                continue
            targets.add(cell.upper())
    return targets


def enumerate_all_fitments(crawler: FitmentCrawler) -> List[str]:
    """All ``Year|Make|Model`` strings (depth-first: year → make → model)."""
    out: List[str] = []
    for year in crawler.get_years():
        for make in crawler.get_makes_for_year(year):
            for model in crawler.get_models_for_year_make(year, make):
                out.append(f"{year}|{make}|{model}")
    return out


def fitment_product_totals(
    product_scraper: ProductScraper,
    fitments: List[str],
    log: logging.Logger,
) -> List[Tuple[str, int]]:
    """
    One lightweight request per fitment (page 1) to read ``total``.
    Returns ``(fitment, total)`` for fitments with ``total > 0``, sorted by ``total`` descending.
    """
    scored: List[Tuple[str, int]] = []
    n = len(fitments)
    for i, fitment in enumerate(fitments):
        if (i + 1) % 500 == 0 or i == 0:
            log.info("Measuring fitment sizes: %s / %s", i + 1, n)
        payload = product_scraper.fetch_page(fitment, page=1)
        if not isinstance(payload, dict):
            continue
        try:
            total = int(payload.get("total") or 0)
        except (TypeError, ValueError):
            total = 0
        if total <= 0:
            continue
        scored.append((fitment, total))

    scored.sort(key=lambda x: x[1], reverse=True)
    log.info(
        "Fitments with products: %s (skipped empty: %s). Order: largest product count first.",
        len(scored),
        n - len(scored),
    )
    return scored


def hunt(
    *,
    targets: Set[str],
    ordered_fitments: List[Tuple[str, int]],
    product_scraper: ProductScraper,
    jsonl_path: str,
    log: logging.Logger,
) -> Tuple[int, Set[str]]:
    """
    Paginate each fitment until all targets found or fitments exhausted.
    Returns ``(hits_written, remaining_targets)``.
    """
    remaining = set(targets)
    hits = 0

    with open(jsonl_path, "a", encoding="utf-8") as jf:

        def write_hit(obj: Dict[str, Any]) -> None:
            jf.write(json.dumps(obj, ensure_ascii=False) + "\n")
            jf.flush()
            try:
                os.fsync(jf.fileno())
            except OSError:
                pass

        for fi, (fitment, total) in enumerate(ordered_fitments):
            if not remaining:
                log.info(
                    "All target SKUs found after %s fitment(s) processed in hunt phase.",
                    fi,
                )
                break
            log.info(
                "Hunt [%s/%s] %r (~%s products) — %s SKU(s) left to find",
                fi + 1,
                len(ordered_fitments),
                fitment,
                total,
                len(remaining),
            )

            for page, batch, _ in product_scraper.iter_pages(fitment):
                if not remaining:
                    break
                for item in batch:
                    if not isinstance(item, dict):
                        continue
                    sid = (item.get("stockid") or "").strip().upper()
                    if not sid or sid not in remaining:
                        continue
                    remaining.discard(sid)
                    write_hit(item)
                    hits += 1
                    log.info("Hit stockid=%s (%s left)", sid, len(remaining))
                    if not remaining:
                        break

    return hits, remaining


def argparse_main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Find Noram Store products by SKU set using fitment + product APIs.",
    )
    parser.add_argument(
        "skus_csv",
        help="CSV: one SKU per row, first column (optional header row).",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="found_products.jsonl",
        help="Append each matched product (full API object) as one JSON line.",
    )
    parser.add_argument(
        "--summary",
        default="sku_hunt_summary.json",
        help="Write run summary (remaining SKUs, counts) when the run ends.",
    )
    parser.add_argument(
        "--api-key",
        default=DEFAULT_API_KEY,
        help="Sunhammer API key (or set SUNHAMMER_API_KEY).",
    )
    parser.add_argument(
        "--group-id",
        type=int,
        default=DEFAULT_GROUP_ID,
        help="Fitment groupId (default 71685).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.1,
        help="Seconds between each Sunhammer HTTP request (default 0.1).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Products API page size (default 100).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="DEBUG logging.",
    )
    args = parser.parse_args(argv)

    log = logging.getLogger("sku_hunter")
    log.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    if not log.handlers:
        h = logging.StreamHandler(sys.stderr)
        h.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
        log.addHandler(h)

    if not args.api_key:
        log.error("Missing API key (--api-key or SUNHAMMER_API_KEY).")
        return 2

    if not os.path.isfile(args.skus_csv):
        log.error("CSV not found: %s", args.skus_csv)
        return 2

    t0 = time.perf_counter()
    targets = load_target_skus_csv(args.skus_csv)
    elapsed_load = time.perf_counter() - t0
    log.info(
        "Loaded %s unique target SKU(s) into a set in %.3fs",
        len(targets),
        elapsed_load,
    )
    if not targets:
        log.error("No SKUs loaded from CSV.")
        return 2

    if os.path.isfile(args.output) and os.path.getsize(args.output) > 0:
        log.warning(
            "Output file %s already exists — new hits will be appended.",
            args.output,
        )

    http_cfg = HttpConfig(sleep_s=args.sleep)
    http = SunhammerHttp(args.api_key, config=http_cfg, log=log)
    crawler = FitmentCrawler(http, group_id=args.group_id, log=log)
    product_scraper = ProductScraper(http, limit=args.limit, log=log)

    log.info("Enumerating Year|Make|Model fitments…")
    fitments = enumerate_all_fitments(crawler)
    log.info("Total fitment combinations: %s", len(fitments))

    log.info("Fetching product counts per fitment (skips empty)…")
    ordered = fitment_product_totals(product_scraper, fitments, log)

    hits, remaining = hunt(
        targets=targets,
        ordered_fitments=ordered,
        product_scraper=product_scraper,
        jsonl_path=args.output,
        log=log,
    )

    summary = {
        "targets_requested": len(targets),
        "hits_written": hits,
        "targets_remaining": len(remaining),
        "remaining_skus_sample": sorted(remaining)[:50],
        "output_jsonl": os.path.abspath(args.output),
        "all_found": len(remaining) == 0,
    }
    with open(args.summary, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    log.info(
        "Done. Hits: %s — remaining: %s — JSONL: %s — summary: %s",
        hits,
        len(remaining),
        args.output,
        args.summary,
    )
    return 0 if not remaining else 1


def main() -> None:
    raise SystemExit(argparse_main())


if __name__ == "__main__":
    main()

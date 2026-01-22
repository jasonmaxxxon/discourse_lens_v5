#!/usr/bin/env python3
import argparse
import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from scraper.fetcher import run_fetcher_test
from webapp.services.ingest_sql import ingest_run


def _fail(message: str) -> None:
    print(f"FAIL: {message}")
    raise SystemExit(1)


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch Threads post and ingest into Supabase (v15 SoT)")
    parser.add_argument("url", help="Threads post URL")
    parser.add_argument("--headless", action="store_true", help="Run browser headless")
    args = parser.parse_args()

    if not os.environ.get("SUPABASE_URL") or not os.environ.get("SUPABASE_KEY"):
        _fail("SUPABASE_URL or SUPABASE_KEY is missing in env")

    fetch_summary = run_fetcher_test(args.url, headless=args.headless)
    run_dir = (fetch_summary.get("summary") or {}).get("output_dir")
    if not run_dir:
        _fail("Fetcher did not return output_dir")

    ingest_info = ingest_run(run_dir)
    print(
        "OK: fetch+ingest "
        f"post_id={ingest_info.get('post_id')} run_id={ingest_info.get('run_id')} "
        f"comments={ingest_info.get('comment_count')} edges={ingest_info.get('edge_count')} "
        f"run_dir={run_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

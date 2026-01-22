#!/usr/bin/env python3
import argparse
import json
import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from webapp.services.ingest_sql import ingest_run
from database.store import supabase

UI_TOKENS = {
    "top",
    "view activity",
    "author",
    "translate",
    "like",
    "reply",
    "repost",
    "share",
}


def _fail(message: str) -> None:
    print(f"FAIL: {message}")
    sys.exit(1)


def _read_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _count_table(table: str, column: str, value) -> int:
    resp = supabase.table(table).select("id", count="exact").eq(column, value).execute()
    return int(resp.count or 0)


def _load_rows(table: str, columns: str, column: str, value) -> list[dict]:
    resp = supabase.table(table).select(columns).eq(column, value).execute()
    return list(resp.data or [])


def main() -> int:
    parser = argparse.ArgumentParser(description="Offline ingest gate for v15 SoT runs")
    parser.add_argument("run_dir", help="Run directory from v15 fetcher output")
    args = parser.parse_args()

    if not os.environ.get("SUPABASE_URL") or not os.environ.get("SUPABASE_KEY"):
        _fail("SUPABASE_URL or SUPABASE_KEY is missing in env")
    if not supabase:
        _fail("Supabase client not configured")

    run_dir = args.run_dir
    posts_raw_path = os.path.join(run_dir, "threads_posts_raw.json")
    comments_path = os.path.join(run_dir, "threads_comments.json")
    edges_path = os.path.join(run_dir, "threads_comment_edges.json")
    comments_flat_path = os.path.join(run_dir, "comments_flat.json")

    if not os.path.exists(posts_raw_path):
        _fail(f"Missing threads_posts_raw.json in {run_dir}")
    if not os.path.exists(comments_path):
        _fail(f"Missing threads_comments.json in {run_dir}")
    if not os.path.exists(edges_path):
        _fail(f"Missing threads_comment_edges.json in {run_dir}")

    posts_raw = _read_json(posts_raw_path)
    post_url = posts_raw.get("post_url")
    run_id = posts_raw.get("run_id")
    if not post_url or not run_id:
        _fail("threads_posts_raw.json missing post_url or run_id")

    comments_json = _read_json(comments_path) or []
    edges_json = _read_json(edges_path) or []
    comment_ids = {c.get("comment_id") for c in comments_json if c.get("comment_id")}
    if os.path.exists(comments_flat_path):
        comments_flat = _read_json(comments_flat_path) or []
        flat_ids = {c.get("comment_id") for c in comments_flat if c.get("comment_id")}
        if len(comment_ids) < len(flat_ids):
            _fail("threads_comments.json count is smaller than comments_flat.json unique ids")

    first = ingest_run(run_dir)
    post_id = first.get("post_id")
    if not post_id:
        _fail("ingest_run did not return post_id")

    posts_count = _count_table("threads_posts", "url", post_url)
    posts_raw_count = _count_table("threads_posts_raw", "run_id", run_id)
    comments_count = _count_table("threads_comments", "post_id", post_id)
    edges_count = _count_table("threads_comment_edges", "post_id", post_id)

    if posts_count != 1:
        _fail(f"threads_posts count mismatch for url={post_url}: {posts_count}")
    if posts_raw_count < 1:
        _fail(f"threads_posts_raw missing run_id={run_id}")
    if comments_count != len(comments_json):
        _fail(f"threads_comments count mismatch: db={comments_count} json={len(comments_json)}")
    if edges_count != len(edges_json):
        _fail(f"threads_comment_edges count mismatch: db={edges_count} json={len(edges_json)}")

    edge_rows = _load_rows(
        "threads_comment_edges",
        "parent_comment_id,child_comment_id",
        "post_id",
        post_id,
    )
    if any(r.get("parent_comment_id") == r.get("child_comment_id") for r in edge_rows):
        _fail("Self-loop edges detected in threads_comment_edges")
    if any(r.get("child_comment_id") not in comment_ids for r in edge_rows):
        _fail("Edge child_comment_id missing in threads_comments.json")
    if any(
        (r.get("parent_comment_id") not in comment_ids and r.get("parent_comment_id") != post_id_external)
        for r in edge_rows
    ):
        _fail("Edge parent_comment_id missing in threads_comments.json and not post_id root")

    comment_rows = _load_rows("threads_comments", "id", "post_id", post_id)
    ids = [r.get("id") for r in comment_rows if r.get("id")]
    if len(ids) != len(set(ids)):
        _fail("Duplicate threads_comments.id detected for post_id")

    for row in comments_json:
        text = (row.get("text") or "").strip().lower()
        if not text:
            continue
        for line in [ln.strip() for ln in text.splitlines() if ln.strip()]:
            if line in UI_TOKENS:
                _fail(f"UI token found in comment text: {line}")

    second = ingest_run(run_dir)
    comments_count_2 = _count_table("threads_comments", "post_id", post_id)
    edges_count_2 = _count_table("threads_comment_edges", "post_id", post_id)
    if comments_count_2 != comments_count:
        _fail(f"Idempotency failed: comments count changed {comments_count} -> {comments_count_2}")
    if edges_count_2 != edges_count:
        _fail(f"Idempotency failed: edges count changed {edges_count} -> {edges_count_2}")

    print(
        "PASS: ingest gate ok "
        f"post_id={post_id} run_id={run_id} "
        f"posts=1 raw={posts_raw_count} comments={comments_count} edges={edges_count} "
        f"idempotent=ok"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

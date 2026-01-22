import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

from database.store import supabase


def _read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _chunk(items: List[Dict[str, Any]], size: int = 200) -> Iterable[List[Dict[str, Any]]]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def ingest_run(run_dir: str) -> Dict[str, Any]:
    if not supabase:
        raise RuntimeError("Supabase client is not configured")

    posts_raw_path = os.path.join(run_dir, "threads_posts_raw.json")
    comments_path = os.path.join(run_dir, "threads_comments.json")
    edges_path = os.path.join(run_dir, "threads_comment_edges.json")
    post_payload_path = os.path.join(run_dir, "post_payload.json")

    posts_raw = _read_json(posts_raw_path)
    run_id = posts_raw.get("run_id")
    crawled_at_utc = posts_raw.get("crawled_at_utc")
    post_url = posts_raw.get("post_url")
    post_id_external = posts_raw.get("post_id")

    if not run_id or not post_url:
        raise RuntimeError("threads_posts_raw.json missing run_id/post_url")

    raw_payload = {
        "run_id": run_id,
        "crawled_at_utc": crawled_at_utc,
        "post_url": post_url,
        "post_id": post_id_external,
        "fetcher_version": posts_raw.get("fetcher_version"),
        "run_dir": posts_raw.get("run_dir") or run_dir,
        "raw_html_initial_path": posts_raw.get("raw_html_initial_path"),
        "raw_html_final_path": posts_raw.get("raw_html_final_path"),
        "raw_cards_path": posts_raw.get("raw_cards_path"),
    }
    supabase.table("threads_posts_raw").upsert(raw_payload, on_conflict="run_id,post_id").execute()

    captured_at = crawled_at_utc or datetime.now(timezone.utc).isoformat()
    supabase.table("threads_posts").upsert(
        {"url": post_url, "captured_at": captured_at},
        on_conflict="url",
    ).execute()
    res = supabase.table("threads_posts").select("id").eq("url", post_url).limit(1).execute()
    if not res.data:
        raise RuntimeError(f"threads_posts upsert ok but cannot re-select id for url={post_url}")
    post_row_id = res.data[0]["id"]

    comments = _read_json(comments_path) or []

    post_payload = {}
    if os.path.exists(post_payload_path):
        try:
            post_payload = _read_json(post_payload_path) or {}
        except Exception:
            post_payload = {}

    post_update: Dict[str, Any] = {}
    if post_payload:
        author = post_payload.get("author")
        post_text = post_payload.get("post_text")
        post_text_raw = post_payload.get("post_text_raw")
        images = post_payload.get("images") or post_payload.get("post_images") or []
        metrics = post_payload.get("metrics") or {}
        if author:
            post_update["author"] = author
        if post_text:
            post_update["post_text"] = post_text
        if post_text_raw:
            post_update["post_text_raw"] = post_text_raw
        if images:
            post_update["images"] = images
        if metrics:
            post_update["like_count"] = int(metrics.get("likes") or 0)
            post_update["view_count"] = int(metrics.get("views") or 0)
            post_update["reply_count_ui"] = int(metrics.get("reply_count") or 0)
            post_update["repost_count"] = int(metrics.get("repost_count") or 0)
            post_update["share_count"] = int(metrics.get("share_count") or 0)
    post_update["reply_count"] = len(comments)
    if post_update:
        post_update["updated_at"] = datetime.now(timezone.utc).isoformat()
        supabase.table("threads_posts").update(post_update).eq("id", post_row_id).execute()

    comment_rows: List[Dict[str, Any]] = []
    metrics_quality = {"exact": 0, "partial": 0, "missing": 0}
    for row in comments:
        quality = row.get("metrics_confidence") or "missing"
        if quality not in metrics_quality:
            quality = "missing"
        metrics_quality[quality] += 1
        comment_rows.append(
            {
                "id": row.get("comment_id"),
                "post_id": post_row_id,
                "text": row.get("text"),
                "author_handle": row.get("author_handle"),
                "like_count": row.get("like_count") or 0,
                "reply_count": row.get("reply_count_ui") or 0,
                "created_at": row.get("approx_created_at_utc"),
                "captured_at": row.get("crawled_at_utc") or crawled_at_utc,
                "parent_comment_id": row.get("parent_comment_id"),
                "run_id": row.get("run_id") or run_id,
                "crawled_at_utc": row.get("crawled_at_utc") or crawled_at_utc,
                "post_url": row.get("post_url") or post_url,
                "time_token": row.get("time_token"),
                "approx_created_at_utc": row.get("approx_created_at_utc"),
                "reply_count_ui": row.get("reply_count_ui") or 0,
                "repost_count_ui": row.get("repost_count_ui") or 0,
                "share_count_ui": row.get("share_count_ui") or 0,
                "metrics_confidence": row.get("metrics_confidence"),
                "source": row.get("source"),
                "comment_images": row.get("comment_images") or [],
            }
        )

    for chunk in _chunk(comment_rows, 200):
        supabase.table("threads_comments").upsert(chunk, on_conflict="id").execute()

    edges = _read_json(edges_path) or []
    edge_rows: List[Dict[str, Any]] = []
    for edge in edges:
        parent_id = edge.get("parent_comment_id")
        child_id = edge.get("child_comment_id")
        if not parent_id or not child_id or parent_id == child_id:
            continue
        edge_rows.append(
            {
                "run_id": run_id,
                "post_id": post_row_id,
                "parent_comment_id": parent_id,
                "child_comment_id": child_id,
                "edge_type": edge.get("edge_type") or "reply",
            }
        )

    for chunk in _chunk(edge_rows, 300):
        supabase.table("threads_comment_edges").upsert(
            chunk,
            on_conflict="post_id,parent_comment_id,child_comment_id,edge_type",
        ).execute()

    return {
        "run_id": run_id,
        "post_id": post_row_id,
        "crawled_at_utc": crawled_at_utc,
        "comment_count": len(comment_rows),
        "edge_count": len(edge_rows),
        "metrics_quality": metrics_quality,
    }

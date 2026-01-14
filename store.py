import os
import json
import hashlib
import logging
from typing import Optional, List, Dict, Any
from supabase import create_client, Client
from dotenv import load_dotenv
from scraper.image_pipeline import process_images_for_post
from datetime import datetime, timezone
import requests

# Safety net: load .env on import so SUPABASE_* exist even if uvicorn misses it.
load_dotenv()

logger_env = logging.getLogger("dl.env")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = (
    os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    or os.environ.get("SUPABASE_SERVICE_KEY")
    or os.environ.get("SUPABASE_ANON_KEY")
    or os.environ.get("SUPABASE_KEY")
)

if not SUPABASE_URL or not SUPABASE_URL.startswith("https://"):
    raise RuntimeError(
        f"CRITICAL: SUPABASE_URL missing/invalid: {SUPABASE_URL!r}. "
        "Check .env and runtime env loading."
    )
if not SUPABASE_KEY:
    raise RuntimeError("CRITICAL: SUPABASE_KEY missing. Check .env and runtime env loading.")

logger_env.info("[ENV] SUPABASE_URL loaded (prefix): %s...", SUPABASE_URL[:24])

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
logger = logging.getLogger("dl")
mode = "SERVICE_ROLE" if (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_SERVICE_KEY")) else "ANON"
if mode == "SERVICE_ROLE":
    logger.info("[DB] Mode: SERVICE_ROLE")
else:
    logger.warning("[DB] Mode: ANON (WARNING: backend running restricted)")


def _cluster_id(post_id: int | str, cluster_key: int | str) -> str:
    return f"{post_id}::c{cluster_key}"


def _normalize_text(val: str) -> str:
    return " ".join((val or "").split()).strip()


def _legacy_comment_id(post_id: str, comment: Dict[str, Any]) -> str:
    """
    Deterministic fallback when native id is missing.
    """
    author = str(comment.get("author_handle") or comment.get("user") or comment.get("author") or "")
    text = _normalize_text(str(comment.get("text") or ""))
    raw = f"{post_id}:{author}:{text}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _normalize_comments_raw(raw_comments: Any) -> List[Dict[str, Any]]:
    if raw_comments is None:
        return []
    if isinstance(raw_comments, str):
        try:
            parsed = json.loads(raw_comments)
            return _normalize_comments_raw(parsed)
        except Exception:
            return []
    if isinstance(raw_comments, dict):
        for key in ("items", "data", "comments"):
            val = raw_comments.get(key)
            if isinstance(val, list):
                return _normalize_comments_raw(val)
        return []
    if isinstance(raw_comments, list):
        return [c for c in raw_comments if isinstance(c, dict)]
    return []

def _fetch_existing_ids_by_source(post_id: str | int, source_ids: List[str]) -> Dict[str, str]:
    """
    Return mapping source_comment_id -> existing id for a post.
    """
    if not source_ids:
        return {}
    existing: Dict[str, str] = {}
    unique_sources = list({s for s in source_ids if s})
    for chunk in _chunked(unique_sources, 200):
        try:
            resp = supabase.table("threads_comments").select("id, source_comment_id").eq("post_id", post_id).in_("source_comment_id", chunk).execute()
            data = getattr(resp, "data", None) or []
            for row in data:
                src = row.get("source_comment_id")
                cid = row.get("id")
                if src and cid:
                    existing[str(src)] = str(cid)
        except Exception as e:
            logger.warning(f"[CommentsSoT] fetch existing ids by source failed for post {post_id}: {e}")
    return existing


def _map_comments_to_rows(comments: List[Dict[str, Any]], post_id: str | int, now_iso: str, existing_by_source: Dict[str, str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for c in comments:
        if not isinstance(c, dict):
            continue
        source_comment_id = c.get("source_comment_id") or c.get("comment_id")
        parent_source_comment_id = c.get("parent_source_comment_id")
        # Hybrid identity: keep existing hash id; reuse prior id when source matches.
        if source_comment_id and source_comment_id in existing_by_source:
            db_comment_id = existing_by_source[source_comment_id]
        elif c.get("id"):
            db_comment_id = str(c.get("id"))
        else:
            db_comment_id = _legacy_comment_id(str(post_id), c)
        c["source_comment_id"] = source_comment_id  # propagate for downstream
        c["id"] = db_comment_id  # keep hash id stable for quant/cluster references
        try:
            like_count = int(c.get("like_count") or c.get("likes") or 0)
        except Exception:
            like_count = 0
        try:
            reply_count = int(c.get("reply_count") or c.get("replies") or 0)
        except Exception:
            reply_count = 0
        rows.append(
            {
                "id": str(db_comment_id),
                "post_id": int(post_id),
                "text": c.get("text"),
                "author_handle": c.get("author_handle") or c.get("user") or c.get("author"),
                "author_id": c.get("author_id"),
                "source_comment_id": source_comment_id,
                "parent_source_comment_id": parent_source_comment_id,
                "parent_comment_id": c.get("parent_comment_id"),
                "like_count": like_count,
                "reply_count": reply_count,
                "created_at": c.get("created_at") or c.get("timestamp"),
                "captured_at": now_iso,
                "raw_json": c,
                "updated_at": now_iso,
            }
        )
    return rows


def _chunked(iterable: List[Dict[str, Any]], size: int = 200):
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def sync_comments_to_table(post_id: str | int, raw_comments: Any) -> Dict[str, Any]:
    comments = _normalize_comments_raw(raw_comments)
    now_iso = datetime.now(timezone.utc).isoformat()
    source_ids = [c.get("source_comment_id") or c.get("comment_id") for c in comments if isinstance(c, dict)]
    existing_by_source = _fetch_existing_ids_by_source(post_id, [s for s in source_ids if s])
    rows = _map_comments_to_rows(comments, post_id, now_iso, existing_by_source)
    if not rows:
        return {"ok": True, "count": 0}
    total = 0
    try:
        for chunk in _chunked(rows, 200):
            supabase.table("threads_comments").upsert(chunk).execute()
            total += len(chunk)
        logger.info(f"✅ [CommentsSoT] upserted {total} comments for post {post_id}")
        return {"ok": True, "count": total}
    except Exception as e:
        logger.warning(f"⚠️ [CommentsSoT] upsert failed for post {post_id}: {e}")
        return {"ok": False, "count": total, "error": str(e)}


def upsert_comment_clusters(post_id: int, clusters: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Upsert post-level clusters into threads_comment_clusters via RPC (set-based).
    """
    if not clusters:
        return {"ok": True, "count": 0, "skipped": True}
    try:
        supabase.rpc("upsert_comment_clusters", {"p_post_id": post_id, "p_clusters": clusters}).execute()
        logger.info("[Clusters] rpc upsert post=%s clusters_attempted=%s", post_id, len(clusters))
        return {"ok": True, "count": len(clusters), "skipped": False}
    except Exception as e:
        logger.warning("⚠️ [Clusters] rpc upsert failed post=%s err=%s", post_id, e)
        return {"ok": False, "count": 0, "error": str(e)}


def apply_comment_cluster_assignments(post_id: int, assignments: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Batch update threads_comments with cluster_id/cluster_key via RPC (single call).
    assignments: [{comment_id, cluster_key, cluster_id?}]
    """
    if not assignments:
        return {"ok": True, "count": 0, "skipped": True}
    try:
        supabase.rpc("set_comment_cluster_assignments", {"p_post_id": post_id, "p_assignments": assignments}).execute()
        logger.info("[Clusters] rpc assignments post=%s assignments_attempted=%s", post_id, len(assignments))
        return {"ok": True, "count": len(assignments), "skipped": False}
    except Exception as e:
        logger.warning("⚠️ [Clusters] assignment rpc failed post=%s err=%s", post_id, e)
        return {"ok": False, "count": 0, "error": str(e)}


def update_cluster_tactics(post_id: int, updates: List[Dict[str, Any]]) -> tuple[bool, int]:
    """
    updates: [{"cluster_key": 0, "tactics": ["..."], "tactic_summary": "..."}]
    Returns (ok, updated_count)
    """
    if not updates:
        return True, 0

    def _normalize_tactics(val: Any) -> Optional[List[str]]:
        if val is None:
            return None
        if isinstance(val, str):
            return [val]
        if isinstance(val, (list, tuple)):
            return [str(x) for x in val if x is not None]
        return None

    updated = 0
    attempted = 0
    missing = 0
    for item in updates:
        if not isinstance(item, dict):
            continue
        key = item.get("cluster_key")
        if key is None:
            continue
        try:
            key_int = int(key)
        except Exception:
            continue
        tactics_norm = _normalize_tactics(item.get("tactics"))
        payload = {
            "tactics": tactics_norm,
            "tactic_summary": item.get("tactic_summary"),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "post_id": post_id,
        }
        attempted += 1
        try:
            resp = supabase.table("threads_comment_clusters").update(payload).eq("post_id", post_id).eq("cluster_key", key_int).execute()
            data = getattr(resp, "data", None) or []
            if data:
                updated += len(data)
            else:
                missing += 1
                logger.warning(f"[Clusters] tactic update missing cluster post={post_id} cluster_key={key_int}")
        except Exception as e:
            logger.warning(f"[Clusters] tactic update failed post={post_id} cluster_key={key_int}: {e}")
    logger.info(
        f"[Clusters] tactics writeback post={post_id} clusters_attempted={attempted} clusters_updated_ok={updated} missing_clusters={missing}"
    )
    return True, updated


def update_cluster_metadata(post_id: int, updates: List[Dict[str, Any]]) -> tuple[bool, int]:
    """
    Idempotently updates label/summary/tactics/tactic_summary by (post_id, cluster_key).
    updates: [{"cluster_key": int, "label": str?, "summary": str?, "tactics": list[str]?, "tactic_summary": str?}]
    Returns (ok, updated_count).
    """
    if not updates:
        return True, 0

    def _normalize_tactics(val: Any) -> Optional[List[str]]:
        if val is None:
            return None
        if isinstance(val, str):
            return [val]
        if isinstance(val, (list, tuple)):
            return [str(x) for x in val if x is not None]
        return None

    updated = 0
    attempted = 0
    missing = 0
    for item in updates:
        if not isinstance(item, dict):
            continue
        key = item.get("cluster_key")
        if key is None:
            continue
        try:
            key_int = int(key)
        except Exception:
            continue
        tactics_norm = _normalize_tactics(item.get("tactics"))
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "post_id": post_id,
        }
        if item.get("label"):
            payload["label"] = item.get("label")
        if item.get("summary"):
            payload["summary"] = item.get("summary")
        if tactics_norm is not None:
            payload["tactics"] = tactics_norm
        if item.get("tactic_summary"):
            payload["tactic_summary"] = item.get("tactic_summary")

        attempted += 1
        try:
            resp = supabase.table("threads_comment_clusters").update(payload).eq("post_id", post_id).eq("cluster_key", key_int).execute()
            data = getattr(resp, "data", None) or []
            if data:
                updated += len(data)
            else:
                missing += 1
                logger.warning(f"[Clusters] metadata update missing cluster post={post_id} cluster_key={key_int}")
        except Exception as e:
            logger.warning(f"[Clusters] metadata update failed post={post_id} cluster_key={key_int}: {e}")

    logger.info(
        f"[Clusters] metadata writeback post={post_id} clusters_attempted={attempted} clusters_updated_ok={updated} missing_clusters={missing}"
    )
    ok = missing == 0 or updated > 0
    return ok, updated

def save_thread(data: dict, ingest_source: Optional[str] = None):
    """
    將解析好的 Threads 貼文存入 Supabase 的 threads_posts 表
    目前 image_pipeline 已進入 link-only 模式，不會保存 OCR 結果，
    Supabase 圖片欄位僅包含遠端 URL，OCR 由之後的 Gemini Pipeline 處理。
    """
    comments = data.get("comments", [])
    post_id = (
        data.get("post_id")
        or data.get("Post_ID")
        or data.get("id")
        or "UNKNOWN_POST"
    )

    raw_images = data.get("images") or []
    try:
        enriched_images = process_images_for_post(post_id, raw_images)
    except Exception:
        enriched_images = raw_images
    data["images"] = enriched_images

    url_val = data["url"]
    if isinstance(url_val, str) and url_val.startswith("https://www.threads.com/"):
        url_val = url_val.replace("https://www.threads.com/", "https://www.threads.net/")
        data["url"] = url_val

    payload = {
        "url": url_val,
        "author": data["author"],
        "post_text": data["post_text"],
        "post_text_raw": data.get("post_text_raw", ""),
        "like_count": data["metrics"].get("likes", 0),
        "view_count": data["metrics"].get("views", 0),
        "reply_count": len(comments),
        "reply_count_ui": data["metrics"].get("reply_count", 0),
        "repost_count": data["metrics"].get("repost_count", 0),
        "share_count": data["metrics"].get("share_count", 0),
        "images": data.get("images", []),
        "raw_comments": comments,
        "ingest_source": ingest_source,
        "is_first_thread": bool(data.get("is_first_thread", False)),
    }

    now_iso = datetime.now(timezone.utc).isoformat()

    try:
        print(f"[DB DEBUG] payload keys: {list(payload.keys())}")
        try:
            payload_size = len(json.dumps(payload))
            print(f"[DB DEBUG] payload json size: {payload_size} bytes")
        except Exception:
            pass
        supabase.table("threads_posts").upsert(payload, on_conflict="url").execute()
        res = (
            supabase.table("threads_posts")
            .select("id")
            .eq("url", payload["url"])
            .limit(1)
            .execute()
        )
        if not res.data:
            raise RuntimeError(f"save_thread upsert ok but cannot re-select id for url={payload['url']}")
        post_row_id = res.data[0]["id"]
        data["post_id"] = post_row_id
        data["id"] = post_row_id
        sync_comments_to_table(post_row_id, comments)
    except Exception as e:
        print(f"❌ 寫入 Supabase 失敗：{e}")
        raise
    print("💾 Saved to DB, id =", post_row_id, "comments_upserted=", len(comments))
    return post_row_id


def update_post_archive(
    supabase_url: str,
    supabase_anon_key: str,
    post_id: str,
    archive_build_id: str,
    archive_html: str,
    archive_dom_json: dict,
) -> None:
    """
    Best-effort PATCH. Only writes archive_* fields.
    """
    payload = {
        "archive_captured_at": datetime.now(timezone.utc).isoformat(),
        "archive_build_id": archive_build_id,
        "archive_html": archive_html,
        "archive_dom_json": archive_dom_json,
    }

    headers = {
        "apikey": supabase_anon_key,
        "Authorization": f"Bearer {supabase_anon_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

    r = requests.patch(
        f"{supabase_url}/rest/v1/threads_posts?id=eq.{post_id}",
        headers=headers,
        json=payload,
        timeout=30,
    )
    if not r.ok:
        raise RuntimeError(f"Supabase archive PATCH failed: {r.status_code} {r.text[:300]}")


def update_post_analysis_forensic(
    supabase_url: str,
    supabase_anon_key: str,
    post_id: str,
    analysis_json: dict | None,
    meta: dict,
) -> None:
    """
    Forensic mode: always patch analysis_json if provided (dict), along with meta.
    """
    payload = {
        **meta,
    }
    if analysis_json is not None:
        payload["analysis_json"] = analysis_json

    headers = {
        "apikey": supabase_anon_key,
        "Authorization": f"Bearer {supabase_anon_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

    r = requests.patch(
        f"{supabase_url}/rest/v1/threads_posts?id=eq.{post_id}",
        headers=headers,
        json=payload,
        timeout=30,
    )
    if not r.ok:
        raise RuntimeError(f"Supabase analysis PATCH failed: {r.status_code} {r.text[:300]}")


def update_vision_meta(
    supabase_url: str,
    supabase_anon_key: str,
    post_id: str,
    *,
    vision_fields: Dict[str, Any],
    images: Optional[list] = None,
) -> None:
    """
    Unified vision writeback for threads_posts.
    - vision_fields: columns like vision_mode/need_score/reasons/stage_ran/v1/v2/sim/metrics_reliable
    - images: optional enriched images array to write back together
    """
    payload: Dict[str, Any] = dict(vision_fields or {})
    payload["vision_updated_at"] = datetime.now(timezone.utc).isoformat()

    if images is not None:
        payload["images"] = images

    headers = {
        "apikey": supabase_anon_key,
        "Authorization": f"Bearer {supabase_anon_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

    r = requests.patch(
        f"{supabase_url}/rest/v1/threads_posts?id=eq.{post_id}",
        headers=headers,
        json=payload,
        timeout=30,
    )
    if not r.ok:
        raise RuntimeError(f"Supabase vision PATCH failed: {r.status_code} {r.text[:300]}")

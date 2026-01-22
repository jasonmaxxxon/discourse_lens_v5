import os
import sys
import json
import hashlib
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone, date
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_removed_root = False
if ROOT_DIR in sys.path:
    sys.path.remove(ROOT_DIR)
    _removed_root = True
try:
    from supabase import create_client, Client
finally:
    if _removed_root:
        sys.path.insert(0, ROOT_DIR)
from dotenv import load_dotenv
from scraper.image_pipeline import process_images_for_post
import requests
from analysis.build_analysis_json import build_and_validate_analysis_json, validate_analysis_json, safe_dump

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

def _env_flag(name: str) -> bool:
    val = os.environ.get(name)
    if val is None:
        return False
    return str(val).lower() in {"1", "true", "yes", "on"}


def _cluster_id(post_id: int | str, cluster_key: int | str) -> str:
    return f"{post_id}::c{cluster_key}"


def _json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def _normalize_text(val: str) -> str:
    return " ".join((val or "").split()).strip()


def _parse_taken_at(val: Any) -> Optional[str]:
    """
    Best-effort normalize timestamptz to ISO string; returns None on failure.
    """
    if val is None:
        return None
    import datetime

    try:
        if isinstance(val, (int, float)) or (isinstance(val, str) and val.strip().isdigit()):
            return datetime.datetime.fromtimestamp(float(val), datetime.timezone.utc).isoformat()
        if isinstance(val, str):
            try:
                return datetime.datetime.fromisoformat(val.replace("Z", "+00:00")).isoformat()
            except Exception:
                pass
        if isinstance(val, datetime.datetime):
            if val.tzinfo is None:
                val = val.replace(tzinfo=datetime.timezone.utc)
            return val.isoformat()
    except Exception:
        return None
    return None


def save_analysis_result(post_id: int | str, analysis_payload: dict) -> None:
    invalid_reason: Optional[str] = None
    missing_keys: Optional[list] = None
    validated_payload: Optional[dict] = None
    is_valid = False

    try:
        validated_model = build_and_validate_analysis_json(analysis_payload)
        validated_payload = safe_dump(validated_model)
        is_valid, invalid_reason, missing_keys = validate_analysis_json(validated_model)
    except Exception as exc:
        invalid_reason = f"{type(exc).__name__}: {exc}"
        if hasattr(exc, "errors"):
            try:
                missing_keys = [
                    ".".join(str(part) for part in err.get("loc", []))
                    for err in (exc.errors() or [])
                    if err.get("type") in {"missing", "value_error.missing"}
                ]
            except Exception:
                missing_keys = None

    if is_valid and validated_payload is not None:
        payload = {
            "analysis_json": validated_payload,
            "analysis_is_valid": True,
            "analysis_invalid_reason": None,
            "analysis_missing_keys": None,
        }
    else:
        payload = {
            "analysis_json": analysis_payload,
            "analysis_is_valid": False,
            "analysis_invalid_reason": invalid_reason or "validation_failed",
            "analysis_missing_keys": missing_keys or None,
        }

    supabase.table("threads_posts").update(_json_safe(payload)).eq("id", post_id).execute()


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
        reply_to_author = c.get("reply_to_author")
        text_fragments = c.get("text_fragments") if isinstance(c.get("text_fragments"), list) else None
        text_val = c.get("text") or ""
        if (not str(text_val).strip()) and text_fragments:
            try:
                text_val = "".join(
                    [
                        frag.get("text", "") if isinstance(frag, dict) else str(frag or "")
                        for frag in text_fragments
                    ]
                ).strip()
            except Exception:
                text_val = str(text_val or "").strip()
        text_val_clean = str(text_val or "").strip()
        normalized_text = _normalize_text(text_val_clean)
        author_handle = c.get("author_handle") or c.get("user") or c.get("author") or ""
        # Hybrid identity: primary key stays legacy hash, but reuse existing id when source matches.
        legacy_id = _legacy_comment_id(str(post_id), {"author_handle": author_handle, "text": normalized_text})
        if source_comment_id and source_comment_id in existing_by_source:
            db_comment_id = existing_by_source[source_comment_id]
        else:
            db_comment_id = legacy_id
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
        taken_at = _parse_taken_at(c.get("taken_at") or c.get("created_at") or c.get("timestamp"))
        root_source_comment_id = c.get("root_source_comment_id")
        if not root_source_comment_id:
            if parent_source_comment_id:
                root_source_comment_id = None
            else:
                root_source_comment_id = source_comment_id
        raw_json = _json_safe(c.get("raw_json") or c)
        rows.append(
            {
                "id": str(db_comment_id),
                "post_id": int(post_id),
                "text": text_val_clean,
                "text_fragments": text_fragments,
                "author_handle": author_handle,
                "author_id": c.get("author_id"),
                "source_comment_id": source_comment_id,
                "parent_source_comment_id": parent_source_comment_id,
                "root_source_comment_id": root_source_comment_id,
                "reply_to_author": reply_to_author,
                "parent_comment_id": c.get("parent_comment_id"),
                "like_count": like_count,
                "reply_count": reply_count,
                "taken_at": taken_at,
                "created_at": c.get("created_at") or c.get("timestamp"),
                "captured_at": now_iso,
                "raw_json": raw_json,
                "depth": c.get("depth"),
                "path": c.get("path"),
                "updated_at": now_iso,
            }
        )
    return rows


def _chunked(iterable: List[Dict[str, Any]], size: int = 200):
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def _repair_comment_tree(post_id: str | int) -> Dict[str, Any]:
    """
    Fill root_source_comment_id / depth / path using best-effort parent resolution.
    """
    try:
        resp = (
            supabase.table("threads_comments")
            .select(
                "id, source_comment_id, parent_source_comment_id, reply_to_author, author_handle, "
                "root_source_comment_id, depth, path, taken_at, created_at"
            )
            .eq("post_id", post_id)
            .order("inserted_at", desc=False)
            .execute()
        )
        rows = getattr(resp, "data", None) or []
    except Exception as e:
        logger.warning(f"[CommentsSoT] repair fetch failed post={post_id}: {e}")
        return {"ok": False, "error": str(e), "updated": 0, "count": 0}

    if not rows:
        return {"ok": True, "updated": 0, "count": 0}

    parsed_rows = []
    original_by_id = {}
    for idx, row in enumerate(rows):
        taken_at = _parse_taken_at(row.get("taken_at") or row.get("created_at"))
        taken_at_dt = None
        if taken_at:
            try:
                taken_at_dt = datetime.fromisoformat(str(taken_at).replace("Z", "+00:00"))
            except Exception:
                taken_at_dt = None
        parsed = {
            "idx": idx,
            "id": str(row.get("id")),
            "source_comment_id": row.get("source_comment_id"),
            "parent_source_comment_id": row.get("parent_source_comment_id"),
            "reply_to_author": row.get("reply_to_author"),
            "author_handle": row.get("author_handle"),
            "root_source_comment_id": row.get("root_source_comment_id"),
            "depth": row.get("depth"),
            "path": row.get("path"),
            "taken_at": taken_at,
            "taken_at_dt": taken_at_dt,
        }
        parsed_rows.append(parsed)
        original_by_id[parsed["id"]] = parsed.copy()

    def _sort_key(item):
        if item["taken_at_dt"]:
            return (0, item["taken_at_dt"])
        return (1, item["idx"])

    ordered = sorted(parsed_rows, key=_sort_key)
    computed_by_id: Dict[str, Dict[str, Any]] = {}
    computed_by_source: Dict[str, Dict[str, Any]] = {}
    latest_by_author: Dict[str, Dict[str, Any]] = {}

    for _ in range(3):  # few passes to stabilize
        progressed = False
        for c in ordered:
            cid = c["id"]
            base_state = computed_by_id.get(cid) or {
                "id": cid,
                "source_comment_id": c.get("source_comment_id"),
                "root_source_comment_id": c.get("root_source_comment_id"),
                "depth": c.get("depth"),
                "path": c.get("path"),
            }
            parent_info = None
            resolved_parent_source_id = None
            if c.get("parent_source_comment_id") and c["parent_source_comment_id"] in computed_by_source:
                parent_info = computed_by_source[c["parent_source_comment_id"]]
                resolved_parent_source_id = c["parent_source_comment_id"]
            elif not c.get("parent_source_comment_id") and c.get("reply_to_author"):
                candidate = latest_by_author.get(c["reply_to_author"])
                if candidate and candidate.get("source_comment_id"):
                    parent_info = candidate
                    resolved_parent_source_id = candidate.get("source_comment_id")

            root = base_state.get("root_source_comment_id")
            depth = base_state.get("depth")
            path = base_state.get("path")

            if parent_info:
                parent_depth = parent_info.get("depth")
                depth = (parent_depth if parent_depth is not None else 0) + 1
                root = parent_info.get("root_source_comment_id") or parent_info.get("source_comment_id") or root
                parent_path = parent_info.get("path")
                segment = c.get("source_comment_id") or c.get("id")
                if parent_path and segment:
                    path = f"{parent_path}/{segment}"
            if root is None and c.get("source_comment_id"):
                root = c.get("source_comment_id")
            if depth is None:
                depth = 0 if not parent_info else depth
            if path is None and root:
                segment = c.get("source_comment_id") or c.get("id")
                if segment:
                    path = f"{root}/{segment}" if segment != root else root

            new_state = {
                "id": cid,
                "source_comment_id": c.get("source_comment_id"),
                "root_source_comment_id": root,
                "depth": depth,
                "path": path,
                "resolved_parent_source_comment_id": resolved_parent_source_id,
            }

            prev = computed_by_id.get(cid)
            if new_state != prev:
                progressed = True
            computed_by_id[cid] = new_state
            if new_state.get("source_comment_id"):
                computed_by_source[new_state["source_comment_id"]] = new_state
            if c.get("author_handle"):
                latest_by_author[c["author_handle"]] = new_state

        if not progressed:
            break

    updates = []
    for cid, state in computed_by_id.items():
        original = original_by_id.get(cid, {})
        payload: Dict[str, Any] = {}
        if state.get("root_source_comment_id") and state.get("root_source_comment_id") != original.get("root_source_comment_id"):
            payload["root_source_comment_id"] = state.get("root_source_comment_id")
        if state.get("depth") is not None and state.get("depth") != original.get("depth"):
            payload["depth"] = state.get("depth")
        if state.get("path") is not None and state.get("path") != original.get("path"):
            payload["path"] = state.get("path")
        if state.get("resolved_parent_source_comment_id") and not original.get("parent_source_comment_id"):
            payload["parent_source_comment_id"] = state.get("resolved_parent_source_comment_id")
        if payload:
            payload["updated_at"] = datetime.now(timezone.utc).isoformat()
            updates.append((cid, payload))

    updated = 0
    for cid, payload in updates:
        try:
            supabase.table("threads_comments").update(payload).eq("post_id", post_id).eq("id", cid).execute()
            updated += 1
        except Exception as e:
            logger.warning(f"[CommentsSoT] repair update failed id={cid} post={post_id}: {e}")

    return {"ok": True, "updated": updated, "count": len(rows)}


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
        repair = _repair_comment_tree(post_id)
        logger.info(f"✅ [CommentsSoT] upserted {total} comments for post {post_id}; repair_updated={repair.get('updated')}")
        return {"ok": True, "count": total, "repair": repair}
    except Exception as e:
        logger.warning(f"⚠️ [CommentsSoT] upsert failed for post {post_id}: {e}")
        return {"ok": False, "count": total, "error": str(e)}


def upsert_comment_clusters(post_id: int, clusters: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Upsert post-level clusters into threads_comment_clusters via RPC (set-based).
    """
    if not clusters:
        return {"ok": True, "count": 0, "skipped": True}
    def _as_text_list(val):
        if val is None:
            return []
        if hasattr(val, "tolist"):
            val = val.tolist()
        if isinstance(val, tuple):
            val = list(val)
        if not isinstance(val, list):
            raise ValueError("must be list")
        return [str(x) for x in val]

    def _as_float_list_384(val, key_int: int):
        if val is None:
            return None
        if hasattr(val, "tolist"):
            val = val.tolist()
        if isinstance(val, tuple):
            val = list(val)
        if not isinstance(val, list):
            return None
        if len(val) != 384:
            logger.warning("⚠️ [Clusters] Dropping invalid vec384 dim=%s cluster=%s", len(val), key_int)
            return None
        return [float(x) for x in val]

    sanitized: List[Dict[str, Any]] = []
    logger.info("🚀 [Clusters] Preparing to upsert %s clusters for post=%s", len(clusters), post_id)
    for c in clusters:
        if not isinstance(c, dict):
            raise ValueError("cluster payload must be dict")
        try:
            key_int = int(c.get("cluster_key", 0))
        except Exception:
            raise ValueError(f"Invalid cluster_key: {c.get('cluster_key')}")
        size_val = c.get("size")
        size_int = int(size_val) if size_val is not None else 0

        centroid_384 = _as_float_list_384(c.get("centroid_embedding_384"), key_int)
        if size_int >= 2 and centroid_384 is None:
            raise ValueError(f"🛑 Critical: centroid_384 missing for cluster={key_int} size={size_int} post={post_id}")

        tactics_val = c.get("tactics")
        if tactics_val is None:
            tactics_val = []
        if not isinstance(tactics_val, list):
            raise ValueError(f"tactics must be list for cluster {key_int}")
        if not all(isinstance(t, str) for t in tactics_val):
            raise ValueError(f"tactics entries must be strings for cluster {key_int}")

        sanitized.append(
            {
                "cluster_key": key_int,
                "label": c.get("label"),
                "summary": c.get("summary"),
                "size": size_int,
                "keywords": _as_text_list(c.get("keywords")),
                "top_comment_ids": _as_text_list(c.get("top_comment_ids")),
                "tactics": tactics_val,
                "tactic_summary": c.get("tactic_summary"),
                "centroid_embedding_384": centroid_384,
            }
        )
    logger.info(
        "[Clusters][witness] post=%s clusters=%s detail=%s",
        post_id,
        len(sanitized),
        [
            {
                "cluster_key": c["cluster_key"],
                "size": c["size"],
                "has_centroid_384": c["centroid_embedding_384"] is not None,
                "len_centroid_384": len(c["centroid_embedding_384"] or []),
                "type_centroid_384": type(c["centroid_embedding_384"]).__name__ if c.get("centroid_embedding_384") is not None else None,
            }
            for c in sanitized
        ],
    )
    logger.info(
        "[Clusters][witness2] post=%s detail=%s",
        post_id,
        [
            {
                "cluster_key": c["cluster_key"],
                "size": c["size"],
                "tactics_type": type(c.get("tactics") or []).__name__,
                "tactics_preview": str(c.get("tactics") or [])[:120],
            }
            for c in sanitized
        ],
    )
    try:
        supabase.rpc("upsert_comment_clusters", {"p_post_id": post_id, "p_clusters": sanitized}).execute()
        logger.info("[Clusters] rpc upsert post=%s clusters_attempted=%s", post_id, len(sanitized))
        gate = verify_cluster_centroids(post_id)
        if not gate.get("ok"):
            raise RuntimeError(f"centroid_persistence_failed bad_clusters={gate.get('bad_clusters')}")
        return {"ok": True, "count": len(sanitized), "skipped": False}
    except Exception as e:
        logger.warning("⚠️ [Clusters] rpc upsert failed post=%s err=%s", post_id, e)
        raise


def apply_comment_cluster_assignments(
    post_id: int,
    assignments: List[Dict[str, Any]],
    enforce_coverage: bool = True,
    unassignable_total: int = 0,
) -> Dict[str, Any]:
    """
    Batch update threads_comments with cluster_id/cluster_key via RPC (single call).
    assignments: [{comment_id, cluster_key, cluster_id?}]
    """
    if not assignments:
        return {"ok": True, "count": 0, "skipped": True}
    strict = _env_flag("DL_STRICT_CLUSTER_WRITEBACK")
    force_reassign = _env_flag("DL_FORCE_REASSIGN")
    coverage_min = float(os.environ.get("DL_ASSIGNMENT_COVERAGE_MIN", "0.95") or 0.0)
    mode = (os.environ.get("DL_ASSIGNMENT_WRITE_MODE", "fill_nulls") or "").lower()
    if mode not in {"fill_nulls", "overwrite"}:
        mode = "fill_nulls"
    if strict and mode == "overwrite" and not force_reassign:
        raise RuntimeError("STRICT: overwrite requires DL_FORCE_REASSIGN=1 to proceed")
    assignments_total = len(assignments)
    target_assignments = assignments
    target_rows = len(assignments)
    db_total_comments = 0
    db_null_before = 0
    db_null_after = 0
    coverage_after = 0.0
    try:
        # Witness before
        before = (
            supabase.table("threads_comments")
            .select("id,cluster_key", count="exact")
            .eq("post_id", post_id)
            .execute()
        )
        before_rows = getattr(before, "data", []) or []
        db_total_comments = before.count or len(before_rows)
        db_null_before = len([r for r in before_rows if r.get("cluster_key") is None])
        if mode == "fill_nulls":
            comment_ids = [a.get("comment_id") for a in assignments if a.get("comment_id") is not None]
            if comment_ids:
                existing = (
                    supabase.table("threads_comments")
                    .select("id,cluster_key")
                    .eq("post_id", post_id)
                    .in_("id", comment_ids)
                    .execute()
                )
                rows = getattr(existing, "data", []) or []
                null_ids = {r.get("id") for r in rows if r.get("cluster_key") is None}
                target_assignments = [a for a in assignments if a.get("comment_id") in null_ids]
                target_rows = len(target_assignments)
            else:
                target_assignments = []
                target_rows = 0
        if target_rows == 0:
            coverage = 1.0
            logger.info(
                "[Clusters] assignment writeback skipped (no eligible rows)",
                extra={
                    "post_id": post_id,
                    "assignments_total": assignments_total,
                    "target_rows": target_rows,
                    "updated_rows": 0,
                    "mode": mode,
                    "coverage_pct": round(coverage * 100, 2),
                },
            )
            return {
                "ok": True,
                "count": assignments_total,
                "target_rows": target_rows,
                "updated_rows": 0,
                "coverage": coverage,
                "mode": mode,
                "skipped": True,
            }
        try:
            resp = supabase.rpc("set_comment_cluster_assignments", {"p_post_id": post_id, "p_assignments": target_assignments}).execute()
            data = getattr(resp, "data", None)
            updated_rows = 0
            if isinstance(data, list):
                updated_rows = len(data)
            elif isinstance(data, dict):
                updated_rows = data.get("rows_updated") or data.get("row_count") or 0
            if not updated_rows:
                updated_rows = target_rows  # optimistic fallback when RPC doesn't echo rows
            already_filled = max(assignments_total - target_rows, 0)
            coverage = ((updated_rows + already_filled) / assignments_total) if assignments_total else 1.0
            # Witness after
            after = (
                supabase.table("threads_comments")
                .select("cluster_key", count="exact")
                .eq("post_id", post_id)
                .execute()
            )
            after_rows = getattr(after, "data", []) or []
            db_null_after = len([r for r in after_rows if r.get("cluster_key") is None])
            if db_total_comments:
                coverage_after = (db_total_comments - db_null_after) / db_total_comments
            logger.info(
                "[Clusters] rpc assignments",
                extra={
                    "post_id": post_id,
                    "assignments_total": assignments_total,
                    "target_rows": target_rows,
                    "assignments_updated_rows": updated_rows,
                    "mode": mode,
                    "coverage_pct": round(coverage * 100, 2),
                    "db_total_comments": db_total_comments,
                    "db_null_cluster_before": db_null_before,
                    "db_null_cluster_after": db_null_after,
                    "db_coverage_after": round(coverage_after * 100, 2) if db_total_comments else None,
                    "unassignable_total": unassignable_total,
                },
            )
            if strict and (updated_rows == 0 or updated_rows < target_rows):
                raise RuntimeError(
                    f"[Clusters] STRICT assignment writeback failed post={post_id} attempted={assignments_total} target_rows={target_rows} updated_rows={updated_rows}"
                )
            coverage_gate = coverage_after if enforce_coverage else coverage
            if enforce_coverage and coverage_gate < coverage_min:
                msg = f"[Clusters] assignment coverage below min post={post_id} coverage={coverage_gate:.3f} min={coverage_min} mode={mode}"
                if strict:
                    raise RuntimeError(msg)
                logger.error(msg)
                try:
                    supabase.table("threads_posts").update(
                        {"analysis_is_valid": False, "analysis_invalid_reason": "assignment_coverage_below_min"}
                    ).eq("id", post_id).execute()
                except Exception as e:
                    logger.warning("[Clusters] failed to mark post invalid on coverage shortfall post=%s err=%s", post_id, e)
            return {
                "ok": True,
                "count": assignments_total,
                "target_rows": target_rows,
                "updated_rows": updated_rows,
                "coverage": coverage,
                "db_total_comments": db_total_comments,
                "db_null_before": db_null_before,
                "db_null_after": db_null_after,
                "db_coverage_after": coverage_after,
                "mode": mode,
                "skipped": False,
            }
        except Exception as e:
            logger.warning("⚠️ [Clusters] assignment rpc failed post=%s err=%s", post_id, e)
            if strict:
                raise
            return {"ok": False, "count": 0, "error": str(e)}
    except Exception as e:
        if strict:
            raise
        logger.warning("⚠️ [Clusters] assignment writeback failed early post=%s err=%s", post_id, e)
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

    strict = _env_flag("DL_STRICT_CLUSTER_WRITEBACK")
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
    if strict and (missing > 0 or updated == 0):
        raise RuntimeError(
            f"[Clusters] STRICT writeback failure post={post_id} attempted={attempted} updated={updated} missing={missing}"
        )
    ok = missing == 0 or updated > 0
    return ok, updated


def verify_cluster_centroids(post_id: int | str) -> Dict[str, Any]:
    """
    Ensure clusters with size>=2 have centroid_embedding_384 persisted.
    """
    try:
        resp = supabase.table("threads_comment_clusters").select("cluster_key,size,centroid_embedding_384").eq("post_id", post_id).execute()
        rows = getattr(resp, "data", []) or []
        bad = []
        for row in rows:
            try:
                size = int(row.get("size") or 0)
            except Exception:
                size = 0
            if size >= 2 and (row.get("centroid_embedding_384") is None):
                bad.append(row.get("cluster_key"))
        return {"ok": len(bad) == 0, "bad_clusters": bad, "total": len([r for r in rows if (r.get('size') or 0) >= 2])}
    except Exception as e:
        logger.warning(f"[Clusters] centroid verification failed post={post_id}: {e}")
        return {"ok": False, "error": str(e), "bad_clusters": []}

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


def comment_debug_summary(post_id: str | int) -> Dict[str, Any]:
    """
    Lightweight sanity check for comment dedupe and tree fields.
    """
    try:
        resp = (
            supabase.table("threads_comments")
            .select("id, source_comment_id, parent_source_comment_id, root_source_comment_id", count="exact")
            .eq("post_id", post_id)
            .execute()
        )
        rows = getattr(resp, "data", None) or []
        total = resp.count or len(rows)
        with_source = sum(1 for r in rows if r.get("source_comment_id"))
        with_root = sum(1 for r in rows if r.get("root_source_comment_id"))
        with_parent = sum(1 for r in rows if r.get("parent_source_comment_id"))
        return {
            "post_id": post_id,
            "count": total,
            "with_source_comment_id": with_source,
            "with_root_source_comment_id": with_root,
            "with_parent_source_comment_id": with_parent,
        }
    except Exception as e:
        logger.warning(f"[CommentsSoT] debug summary failed post={post_id}: {e}")
        return {"post_id": post_id, "error": str(e)}


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
    # DEPRECATED (CDX-106): analysis_json writes must go through save_analysis_result.
    payload = dict(meta or {})
    if analysis_json is not None:
        save_analysis_result(post_id, analysis_json)
        for key in ("analysis_json", "analysis_is_valid", "analysis_invalid_reason", "analysis_missing_keys"):
            payload.pop(key, None)

    if payload:
        supabase.table("threads_posts").update(_json_safe(payload)).eq("id", post_id).execute()


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


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Quick sanity checks for threads_comments")
    parser.add_argument("--post-id", required=True, help="threads_posts.id to inspect")
    parser.add_argument("--repair", action="store_true", help="run repair pass before summarizing")
    args = parser.parse_args()

    if args.repair:
        result = _repair_comment_tree(args.post_id)
        print("[debug] repair_result:", result)
    summary = comment_debug_summary(args.post_id)
    print("[debug] summary:", json.dumps(summary, indent=2, ensure_ascii=False))

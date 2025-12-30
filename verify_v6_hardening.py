import argparse
import asyncio
import os
import sys

from database.store import save_analysis_result, supabase
from webapp.services.job_manager import JobManager


def _pick_latest_post_id() -> str | None:
    res = (
        supabase.table("threads_posts")
        .select("id")
        .order("updated_at", desc=True)
        .limit(1)
        .execute()
    )
    data = getattr(res, "data", None) or []
    if not data:
        return None
    return str(data[0]["id"])


def _pick_latest_job_id() -> str | None:
    res = (
        supabase.table("job_batches")
        .select("id")
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    data = getattr(res, "data", None) or []
    if not data:
        return None
    return str(data[0]["id"])


def test_analysis_gate(post_id: str) -> None:
    invalid_payload = {
        "post": {
            "post_id": str(post_id),
        }
    }
    save_analysis_result(post_id, invalid_payload)
    row = (
        supabase.table("threads_posts")
        .select("analysis_is_valid, analysis_invalid_reason")
        .eq("id", post_id)
        .single()
        .execute()
    )
    data = getattr(row, "data", None) or {}
    if data.get("analysis_is_valid") is not False:
        raise AssertionError("analysis_is_valid should be False for invalid payload")
    if not data.get("analysis_invalid_reason"):
        raise AssertionError("analysis_invalid_reason should be non-empty for invalid payload")


async def test_polling_cap(job_id: str) -> None:
    manager = JobManager()
    items_default, _ = await manager.get_job_items(job_id)
    if len(items_default) > 200:
        raise AssertionError(f"default limit returned {len(items_default)} items > 200")

    items_capped, _ = await manager.get_job_items(job_id, limit=5000)
    if len(items_capped) > 1000:
        raise AssertionError(f"cap limit returned {len(items_capped)} items > 1000")


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify V6.0 hardening safeguards.")
    parser.add_argument("--post-id", dest="post_id")
    parser.add_argument("--job-id", dest="job_id")
    args = parser.parse_args()

    post_id = args.post_id or os.getenv("V6_TEST_POST_ID") or _pick_latest_post_id()
    if not post_id:
        print("[verify] No post_id available. Provide --post-id or V6_TEST_POST_ID.")
        return 2

    job_id = args.job_id or os.getenv("V6_TEST_JOB_ID") or _pick_latest_job_id()
    if not job_id:
        print("[verify] No job_id available. Provide --job-id or V6_TEST_JOB_ID.")
        return 2

    print(f"[verify] Using post_id={post_id} job_id={job_id}")

    test_analysis_gate(post_id)
    print("[verify] Test 1 (analysis gate) passed")

    asyncio.run(test_polling_cap(job_id))
    print("[verify] Test 2 (polling cap) passed")

    return 0


if __name__ == "__main__":
    sys.exit(main())

import asyncio
import importlib
import logging
import os
import time
from concurrent.futures import TimeoutError as FutureTimeout
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpcore
import httpx

try:
    from database.store import supabase as default_supabase
except Exception:
    logging.warning("Could not import 'database.store'. Please inject supabase client manually.")
    default_supabase = None

from webapp.schemas.jobs import JobCreate
from webapp.services.pipeline_runner import run_pipeline_a_job, process_pipeline_b_backend

logger = logging.getLogger(__name__)

# ----------------------------
# In-memory cache (bounded)
# ----------------------------
_CACHE_STORE: Dict[str, Dict[str, Any]] = {}
_CACHE_MAX_KEYS = 256


def _cache_get(key: str):
    return _CACHE_STORE.get(key)


def _cache_set(key: str, data: Any):
    if key not in _CACHE_STORE and len(_CACHE_STORE) >= _CACHE_MAX_KEYS:
        oldest_key = min(_CACHE_STORE.keys(), key=lambda k: _CACHE_STORE[k]["time"])
        _CACHE_STORE.pop(oldest_key, None)
    _CACHE_STORE[key] = {"time": time.time(), "data": data}


def _cache_del_prefix(prefix: str):
    for k in list(_CACHE_STORE.keys()):
        if k.startswith(prefix):
            _CACHE_STORE.pop(k, None)


class JobManager:
    _lock: Optional[asyncio.Lock] = None

    def __init__(self, db_client=None):
        self.db = db_client or default_supabase
        if not self.db:
            raise RuntimeError("Supabase client not available. Check imports.")
        if JobManager._lock is None:
            JobManager._lock = asyncio.Lock()
        self._lock = JobManager._lock
        self.last_degraded: bool = False

    # ------------------------------------------------------------------
    # Core retry + cache helpers
    # ------------------------------------------------------------------
    async def _retry_db(self, func: Callable[[], Awaitable[Any]], retries: int = 3, base_sleep: float = 0.3):
        last_error = None
        for i in range(retries):
            try:
                return await func()
            except (
                httpx.RemoteProtocolError,
                httpx.ReadError,
                httpcore.RemoteProtocolError,
                httpcore.ReadError,
                httpx.ReadTimeout,
                httpx.ConnectError,
                httpcore.ConnectError,
                httpx.PoolTimeout,
                OSError,
            ) as e:
                last_error = e
                wait_time = base_sleep * (2**i)
                logger.warning("⚠️ [Supabase Retry %s/%s] %s | backoff=%.2fs", i + 1, retries, repr(e), wait_time)
                await asyncio.sleep(wait_time)
            except Exception as e:
                logger.error("❌ [DB_LOGIC_ERROR] %s", repr(e))
                raise

        logger.error("❌ [OPS_DEGRADED][DB_RETRY_FAILED] %s", repr(last_error))
        return None

    async def _cached_call(self, key: str, ttl: float, func: Callable[[], Awaitable[Any]]) -> Tuple[Any, bool]:
        now = time.time()
        cached = _cache_get(key)
        if cached and (now - cached["time"] < ttl):
            self.last_degraded = False
            return cached["data"], False

        res = await self._retry_db(func)
        if res is None:
            if cached:
                logger.warning("⚠️ [OPS_DEGRADED] Serving STALE cache for key=%s", key)
                self.last_degraded = True
                return cached["data"], True
            self.last_degraded = True
            return [], True

        data = res.data if hasattr(res, "data") else res
        _cache_set(key, data)
        self.last_degraded = False
        return data, False

    # ------------------------------------------------------------------
    # Read endpoints (cached)
    # ------------------------------------------------------------------
    async def get_job_list(self, limit: int = 50) -> Tuple[List[Dict[str, Any]], bool]:
        cache_key = f"jobs_list:{limit}"

        async def _call():
            return await asyncio.to_thread(
                lambda: self.db.table("job_batches").select("*").order("created_at", desc=True).limit(limit).execute()
            )

        async with self._lock:
            return await self._cached_call(cache_key, 2.0, _call)

    async def get_job_items(self, job_id: str, limit: int = 200) -> Tuple[List[Dict[str, Any]], bool]:
        if limit > 1000:
            limit = 1000
        cache_key = f"job_items:{job_id}:{limit}"

        async def _call():
            return await asyncio.to_thread(
                lambda: self.db.table("job_items")
                .select("*")
                .eq("job_id", job_id)
                .order("updated_at", desc=True)
                .order("id", desc=True)
                .limit(limit)
                .execute()
            )

        async with self._lock:
            return await self._cached_call(cache_key, 2.0, _call)

    # ------------------------------------------------------------------
    # Write helpers
    # ------------------------------------------------------------------
    async def _rpc(self, func_name: str, params: Dict[str, Any]) -> Any:
        async def _call():
            return await asyncio.to_thread(lambda: self.db.rpc(func_name, params).execute())

        async with self._lock:
            res = await self._retry_db(_call)
            if res is None:
                raise RuntimeError(f"[RPC_FAILED] {func_name} connection error")
            return res.data

    async def _table_update(self, table: str, record_id: str, patch: Dict[str, Any]) -> Any:
        async def _call():
            return await asyncio.to_thread(lambda: self.db.table(table).update(patch).eq("id", record_id).execute())

        async with self._lock:
            res = await self._retry_db(_call)
            if res is None:
                raise RuntimeError(f"[UPDATE_FAILED] {table} connection error")
            return res.data

    async def _table_insert(self, table: str, data: Any) -> Any:
        async def _call():
            return await asyncio.to_thread(lambda: self.db.table(table).insert(data).execute())

        async with self._lock:
            res = await self._retry_db(_call)
            if res is None:
                raise RuntimeError(f"[INSERT_FAILED] {table} connection error")
            if table == "job_batches":
                _cache_del_prefix("jobs_list:")
            if table == "job_items":
                _cache_del_prefix("job_items:")
            return res.data

    async def _table_select_single(self, table: str, record_id: str) -> Optional[Dict[str, Any]]:
        async def _call():
            return await asyncio.to_thread(
                lambda: self.db.table(table).select("*").eq("id", record_id).single().execute()
            )

        async with self._lock:
            res = await self._retry_db(_call)
            if res is None:
                return None
            return res.data

    # ------------------------------------------------------------------
    # Job lifecycle helpers
    # ------------------------------------------------------------------
    async def create_job(self, job_in: JobCreate) -> str:
        data = {
            "pipeline_type": (job_in.pipeline_type or "").strip().upper(),
            "mode": (job_in.mode or "ingest").strip().lower(),
            "input_config": job_in.input_config,
            "status": "discovering",
        }
        res_data = await self._table_insert("job_batches", data)
        job_id = res_data[0]["id"]
        logger.info("Job created: %s", job_id)
        return job_id

    async def create_job_from_payload(self, pipeline_type: str, mode: str, input_config: Dict[str, Any]) -> str:
        job = JobCreate(
            pipeline_type=(pipeline_type or "").strip().upper(),
            mode=(mode or "ingest").strip().lower(),
            input_config=input_config or {},
        )
        return await self.create_job(job)

    async def start_discovery(self, job_id: str) -> int:
        job_data = await self._table_select_single("job_batches", job_id)
        config = job_data.get("input_config") or {}

        raw_targets: list[str] = []

        def add_target(val: Any):
            if isinstance(val, str):
                raw_targets.append(val.strip())
            elif isinstance(val, list):
                for v in val:
                    if isinstance(v, str):
                        raw_targets.append(v.strip())

        add_target(config.get("url"))
        add_target(config.get("target"))
        add_target(config.get("targets"))
        add_target(config.get("lines"))
        add_target(config.get("keywords"))

        seen = set()
        targets: list[str] = []
        for t in raw_targets:
            if not t or t in seen:
                continue
            seen.add(t)
            targets.append(t)

        if not targets:
            targets = [f"mock://{job_id}/{i}" for i in range(1, 6)]

        items_data = [{"job_id": job_id, "target_id": t, "status": "pending", "stage": "init"} for t in targets]
        await self._table_insert("job_items", items_data)

        async def _update():
            return await asyncio.to_thread(
                lambda: self.db.table("job_batches")
                .update({"total_count": len(items_data), "status": "processing"})
                .eq("id", job_id)
                .execute()
            )

        async with self._lock:
            await self._retry_db(_update)

        return len(items_data)

    async def mark_job_processing(self, job_id: str, total_count: Optional[int] = None):
        patch: Dict[str, Any] = {"status": "processing"}
        if total_count is not None:
            patch["total_count"] = total_count
        await self._table_update("job_batches", job_id, patch)

    async def set_job_heartbeat(self, job_id: str):
        await self._table_update("job_batches", job_id, {"last_heartbeat_at": datetime.now(tz=timezone.utc).isoformat()})

    async def claim_next_item(self, job_id: str, worker_id: str) -> Optional[Dict[str, Any]]:
        data = await self._rpc(
            "claim_job_item",
            {"p_job_id": job_id, "p_worker_id": worker_id, "p_lock_ttl_seconds": 60},
        )
        return data[0] if data else None

    async def set_item_stage(self, item_id: str, stage: str):
        await self._rpc("set_job_item_stage", {"p_item_id": item_id, "p_stage": stage})

    async def complete_item(self, item_id: str, result_post_id: Optional[str] = None):
        await self._rpc("complete_job_item", {"p_item_id": item_id, "p_result_post_id": result_post_id})

    async def fail_item(self, item_id: str, stage: str, prefix: str, msg: str):
        await self._rpc(
            "fail_job_item",
            {"p_item_id": item_id, "p_stage": stage, "p_error_log": f"{prefix}: {msg}"},
        )

    async def touch_item(self, item_id: str, stage: Optional[str] = None):
        patch: Dict[str, Any] = {"updated_at": datetime.now(tz=timezone.utc).isoformat()}
        if stage:
            patch["stage"] = stage
        await self._table_update("job_items", item_id, patch)

    async def get_job_summary(self, job_id: str) -> Tuple[Optional[Dict[str, Any]], bool]:
        degraded = False
        header = await self._table_select_single("job_batches", job_id)
        if header is None:
            return None, degraded

        async def _call_items():
            return await asyncio.to_thread(
                lambda: self.db.table("job_items")
                .select("status,stage,updated_at")
                .eq("job_id", job_id)
                .order("updated_at", desc=True)
                .limit(5000)
                .execute()
            )

        async with self._lock:
            res = await self._retry_db(_call_items)

        items: List[Dict[str, Any]] = []
        if res is None:
            degraded = True
        else:
            items = res.data or []

        def _parse_ts(ts: Any) -> Optional[datetime]:
            if not ts:
                return None
            try:
                return datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except Exception:
                return None

        total = len(items)
        success = sum(1 for it in items if (it.get("status") == "completed") or (it.get("stage") == "completed"))
        failed = sum(1 for it in items if (it.get("status") == "failed") or (it.get("stage") == "failed"))
        processed = success + failed
        last_item_ts = None
        if items:
            last_item_ts = max((_parse_ts(it.get("updated_at")) for it in items if _parse_ts(it.get("updated_at"))), default=None)

        hb_ts = _parse_ts(header.get("last_heartbeat_at"))

        status = "processing"
        if total > 0 and processed >= total:
            status = "completed"
            if failed > 0:
                status = "failed"
        if hb_ts and total > 0 and processed < total:
            age = (datetime.now(timezone.utc) - hb_ts).total_seconds()
            if age > 60:
                status = "stale"

        summary = {
            "job_id": job_id,
            "pipeline_type": header.get("pipeline_type"),
            "status": status,
            "total_count": total,
            "processed_count": processed,
            "success_count": success,
            "failed_count": failed,
            "last_item_updated_at": last_item_ts.isoformat() if last_item_ts else None,
            "last_heartbeat_at": header.get("last_heartbeat_at"),
            "degraded": degraded,
        }
        return summary, degraded

    # ------------------------------------------------------------------
    # Runner resolution + URL candidates
    # ------------------------------------------------------------------
    def _resolve_runner(self) -> Optional[Callable[[str, str], None]]:
        try:
            logger.info("Linked Pipeline A runner: %s.%s", run_pipeline_a_job.__module__, run_pipeline_a_job.__name__)
            return run_pipeline_a_job
        except Exception as e:
            logger.warning("Direct import of run_pipeline_a_job failed: %s", e)

        candidates = [
            ("webapp.services.pipeline_runner", "run_pipeline_a_job"),
        ]
        for mod, fn in candidates:
            try:
                module = importlib.import_module(mod)
                runner = getattr(module, fn, None)
                if callable(runner):
                    logger.info("Linked Pipeline A runner via fallback: %s.%s", mod, fn)
                    return runner
            except Exception as err:
                logger.warning("Runner import failed for %s.%s: %s", mod, fn, err)
        return None

    def _url_candidates(self, url: str) -> List[str]:
        raw = url.strip() if isinstance(url, str) else ""
        candidates: List[str] = []

        def _append(u: str):
            if not u:
                return
            while "www.www" in u:
                u = u.replace("www.www", "www.")
            if u not in candidates:
                candidates.append(u)

        _append(raw)
        if "?" in raw:
            _append(raw.split("?", 1)[0])

        # optional threads.net rebuild (only if safe)
        parsed = urlparse(raw)
        if parsed.netloc and "threads.com" in parsed.netloc:
            rebuilt = f"{parsed.scheme or 'https'}://www.threads.net{parsed.path or ''}"
            _append(rebuilt)

        return candidates

    async def _fetch_post_id_by_url(self, url: str) -> Optional[str]:
        async def _call():
            return await asyncio.to_thread(
                lambda: self.db.table("threads_posts").select("id").eq("url", url).limit(1).execute()
            )

        async with self._lock:
            res = await self._retry_db(_call)
        if res is None:
            return None
        rows = res.data or []
        if not rows:
            return None
        pid = rows[0].get("id")
        return str(pid) if pid is not None else None

    async def _fetch_post_id_by_shortcode(self, shortcode: str) -> Optional[str]:
        async def _call():
            return await asyncio.to_thread(
                lambda: self.db.table("threads_posts").select("id").ilike("url", f"%{shortcode}%").limit(1).execute()
            )

        async with self._lock:
            res = await self._retry_db(_call)
        if res is None:
            return None
        rows = res.data or []
        if not rows:
            return None
        pid = rows[0].get("id")
        return str(pid) if pid is not None else None

    async def _recover_post_id(self, url: str, attempts: int = 3, delay: float = 1.0) -> Tuple[Optional[str], List[str]]:
        tried: List[str] = []
        candidates = self._url_candidates(url)
        shortcode = ""
        parsed = urlparse(url)
        if parsed.path:
            parts = [p for p in parsed.path.split("/") if p]
            if parts:
                shortcode = parts[-1]

        for attempt in range(1, attempts + 1):
            for candidate in candidates:
                tried.append(candidate)
                pid = await self._fetch_post_id_by_url(candidate)
                if pid:
                    logger.info("✅ Post ID recovered via url=%s", candidate)
                    return pid, tried
            if shortcode:
                pid = await self._fetch_post_id_by_shortcode(shortcode)
                if pid:
                    logger.info("✅ Post ID recovered via shortcode match=%s", shortcode)
                    return pid, tried
            if attempt < attempts:
                await asyncio.sleep(delay)
        return None, tried

    # ------------------------------------------------------------------
    # Worker (Pipeline A real, others mocked)
    # ------------------------------------------------------------------
    async def run_worker_mock(self, job_id: str):
        logger.info("Starting workers for Job %s", job_id)

        job = await self._table_select_single("job_batches", job_id)
        pipeline_type = (job.get("pipeline_type") or "").strip().upper() if job else ""
        runner = self._resolve_runner() if pipeline_type == "A" else None
        last_stage_by_item: Dict[str, str] = {}

        # Dedicated handler for Pipeline B (batch) using Supabase-backed progress.
        if pipeline_type == "B":
            config = (job or {}).get("input_config") or {}
            keyword = config.get("keyword")
            targets = config.get("targets") or config.get("urls")
            max_posts = int(config.get("max_posts") or 20)
            exclude_existing = bool(config.get("exclude_existing") if "exclude_existing" in config else True)
            reprocess_policy = config.get("reprocess_policy") or "skip_if_exists"
            ingest_source = config.get("ingest_source") or "B"
            mode = config.get("mode") or (job or {}).get("mode") or "run"
            pipeline_mode = config.get("pipeline_mode") or "full"
            concurrency = int(config.get("concurrency") or 2)
            vision_mode = config.get("vision_mode") or os.environ.get("VISION_MODE") or "auto"
            vision_stage_cap = config.get("vision_stage_cap") or os.environ.get("VISION_STAGE_CAP") or "auto"

            await self.mark_job_processing(job_id)
            await self.set_job_heartbeat(job_id)
            try:
                summary = await process_pipeline_b_backend(
                    keyword=keyword,
                    urls=targets,
                    max_posts=max_posts,
                    exclude_existing=exclude_existing,
                    reprocess_policy=reprocess_policy,
                    ingest_source=ingest_source,
                    mode=mode,
                    concurrency=concurrency,
                    pipeline_mode=pipeline_mode,
                    vision_mode=vision_mode,
                    vision_stage_cap=vision_stage_cap,
                    job_id=job_id,
                )
                success = int((summary or {}).get("success_count") or 0)
                fail = int((summary or {}).get("fail_count") or 0)
                processed = success + fail
                await self._table_update(
                    "job_batches",
                    job_id,
                    {
                        "status": "completed" if fail == 0 else "failed",
                        "processed_count": processed,
                        "success_count": success,
                        "failed_count": fail,
                    },
                )
            except Exception as e:
                logger.error("[worker] Pipeline B failed job_id=%s err=%s", job_id, e, exc_info=True)
                await self._table_update(
                    "job_batches",
                    job_id,
                    {
                        "status": "failed",
                        "error_summary": str(e)[:200],
                    },
                )
            return

        async def worker_loop(w_id: str):
            loop = asyncio.get_running_loop()
            while True:
                await self.set_job_heartbeat(job_id)
                item = await self.claim_next_item(job_id, w_id)
                if not item:
                    break

                item_id = item["id"]
                target = item["target_id"]
                current_stage = "init"
                await self.set_item_stage(item_id, "init")

                try:
                    current_stage = "processing"
                    await self.set_item_stage(item_id, "processing")

                    if pipeline_type == "A":
                        if not runner:
                            raise RuntimeError("Pipeline A runner not resolved")

                        logger.info("[worker %s] 🕸️ Crawling pipeline=A url=%s (runner=%s.%s)", w_id, target, runner.__module__, runner.__name__)

                        current_stage = "processing"

                        def stage_cb(stage: str):
                            nonlocal current_stage
                            if not stage:
                                return
                            if last_stage_by_item.get(item_id) == stage:
                                return
                            last_stage_by_item[item_id] = stage
                            current_stage = stage
                            try:
                                fut = asyncio.run_coroutine_threadsafe(self.set_item_stage(item_id, stage), loop)
                                try:
                                    fut.result(timeout=2.0)
                                except FutureTimeout:
                                    logger.warning("[worker %s] stage_cb timeout stage=%s item_id=%s", w_id, stage, item_id)
                                except Exception:
                                    logger.warning("[worker %s] stage_cb result error stage=%s item_id=%s", w_id, stage, item_id, exc_info=True)
                            except Exception:
                                logger.warning("[worker %s] stage_cb failed stage=%s item_id=%s", w_id, stage, item_id, exc_info=True)

                        try:
                            post_id = await asyncio.to_thread(runner, job_id, target, item_id=item_id, stage_cb=stage_cb)
                        except Exception as exc:
                            logger.exception("[worker %s] Runner threw for url=%s", w_id, target)
                            await self.fail_item(item_id, current_stage, "RUNNER_ERROR", f"{type(exc).__name__}: {exc}")
                            await self._rpc(
                                "bump_job_counters",
                                {"p_job_id": job_id, "p_is_success": False, "p_is_failed": True},
                            )
                            continue

                        if not post_id:
                            logger.info("[worker %s] runner returned no post_id, recovering via DB...", w_id)
                            post_id, tried = await self._recover_post_id(target)
                            if not post_id:
                                msg = f"post_id not found; tried={tried}"
                                logger.error("[worker %s] ❌ %s", w_id, msg)
                                await self.fail_item(item_id, current_stage, "POST_ID_NOT_FOUND", msg)
                                await self._rpc(
                                    "bump_job_counters",
                                    {"p_job_id": job_id, "p_is_success": False, "p_is_failed": True},
                                )
                                continue

                        post_row = await self._table_select_single("threads_posts", post_id)
                        has_analysis = False
                        if post_row:
                            has_analysis = bool(post_row.get("analysis_json")) or bool(post_row.get("full_report"))
                        if not has_analysis:
                            stage_for_fail = "analyst" if current_stage in ("analyst", "store") else current_stage
                            await self.fail_item(item_id, stage_for_fail, "ANALYSIS_MISSING", f"post_id={post_id}")
                            await self._rpc(
                                "bump_job_counters",
                                {"p_job_id": job_id, "p_is_success": False, "p_is_failed": True},
                            )
                            continue

                        await self.complete_item(item_id, post_id)
                        await self._rpc(
                            "bump_job_counters",
                            {"p_job_id": job_id, "p_is_success": True, "p_is_failed": False},
                        )
                        logger.info("[worker %s] ✅ Done. Result Post ID: %s", w_id, post_id)
                    else:
                        # Mock path for non-A pipelines
                        await asyncio.sleep(0.5)
                        await self.set_item_stage(item_id, "vision")
                        await asyncio.sleep(0.5)
                        await self.set_item_stage(item_id, "analyst")
                        await self.complete_item(item_id, f"mock_res:{item_id}")
                        await self._rpc(
                            "bump_job_counters",
                            {"p_job_id": job_id, "p_is_success": True, "p_is_failed": False},
                        )

                except Exception as e:
                    logger.error("[worker %s] ❌ Error on %s: %s", w_id, target, e, exc_info=True)
                    stage_for_fail = current_stage if current_stage else "processing"
                    await self.fail_item(item_id, stage_for_fail, "RUNTIME_ERR", str(e))
                    await self._rpc(
                        "bump_job_counters",
                        {"p_job_id": job_id, "p_is_success": False, "p_is_failed": True},
                    )

                finally:
                    await self.set_job_heartbeat(job_id)
                    await self._rpc("finalize_job_if_done", {"p_job_id": job_id})

        await asyncio.gather(worker_loop("worker-alpha"), worker_loop("worker-beta"))

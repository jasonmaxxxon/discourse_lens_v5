# DiscourseLens V5 — Technical Specs (Updated)

## Backend (FastAPI @ webapp/app.py)
- App factory: `create_app()` in `webapp/app.py` (routers/middleware/templates)
- Entrypoint: `uvicorn webapp.main:app --reload --port 8000`
- CORS: localhost:5173/5174 allowed.
- Key endpoints:
  - Jobs (Supabase-backed JobManager, progressive job_items)
    - `POST /api/jobs/` create job (pipeline_type/mode/input_config); HTTP errors bubble up to caller.
    - `GET /api/jobs/{job_id}` head + 20 items; degraded cache signaled via `x-ops-degraded: 1`.
    - `GET /api/jobs/{job_id}/items` items ordered by `updated_at desc` including `result_post_id`; default limit 200, cap 1000; degraded header when DB unreachable.
    - `GET /api/jobs/{job_id}/summary` counters (total/processed/success/failed/heartbeat) with degraded header.
    - Legacy `GET /api/status/{job_id}` remains for in-memory JOBS compatibility.
  - `GET /api/posts` — latest analyzed posts (analysis_json present).
  - `GET /api/analysis-json/{post_id}` — structured `analysis_json` for UI.
  - `GET /api/analysis/{post_id}` — legacy markdown full report.
  - `GET /api/debug/latest-post` — debug helper to inspect latest row.
- Data store: Supabase tables `threads_posts` + `job_batches`/`job_items` (progress SoT). Fields include `analysis_json`, `full_report`, `images`, `raw_comments`, `ingest_source`, plus job_items.stage/status/result_post_id/error_log.
- Vision (CDX-081): VisionGate gating + TwoStageVisionWorker (Gemini 2.5 Flash). Unified writeback via `database.store.update_vision_meta` to `threads_posts.vision_*` + `images`.

## Analyst Layer (analysis/analyst.py)
- **Role:** The Intelligence Engine. Fuses deterministic crawler data (Physics) with LLM interpretations (Semantics) into `analysis_json`.
- **L2 Tactic Registry (Sociological Strategy):**
  - Identifies specific rhetorical tactics (e.g., "Muddling", "Slippery Slope") defined in `analysis/knowledge_base/academic_theory.txt`.
  - Parses structured tactics from AI output to populate `analysis_json.tactics` (future source for `Narrative_Tactics` table).
- **L3 Narrative Layer (Deep Interpretation):** Generates interpretive summaries of the phenomenon's structural intent.
- **Metrics Authority (SoT):** `like_count/reply_count/view_count` are STRICTLY sourced from crawler (`post_data`). AI-generated numbers are treated as hallucinations and discarded during fusion.
- **Extraction Logic:** Robust regex extraction for `L1/L2/L3` headers (colon/dash/space, case-insensitive) stopping at section boundaries.
- **Validation:** `analysis/build_analysis_json.py` uses `build_and_validate_analysis_json` to enforce Schema V4 (Pydantic), rejecting payloads that fail "Garbage Logic" checks (e.g., missing keys, hallucinated metrics).
- **Outputs:** Persisted via `database.store.save_analysis_result` to `threads_posts.analysis_json` and rendered as `reports/Analysis_<Post_ID>.md`.

## Frontend (dlcs-ui, Vite + React)
- Dev: `npm run dev -- --port 5173` (backend at http://127.0.0.1:8000).
- Routing（預設入口 / default landing）：`/` → `/archive`; `/pipeline/a|b|c`; `/history`; `/archive`; `/narrative/:postId`; `/legacy/home`（舊版保留）。`/pipeline/a` 是單頁 console+monitor（無導航，僅 state 切換）；進度頁 `/pipeline/progress/:jobId` 直接輪詢 `/api/jobs/{id}/items` + `/summary` 並即時渲染完成貼文連結。
- Dark mode: enforced via `<html class="dark">`, Tailwind `darkMode: "class"`, App wrapper `bg-midnight text-white min-h-screen`.
- API client: `src/api/client.ts` uses `VITE_API_BASE_URL` or defaults to `http://127.0.0.1:8000`.
- Narrative detail route: `/narrative/:postId` uses `useNarrativeAnalysis` → `GET /api/analysis-json/{postId}` → `normalizeAnalysisJson`.
- Legacy endpoints `/api/run/*` + `/api/status/{job_id}` retained for compatibility only (DEPRECATED); prefer `/api/jobs/*` (JobManager Supabase SoT).

## Pipeline Contract (API)
- **Canonical Job API (SoT = Supabase)**
  - `POST /api/jobs/`: The **ONLY** legitimate job creation endpoint for SPA and programmatic clients. It writes to `job_batches` (header) + `job_items` (targets) immediately.
  - `GET /api/jobs/{job_id}/items` & `/summary`: The **ONLY** progress endpoints used by the SPA. They MUST return partial results (progressive disclosure) while `status=running`.
  - **Legacy Compatibility:** `POST /api/run/*` and `GET /api/status/{job_id}` are compatibility wrappers. `POST /api/run/*` MUST internally delegate to the `JobManager.create_job()` logic to ensure persistence in Supabase. In-memory-only jobs are strictly forbidden.

- **Endpoints**
  - `POST /api/jobs/`: Create job (pipeline_type A/B/C, mode, input_config).
  - `POST /api/run/{pipeline}` (Legacy): Wraps `POST /api/jobs/` logic. Returns `{ job_id, status }`.
  - `GET /api/posts`: "LatestFeed" (MVP limit 20, no pagination yet). V5.1 will add cursor-based pagination.

## Archive Contract
- `GET /api/posts` 回傳含分析的貼文列表（條件 `analysis_json IS NOT NULL`，不過濾 `analysis_is_valid`）：  
  - 欄位：`id, snippet, created_at, author, like_count, view_count, reply_count, has_analysis, analysis_is_valid, analysis_version, analysis_build_id, ai_tags, archive_captured_at, archive_build_id, has_archive, phenomenon_id/status/case_id/name`  
  - 排序/範圍：`ORDER BY created_at DESC LIMIT 20`，無分頁；snippet 由 `post_text` 清洗  
  - 用途：ArchivePage 顯示清單，點擊導向 `/narrative/{id}`。

## Data Ownership / Persistence
- Comments: `threads_comments` = SoT（查詢/搜尋/聚類）；`threads_posts.raw_comments` = legacy snapshot，僅供狀態頁/快速預覽。
- Images: `threads_posts.images` 保存持久化 metadata；`images/` 本地目錄為暫存 cache（gitignored）。
- Clusters: metadata upserted via RPC `upsert_comment_clusters(post_id, clusters_jsonb)` (sets label/summary/tactics/tactic_summary). Assignments are bulk-updated via RPC `set_comment_cluster_assignments(post_id, assignments_jsonb)` when `DL_PERSIST_ASSIGNMENTS=1`; flag off skips assignments but metadata still upserts.

## OCR / Media (Vision)
- Gating: `analysis/vision_gate.VisionGate` (regex-free) writes `vision_mode/vision_need_score/vision_reasons/vision_stage_ran/vision_sim_post_comments/vision_metrics_reliable`.
- Two-stage worker: `analysis/vision_worker_two_stage.TwoStageVisionWorker` (V1 classify, optional V2 extraction with Gemini 2.5 Flash). Rate limit controlled via `VISION_RATE_LIMIT_SECONDS` (default 2s).
- Writeback: `database.store.update_vision_meta` patches `threads_posts` vision columns + enriched `images` (first image today).
- Images stored temporarily under `images/`/tempfiles; only enriched metadata is persisted in DB.

## Pipelines
- Pipeline A: Single post ingest (URL) with comments + VisionGate + Two-Stage Vision + Analyst. Worker reports stages to `job_items.stage` and writes `result_post_id` on completion.
- Pipeline B: Keyword crawl; `mode=ingest`（預設）僅入庫，`mode=analyze` 跑 Analyst；`mode=hotlist` 輸出熱帖清單。Batch 路徑用 `pipeline_mode=ingest|full` 控制是否分析，且每個 target 會即時寫入 job_items（running/completed_post/failed_post + result_post_id/error_log）。
- Pipeline C: Profile timeline sample crawl；`mode=ingest`（預設）入庫，`mode=analyze` 跑 Analyst。
- Shared logic in `pipelines/core.py`.

## Dev Tooling
- Start both stacks: `./start.sh` (kills :8000/:5173, launches uvicorn + Vite, health-checks).
- Python deps: `requirements.txt`; create venv + `pip install -r requirements.txt`.
- Frontend: `npm install` inside `dlcs-ui/`.

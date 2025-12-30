# DiscourseLens V5

Threads 貼文抓取 + 圖片 OCR + Supabase 儲存的輕量控制台（FastAPI + Jinja）。

## 架構總覽
- Web dashboard：`webapp/app.py`（FastAPI app factory） + `webapp/main.py`（uvicorn 入口） + `webapp/templates/index.html` / `webapp/templates/status.html`
- Pipelines：
  - **Pipeline A**：單貼 Threads 抓取，包含留言樣本與圖片 OCR
  - **Pipeline B**：關鍵字批量抓取熱帖
  - **Pipeline C**：個人主頁批量抓取（即時牆樣本）
- 資料儲存：Supabase `threads_posts`（含 `images`、`raw_comments`、`ingest_source` 等欄位）
- 圖片 / Vision：CDX-081 兩階段（VisionGate → Two-Stage Gemini）。先用 VisionGate 決策是否跑；V1 便宜分類，必要時才跑 V2 深度抽取/OCR。統一寫回 `update_vision_meta()`（`threads_posts.vision_*` + `images` enrich）。
- 留言資料：`threads_posts.raw_comments`（legacy 快照）+ `threads_comments`（SoT，可檢索）
  - `threads_comments` = source-of-truth（查詢/搜尋/聚類用）
  - `threads_posts.raw_comments` = legacy snapshot，僅供狀態頁/快速預覽

## V6.0 Hardening
- `analysis_json` 寫入改為單一入口：`database.store.save_analysis_result`（先做 Schema V4 驗證，失敗也會保留 raw payload 供追溯）
- `/api/jobs/{job_id}/items` 預設最多 200 筆，debug 可帶 `limit` 但上限 1000

## 重要檔案地圖
- `webapp/`：API routes + Jinja 模板  
  - `webapp/app.py`：FastAPI app factory（含路由/中介層/模板/exception handler）  
  - `webapp/main.py`：精簡 uvicorn 入口（builds `app = create_app()`）  
  - `webapp/templates/index.html`：Dashboard 首頁  
  - `webapp/templates/status.html`：Pipeline 狀態 + Threads 風格卡片
- `analysis/analyst.py`：核心邏輯，縫合「爬蟲數據」與「AI 分析」，含 L1/L2/L3 提取與 metrics 組裝
- `analysis/phenomenon_fingerprint.py` / `analysis/phenomenon_enricher.py`：CDX-044.1 現象指紋與非阻塞 Match-or-Mint
- `analysis/embeddings.py`：現象向量嵌入（deterministic placeholder，用於 hybrid match/mint）
- `pipelines/core.py`：封裝 Pipeline A/B/C 邏輯
- `scraper/fetcher.py` / `scraper/parser.py`：抓取、解析 Threads HTML
- `scraper/image_pipeline.py`：圖片下載（暫存）+ OCR + enrich metadata
- `ocr/engine.py`：PaddleOCR 包裝
- `database/store.py`：寫入 Supabase
- `images/`：本地暫存圖片（Git 忽略，僅作暫存；持久化 metadata 寫入 `threads_posts.images`）
- `main.py`（repo 根目錄）：`DEPRECATED` 遺留入口，保留歷史參考
更多 CDX-044.1 規格見 `docs/phenomenon_registry.md`。

## Quick Start（macOS / zsh）
### Backend
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # 填入 SUPABASE_URL/SUPABASE_KEY/GEMINI_API_KEY 等
uvicorn webapp.main:app --reload --port 8000
```
### Frontend (dlcs-ui)
```bash
cd dlcs-ui
npm install
npm run dev -- --port 5173   # 主入口 http://localhost:5173/
```
環境變數放根目錄 `.env`，不要提交；Threads cookie 存 `auth_threads.json`（勿提交）。勿提交 `.vscode/`、`node_modules/`、`images/`、`dlcs-ui/dist/`。

## API（webapp/app.py 現有）
- Programmatic JSON（預設入口）  
  - `POST /api/run/{pipeline}`（A/B/C）：  
    - A: `{ "url": "...", "mode"?: "analyze" }`，預設 `analyze`  
    - B: `{ "keyword": "...", "max_posts"?: int, "mode"?: "ingest" | "analyze" | "hotlist" }`，預設 `ingest`；`hotlist` 僅輸出熱帖清單、不跑分析  
    - C: `{ "max_posts"?: int, "threshold"?: int, "mode"?: "ingest" | "analyze" }`，預設 `ingest`  
  - `POST /api/run`：legacy，固定 Pipeline A（`mode=analyze`），保留相容性  
  - `POST /api/run/batch`（Pipeline B 批次）：`mode` = `run`（預設）或 `preview`（僅決策不執行）；`pipeline_mode` = `full`（預設，含分析）或 `ingest`（只入庫）；`concurrency` ≤ 3
- Jobs API（JobManager on Supabase job_batches/job_items，進度頁的 SoT）  
  - `POST /api/jobs/`：建立 batch job（pipeline_type + mode + input_config），HTTP 非 2xx 會帶回錯誤 body；前端 fail-fast 若缺 jobId  
  - `GET /api/jobs/{job_id}`：job head + 最多 20 items  
  - `GET /api/jobs/{job_id}/items`：job_items（預設 limit=200，上限 1000；stage/status/result_post_id）；DB degraded 時加 `x-ops-degraded: 1` 並回空陣列  
  - `GET /api/jobs/{job_id}/summary`：聚合 counters + degraded 標記（同上 header）  
  - `GET /api/status/{job_id}`：legacy in-memory JobResult（仍保留，但 UI 優先用 /api/jobs/**）
- HTML Console（Jinja）  
  - `GET /run/a|b|c`：表單觸發 A/B/C（手動測試入口）  
  - `GET /status/{job_id}`：Job 狀態頁 + Threads 風格 UI  
- 其他 API  
  - `GET /api/status/{job_id}`：JobResult JSON（status/pipeline/mode/post_id/posts/logs）  
  - `GET /api/analysis-json/{post_id}`：AnalysisV4 + validity meta + phenomenon merge  
  - `GET /api/analysis/{post_id}`：legacy markdown full_report  
  - `GET /api/posts`：列表含分析的貼文（條件 `analysis_json IS NOT NULL`，未濾 `analysis_is_valid`）；`ORDER BY created_at DESC LIMIT 20`，無分頁  
  - `GET /api/comments/by-post/{post_id}`：留言列表（排序 `likes|time`），來源 SoT = `threads_comments`  
  - `GET /api/comments/search`：留言搜尋（ilike），來源 SoT = `threads_comments`  
  - `GET /api/debug/latest-post`：最新 threads_posts 快照  
  - `/proxy_image`：圖片代理（持久化 metadata 在 `threads_posts.images`；本地 `images/` 只是暫存）  
  - `/docs`（Swagger）：http://127.0.0.1:8000/docs

## Pipelines A/B/C（現行行為）
- Pipeline A：單貼 URL 抓取 → VisionGate 判斷 → Two-Stage Gemini (V1/V2) enrich → `update_vision_meta` 寫入 `vision_*` + `images` → Analyst 產生 `analysis_json`（AnalysisV4）與 meta（analysis_is_valid/version/build_id/missing_keys）。Archive（HTML/DOM）會在頁面載入成功後 best-effort 寫入。
- Pipeline B：關鍵字批次抓取；`mode=ingest`（預設）只抓文入庫，`mode=analyze` 跑 Analyst；`mode=hotlist` 只輸出熱帖清單。批次 `/api/run/batch` 另外用 `pipeline_mode=ingest|full` 控制是否跑分析；每個 target 成功/失敗都即時寫入 `job_items`（stage/status/result_post_id/error_log）供 UI 增量展示。
- Pipeline C：個人主頁樣本抓取；`mode=ingest`（預設）入庫，`mode=analyze` 跑 Analyst。
- 寫入 Supabase 的核心欄位：url, author, post_text/raw, like_count/reply_count/view_count, images, raw_comments, analysis_json, analysis_is_valid, analysis_invalid_reason, analysis_missing_keys, analysis_version, analysis_build_id, raw_json, full_report, archive_html/dom_json/archive_captured_at/archive_build_id；並同步留言到 `threads_comments`（SoT）。

## 前端路由（React, 5173）
- `/pipeline/a|b|c`（啟動 Pipeline；Pipeline A 為單頁 console + inline monitor，無跳轉）
- `/history`（殼層）
- `/archive`（列表 `/api/posts`，點入 `/narrative/{postId}`）
- `/narrative/{postId}`（敘事詳情，SSOT `/api/analysis-json/{postId}`）
- `/demo`（本地 mock）
- `/legacy/home`（舊版保留）

Legacy /api/run/* 仍存在但已標記 DEPRECATED，請改用 `/api/jobs/` + `/api/jobs/{id}/items|/summary`（Supabase JobManager SoT）。

## DB Schema 提示（threads_posts 必要欄位）
- 基本：id, url, author, post_text, post_text_raw, like_count, reply_count, view_count, images(jsonb), raw_comments(jsonb), ingest_source, created_at/captured_at
- 留言 SoT：`threads_comments`（id text pk, post_id bigint fk -> threads_posts, text, author_handle, like_count, reply_count, created_at, raw_json, inserted_at/updated_at）
- 留言分群 SoT：`threads_comment_clusters`（id text pk, post_id bigint fk, cluster_key int, label/summary/size/top_comment_ids/centroid_embedding, created_at/updated_at），comments 有 `cluster_id/cluster_key` 連結欄位。
- 分析：analysis_json(jsonb), analysis_is_valid(bool), analysis_invalid_reason(text), analysis_missing_keys(jsonb), analysis_version(text), analysis_build_id(text), raw_json(jsonb), full_report(text)
- 封存：archive_captured_at(timestamptz), archive_build_id(text), archive_dom_json(jsonb), archive_html(text)
- 其他：ai_tags, quant_summary, cluster_summary 等

### Comment Cluster SoT (CDX-060)
- Source-of-truth tables: `threads_comment_clusters` (id = `post_id::c<cluster_key>`, label/summary/size/keywords/top_comment_ids/centroid_embedding) + `threads_comments.cluster_id/cluster_key/cluster_label`.
- Quant Engine now writes cluster rows + assignments immediately after KMeans; `analysis_json` remains display cache.
- Cluster persistence is set-based: metadata via RPC `upsert_comment_clusters(post_id, clusters_json)`; assignments via RPC `set_comment_cluster_assignments(post_id, assignments_json)` when `DL_PERSIST_ASSIGNMENTS=1` (default 0 skips assignments but still upserts metadata).
- Quick probe:
  - `select post_id, count(*) from threads_comment_clusters group by post_id order by count desc limit 5;`
  - `select post_id, count(*) from threads_comments where cluster_id is not null group by post_id order by count desc limit 5;`
  - Join example: `select c.author_handle, c.like_count, c.text, k.cluster_key, k.label from threads_comments c join threads_comment_clusters k on c.cluster_id=k.id where c.post_id=<POST_ID> order by c.like_count desc limit 20;`

### Comment Identity (CDX-061)
- threads_comments.id stays the legacy hash (stable for existing links); native Threads comment id is stored in source_comment_id for dedupe/traceability. Fallback hash = `sha256(f"{post_id}:{author_handle}:{normalized_text}")`.
- Parser emits native ids/parent ids when present and logs coverage; store reuses existing rows when (post_id, source_comment_id) matches and keeps captured_at timestamps.
- Cluster assignments are update-only (no inserts) and log missing id coverage.
- Verification: rerun Pipeline A and check `select count(*) from threads_comments where post_id=<POST_ID> and source_comment_id is not null;` and ensure row count does not increase on rerun.

### Cluster Centroid (CDX-063)
- SBERT centroids are 384-d and stored in `threads_comment_clusters.centroid_embedding_384` (legacy `centroid_embedding` 1536-d unused unless provided).
- Assignments are skipped if cluster upsert fails; payloads always include post_id to avoid FK noise.

### Cluster Tactics (CDX-065)
- Analyst writes tactics/tactic_summary back to `threads_comment_clusters` keyed by (post_id, cluster_key); idempotent overwrite.
- Probe: `select post_id, cluster_key, label, tactics, tactic_summary, size, keywords from threads_comment_clusters where post_id=<POST_ID> order by size desc;`

## Troubleshooting
- `job not found`：/api/status job_id 錯或已清理，需重跑 pipeline。
- `job launched but no jobId`：前端已阻擋；若 API 真不回 job_id，/api/jobs/ 會以 HTTP error 返回並帶狀態碼/錯誤內容。
- `analysis_json not available`：/api/analysis-json 404，detail 會附 reason_code/hint；檢查 pipeline log 或重跑分析。
- `AttributeError ... model_dump`：升級到最新 commit（已改 safe_dump），重啟 backend。
- `/api/docs 404`：Swagger 在 `/docs`，請用 `http://127.0.0.1:8000/docs`。
- `rg/watch not found`：mac 安裝 `brew install ripgrep` 或改 `grep -R`。

## Git Hygiene
- 勿提交：`.env`, `auth_threads.json`, `.vscode/`, `node_modules/`, `images/`, `dlcs-ui/dist/`, `__pycache__/`
- 建議 `.gitignore` 片段：
  ```
  .env
  auth_threads.json
  .vscode/
  __pycache__/
  node_modules/
  images/
  dlcs-ui/dist/
  ```

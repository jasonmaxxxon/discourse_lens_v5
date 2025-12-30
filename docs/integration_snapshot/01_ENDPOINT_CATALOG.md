# Endpoint Catalog (FastAPI)

Source baseline: `webapp/main.py` (legacy) + `webapp/routers/jobs.py` (Supabase JobManager).

## JobManager (preferred for progress)
- `POST /api/jobs/` — create job (pipeline_type/mode/input_config); returns job_id or HTTP error body
- `GET /api/jobs/{job_id}` — job head + items (degraded header when serving cached/stale)
- `GET /api/jobs/{job_id}/items` — items ordered by updated_at desc; default limit 200, cap 1000; includes `result_post_id` for progressive links
- `GET /api/jobs/{job_id}/summary` — counters + degraded flag

## Legacy (deprecated, compatibility)
- `POST /api/run/{pipeline}` — legacy wrapper; prefer `/api/jobs/`
- `GET /api/status/{job_id}` — legacy status; prefer `/api/jobs/{id}` + `/items` + `/summary`

## Supabase RPC (clusters)
- `upsert_comment_clusters(post_id, clusters_jsonb)` — upsert cluster metadata
- `set_comment_cluster_assignments(post_id, assignments_jsonb)` — bulk cluster assignments (DL_PERSIST_ASSIGNMENTS=1)

## Legacy + other routes (`webapp/main.py`)

| Method | Path | Handler | Input | Output | DB Touchpoints | Source |
| --- | --- | --- | --- | --- | --- | --- |
| GET | `/` | root | query: none | HTML dashboard (`index.html` template) | reads Supabase hotlists via helpers | `webapp/main.py:806-820` |
| GET | `/run/a` | run_a_form | query: none | HTML form for Pipeline A | none | `webapp/main.py:822-830` |
| POST | `/run/a` | run_a_submit | form `{url}` | Redirect to `/status/{job_id}` | inserts JOB entry; pipeline execution via `run_pipeline` | `webapp/main.py:831-868` |
| POST | `/run/b` | run_b_submit | form `{keyword}` | Redirect to `/status/{job_id}` | JOB entry; pipeline execution | `webapp/main.py:869-904` |
| POST | `/run/c` | run_c_submit | form `{threshold}` | Redirect to `/status/{job_id}` | JOB entry; pipeline execution | `webapp/main.py:905-970` |
| POST | `/api/run/batch` | run_pipeline_b_backend_api | JSON `{keyword?, urls?, max_posts?<=20, exclude_existing?, reprocess_policy?, ingest_source?, mode: preview|run, pipeline_mode: full|ingest, concurrency?<=3}` | structured summary `{discovery_count,deduped_count,selected_count,skipped_exists,skipped_policy,success_count,fail_count,items[],logs,posts?}`; preview mode returns decisions only; run mode executes Pipeline A stages (ingest|full) in threadpool with controlled concurrency | orchestrates Pipeline A per URL, dedupes via canonical URL, optional reprocess policy, hard cap 20 | `webapp/main.py` |
| POST | `/api/run/{pipeline}` | api_run | JSON payload per variant (A: `url`; B: `keyword`; C: `threshold`) | `{job_id,status,pipeline,post_id?,posts?}` | starts pipeline via `pipelines/core.py` run; stores JOB in-memory | `webapp/main.py:971-1011` |
| GET | `/api/status/{job_id}` | api_status | path `job_id`; query `mode` optional | `JobResult` {status,pipeline,job_id,mode?,post_id?,posts?,logs?} | reads JOBS; may refresh post from Supabase; for Pipeline A can backfill images | `webapp/main.py:1012-1032` |
| GET | `/status/{job_id}` | status_page | path `job_id` | HTML status page | reads JOBS | `webapp/main.py:1033-1105` |
| GET | `/proxy_image` | proxy_image | query `url` | StreamingResponse image | no DB | `webapp/main.py:1106-1118` |
| GET | `/api/posts` | api_posts | query none | list of `PostListItem` with analysis meta, ai_tags, **phenomenon_id/status/case_id/name** | selects `threads_posts` columns including `analysis_json`, archive fields, phenomenon columns | `webapp/main.py:1119-1188` |
| GET | `/api/analysis-json/{post_id}` | api_analysis_json | path `post_id` | `{analysis_json, analysis_is_valid, analysis_version, analysis_build_id, analysis_invalid_reason, analysis_missing_keys, phenomenon{ id,status,case_id,canonical_name,source }}` | selects `threads_posts` row incl. phenomenon columns; merges with analysis_json | `webapp/main.py:1232-1265` |
| GET | `/api/library/phenomena` | list_library_phenomena | query: `status?`, `q?`, `limit?` | list of phenomena `{id, canonical_name, description, status, total_posts, last_seen_at}` | reads `narrative_phenomena`; aggregates stats from `threads_posts.phenomenon_id` | `webapp/main.py:1294-1330` |
| GET | `/api/library/phenomena/{id}` | get_library_phenomenon | path `id`, query `limit?` | `{meta, stats{total_posts,total_likes,last_seen_at}, recent_posts[]}` | reads `narrative_phenomena`, `threads_posts` (by phenomenon_id) | `webapp/main.py:1333-1385` |
| POST | `/api/library/phenomena/{id}/promote` | promote_phenomenon | path `id` | `{ok, id, status}` or 409 if not provisional | updates `narrative_phenomena.status` | `webapp/main.py:1388-1416` |
| GET | `/api/comments/by-post/{post_id}` | comments_by_post | path `post_id`, query `limit?`, `offset?`, `sort=likes|time` | `{post_id,total,items[]}` items include `id,text,author_handle,like_count,reply_count,created_at` | reads `threads_comments` | `webapp/main.py:...` |
| GET | `/api/comments/search` | comments_search | query `q?`, `author_handle?`, `post_id?`, `limit?` | `{items[]}` with comment snippets | reads `threads_comments` | `webapp/main.py:...` |
| GET | `/api/debug/latest-post` | debug_latest_post | none | latest row snapshot: `{id,post_text,created_at,captured_at,ai_tags,analysis_json,...}` | selects latest from `threads_posts` | `webapp/main.py:1214-1247` |
| GET | `/api/analysis/{post_id}` | api_analysis | path `post_id` | `{post_id, full_report_markdown}` | selects `threads_posts.full_report` | `webapp/main.py:1248-1268` |

Notes:
- DB table referenced: `threads_posts` via Supabase client in `webapp/main.py` and `database/store.py`.
- JOBS are in-memory; pipelines run in background thread via `run_pipeline` / `run_pipelines` from `pipelines/core.py`.

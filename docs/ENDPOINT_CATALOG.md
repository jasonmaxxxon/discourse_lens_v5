# Endpoint Catalog (initial)

This catalog is generated/baselined from `.codex/artifacts/endpoint_map.json`. Update after route changes.

## Jobs
- `GET /api/jobs/` — list jobs (Cache-Control: max-age=2; `x-ops-degraded:1` when serving stale)
- `GET /api/jobs/{job_id}` — job detail (head + items)
- `GET /api/jobs/{job_id}/items` — job items (default limit 200, cap 1000; `result_post_id`, stage/status; `x-ops-degraded:1` on degraded)
- `GET /api/jobs/{job_id}/summary` — job summary (counters, degraded flag)
- `POST /api/jobs/` — create job (fails with HTTP status + body; frontends must fail-fast if no jobId)

## Pipeline Run
- `POST /api/jobs/` — create + run via JobManager (progress via job_items/summary)
- Legacy (deprecated): `POST /api/run/{pipeline}` and `GET /api/status/{job_id}` — wrappers only, prefer `/api/jobs/*`

## Supabase RPC (cluster persistence)
- `upsert_comment_clusters(post_id, clusters_jsonb)` — set-based upsert of `threads_comment_clusters`
- `set_comment_cluster_assignments(post_id, assignments_jsonb)` — bulk assignments to `threads_comments` (used when `DL_PERSIST_ASSIGNMENTS=1`)

## Narrative / Analysis
- `GET /api/analysis/{post_id}` — analysis detail (backend)
- `GET /api/narrative/{post_id}` — narrative detail (frontend API expectation)

Regenerate via: `python .codex/tools/dump_routes.py --root . --out .codex/artifacts/endpoint_map.json` then update this file if needed.

# DB Schema Map

## Tables Referenced in Code

### threads_posts (Supabase)
- Used in: `database/store.py`, `webapp/main.py`, `analysis/analyst.py`.
- Columns inserted/updated:
  - `url`, `author`, `post_text`, `post_text_raw`
  - `like_count`, `view_count`, `reply_count`, `reply_count_ui`, `repost_count`, `share_count`
  - `images` (jsonb), `raw_comments` (jsonb), `ingest_source`, `is_first_thread`
  - `analysis_json` (jsonb), `analysis_is_valid` (bool), `analysis_invalid_reason` (text), `analysis_missing_keys` (jsonb)
  - `analysis_version` (text), `analysis_build_id` (text)
  - `raw_json` (jsonb), `full_report` (text)
  - `ai_tags` (jsonb), `cluster_summary` (jsonb), `quant_summary` (jsonb)
  - `archive_captured_at` (timestamptz), `archive_build_id` (text), `archive_dom_json` (jsonb), `archive_html` (text)
  - `phenomenon_id` (uuid, optional FK from migration)
  - Timestamps: `created_at`, `captured_at`, `updated_at`
- Selects (see `webapp/main.py`):
  - `/api/posts` selects id, post_text, created_at/captured_at, ai_tags, like_count/reply_count/view_count, analysis_* meta, archive_*.
  - `/api/analysis-json/{post_id}` selects id, analysis_json, analysis_is_valid, analysis_version, analysis_build_id, analysis_invalid_reason, analysis_missing_keys, full_report, updated_at.
  - `/api/debug/latest-post` selects latest row with analysis_json/full_report snapshot.
  - `/api/analysis/{post_id}` selects full_report.

### threads_posts_raw (run-level audit)
- Added by migration `supabase/migrations/20260122045035_20260122_pipeline_a_sot_fix.sql`.
- Columns: `run_id`, `post_id` (external Threads id), `post_url`, `crawled_at_utc`, `fetcher_version`, `run_dir`, `raw_html_*_path`, `raw_cards_path`, `created_at`.
- Writers: `webapp/services/ingest_sql.py` via `ingest_run(run_dir)`.
- Purpose: immutable run audit; does not drive clustering/analysis directly.

### narrative_phenomena (registry)
- Added by migration `database/migrations/2025-phenomenon-registry.sql`.
- Columns: `id` (uuid, no default), `canonical_name`, `description`, `embedding` vector(1536) legacy, `embedding_v768` vector(768, Google text-embedding-004 SoT), `status` (default PROVISIONAL), `minted_by_case_id` (uuid), `occurrence_count` (int NOT NULL DEFAULT 0), `created_at` (timestamptz), `updated_at` (timestamptz).
- Related tables:
  - `narrative_phenomenon_aliases` (id uuid, phenomenon_id FK, alias_text, language, unique(alias_text, language)).
  - `narrative_phenomenon_relations` (from_id FK, to_id FK, relation_type, created_at).
- RPC: `match_phenomena(query_embedding, match_threshold, match_count)` (1536 legacy) and `match_phenomena_v768(query_embedding, match_threshold, match_count)` for 768-d embeddings.
- RPC (CDX-083a): `increment_occurrence(phenomenon_id uuid)` increments `occurrence_count` and returns the updated integer (0 if row not found); SECURITY DEFINER with `search_path=public`; EXECUTE granted to `authenticated`/`anon`.
- Optional FK on `threads_posts.phenomenon_id`.

### threads_comments (raw evidence table, hybrid identity)
- Added by migration `database/migrations/2025-threads-comments.sql`; hardened by `2025-comment-identity-hardening.sql`.
- Columns: id (text PK, legacy hash), post_id (bigint FK -> threads_posts.id), text, author_handle, author_id, source_comment_id (native Threads id when available), parent_comment_id (legacy/unknown), parent_source_comment_id (native parent when available), like_count, reply_count, created_at (platform), captured_at (ingest time), raw_json (jsonb), cluster_label, tactic_tag, embedding vector(1536), inserted_at, updated_at.
- Writers: dual write in `database/store.py` on save_thread; backfill scripts.
- Readers: `/api/comments/by-post`, `/api/comments/search`, quant/cluster joins.
- Indices: post_id, author_handle, created_at, source_comment_id (partial), parent_source_comment_id (partial), unique (post_id, source_comment_id) partial to prevent duplicate native IDs.

### threads_comment_edges (reply graph)
- Added by migration `supabase/migrations/20260122045035_20260122_pipeline_a_sot_fix.sql`.
- Columns: `run_id`, `post_id` (bigint FK to threads_posts), `parent_comment_id`, `child_comment_id`, `edge_type`, `created_at`.
- Writers: `webapp/services/ingest_sql.py` via `ingest_run(run_dir)`.
- Purpose: rebuild reply graph; `parent_comment_id == post_id` represents L1.

### threads_comment_clusters (cluster registry)
- Added by migration `database/migrations/2025-threads-comment-clusters.sql`.
- Columns: id (text PK), post_id (bigint FK -> threads_posts), cluster_key (int), label, summary, size (int), top_comment_ids (jsonb), keywords (jsonb/text[]), tactics (text[]), tactic_summary (text), centroid_embedding_384 vector(384) (SBERT), centroid_embedding vector(1536) reserved, created_at, updated_at.
- Writers: cluster upsert helpers in `database/store.py`; quant_engine persists Layer 0.5; Analyst writes tactics/tactic_summary back after AnalysisV4.
- Readers: cross-post cluster analysis.
- Indices: unique(post_id, cluster_key), embedding ivfflat, trigram label.

### cluster_naming_staging (naming audit)
- Added by migration `supabase/migrations/20260105_cluster_naming_staging.sql`.
- Columns: post_id, cluster_key, run_id, quant_health, backend_name/params, model_provider/name, prompt_hash, label, summary, evidence_comment_ids (jsonb), raw_evidence (jsonb).
- Writers: `analysis/v7/naming/gemini_flash.py` (staging-only, no SoT writes).

## Migrations Inventory
- `database/migrations/20250101_cdx_083a_fix_increment_occurrence_rpc.sql`:
  - Ensures `narrative_phenomena.occurrence_count` exists, is non-null default 0, and backfilled.
  - Adds RPC `public.increment_occurrence(phenomenon_id uuid)` (SECURITY DEFINER, search_path=public) with grants to `authenticated`/`anon`.
- `database/migrations/2025-phenomenon-registry.sql`:
  - Enables `vector` and `uuid-ossp` extensions.
  - Creates registry tables above.
  - Adds `threads_posts.phenomenon_id`.
  - Defines `match_phenomena` RPC for future vector search.
- `database/migrations/2025-threads-comments.sql`:
  - Enables `vector` (safe if already present).
  - Creates `threads_comments` with raw comment fields and indexes.

## Notes
- No additional migration files present.
- Registry currently used by async enrichment stub for future match-or-mint; IDs are patched into `analysis_json.phenomenon.id` and optional `threads_posts.phenomenon_id` (write path not yet wired).

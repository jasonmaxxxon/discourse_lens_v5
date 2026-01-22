# analysis_json Contract (V6.1)

Derived from `analysis/schema.py`, `analysis/build_analysis_json.py`, `analysis/analyst.py`, frontend types `dlcs-ui/src/types/analysis.ts`.

## Top-Level Keys (AnalysisV6.1 payload)
- `post`: {`post_id`, `author`, `text`, `link`, `images[]`, `timestamp`, `metrics{likes,views,replies}`}
- `phenomenon`: {`id` (registry-owned, may be null/pending), `status` (pending/minted/matched/failed), `name` (optional legacy), `description` (interpretive), `ai_image`?}
- `emotional_pulse`: {`primary`?, `cynicism`, `hope`, `outrage`, `notes`?}
- `segments`: array of {`label`, `share` 0–1, `samples[]` with `comment_id`,`user`,`text`,`likes`, `linguistic_features`[]}
- `hard_metrics`: deterministic output from `QuantCalculator` (authoritative; LLM cannot override)  
  - `n_comments`, `n_clusters`, `cluster_size_share[]`, `cluster_like_share[]`, `per_capita_like[]`, `gini_like_share`, `entropy_like_share`, `dominance_ratio_top1`, `minority_dominance_index{top_k_clusters,like_share,size_share}`
- `per_cluster_metrics`: [{`cluster_id`,`size`,`size_share`,`like_sum`,`like_share`,`likes_per_comment`}]
- `battlefield_map`: [{`cluster_id`,`role`,`tactic`,`power_metrics{intensity,population,asymmetry_score}`, `evidence_comment_ids`[]}]
- `strategic_verdict`: {`verdict`,`rationale`,`evidence_comment_ids`[]}
- `structural_insight`: {`keystone_cluster_id`,`counterfactual_analysis`,`evidence_comment_ids`[]}
- `narrative_stack`: {`l1`,`l2`,`l3`} (strings; may be extracted from full_report)
- `danger`: {`bot_homogeneity_score`, `notes`} (optional)
- `axis_alignment` (optional): {`meta{library_version,is_extension_candidate,extension_reason}`, `axes[]` with `axis_name`,`score`,`reasoning`,`matched_anchor_id`,`is_affirmative`}
- `full_report`: markdown text (optional)
- Compatibility wrappers:
  - `summary`: {`one_line`, `narrative_type`}
  - `battlefield`: {`factions`: mirrors `segments`}

## Ephemeral Evidence IDs
- Before LLM call, evidence comments are aliased to short ids `c1`, `c2`, … and only these aliases appear in the LLM prompt/output.
- After the LLM returns, aliases are reverse-mapped to real `threads_comments.id`. Any unknown alias marks the payload invalid (analysis_is_valid=false).
- Evidence compliance: every battlefield_map entry must cite **≥2** `evidence_comment_ids`.

## Axis Alignment (Optional)
- `axis_alignment.meta.library_version` is authoritative from the on-disk axis library.
- `axis_alignment.axes[].is_affirmative` is computed in code as `score >= 0.6`.
- `axis_alignment.meta.is_extension_candidate` is computed via deterministic lexical novelty (high semantic score + low Jaccard).
- Lexical novelty uses CJK-safe tokenization (bigrams) and max-Jaccard across all positive examples; matched_anchor_id is ignored for lexical comparison.

## V7 Live Testing Input (Non-Production)
- A separate input schema is defined for read-only V7 testing harnesses.
- See `docs/tests/v7_live_input_schema.md` for the minimal JSON shape used by live runners.
- This schema is **not** part of `analysis_json` and does not affect the V6.1 contract.

## Turbo Fetcher Test Artifacts (Non-Production)
- `scraper/fetcher.py` (v15) emits offline artifacts under `artifacts/fetcher_test_turbo_v15_linux/run_<ts>/`.
- `merged_comments_parsed.json` and `comments_flat.json` are debug-only projections (not SoT). Do not treat them as ingestion schema.
- `merged_comments_parsed.json` includes a parsed tree with `comment_id`, `parent_id`, `author`, `text`, `metrics`, nested `replies`, plus time projection fields (`time_token`, `approx_created_at_utc`, `time_precision`).
- `comments_flat.json` contains flat rows with `comment_id`, `parent_id`, `author`, `text`, `metrics`, `fingerprint`, plus `comment_images[]`, `time_token`, `approx_created_at_utc`, `time_precision`.
- `post_payload.json` includes `post_images[]` URLs (deduped, non-profile/thumbnail).
- `threads_posts_raw.json` includes `run_id`, `crawled_at_utc`, `post_url`, `post_id`, `fetcher_version`, `run_dir`, plus paths to `raw_html_initial.html`, `raw_html_final.html`, and `raw_cards.json`.
- `threads_comments.json` includes canonical rows (`run_id`, `crawled_at_utc`, `post_id`, `post_url`, `comment_id`, `parent_comment_id`, `author_handle`, `text`, `time_token`, `approx_created_at_utc`, `like_count`, `reply_count_ui`, `repost_count_ui`, `share_count_ui`, `metrics_confidence`, `source`, `comment_images[]`).
- `threads_comment_edges.json` includes `run_id`, `post_id`, `parent_comment_id`, `child_comment_id`, `edge_type="reply"`.
- These artifacts are test-only and do not modify `analysis_json` or any API contract.

## Reply Model Contract (Summary)
- Pipeline A / clustering / embedding must use `threads_comments.json` as SoT.
- Reply graph reconstruction uses `threads_comment_edges.json`.
- Treat `parent_comment_id == post_id` as L1 (top-level) for the thread.
- Full contract: `docs/integration_snapshot/REPLY_MODEL_CONTRACT.md`.

## SQL Storage Summary (Pipeline A v15)
- `threads_posts_raw`: run-level audit (run_id/post_id/post_url/crawled_at_utc + raw_html/raw_cards paths).
- `threads_posts`: canonical post row (post_text + metrics + images) keyed by url.
- `threads_comments`: canonical comment rows (comment_id PK, post_id bigint FK, metrics + run_id/crawled_at_utc/source).
- `threads_comment_edges`: reply graph edges (parent_comment_id/child_comment_id/edge_type).

## Validation / Invalidation (V6.1)
- `analysis_version` must be `v6.1`.
- Shares in `hard_metrics` and `per_cluster_metrics` must be within [0,1] and totals may not exceed 1.01.
- Any battlefield_map / structural_insight / strategic_verdict evidence alias left as `c*` or with <2 entries (battlefield_map) causes invalidation.
- If `axis_alignment` is present but malformed, analysis is marked invalid with `analysis_invalid_reason` prefixed by `axis_alignment_invalid`.

## Cluster SoT (threads_comment_clusters)
- Upserted via `upsert_comment_clusters(post_id, clusters_jsonb)` (RPC or table fallback) with fields: `label`, `summary`, `size`, `keywords`, `top_comment_ids`, `centroid_embedding`, `centroid_embedding_384`, `tactics`, `tactic_summary`.
- Upsert strictly enforces centroid presence: if `size>=2` and `centroid_embedding_384` missing/empty, the writer raises before touching Supabase.
- Gate: clusters with `size>=2` must persist `centroid_embedding_384` (non-null). After RPC write, a read-after-write check runs; failures raise `centroid_persistence_failed` and abort Analyst/LLM (no fallback path).
- Hard contract: `tactics` must be JSON array of strings (`text[]`), never string/null. Writers validate and raise on violation; witness logs emit `tactics_type` and preview before RPC.
- V7 quant connector: `v7_payload_to_v6_structure` maps V7 PreAnalystPayload into the minimal V6 shape (clusters, assignments, hard_metrics, provenance) and applies deterministic labels (`Cluster <k>` or `Unidentified / Context-External`) plus BERTopic keywords; no schema changes are introduced.
- Optional LLM naming hook (off by default via `DL_ENABLE_CLUSTER_NAMING_LLM=0`): when enabled, it may enrich cluster label/summary using sampled evidence IDs; failures to resolve evidence must skip writes (no partial/guessed labels).
- Naming staging: `cluster_naming_staging` stores draft naming rows (post_id, cluster_key, run_id, quant_health, backend params, model info, label/summary, evidence_comment_ids, prompt_hash, raw_evidence). When enabled, `staging_only` always writes even if quant_health=RED; writeback to `threads_comment_clusters` is still gated. Env toggles: `DL_ENABLE_CLUSTER_NAMING_LLM`, `DL_NAMING_WRITEBACK_MODE`, `DL_NAMING_EVIDENCE_K` (default 5) capped by `DL_NAMING_EVIDENCE_K_MAX` (default 8).
- V7 quant audit tables (append-only, non-SoT): `v7_quant_runs` captures per-run stats/provenance (backend params, input_comment_ids_hash, health levels, coverage, centroid_missing_count, embedding_preprocess_version, canonical_embed_text_hash) and `v7_quant_clusters` stores cluster snapshots per run_id (size, like_sum, keywords, top_comment_ids, centroid hash). No writes to SoT tables.
- V7 behavior signals (phase1): cluster-level signals_json includes `signal_version`, `embedding_fingerprint` (embedding_model_id, embedding_config_hash, handoff_mode), `axes_used`, `axis_stats` (mean/std/p10/p90 per axis), `coverage`, `float_policy`, `rep_comment_ids`, `anti_rep_comment_ids`, and `signals_hash` = sha256(canonical JSON of axis_stats + rep/anti ids + embedding_fingerprint + axes_used + float_policy).
- Noise cluster invariant: cluster_key=-1 is persisted only when it exists in payload or real noise is present (noise_count>0). No placeholder rows are created by quant audit. Signals for -1 carry `meta.noise_count` and may include placeholder flags at the signals layer; downstream/S7 should gate out placeholder signals as needed.

## Meta Added During Persist
- `analysis_version` (string, e.g., "v6.1")
- `analysis_build_id` (uuid string)
- `analysis_is_valid` (bool), `analysis_invalid_reason`, `analysis_missing_keys`
- Optional: `match_ruleset_version`, `fingerprint_version`, `registry_version`, `phenomenon_status`, `phenomenon_case_id` (patched by async enrichment)
- `ai_tags` writeback now also carries provenance: `Sub_Variant`, `Phenomenon_Desc`, and `Sub_Variant_Source` / `Phenomenon_Desc_Source` (`llm_present|llm_missing|parser_missing|writeback_missing`).
- Cluster assignment invariants: every comment gets a `cluster_key` (noise/unassignable -> `-1`), so `threads_comments.cluster_key` is never null after Pipeline A/hydration.
- Cluster metadata fallbacks: all clusters persisted (0..k-1 plus noise -1) receive non-null `label/summary`; `-1` labeled `Unidentified / Context-External`, others default to `Cluster N` with auto summaries when LLM omits them.

## Frontend Expectations (dlcs-ui)
- `AnalysisJson` type expects: `post_id`, `meta`, `summary`, `tone`, `strategies`, `battlefield`, `metrics`, `layers`, `discovery`, `raw_markdown`, `raw_json`.
- Normalizer (`normalizeAnalysisJson.ts`) maps narrative decks using `metrics`, `battlefield.factions`, `layers.l1/2/3`, `tone` fields.
- Phenomenon identity surfaced via `insight_deck.phenomenon.issue_id` built from post_id/sector_id; registry IDs not yet displayed in UI.

## JSON Skeleton Example
```json
{
  "post": {
    "post_id": "123",
    "author": "user",
    "text": "...",
    "link": "...",
    "images": ["..."],
    "timestamp": "2025-01-01T00:00:00",
    "metrics": { "likes": 10, "views": 100, "replies": 3 }
  },
  "phenomenon": {
    "id": null,
    "status": "pending",
    "name": null,
    "description": "...",
    "ai_image": null
  },
  "emotional_pulse": { "primary": null, "cynicism": 0.2, "hope": 0.1, "outrage": 0.3, "notes": null },
  "segments": [
    {
      "label": "Cluster 0",
      "share": 0.6,
      "samples": [
        { "comment_id": "c1", "user": "anon", "text": "...", "likes": 12, "linguistic_features": [] }
      ],
      "linguistic_features": []
    }
  ],
  "narrative_stack": { "l1": "...", "l2": "...", "l3": "..." },
  "danger": { "bot_homogeneity_score": 0.8, "notes": "..." },
  "summary": { "one_line": "...", "narrative_type": "..." },
  "battlefield": { "factions": [ { "label": "Cluster 0", "share": 0.6, "samples": [] } ] },
  "axis_alignment": {
    "meta": { "library_version": "2.6", "is_extension_candidate": false, "extension_reason": null },
    "axes": [
      { "axis_name": "Axis A", "score": 0.88, "reasoning": "...", "matched_anchor_id": "a1", "is_affirmative": true }
    ]
  },
  "full_report": "...",
  "analysis_version": "v6.1",
  "analysis_build_id": "uuid",
  "analysis_is_valid": true
}
```

## Boundaries
- `phenomenon.id` is set only by registry/enrichment; Step3/LLM outputs are ignored for identity.
- Validation (`validate_analysis_json`) requires `post.id/text/timestamp` and either `phenomenon.id` or `phenomenon.name`; pending status bypasses name requirement.
- Registry counter: `narrative_phenomena.occurrence_count` (default 0) tracks match/mint usage via RPC `increment_occurrence(phenomenon_id uuid)` and is not part of `analysis_json`; callers should use the RPC to mutate counts.
- Ops pipeline completion gate: Pipeline A items only complete when `threads_posts.analysis_json` or `threads_posts.full_report` is non-null for the returned `post_id`; missing analysis causes job failure at analyst/store stage (ingest may still succeed).
- Assignment persistence invariants (Pipeline A + hydration):
  - `DL_ASSIGNMENT_WRITE_MODE`: `fill_nulls` (default) only touches `threads_comments.cluster_key` when null; `overwrite` replaces existing keys (STRICT requires `DL_FORCE_REASSIGN=1`).
  - Coverage gate: `DL_ASSIGNMENT_COVERAGE_MIN` (default 0.95) compares `updated_rows` vs `assignments_total` (plus already-filled rows when mode=fill_nulls). STRICT raises on failure; non-strict logs and marks analysis invalid.
  - Hydration script logs `assignment_source` (`full_assignments|samples|top_comment_ids|unknown`) and `partial_hydration`; coverage enforcement is skipped for partial sources unless `--allow-partial`.
  - Assignment payload always includes every comment_id with a cluster_key (unclustered as `-1`), ensuring non-null cluster keys downstream.

## API Phenomenon Envelope (analysis-json endpoint)
- `/api/analysis-json/{post_id}` now returns a sibling `phenomenon` object:
  - `id`, `status`, `case_id`, `canonical_name`, `source` ("db_columns" | "analysis_json" | "default")
  - Merged with DB-first precedence; `analysis_json` remains unchanged for backward compatibility.

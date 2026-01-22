```mermaid
flowchart TD
  subgraph Ops Job System (Pipeline A)
    UI[Logistics Dashboard\n/ops/jobs] -->|POST /api/jobs| JM[JobManager\nwebapp/services/job_manager.py]
    JM -->|claim + stage: fetch/vision/analyst/store| RAJ[run_pipeline_a_job\nwebapp/services/pipeline_runner.py]
    RAJ -->|ingest| C[pipelines/core.py\nrun_pipeline]
    RAJ -->|vision (soft-fail)| V[VisionGate + TwoStageVisionWorker\nanalysis/vision_*]
    RAJ -->|quant calc| QC[QuantCalculator\nanalysis/quant_calculator.py]
    QC -->|hard_metrics+evidence set| E[Analyst\nanalysis/analyst.py]
    RAJ -->|analyst (required)| E
    RAJ -->|complete_job_item(p_result_post_id)| JM
    JM -->|job_items.stage + status| UI
  end
  subgraph Deduplication Gate
    D1[post_id + canonical_text_hash + backend_params_hash] --> V7Audit[v7_quant_runs / v7_quant_clusters (append-only)]
  end
  V7Audit -->|embedding handoff (in-memory)| SIG[V7 Behavior Signals\naxis projection + signals_hash]
  SIG -->|axis vectors bootstrap (NULL->fill)| AX[v8_signal_axes registry]
  AXLIB[axis_library.v2.6.json] --> AM[AxisManager\naxis few-shot + validation]
  AM --> E

  subgraph Content DB (Supabase)
    C -->|threads_posts + threads_comments ingest| F[threads_posts / threads_comments]
    V -->|update_vision_meta| F
    E -->|analysis_json/full_report via save_analysis_result (axis_alignment included)| F
  end

  subgraph Pipeline B (Discovery -> A-engine)
    B1[Discovery\nkeyword/account] --> B2[Dedup + reprocess policy]
    B2 -->|invoke Pipeline A per URL (ingest/full, threadpool)| C
  end

  subgraph Overnight Batch Runner (Safe Mode)
    BR[tools/batch_runner.py\nstateful queue] --> BRDISC[discover_thread_urls\nkeyword]
    BRDISC --> BRDED[resume state + dedupe]
    BRDED -->|invoke run_pipeline per URL with jitter/retries| C
  end

  F -->|GET /api/posts| G[Archive UI\nArchivePage/PostSelector]
  F -->|GET /api/analysis-json/{post}| H[NarrativeDetailPage\nuseNarrativeAnalysis]
  F -->|GET /api/analysis/{post}| I[RawReportCard]

  subgraph Async Enrichment
    J[Phenomenon Fingerprint\nanalysis/phenomenon_fingerprint.py]
    K[Phenomenon Enricher (ThreadPool)\nanalysis/phenomenon_enricher.py]
    J -->|fingerprint/case_id| K
    K -->|patch analysis_json.phenomenon.id/status via save_analysis_result| F
  end
  E -. submits bundle .-> J

  JM -->|GET /api/jobs, /items, /summary| UI
```

Notes (Pipeline A Ops bridge):
- Stages surfaced to `job_items.stage`: init → processing/fetch → vision → analyst → store → completed/failed.
- Completion gate: item completes only when runner returns a post_id and `threads_posts` row has `analysis_json` or `full_report`; otherwise item fails (analyst/store stage).
- Vision is soft-fail (logged, continues); Analyst is required (failure bubbles to job_items failed).
- Analysis payloads are persisted through the single writer `database.store.save_analysis_result` (Schema V6.1 validation gate + invalid reason capture).
- QuantCalculator runs after clustering and before LLM, producing hard_metrics/per_cluster_metrics and sampled evidence; evidence IDs are aliased in-memory (c1, c2…) and reverse-mapped after LLM.
- Axis alignment novelty detection uses top-liked comment text (up to 3, capped length) for lexical Jaccard; normalized axis block is written to both raw_json and analysis_json.
- V7 live testing harness runs are read-only (no DB writes) and emit artifacts only under `artifacts/v7_*_runs/` for audit.
- SoT fetcher (`scraper/fetcher.py`, v15) is read-only for tests and emits `merged_comments_parsed.json`, `comments_flat.json`, `post_payload.json`, plus SQL-aligned `threads_posts_raw.json`, `threads_comments.json`, and `threads_comment_edges.json` under `artifacts/fetcher_test_turbo_v15_linux/run_<ts>/`; v15 uses fast main snapshot + targeted drill with UI token cleanup and fingerprint dedupe (comments_flat includes comment images + approximate time tokens, post_payload includes post_images).
- Reply model contract for Pipeline A ingestion is defined in `docs/integration_snapshot/REPLY_MODEL_CONTRACT.md`.
- SQL ingest writes:
  - `threads_posts_raw` (run audit),
  - `threads_posts` (canonical post),
  - `threads_comments` (canonical comments),
  - `threads_comment_edges` (reply graph).
- Verification gate: `scripts/verify_v61.py` asserts quant determinism, alias resolution, and evidence compliance; runtime validation rejects unresolved aliases or insufficient evidence (no silent drops).
- Cluster persistence gate: `analysis/quant_engine.py` upserts clusters via `database.store.upsert_comment_clusters` (single boundary, no fallback). Writer validates centroid lists for size>=2, logs witness lengths/types, calls RPC, then `verify_cluster_centroids` read-after-write; any missing centroid triggers exception and stops pipeline before LLM.
- Assignment persistence gate: `database.store.apply_comment_cluster_assignments` honors `DL_ASSIGNMENT_WRITE_MODE` (fill_nulls default) and `DL_ASSIGNMENT_COVERAGE_MIN`; STRICT + overwrite requires `DL_FORCE_REASSIGN=1`. Coverage shortfall logs/raises and marks analysis invalid.
- Hydration backfill: `scripts/hydrate_post_assignments.py` rehydrates legacy posts; logs `assignment_source` and `partial_hydration`, and only enforces coverage for full sources or when `--allow-partial` is passed.
- Non-null cluster keys: quant assignments now include every comment (unclustered/noise -> cluster_key=-1) so `threads_comments.cluster_key` is always set after pipeline/hydration. Cluster metadata writeback ensures every cluster row (including -1 noise) has non-null label/summary defaults.
- V7 quant (read-only) path: `analysis/v7/quant/facade.run_pre_analyst` can run legacy or BERTopic adapters without DB writes; `v7_payload_to_v6_structure` bridges V7 payloads into the minimal V6 downstream shape (clusters/assignments/hard_metrics + provenance) so Analyst/LLM can be exercised in parallel without schema changes.
- Quant inspector: `scripts/test_v7_parallel.py` fetches real posts and renders terminal previews (cluster contents, guardrail before/after); snapshots saved when guardrails trigger. Optional LLM naming hook (`DL_ENABLE_CLUSTER_NAMING_LLM`) remains disabled by default.
- Naming pipeline (staging-aware): `analysis/v7/naming/gemini_flash.py` computes quant_health and routes Gemini 2.0 Flash naming results — when enabled, `staging_only` always writes to `cluster_naming_staging` (including noise cluster rows) regardless of quant_health; writeback is still gated. Strict evidence validation prevents hallucinated IDs; staging rows include evidence IDs + raw_evidence snapshots for audit.
- Quant audit (append-only): V7 quant runs write to `v7_quant_runs` and `v7_quant_clusters` (hash of input IDs, seed, backend params, health, centroid hash, embedding_preprocess_version, canonical_embed_text_hash) without touching SoT cluster tables; allows stability checks across repeated runs.
- Determinism: BERTopic uses explicit UMAP with seeded `random_state` (DL_V7_SEED) captured in backend_params; dedup gate keys on canonical_text_hash + backend_params_hash to ensure same content/params reuse the same run identity.

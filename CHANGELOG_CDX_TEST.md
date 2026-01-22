# CDX Test Changelog

## Unreleased
- Consolidated fetcher tests into v15 (`scraper/fetcher.py`) with targeted drill + UI token cleanup.
- Archived legacy fetcher/parser variants under `scraper/_archive/*` (deprecated, reference-only).
- Added v15 SoT ingest path (`webapp/services/ingest_sql.py`) and one-command runner (`scripts/run_fetcher_and_ingest.py`).
- Added offline ingest gate (`scripts/gates/gate_ingest_offline_v15.py`) and minimal golden fixtures in `tests/fixtures/v15_golden`.
- Consolidated entrypoints; old runner scripts now emit deprecation errors.

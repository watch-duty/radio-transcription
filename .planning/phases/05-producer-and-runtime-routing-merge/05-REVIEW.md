---
phase: 05-producer-and-runtime-routing-merge
status: clean
depth: standard
files_reviewed: 14
findings:
  critical: 0
  warning: 0
  info: 0
  total: 0
reviewed_at: 2026-06-15T18:44:18Z
---

# Phase 05 Code Review

Reviewed backend source and test changes from Phase 5:

- `backend/pipeline/ingestion/collector_runtime.py`
- `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py`
- `backend/pipeline/ingestion/collectors/fire_notifications/collector.py`
- `backend/pipeline/ingestion/collectors/icecast/icecast_collector.py`
- `backend/pipeline/ingestion/collectors/openmhz/collector.py`
- `backend/pipeline/ingestion/collectors/payloads.py`
- `backend/pipeline/ingestion/collectors/tests/test_bcfy_calls_collector.py`
- `backend/pipeline/ingestion/collectors/tests/test_collector_failure_semantics_regression.py`
- `backend/pipeline/ingestion/collectors/tests/test_collector_semantics_helpers.py`
- `backend/pipeline/ingestion/collectors/tests/test_fire_notifications_collector.py`
- `backend/pipeline/ingestion/collectors/tests/test_icecast_collector.py`
- `backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py`
- `backend/pipeline/ingestion/tests/test_collector_runtime.py`
- `backend/pipeline/storage/tests/test_feed_store.py`

## Result

No open findings.

## Review Notes

- Runtime `_PipelineFailure` routing now classifies status plus evidence through `failure_policy.classify_failure_policy(...)` before choosing the budgeted or non-budgeted store path.
- Pub/Sub post-bookmark publish failures are the only Phase 5 pipeline route that consumes the feed failure budget, and quarantine telemetry remains threshold-driven by `report_feed_failure(...)`.
- GCS upload and bookmark write failures remain non-budgeted pipeline releases.
- Producer status splits preserve the Phase 4 policy table: runtime config and malformed source payloads are budgeted, credential access is non-budgeted, and ambiguous collector errors remain telemetry gaps.
- The phase did not touch frontend, OpenAPI, generated metadata, database migrations, or schema files.

## Verification

```bash
safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_collector_runtime.py backend/pipeline/ingestion/collectors/tests/test_bcfy_calls_collector.py backend/pipeline/ingestion/collectors/tests/test_fire_notifications_collector.py backend/pipeline/ingestion/collectors/tests/test_icecast_collector.py backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py backend/pipeline/ingestion/collectors/tests/test_aiohttp_requests.py backend/pipeline/ingestion/collectors/tests/test_failure_classification.py backend/pipeline/ingestion/tests/test_failure_policy.py backend/pipeline/storage/tests/test_feed_store.py::TestNonBudgetedFailureSql backend/pipeline/storage/tests/test_feed_store.py::TestReleaseNonBudgetedFailure -q -n 0
```

Result: `305 passed, 47 subtests passed in 8.57s`.

```bash
safe-run -- uv run ruff check backend/pipeline/ingestion/collector_runtime.py backend/pipeline/ingestion/tests/test_collector_runtime.py backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py backend/pipeline/ingestion/collectors/tests/test_bcfy_calls_collector.py backend/pipeline/ingestion/collectors/fire_notifications/collector.py backend/pipeline/ingestion/collectors/tests/test_fire_notifications_collector.py backend/pipeline/ingestion/collectors/icecast/icecast_collector.py backend/pipeline/ingestion/collectors/tests/test_icecast_collector.py backend/pipeline/ingestion/collectors/openmhz/collector.py backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py backend/pipeline/ingestion/collectors/payloads.py backend/pipeline/ingestion/collectors/tests/test_aiohttp_requests.py backend/pipeline/storage/tests/test_feed_store.py
```

Result: `All checks passed!`.


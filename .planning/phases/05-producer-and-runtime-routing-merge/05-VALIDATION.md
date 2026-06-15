---
phase: 05
slug: producer-and-runtime-routing-merge
status: draft
nyquist_compliant: true
wave_0_complete: true
created: 2026-06-15
---

# Phase 05 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest |
| **Config file** | `pyproject.toml` |
| **Quick run command** | `safe-run -- uv run python -m pytest <changed-test-file> -q -n 0` |
| **Full suite command** | `safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_collector_runtime.py backend/pipeline/ingestion/collectors/tests/test_bcfy_calls_collector.py backend/pipeline/ingestion/collectors/tests/test_fire_notifications_collector.py backend/pipeline/ingestion/collectors/tests/test_icecast_collector.py backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py backend/pipeline/ingestion/collectors/tests/test_aiohttp_requests.py backend/pipeline/ingestion/collectors/tests/test_failure_classification.py backend/pipeline/ingestion/tests/test_failure_policy.py backend/pipeline/storage/tests/test_feed_store.py::TestNonBudgetedFailureSql backend/pipeline/storage/tests/test_feed_store.py::TestReleaseNonBudgetedFailure -q -n 0` |
| **Estimated runtime** | ~5-30 seconds for focused local slices |

## Sampling Rate

- **After every task commit:** Run the task's targeted pytest command.
- **After every plan wave:** Run the plan-level verification command from that plan.
- **Before `$gsd-verify-work`:** Run the combined focused backend command in `05-03-PLAN.md`.
- **Max feedback latency:** 30 seconds for focused local checks.

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 05-01-01 | 01 | 1 | TEST-15 | T-05-01-01 | Split producer mappings are test-defined before implementation | unit | `safe-run -- uv run python -m pytest backend/pipeline/ingestion/collectors/tests/test_bcfy_calls_collector.py backend/pipeline/ingestion/collectors/tests/test_fire_notifications_collector.py backend/pipeline/ingestion/collectors/tests/test_icecast_collector.py backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py backend/pipeline/ingestion/collectors/tests/test_aiohttp_requests.py -q -n 0` | ✅ | ⬜ pending |
| 05-01-02 | 01 | 1 | TEST-15 | T-05-01-01 | Producers emit precise status values with matching evidence | unit | `safe-run -- uv run python -m pytest backend/pipeline/ingestion/collectors/tests/test_bcfy_calls_collector.py backend/pipeline/ingestion/collectors/tests/test_fire_notifications_collector.py backend/pipeline/ingestion/collectors/tests/test_icecast_collector.py backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py backend/pipeline/ingestion/collectors/tests/test_aiohttp_requests.py -q -n 0` | ✅ | ⬜ pending |
| 05-02-01 | 02 | 2 | TEST-13 | T-05-02-01 | Pub/Sub post-bookmark failures are budgeted through threshold path | unit | `safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_collector_runtime.py -q -n 0` | ✅ | ⬜ pending |
| 05-02-02 | 02 | 2 | RUN-11, RUN-12, RUN-13, RUN-15, RUN-16 | T-05-02-01 | Runtime executes policy decision without bypassing policy table | unit | `safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_collector_runtime.py -q -n 0` | ✅ | ⬜ pending |
| 05-03-01 | 03 | 3 | TEST-14 | T-05-03-01 | Non-budgeted paths reset/release and never emit quarantine telemetry | unit | `safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_collector_runtime.py backend/pipeline/storage/tests/test_feed_store.py::TestNonBudgetedFailureSql backend/pipeline/storage/tests/test_feed_store.py::TestReleaseNonBudgetedFailure -q -n 0` | ✅ | ⬜ pending |
| 05-03-02 | 03 | 3 | RUN-14, RUN-15, TEST-13, TEST-14, TEST-15 | T-05-03-02 | Final backend slice proves producer/runtime merge and telemetry boundaries | unit | `safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_collector_runtime.py backend/pipeline/ingestion/collectors/tests/test_bcfy_calls_collector.py backend/pipeline/ingestion/collectors/tests/test_fire_notifications_collector.py backend/pipeline/ingestion/collectors/tests/test_icecast_collector.py backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py backend/pipeline/ingestion/collectors/tests/test_aiohttp_requests.py backend/pipeline/ingestion/collectors/tests/test_failure_classification.py backend/pipeline/ingestion/tests/test_failure_policy.py backend/pipeline/storage/tests/test_feed_store.py::TestNonBudgetedFailureSql backend/pipeline/storage/tests/test_feed_store.py::TestReleaseNonBudgetedFailure -q -n 0` | ✅ | ⬜ pending |

## Wave 0 Requirements

Existing infrastructure covers all phase requirements.

## Manual-Only Verifications

All Phase 5 behaviors have automated verification.

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 30s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** pending execution

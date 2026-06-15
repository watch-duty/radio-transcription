---
phase: 1
slug: policy-and-storage-foundation
status: draft
nyquist_compliant: true
wave_0_complete: false
created: 2026-06-15
---

# Phase 1 - Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 9.0.3 plus pytest-asyncio 1.3.0 |
| **Config file** | `pyproject.toml` |
| **Quick run command** | `safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_failure_policy.py backend/pipeline/storage/tests/test_feed_store.py -q -n 0` |
| **Full suite command** | Targeted Phase 1 command above plus `git diff --check`; do not run broad local suites without approval |
| **Estimated runtime** | less than 60 seconds for targeted unit/storage tests |

---

## Sampling Rate

- **After every task commit:** Run the narrowest targeted pytest command listed below.
- **After every plan wave:** Run `safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_failure_policy.py backend/pipeline/storage/tests/test_feed_store.py -q -n 0`.
- **Before `$gsd-verify-work`:** Targeted Phase 1 unit/storage tests and `git diff --check` must pass.
- **Max feedback latency:** 60 seconds for targeted checks.

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 01-01-01 | 01-01 | 1 | POL-01 | T-01-01 | Policy routing uses typed evidence and enum values, not raw string parsing | unit | `safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_failure_policy.py -q -n 0` | No; create in Wave 1 | pending |
| 01-01-02 | 01-01 | 1 | POL-02 | T-01-01 | Evidence facts are separated from decision verdict/action fields | unit | `safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_failure_policy.py -q -n 0` | No; create in Wave 1 | pending |
| 01-01-03 | 01-01 | 1 | POL-03 | T-01-01 | Pipeline-owned evidence records the pipeline stage explicitly | unit | `safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_failure_policy.py -q -n 0` | No; create in Wave 1 | pending |
| 01-02-01 | 01-02 | 2 | STORE-01 | T-01-02 | Non-budgeted release clears the lease and keeps the row schedulable as `failing` | unit | `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestNonBudgetedFailureSql backend/pipeline/storage/tests/test_feed_store.py::TestReleaseNonBudgetedFailure -q -n 0` | Yes; update as needed | pending |
| 01-02-02 | 01-02 | 2 | STORE-02 | T-01-02 | Non-budgeted release resets old budget debt with `failure_count = 0` | unit | `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestNonBudgetedFailureSql -q -n 0` | Yes; update as needed | pending |
| 01-02-03 | 01-02 | 2 | STORE-03 | T-01-02 | Non-budgeted release writes both `retry_after` and `status_reason` | unit | `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestNonBudgetedFailureSql backend/pipeline/storage/tests/test_feed_store.py::TestReleaseNonBudgetedFailure -q -n 0` | Yes; update as needed | pending |
| 01-02-04 | 01-02 | 2 | STORE-04 | T-01-02 | Non-budgeted release SQL has no `quarantine_reason` assignment | unit | `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestNonBudgetedFailureSql -q -n 0` | Yes; update as needed | pending |
| 01-03-01 | 01-03 | 3 | STORE-05 | T-01-03 | Only `report_feed_failure(...)` increments the feed budget | unit | `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestNonBudgetedFailureSql backend/pipeline/storage/tests/test_feed_store.py::TestReportFailureSqlStatusReason -q -n 0` | Yes; update as needed | pending |
| 01-03-02 | 01-03 | 3 | STORE-06 | T-01-03 | Progress and `SourceObservation` continue clearing stale failure state | unit | `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestStatusReasonClearSql backend/pipeline/storage/tests/test_feed_store.py::TestRecordSourceObservation -q -n 0` | Yes; update as needed | pending |
| 01-03-03 | 01-03 | 3 | STAT-01 | T-01-03 | `FeedStatusReason` parses and emits `pipeline_publish_after_bookmark_failed` | unit | `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestFeedStatusReason -q -n 0` | Yes; update as needed | pending |

---

## Wave 0 Requirements

- [ ] `backend/pipeline/ingestion/failure_policy.py` - create as the pure policy owner.
- [ ] `backend/pipeline/ingestion/tests/test_failure_policy.py` - create focused policy contract tests.
- [ ] Existing storage tests - update only the narrow classes needed for non-budgeted release, budget increment isolation, status reason parsing, and recovery semantics.

---

## Manual-Only Verifications

All Phase 1 behaviors have automated verification through focused unit/storage tests. Broad E2E, API, Docker, and integration suites remain manual/CI-only unless the user explicitly approves local resource-heavy runs.

---

## Threat Model

| Threat Ref | Area | Risk | Required Mitigation | Verification |
|------------|------|------|---------------------|--------------|
| T-01-01 | Policy classification | Raw `reason` text or status prefix parsing could route a failure to quarantine incorrectly | `failure_policy.py` exposes pure enum/dataclass decisions; tests assert classification uses `status_reason` plus structured evidence | `test_failure_policy.py` |
| T-01-02 | Storage mutation | Non-budgeted failure path could leave stale budget debt or quarantine forensic fields | SQL sets `failure_count = 0`, releases `worker_id`, writes `retry_after`/`status_reason`, and never assigns `quarantine_reason` | `TestNonBudgetedFailureSql` |
| T-01-03 | Recovery semantics | New storage path could regress successful progress or `SourceObservation` stale-state clearing | Existing progress and observation SQL/tests remain green and include failure count/status reason clearing assertions | `TestStatusReasonClearSql`, `TestRecordSourceObservation` |

---

## Validation Sign-Off

- [x] All tasks have automated verify commands or Wave 0 dependencies.
- [x] Sampling continuity: no 3 consecutive tasks without automated verify.
- [x] Wave 0 covers all missing test/policy files.
- [x] No watch-mode flags.
- [x] Feedback latency target is less than 60 seconds for targeted checks.
- [x] `nyquist_compliant: true` set in frontmatter.

**Approval:** pending execution

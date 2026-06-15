---
phase: 03-verification-and-compatibility
plan: 01
subsystem: testing
tags: [quarantine-policy, storage, ingestion-runtime, telemetry]

requires:
  - phase: 01-policy-and-storage-foundation
    provides: Non-budgeted storage primitive and status reason semantics.
  - phase: 02-runtime-routing-and-telemetry
    provides: Runtime routing and policy telemetry implementation.
provides:
  - Focused storage proof that non-budgeted release cannot consume quarantine budget.
  - Focused runtime proof for post-bookmark publish gaps, non-actionable failures, telemetry gaps, and feed-config quarantine.
  - Requirement evidence for TEST-01 through TEST-08.
affects: [verification, quarantine-policy, ingestion-runtime]

tech-stack:
  added: []
  patterns:
    - Store-call assertions prove routing before telemetry assertions.
    - Non-budgeted SQL invariants prove quarantine budget isolation.

key-files:
  created:
    - .planning/phases/03-verification-and-compatibility/03-01-SUMMARY.md
  modified:
    - backend/pipeline/storage/tests/test_feed_store.py
    - backend/pipeline/ingestion/tests/test_collector_runtime.py

key-decisions:
  - "No production changes were required; existing implementation already satisfied the targeted behavior."
  - "Task execution added focused test hardening rather than incident-label-specific test duplication."

patterns-established:
  - "Storage wrapper tests assert the exact SQL constant used before parameter ordering."
  - "Post-bookmark publish-gap tests assert both policy and data-gap events plus no quarantine telemetry."

requirements-completed: [TEST-01, TEST-02, TEST-03, TEST-04, TEST-05, TEST-06, TEST-07, TEST-08]

duration: 3min
completed: 2026-06-15
---

# Phase 03 Plan 01: Focused Storage and Runtime Tests Summary

**Focused quarantine-policy tests prove non-budgeted failures cannot spend feed quarantine budget while feed-owned configuration failures still can.**

## Performance

- **Duration:** 3 min
- **Started:** 2026-06-15T04:43:02Z
- **Completed:** 2026-06-15T04:46:01Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments

- Hardened storage tests so budgeted and non-budgeted store wrappers assert their dedicated SQL constants before parameter ordering.
- Hardened the post-bookmark publish-gap runtime test to prove no quarantine telemetry is emitted.
- Added explicit policy-decision telemetry assertions for hold-for-replay publish gaps.

## Task Commits

Each task was committed atomically:

1. **Task 1: Prove non-budgeted storage cannot consume quarantine budget** - `39afddeb` (test)
2. **Task 2: Prove runtime routing and telemetry scenarios** - `9ef6e9e7` (test)

**Plan metadata:** committed separately with SUMMARY, STATE, ROADMAP, and REQUIREMENTS updates.

## Files Created/Modified

- `backend/pipeline/storage/tests/test_feed_store.py` - Adds SQL-constant assertions for budgeted and non-budgeted store wrapper calls.
- `backend/pipeline/ingestion/tests/test_collector_runtime.py` - Adds no-quarantine telemetry and policy-decision payload assertions for post-bookmark publish gaps.
- `.planning/phases/03-verification-and-compatibility/03-01-SUMMARY.md` - Captures plan evidence and verification results.

## Requirement Evidence

| Requirement | Proof |
|-------------|-------|
| TEST-01 | `TestNonBudgetedFailureSql.test_non_budgeted_failure_sql_releases_without_quarantine_budget` asserts `status='failing'`, `failure_count=0`, `retry_after=$4`, `status_reason=$5`, and `worker_id=NULL`; `TestReleaseNonBudgetedFailure` asserts return status and SQL parameters. |
| TEST-02 | `TestNonBudgetedFailureSql` asserts non-budgeted SQL omits `quarantine_reason` and `failure_count + 1`, while incrementing remains isolated to `REPORT_FAILURE_SQL`. |
| TEST-03 | `TestProcessFeedRetry.test_non_retryable_pubsub_failure_records_publish_gap_without_feed_budget` asserts `report_feed_failure.assert_not_awaited()` and `release_non_budgeted_failure.assert_awaited_once()`. |
| TEST-04 | The same publish-gap test asserts `feed_failure_policy_decision` and `post_bookmark_publish_failure` with `hold_for_replay`, `suppress_feed_quarantine_record_publish_gap`, `replay_missing=True`, and `data_gap_known=True`. |
| TEST-05 | `test_non_actionable_collector_failures_use_non_budgeted_release` covers source-offline, shared-auth, rate-limit, capture-timeout, and source-class cases through the non-budgeted path. |
| TEST-06 | `test_untyped_runtime_exception_routes_to_telemetry_gap` asserts UNKNOWN evidence, telemetry-gap intent, and non-budgeted release. |
| TEST-07 | `test_feed_config_quarantine_emits_telemetry` asserts feed configuration failures use `report_feed_failure(...)` and not non-budgeted release. |
| TEST-08 | Representative non-budgeted tests assert `quarantine_telemetry.emit_quarantine_event.assert_not_awaited()`, including post-bookmark publish gaps and shared/source-class failures. |

`STAT-02` is intentionally not marked complete in this plan; it is assigned to plan 03-02 compatibility surfaces.

## Incident Scenario Mapping

| Incident Category | Covered Policy Scenario |
|-------------------|-------------------------|
| Pub/Sub schema validation or publish failure after bookmark | Post-bookmark publish-gap runtime test with hold-for-replay telemetry and non-budgeted release. |
| Paused ordering key that becomes retryable | Existing transient Pub/Sub retry tests verify retry before failure routing. |
| Broadcastify Calls or Fire Notifications shared auth failure | Shared-auth case in the non-actionable collector failure table. |
| Source offline or provider unavailable | Source-offline and capture-timeout cases in the non-actionable collector failure table. |
| Rate-limited source class | Rate-limit case in the non-actionable collector failure table. |
| Source-class or provider-wide incident | Source-class case in the non-actionable collector failure table. |
| Untyped runtime bug or telemetry gap | UNKNOWN telemetry-gap runtime test. |
| Feed-owned configuration error | Feed-config quarantine control test. |

## Decisions Made

- No production code was changed because the targeted storage and runtime behavior already existed.
- Tests were hardened at shared policy scenario boundaries instead of adding one test per historic incident label.

## Deviations from Plan

### Auto-fixed Issues

None - no Rule 1-3 auto-fixes were required.

**Total deviations:** 0 auto-fixed.
**Impact on plan:** The plan stayed within the requested test-only scope.

## Issues Encountered

- The TDD-labeled targets were already green before edits. I treated this as pre-existing coverage and added focused hardening assertions rather than manufacturing a failing production change.

## Verification

```bash
safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestNonBudgetedFailureSql backend/pipeline/storage/tests/test_feed_store.py::TestReleaseNonBudgetedFailure -q -n 0
```

Result: `6 passed in 0.04s`.

```bash
safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_collector_runtime.py::TestProcessFeedRetry backend/pipeline/ingestion/tests/test_collector_runtime.py::TestProcessFeedQuarantine -q -n 0
```

Result: `16 passed, 5 subtests passed in 0.96s`.

```bash
git diff --check
```

Result: passed.

## Known Stubs

None. Stub-pattern scan only found normal test fixture empty lists/dicts and `None` assignments, not UI or runtime stubs.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Plan 03-02 can proceed to status compatibility surfaces with TEST-01 through TEST-08 covered by focused backend tests. No broad local E2E, API, Docker, or component stack was run.

## Self-Check: PASSED

- Found summary file: `.planning/phases/03-verification-and-compatibility/03-01-SUMMARY.md`
- Found modified test files on disk.
- Found task commits `39afddeb` and `9ef6e9e7` in git history.
- `git diff --check` passed.

---
*Phase: 03-verification-and-compatibility*
*Completed: 2026-06-15*

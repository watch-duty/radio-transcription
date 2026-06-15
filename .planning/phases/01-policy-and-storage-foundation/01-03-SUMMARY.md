---
phase: 01-policy-and-storage-foundation
plan: 03
subsystem: storage
tags: [alloydb, feed-store, non-budgeted-failure, quarantine]
requires:
  - phase: 01-01
    provides: Canonical status reasons and policy decisions.
  - phase: 01-02
    provides: Collector evidence that can classify non-budgeted lanes.
provides:
  - Fenced non-budgeted failure SQL transition.
  - FeedStore.release_non_budgeted_failure wrapper.
  - Storage tests for no quarantine budget increment and no quarantine_reason write.
affects: [storage, ingestion-runtime, recovery]
tech-stack:
  added: []
  patterns:
    - "Non-budgeted failures release leases into failing state with failure_count reset to zero."
    - "report_feed_failure remains the only feed-budget increment path."
key-files:
  created: []
  modified:
    - backend/pipeline/storage/feed_queries.py
    - backend/pipeline/storage/feed_store.py
    - backend/pipeline/storage/tests/test_feed_store.py
key-decisions:
  - "Non-budgeted failures store retry_after and status_reason but never quarantine_reason."
  - "Non-budgeted failures release the active lease with worker_id = NULL."
requirements-completed: [STORE-01, STORE-02, STORE-03, STORE-04]
duration: inline phase execution
completed: 2026-06-15
---

# Phase 01 Plan 03: Non-Budgeted Storage Primitive Summary

**Fenced feed-store path for retryable non-budgeted failures without quarantine debt**

## Performance

- **Duration:** Inline phase execution
- **Started:** 2026-06-15T02:00:00Z
- **Completed:** 2026-06-15T02:41:04Z
- **Tasks:** 3
- **Files modified:** 3

## Accomplishments

- Added `RELEASE_NON_BUDGETED_FAILURE_SQL` to set `status='failing'`, `failure_count=0`, `worker_id=NULL`, `retry_after`, and `status_reason`.
- Added `FeedStore.release_non_budgeted_failure(...)` with exact parameter ordering and no telemetry or quarantine side effects.
- Added focused storage tests for SQL shape, fencing, return diagnostics, and method parameters.

## Task Commits

Executed inline in a shared dirty worktree; implementation was committed as one coherent phase slice:

1. **Phase implementation** - `f502e518` (`feat(ingestion): add quarantine failure policy foundation`)

## Files Created/Modified

- `backend/pipeline/storage/feed_queries.py` - Non-budgeted failure SQL.
- `backend/pipeline/storage/feed_store.py` - Thin store wrapper.
- `backend/pipeline/storage/tests/test_feed_store.py` - SQL and method contract tests.

## Decisions Made

- Non-budgeted failure storage resets `failure_count` rather than preserving old debt.
- The method returns the resulting status string and otherwise returns `None` on lost lease.

## Deviations from Plan

None beyond the shared phase commit noted in prior summaries.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Ready for recovery clearing and final foundation verification.

## Self-Check: PASSED

Verified by `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestNonBudgetedFailureSql backend/pipeline/storage/tests/test_feed_store.py::TestReleaseNonBudgetedFailure -q -n 0` with `5 passed`.

---
*Phase: 01-policy-and-storage-foundation*
*Completed: 2026-06-15*

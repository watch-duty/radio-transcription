---
phase: 05-producer-and-runtime-routing-merge
plan: 03
subsystem: ingestion-runtime-storage
tags: [quarantine-policy, runtime-routing, storage, pytest, ruff]
requires:
  - phase: 05-producer-and-runtime-routing-merge
    provides: Producer split mappings and runtime pipeline-failure policy execution.
provides:
  - Non-budgeted runtime routes have explicit store-call guard coverage.
  - Non-budgeted storage SQL reset semantics have explicit regression coverage.
  - Final focused Phase 5 backend verification passed.
affects: [phase-05, phase-06, runtime-routing, storage-semantics]
tech-stack:
  added: []
  patterns:
    - Store-call assertions are the primary runtime routing proof.
    - SQL contract tests guard non-budgeted release from quarantine-state writes.
key-files:
  created:
    - .planning/phases/05-producer-and-runtime-routing-merge/05-03-SUMMARY.md
  modified:
    - backend/pipeline/ingestion/tests/test_collector_runtime.py
    - backend/pipeline/storage/tests/test_feed_store.py
    - backend/pipeline/ingestion/collector_runtime.py
    - backend/pipeline/ingestion/collectors/fire_notifications/collector.py
    - backend/pipeline/ingestion/collectors/icecast/icecast_collector.py
    - backend/pipeline/ingestion/collectors/payloads.py
    - backend/pipeline/ingestion/collectors/tests/test_fire_notifications_collector.py
key-decisions:
  - "Non-budgeted guard coverage remains focused on store method calls and storage row-state semantics."
  - "Final verification remains backend-only; Phase 6 owns OpenAPI/frontend/generated compatibility."
patterns-established:
  - "Credential-access and unknown collector evidence stay on release_non_budgeted_failure(...)."
  - "release_non_budgeted_failure SQL must not write quarantine_reason or COALESCE stale quarantine text."
requirements-completed: [RUN-14, TEST-14]
duration: 6 min
completed: 2026-06-15
---

# Phase 05 Plan 03: Non-Budgeted Reset Guard Summary

**Non-budgeted runtime and storage paths now have focused guard tests, and the final Phase 5 backend verification slice passes.**

## Performance

- **Duration:** 6 min
- **Started:** 2026-06-15T18:35:10Z
- **Completed:** 2026-06-15T18:41:29Z
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments

- Added runtime guard coverage proving credential-access and unknown collector evidence use `release_non_budgeted_failure(...)` and do not call `report_feed_failure(...)`.
- Added storage SQL coverage proving non-budgeted release does not write `quarantine_reason` or preserve stale quarantine text through `COALESCE`.
- Re-ran the focused Phase 5 backend verification and focused ruff checks after cleanup.

## Task Commits

1. **Task 1: Add non-budgeted runtime/storage guard tests** - `44431b83` (test)
2. **Task 2: Final lint cleanup for focused verification** - `b5142cb6` (chore)

## Files Created/Modified

- `backend/pipeline/ingestion/tests/test_collector_runtime.py` - Added non-budgeted credential-access and unknown collector runtime guard cases.
- `backend/pipeline/storage/tests/test_feed_store.py` - Added SQL contract coverage for non-budgeted release and stale quarantine text isolation.
- `backend/pipeline/ingestion/collector_runtime.py` - Ruff import-order cleanup only.
- `backend/pipeline/ingestion/collectors/fire_notifications/collector.py` - Ruff import-order cleanup only.
- `backend/pipeline/ingestion/collectors/icecast/icecast_collector.py` - Ruff import-order cleanup only.
- `backend/pipeline/ingestion/collectors/payloads.py` - Moved policy import behind `TYPE_CHECKING`.
- `backend/pipeline/ingestion/collectors/tests/test_fire_notifications_collector.py` - Renamed a duplicate test function.

## Decisions Made

- Kept Phase 5 verification scoped to backend producer/runtime/storage coverage.
- Treated lint-only cleanup as part of Plan 03 because it was required by the final focused ruff gate.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Fixed focused ruff failures exposed by final verification**
- **Found during:** Task 2 final verification
- **Issue:** Focused ruff reported import ordering, a type-checking import, and a duplicate Fire Notifications test name.
- **Fix:** Applied ruff import ordering, moved the payload policy import under `TYPE_CHECKING`, and renamed the duplicate test function.
- **Files modified:** `backend/pipeline/ingestion/collector_runtime.py`, `backend/pipeline/ingestion/collectors/fire_notifications/collector.py`, `backend/pipeline/ingestion/collectors/icecast/icecast_collector.py`, `backend/pipeline/ingestion/collectors/payloads.py`, `backend/pipeline/ingestion/collectors/tests/test_fire_notifications_collector.py`
- **Verification:** Focused ruff and combined focused backend pytest passed after cleanup.
- **Committed in:** `b5142cb6`

---

**Total deviations:** 1 auto-fixed (1 missing critical)
**Impact on plan:** The cleanup was mechanical and stayed within the planned backend verification surface.

## Issues Encountered

- The focused pytest slice passed before lint cleanup; ruff then exposed mechanical style issues that were fixed and reverified.

## Verification

- `safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_collector_runtime.py backend/pipeline/storage/tests/test_feed_store.py::TestNonBudgetedFailureSql backend/pipeline/storage/tests/test_feed_store.py::TestReleaseNonBudgetedFailure -q -n 0` - 97 passed, 6 subtests passed.
- `safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_collector_runtime.py backend/pipeline/ingestion/collectors/tests/test_bcfy_calls_collector.py backend/pipeline/ingestion/collectors/tests/test_fire_notifications_collector.py backend/pipeline/ingestion/collectors/tests/test_icecast_collector.py backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py backend/pipeline/ingestion/collectors/tests/test_aiohttp_requests.py backend/pipeline/ingestion/collectors/tests/test_failure_classification.py backend/pipeline/ingestion/tests/test_failure_policy.py backend/pipeline/storage/tests/test_feed_store.py::TestNonBudgetedFailureSql backend/pipeline/storage/tests/test_feed_store.py::TestReleaseNonBudgetedFailure -q -n 0` - 305 passed, 47 subtests passed.
- `safe-run -- uv run ruff check backend/pipeline/ingestion/collector_runtime.py backend/pipeline/ingestion/tests/test_collector_runtime.py backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py backend/pipeline/ingestion/collectors/tests/test_bcfy_calls_collector.py backend/pipeline/ingestion/collectors/fire_notifications/collector.py backend/pipeline/ingestion/collectors/tests/test_fire_notifications_collector.py backend/pipeline/ingestion/collectors/icecast/icecast_collector.py backend/pipeline/ingestion/collectors/tests/test_icecast_collector.py backend/pipeline/ingestion/collectors/openmhz/collector.py backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py backend/pipeline/ingestion/collectors/payloads.py backend/pipeline/ingestion/collectors/tests/test_aiohttp_requests.py backend/pipeline/storage/tests/test_feed_store.py` - All checks passed.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Phase 5 backend routing and reset coverage is ready for Phase 6 compatibility work. Frontend/OpenAPI/generated compatibility remains deferred by design.

## Self-Check: PASSED

Plan tasks are complete, final focused verification passed, and deviations are documented.

---
*Phase: 05-producer-and-runtime-routing-merge*
*Completed: 2026-06-15*

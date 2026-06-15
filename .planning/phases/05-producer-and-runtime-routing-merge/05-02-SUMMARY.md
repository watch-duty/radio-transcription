---
phase: 05-producer-and-runtime-routing-merge
plan: 02
subsystem: ingestion-runtime
tags: [quarantine-policy, pipeline-failure, runtime-routing, pytest]
requires:
  - phase: 04-strict-policy-table-and-status-vocabulary
    provides: Explicit policy table routes for pipeline publish evidence.
provides:
  - Runtime `_PipelineFailure` execution through policy decisions.
  - Budgeted Pub/Sub post-bookmark publish failure store routing.
  - Thresholded quarantine telemetry coverage for pipeline publish failures.
affects: [phase-05, phase-06, runtime-routing]
tech-stack:
  added: []
  patterns:
    - Shared classify-and-execute branch for collector and pipeline failures.
    - Canonical policy telemetry carries data-gap flags on budgeted publish gaps.
key-files:
  created:
    - .planning/phases/05-producer-and-runtime-routing-merge/05-02-SUMMARY.md
  modified:
    - backend/pipeline/ingestion/collector_runtime.py
    - backend/pipeline/ingestion/tests/test_collector_runtime.py
key-decisions:
  - "Pub/Sub post-bookmark publish failures now call report_feed_failure through the policy decision path."
  - "Legacy post_bookmark_publish_failure telemetry is not required for the budgeted v1.1 route."
patterns-established:
  - "`_PipelineFailure` uses `failure_policy.classify_failure_policy(...)` and branches by `is_feed_quarantine(...)`."
  - "`_record_feed_failure(...)` can emit replay/data-gap fields in canonical policy telemetry."
requirements-completed: [RUN-11, RUN-12, RUN-13, RUN-15, RUN-16, TEST-13]
duration: 6 min
completed: 2026-06-15
---

# Phase 05 Plan 02: Runtime Pipeline Failure Routing Summary

**Runtime pipeline failures now execute the same policy branch as collector failures, with Pub/Sub post-bookmark publish failures consuming thresholded feed budget.**

## Performance

- **Duration:** 6 min
- **Started:** 2026-06-15T18:29:00Z
- **Completed:** 2026-06-15T18:35:04Z
- **Tasks:** 3
- **Files modified:** 2

## Accomplishments

- `_PipelineFailure` now classifies `status_reason + evidence` through the policy table and branches to budgeted or non-budgeted store paths.
- Pub/Sub post-bookmark publish failures call `report_feed_failure(...)`, preserve `replay_missing`/`data_gap_known` in policy telemetry, and only emit quarantine telemetry when the store returns `"quarantined"`.
- GCS upload and bookmark-write pipeline failures continue to use `release_non_budgeted_failure(...)`.

## Task Commits

1. **Task 1: Add failing runtime tests for _PipelineFailure policy execution** - `c6b8d9ed` (test)
2. **Task 2/3: Implement shared policy execution and threshold telemetry** - `f84ff65c` (feat)

## Files Created/Modified

- `backend/pipeline/ingestion/collector_runtime.py` - Shared `_PipelineFailure` policy execution and budgeted policy telemetry flags.
- `backend/pipeline/ingestion/tests/test_collector_runtime.py` - Pub/Sub budgeted route and thresholded quarantine telemetry assertions.

## Decisions Made

- Data-gap visibility is now asserted through `feed_failure_policy_decision` fields, not through the legacy `post_bookmark_publish_failure` event.
- Removed a stale `system_authentication_failed` non-budgeted runtime test case because current policy treats upstream auth rejection as budgeted; `system_credential_access_failed` remains the non-budgeted credential-access split.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Removed stale shared-auth non-budgeted case**
- **Found during:** Task 1 RED verification
- **Issue:** The runtime test file still classified `system_authentication_failed` credential-scope evidence as non-budgeted, conflicting with Phase 4 policy.
- **Fix:** Removed that stale case from the non-actionable collector test matrix. Credential-access non-budgeted coverage remains for `05-03`.
- **Files modified:** `backend/pipeline/ingestion/tests/test_collector_runtime.py`
- **Verification:** `safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_collector_runtime.py -q -n 0`
- **Committed in:** `c6b8d9ed`

---

**Total deviations:** 1 auto-fixed (1 missing critical)
**Impact on plan:** The fix aligned runtime tests with already-approved policy semantics and did not broaden runtime routing.

## Issues Encountered

- RED run initially also failed on the stale shared-auth case; after removing it, the only expected RED failures were the two Pub/Sub budgeted-route assertions.
- After implementation, log extraction needed to filter only records carrying `event_type` because budgeted failure logging also emits non-event diagnostic fields.

## Verification

- `safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_collector_runtime.py -q -n 0` - 90 passed, 4 subtests passed.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Runtime routing is ready for `05-03` non-budgeted reset and telemetry-boundary guard coverage. Phase 6 still owns frontend/OpenAPI/generated compatibility.

## Self-Check: PASSED

Plan tasks are complete, runtime verification passed, and deviations are documented.

---
*Phase: 05-producer-and-runtime-routing-merge*
*Completed: 2026-06-15*

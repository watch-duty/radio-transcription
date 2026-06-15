---
phase: 04-strict-policy-table-and-status-vocabulary
plan: 01
subsystem: ingestion-policy
tags: [quarantine-policy, failure-policy, backend, pytest]
requires:
  - phase: 03-verification-and-compatibility
    provides: "focused proof that non-budgeted failures do not consume feed quarantine budget"
provides:
  - "explicit status/evidence policy table for current backend status reasons"
  - "fail-closed telemetry-gap fallback for unmatched policy evidence"
  - "policy tests for current budgeted, non-budgeted, and mismatched routes"
affects: [phase-05-runtime-routing, phase-06-compatibility]
tech-stack:
  added: []
  patterns:
    - "internal dataclass rule table for pure policy classification"
key-files:
  created:
    - .planning/phases/04-strict-policy-table-and-status-vocabulary/04-01-SUMMARY.md
  modified:
    - backend/pipeline/ingestion/failure_policy.py
    - backend/pipeline/ingestion/tests/test_failure_policy.py
key-decisions:
  - "Policy routing now matches explicit status/evidence rows before returning a decision."
  - "Unsupported status/evidence combinations return telemetry-gap non-budgeted decisions."
  - "Existing FailureScope.PIPELINE is used for Pub/Sub stage evidence rather than adding a new scope enum."
patterns-established:
  - "Status-specific policy rows constrain owner scope, failure scope, endpoint kind, and pipeline stage."
  - "Policy fallback is telemetry gap, not a broad retry or quarantine default."
requirements-completed: [POL-11, POL-12, POL-13, POL-14, TEST-11, TEST-12]
duration: 6 min
completed: 2026-06-15
---

# Phase 04 Plan 01: Strict Policy Table Summary

**Pure failure policy classification now uses explicit status/evidence rows with telemetry-gap fallback.**

## Performance

- **Duration:** 6 min
- **Started:** 2026-06-15T16:38:50Z
- **Completed:** 2026-06-15T16:44:55Z
- **Tasks:** 3
- **Files modified:** 2

## Accomplishments

- Added policy route tests covering every current backend `FeedStatusReason`.
- Replaced broad owner-scope classification with an internal `_FailurePolicyRule` table.
- Added fail-closed mismatch coverage for wrong status/evidence combinations.
- Changed `pipeline_publish_after_bookmark_failed` policy intent to feed-budgeted quarantine for matching Pub/Sub publish-stage evidence.

## Task Commits

1. **Task 1: Add failing tests for explicit current policy routes** - `08a65aeb` (test)
2. **Task 2: Implement the explicit fail-closed policy table** - `ebcc69dd` (feat)
3. **Task 3: Verify Phase 4 policy boundaries** - verified by focused pytest and diff inspection; no code commit needed

## Files Created/Modified

- `backend/pipeline/ingestion/tests/test_failure_policy.py` - Adds table-route and mismatch tests for policy decisions.
- `backend/pipeline/ingestion/failure_policy.py` - Adds `_FailurePolicyRule`, `_POLICY_RULES`, and telemetry-gap fallback classification.

## Decisions Made

- Source status reasons route to non-budgeted suppress-retry decisions, not source-class breaker intent, because breaker state remains future work.
- Existing `FailureScope.PIPELINE` represents pipeline-stage evidence in v1.1; no new evidence enum was added.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Reused existing pipeline failure scope**

- **Found during:** Task 2 (Implement the explicit fail-closed policy table)
- **Issue:** The plan text referenced `FailureScope.PIPELINE_STAGE`, but the current evidence model only has `FailureScope.PIPELINE`, and Phase 4 explicitly avoids reshaping evidence.
- **Fix:** Used `FailureScope.PIPELINE` with `pipeline_stage=PipelineStage.PUBSUB_PUBLISH` for Pub/Sub publish-stage matching.
- **Files modified:** `backend/pipeline/ingestion/failure_policy.py`, `backend/pipeline/ingestion/tests/test_failure_policy.py`
- **Verification:** `safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_failure_policy.py -q -n 0`
- **Committed in:** `08a65aeb`, `ebcc69dd`

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Preserves the existing evidence contract and avoids an unnecessary enum expansion.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Verification

```bash
safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_failure_policy.py -q -n 0
```

Result: `5 passed, 13 subtests passed in 0.03s`.

## Next Phase Readiness

04-02 can add the three backend status reason enum values and extend this same rule table with split-status rows.

---
*Phase: 04-strict-policy-table-and-status-vocabulary*
*Completed: 2026-06-15*

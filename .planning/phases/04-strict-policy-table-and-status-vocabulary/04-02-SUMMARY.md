---
phase: 04-strict-policy-table-and-status-vocabulary
plan: 02
subsystem: ingestion-policy
tags: [status-reason, failure-policy, backend, pytest]
requires:
  - phase: 04-strict-policy-table-and-status-vocabulary
    provides: "04-01 explicit policy table and fail-closed fallback"
provides:
  - "backend FeedStatusReason values for runtime configuration, credential access, and source payload failures"
  - "owner-scope mapping for the new split status reasons"
  - "policy rows and tests for the split status reasons"
  - "promoted item-scope evidence remains explicitly routed for source/auth failures"
affects: [phase-05-producer-runtime-routing, phase-06-compatibility]
tech-stack:
  added: []
  patterns:
    - "backend-only enum split with compatibility surfaces deferred"
key-files:
  created:
    - .planning/phases/04-strict-policy-table-and-status-vocabulary/04-02-SUMMARY.md
  modified:
    - backend/pipeline/storage/feed_store.py
    - backend/pipeline/storage/tests/test_feed_store.py
    - backend/pipeline/ingestion/collectors/failure_classification.py
    - backend/pipeline/ingestion/collectors/tests/test_failure_classification.py
    - backend/pipeline/ingestion/failure_policy.py
    - backend/pipeline/ingestion/tests/test_failure_policy.py
key-decisions:
  - "Added only the three backend status reason values required for current v1.1 routing."
  - "Mapped runtime configuration invalid and source payload invalid to source-class evidence."
  - "Mapped credential access failed to credential-scope evidence and kept it non-budgeted."
  - "Left OpenAPI, generated metadata, shared frontend types, and UI labels deferred to Phase 6."
patterns-established:
  - "Split enum additions require canonical backend tests, owner mapping tests, and policy route tests."
  - "Non-budgeted credential access remains a single policy row for easy future route changes."
requirements-completed: [STAT-11, STAT-12, STAT-13, STAT-14, TEST-11, TEST-12]
duration: 8 min
completed: 2026-06-15
---

# Phase 04 Plan 02: Status Vocabulary Split Summary

**Backend status reason splits now feed owner-scope mapping and explicit policy routes.**

## Performance

- **Duration:** 8 min
- **Started:** 2026-06-15T16:45:39Z
- **Completed:** 2026-06-15T16:53:44Z
- **Tasks:** 4
- **Files modified:** 6

## Accomplishments

- Added backend enum values for `system_runtime_configuration_invalid`, `system_credential_access_failed`, and `system_source_payload_invalid`.
- Updated collector owner-scope mapping for the new statuses.
- Extended policy route tests and `_POLICY_RULES` for the split statuses.
- Verified backend-only scope without touching frontend/OpenAPI/generated compatibility files.
- Fixed a review-found gap where promoted all-items-failed source/auth evidence used `FailureScope.ITEM`.

## Task Commits

1. **Task 1: Add failing tests for backend enum and owner mapping splits** - `f62d9d46` (test)
2. **Task 2: Add backend enum values and owner mapping** - `8cef80d4` (feat)
3. **Task 3: Extend policy-table tests and rows for split reasons** - `65eb5ea1` (test), `c0b4a29c` (feat)
4. **Task 4: Verify backend-only scope and known compatibility deferral** - `cf83fba0` (test), `11791cc9` (fix)

## Files Created/Modified

- `backend/pipeline/storage/feed_store.py` - Adds the three backend `FeedStatusReason` values.
- `backend/pipeline/storage/tests/test_feed_store.py` - Adds backend canonical enum coverage.
- `backend/pipeline/ingestion/collectors/failure_classification.py` - Maps split statuses to owner scopes.
- `backend/pipeline/ingestion/collectors/tests/test_failure_classification.py` - Adds owner-scope mapping assertions.
- `backend/pipeline/ingestion/failure_policy.py` - Adds split-status policy rows.
- `backend/pipeline/ingestion/tests/test_failure_policy.py` - Adds split-status route and mismatch coverage.

## Decisions Made

- `system_runtime_configuration_invalid` and `system_source_payload_invalid` are quarantine-budgeted only with source-class evidence.
- `system_credential_access_failed` is explicit non-budgeted suppress-retry, preserving a one-row future change if production evidence shows retry cannot recover.
- OpenAPI/frontend/generated/UI compatibility remains deferred to Phase 6 per the Phase 4 boundary.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Preserve promoted item-scope policy routes**

- **Found during:** Code review gate after Task 4
- **Issue:** Existing collectors can promote all-items-failed source/auth failures with `FailureScope.ITEM`; the new policy rows omitted `ITEM`, so those failures would fall through to telemetry gap.
- **Fix:** Added an item-scope regression test and included `FailureScope.ITEM` in the relevant explicit source-class and credential-scope policy rows.
- **Files modified:** `backend/pipeline/ingestion/tests/test_failure_policy.py`, `backend/pipeline/ingestion/failure_policy.py`
- **Verification:** `safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_failure_policy.py backend/pipeline/ingestion/collectors/tests/test_failure_classification.py backend/pipeline/storage/tests/test_feed_store.py::TestFeedStatusReason::test_canonical_reason_values -q -n 0`
- **Committed in:** `cf83fba0`, `11791cc9`

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Keeps the explicit policy table aligned with existing collector evidence shapes without expanding Phase 4 beyond backend policy.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Verification

```bash
safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_failure_policy.py backend/pipeline/ingestion/collectors/tests/test_failure_classification.py backend/pipeline/storage/tests/test_feed_store.py::TestFeedStatusReason::test_canonical_reason_values -q -n 0
```

Result: `20 passed, 23 subtests passed in 0.04s`.

## Next Phase Readiness

Phase 5 can now update producers and runtime routing against the explicit table and new backend statuses. Phase 6 remains responsible for enum compatibility surfaces.

---
*Phase: 04-strict-policy-table-and-status-vocabulary*
*Completed: 2026-06-15*

---
phase: 02-runtime-routing-and-telemetry
plan: 01
subsystem: ingestion-runtime
tags: [quarantine-policy, runtime-routing, tests]
requires:
  - phase: 01-policy-and-storage-foundation
    provides: failure policy vocabulary, strict FeedFailure evidence, non-budgeted storage path
provides:
  - Tests proving feed quarantine remains a narrow feed-owned decision.
  - Tests proving shared/source-class and UNKNOWN policy decisions are non-budgeted.
  - Runtime tests aligned with the strict typed FeedFailure boundary.
affects: [phase-02-runtime-routing-and-telemetry, phase-03-verification-and-compatibility]
tech-stack:
  added: []
  patterns: [pure policy tests, runtime store-call assertions]
key-files:
  created:
    - .planning/phases/02-runtime-routing-and-telemetry/02-01-SUMMARY.md
  modified:
    - backend/pipeline/ingestion/tests/test_failure_policy.py
    - backend/pipeline/ingestion/tests/test_collector_runtime.py
key-decisions:
  - "No runtime implementation change was needed for 02-01; existing Phase 1 routing already classified before choosing the store path."
  - "UNKNOWN policy behavior is represented by untyped runtime exceptions, while typed FeedFailure without policy evidence remains invalid."
patterns-established:
  - "Policy tests assert both intent and executed_action so OPEN_BREAKER cannot be mistaken for real v1 breaker state."
  - "Runtime tests prove routing through store calls before inspecting telemetry."
requirements-completed: [POL-04, RUN-01, RUN-07]
duration: 2 min
completed: 2026-06-15
---

# Phase 02 Plan 01: Runtime Quarantine Guard Summary

**Focused tests now prove only feed-owned quarantine decisions can use the feed failure budget, while shared and UNKNOWN failures stay non-budgeted.**

## Performance

- **Duration:** 2 min
- **Started:** 2026-06-15T03:18:40Z
- **Completed:** 2026-06-15T03:20:53Z
- **Tasks:** 3
- **Files modified:** 2

## Accomplishments

- Hardened pure policy coverage for credential-scope, source-class, and UNKNOWN decisions.
- Proved feed-config failures still reach `report_feed_failure(...)`.
- Replaced the misleading UNKNOWN `FeedFailure` runtime test with the intended untyped exception telemetry-gap path.

## Task Commits

1. **Task 1: Harden pure policy guard tests** - `c92b3878`
2. **Tasks 2-3: Prove runtime quarantine guard and strict fallback** - `2675e7dc`

## Files Created/Modified

- `backend/pipeline/ingestion/tests/test_failure_policy.py` - Added intent/action assertions for source-class, credential-scope, and UNKNOWN policy decisions.
- `backend/pipeline/ingestion/tests/test_collector_runtime.py` - Asserted feed-config budgeted store calls and updated telemetry-gap coverage to use the untyped runtime exception fallback.
- `.planning/phases/02-runtime-routing-and-telemetry/02-01-SUMMARY.md` - Plan completion summary.

## Decisions Made

- No production code change was needed; the current runtime already classifies `FeedFailure` before choosing budgeted versus non-budgeted storage.
- `OPEN_BREAKER` remains a policy intent only in v1 and is paired with `release_non_budgeted_failure`.

## Deviations from Plan

None - plan executed exactly as written. The runtime tasks landed in one test commit because both changed the same test class and did not alter production code.

## Issues Encountered

None.

## Verification

```bash
safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_failure_policy.py backend/pipeline/ingestion/tests/test_collector_runtime.py::TestFeedFailureContract backend/pipeline/ingestion/tests/test_collector_runtime.py::TestProcessFeedQuarantine -q -n 0
```

Result: `20 passed, 2 subtests passed in 0.34s`.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Ready for `02-02`: non-budgeted routing coverage can build on the now-explicit budget guard and strict fallback tests.

---
*Phase: 02-runtime-routing-and-telemetry*
*Completed: 2026-06-15*

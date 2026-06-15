---
phase: 02-runtime-routing-and-telemetry
plan: 03
subsystem: ingestion-runtime
tags: [telemetry, publish-gap, policy-decision]
requires:
  - phase: 02-runtime-routing-and-telemetry
    provides: 02-02 suppressed retry routing coverage
provides:
  - Policy decision telemetry contract tests for budgeted and non-budgeted failures.
  - Post-bookmark publish-gap telemetry tests with replay flags.
  - Negative telemetry coverage for non-publish pipeline failures and non-budgeted quarantine suppression.
affects: [phase-02-runtime-routing-and-telemetry, phase-03-verification-and-compatibility]
tech-stack:
  added: []
  patterns: [structured json_fields assertions, telemetry-as-audit tests]
key-files:
  created:
    - .planning/phases/02-runtime-routing-and-telemetry/02-03-SUMMARY.md
  modified:
    - backend/pipeline/ingestion/tests/test_collector_runtime.py
key-decisions:
  - "Telemetry remains an audit mirror; routing proof stays in store-call assertions."
  - "GCS and bookmark pipeline errors emit policy-decision telemetry but not post_bookmark_publish_failure."
patterns-established:
  - "Assert stable policy telemetry fields without adding new runtime log fields."
  - "Use event_type filtering on json_fields logs for focused telemetry contract tests."
requirements-completed: [RUN-04, RUN-05, RUN-06, TEL-01, TEL-02, TEL-03, TEL-04, TEL-05]
duration: 2 min
completed: 2026-06-15
---

# Phase 02 Plan 03: Runtime Failure Telemetry Summary

**Runtime telemetry tests now prove policy decisions and post-bookmark publish gaps are logged with the required intent, action, source, evidence, and replay flags.**

## Performance

- **Duration:** 2 min
- **Started:** 2026-06-15T03:23:45Z
- **Completed:** 2026-06-15T03:25:51Z
- **Tasks:** 3
- **Files modified:** 1

## Accomplishments

- Added budgeted policy-decision telemetry assertions for feed configuration quarantine.
- Added non-budgeted telemetry assertions for UNKNOWN telemetry gap and pipeline-owned failures.
- Proved Pub/Sub publish-after-bookmark emits `post_bookmark_publish_failure` with hold-for-replay intent, suppress-publish-gap action, and replay/data-gap flags.
- Proved GCS and bookmark pipeline failures do not emit the post-bookmark publish-gap event.

## Task Commits

1. **Tasks 1-3: Policy decision, publish-gap, and no-quarantine telemetry coverage** - `c9e1cb85`

## Files Created/Modified

- `backend/pipeline/ingestion/tests/test_collector_runtime.py` - Added structured telemetry contract assertions.
- `.planning/phases/02-runtime-routing-and-telemetry/02-03-SUMMARY.md` - Plan completion summary.

## Decisions Made

- Kept runtime telemetry payloads unchanged; existing helpers already emitted the required stable fields.
- Avoided testing incidental log ordering or unrelated log fields.

## Deviations from Plan

None - plan executed exactly as written. The telemetry assertions were committed together because all tasks harden the same runtime test class.

## Issues Encountered

None.

## Verification

```bash
safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_failure_policy.py backend/pipeline/ingestion/tests/test_collector_runtime.py::TestFeedFailureContract backend/pipeline/ingestion/tests/test_collector_runtime.py::TestProcessFeedRetry backend/pipeline/ingestion/tests/test_collector_runtime.py::TestProcessFeedQuarantine -q -n 0
```

Result: `27 passed, 7 subtests passed in 0.86s`.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Phase 2 is ready for phase-level verification. Phase 3 can focus on storage compatibility and broader verification coverage.

---
*Phase: 02-runtime-routing-and-telemetry*
*Completed: 2026-06-15*

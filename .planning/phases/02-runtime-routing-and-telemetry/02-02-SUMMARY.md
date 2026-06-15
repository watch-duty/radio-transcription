---
phase: 02-runtime-routing-and-telemetry
plan: 02
subsystem: ingestion-runtime
tags: [suppressed-retry, non-budgeted-routing, pipeline-failures]
requires:
  - phase: 02-runtime-routing-and-telemetry
    provides: 02-01 quarantine budget guard tests
provides:
  - Runtime tests for pipeline-owned non-budgeted release.
  - Runtime tests for source-offline, rate-limit, capture-timeout, shared-auth, and source-class suppressed retry.
  - Retry-after propagation checks without over-specifying backoff timing.
affects: [phase-02-runtime-routing-and-telemetry, phase-03-verification-and-compatibility]
tech-stack:
  added: []
  patterns: [sentinel retry_after assertions, table-driven policy routing tests]
key-files:
  created:
    - .planning/phases/02-runtime-routing-and-telemetry/02-02-SUMMARY.md
  modified:
    - backend/pipeline/ingestion/tests/test_collector_runtime.py
key-decisions:
  - "No reason-specific backoff was added; tests assert retry_after propagation only."
  - "Source-class and credential-scope decisions remain non-budgeted release in v1 rather than real breaker state."
patterns-established:
  - "Patch _non_budgeted_retry_after() to a sentinel when tests need exact retry propagation."
  - "Use typed FeedFailure with structured evidence for known non-actionable collector cases."
requirements-completed: [RUN-02, RUN-03]
duration: 2 min
completed: 2026-06-15
---

# Phase 02 Plan 02: Suppressed Retry Routing Summary

**Runtime tests now prove pipeline and non-actionable source/system failures use the non-budgeted release path with retry timing and no feed quarantine budget.**

## Performance

- **Duration:** 2 min
- **Started:** 2026-06-15T03:21:30Z
- **Completed:** 2026-06-15T03:23:18Z
- **Tasks:** 3
- **Files modified:** 1

## Accomplishments

- Added retry-after sentinel assertions to pipeline-owned suppressed retry tests.
- Added table-driven coverage for source-offline, rate-limit, capture-timeout-as-unreachable, shared auth, and source-class decisions.
- Proved post-bookmark Pub/Sub publish failures use `pipeline_publish_after_bookmark_failed` and do not normal-release the feed.

## Task Commits

1. **Tasks 1-3: Pipeline, collector, and retry_after suppressed routing coverage** - `24b092c4`

## Files Created/Modified

- `backend/pipeline/ingestion/tests/test_collector_runtime.py` - Added non-budgeted routing cases and retry-after propagation assertions.
- `.planning/phases/02-runtime-routing-and-telemetry/02-02-SUMMARY.md` - Plan completion summary.

## Decisions Made

- Kept the production routing code unchanged because the existing runtime already used `_record_non_budgeted_failure(...)` for these lanes.
- Represented capture timeout with the existing `source_unreachable` status reason and raw reason `capture_timeout`; no new status enum was needed.

## Deviations from Plan

None - plan executed exactly as written. The three test-hardening tasks were committed together because they all modify the same runtime test class.

## Issues Encountered

None.

## Verification

```bash
safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_collector_runtime.py::TestProcessFeedRetry backend/pipeline/ingestion/tests/test_collector_runtime.py::TestProcessFeedQuarantine -q -n 0
```

Result: `16 passed, 5 subtests passed in 1.27s`.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Ready for `02-03`: telemetry contract tests can build on the now-covered routing matrix.

---
*Phase: 02-runtime-routing-and-telemetry*
*Completed: 2026-06-15*

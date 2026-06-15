---
phase: 01-policy-and-storage-foundation
plan: 02
subsystem: ingestion
tags: [collectors, failure-policy, policy-evidence, ast-tests]
requires:
  - phase: 01-01
    provides: Pure failure policy evidence types.
provides:
  - Facts-only policy evidence at every current collector_failure call site.
  - Shared status-enum to owner-scope helper.
  - AST omission detector for collector_failure policy_evidence.
affects: [collectors, ingestion-runtime]
tech-stack:
  added: []
  patterns:
    - "Collectors construct facts-only evidence at the source-specific boundary."
    - "AST tests guard missing policy_evidence at collector_failure calls."
key-files:
  created: []
  modified:
    - backend/pipeline/ingestion/collectors/failure_classification.py
    - backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py
    - backend/pipeline/ingestion/collectors/openmhz/collector.py
    - backend/pipeline/ingestion/collectors/icecast/icecast_collector.py
    - backend/pipeline/ingestion/collectors/fire_notifications/collector.py
    - backend/pipeline/ingestion/collectors/tests/test_failure_classification.py
    - backend/pipeline/ingestion/collectors/tests/test_icecast_collector.py
key-decisions:
  - "Owner mapping is based on FeedStatusReason enum values, not raw reason text."
  - "Item promotions stay item-scoped in evidence."
requirements-completed: [POL-01, POL-02, POL-03]
duration: inline phase execution
completed: 2026-06-15
---

# Phase 01 Plan 02: Collector Evidence Wiring Summary

**Every current source collector raises typed failures with facts-only policy evidence**

## Performance

- **Duration:** Inline phase execution
- **Started:** 2026-06-15T02:00:00Z
- **Completed:** 2026-06-15T02:41:04Z
- **Tasks:** 3
- **Files modified:** 7

## Accomplishments

- Added `policy_evidence_for_status_reason(...)` and enum-based owner mapping in `failure_classification.py`.
- Wired BCFY Calls, OpenMHz, Icecast, and Fire Notifications collector failures with owner/scope/endpoint evidence.
- Added `TestCollectorFailureCallSites`, which parses current collector files with `ast` and fails any `collector_failure(...)` call without `policy_evidence=`.

## Task Commits

Executed inline in a shared dirty worktree; implementation was committed as one coherent phase slice:

1. **Phase implementation** - `f502e518` (`feat(ingestion): add quarantine failure policy foundation`)

## Files Created/Modified

- `backend/pipeline/ingestion/collectors/failure_classification.py` - Strict helper and shared evidence builder.
- `backend/pipeline/ingestion/collectors/*` - Current source collector call-site evidence.
- `backend/pipeline/ingestion/collectors/tests/test_failure_classification.py` - Evidence helper tests and AST omission guard.
- `backend/pipeline/ingestion/collectors/tests/test_icecast_collector.py` - Focused Icecast evidence assertions.

## Decisions Made

- BCFY JWT and Fire Notifications env/auth config failures are credential-scope evidence.
- OpenMHz reconnect exhaustion is source-class evidence; invalid transport is feed-configuration evidence.
- Icecast ffmpeg/probe failures use stream endpoint evidence with observation scope where the signal comes from one capture/probe attempt.

## Deviations from Plan

The phase was committed as one implementation slice because source, runtime, and storage changes were already interleaved in the worktree. No behavior outside the planned collector and policy boundaries was added.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Ready for the non-budgeted storage primitive and runtime routing to consume the evidence.

## Self-Check: PASSED

Verified by `safe-run -- uv run python -m pytest backend/pipeline/ingestion/collectors/tests/test_failure_classification.py backend/pipeline/ingestion/collectors/tests/test_icecast_collector.py -q -n 0` with `38 passed`.

---
*Phase: 01-policy-and-storage-foundation*
*Completed: 2026-06-15*

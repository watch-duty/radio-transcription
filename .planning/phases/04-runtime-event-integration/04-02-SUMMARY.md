---
phase: 04-runtime-event-integration
plan: "02"
subsystem: runtime
tags: [feed-audit-events, collector-runtime, audit-actors, pytest]

requires:
  - phase: 04-runtime-event-integration
    provides: async FeedStore runtime audit primitives from 04-01
provides:
  - Collector runtime actor and prior-state wiring for failure paths
  - Collector runtime actor and prior-state wiring for success/recovery paths
  - Updated async collector integration tests for explicit runtime store signatures
affects: [04-runtime-event-integration, collector-runtime, echo-sync-parity]

tech-stack:
  added: []
  patterns:
    - Runtime passes causal actor and leased prior state into storage-owned audit methods
    - Runtime keeps audit row construction out of collector/runtime callers

key-files:
  created:
    - .planning/phases/04-runtime-event-integration/04-02-SUMMARY.md
  modified:
    - backend/pipeline/ingestion/collector_runtime.py
    - backend/pipeline/ingestion/tests/test_collector_runtime.py
    - backend/pipeline/ingestion/tests/test_chunk_ingested.py
    - backend/pipeline/ingestion/collectors/tests/test_bcfy_calls_collector_integration.py
    - backend/pipeline/ingestion/collectors/tests/test_icecast_collector_integration.py
    - backend/pipeline/ingestion/collectors/tests/test_openmhz_collector_integration.py

key-decisions:
  - "Async collector runtime uses the stable semantic actor service:collector-runtime for all audit-capable runtime storage calls."
  - "Runtime passes leased previous_status, failure_count, status_reason, and diagnostic reason to storage, while storage remains the only audit row writer."
  - "Docker/Testcontainers collector integration tests were updated statically but not executed locally under AGENTS.md safety rules."

patterns-established:
  - "Runtime success writes wrap retryable storage calls in local coroutines so keyword-only audit inputs remain explicit."
  - "Collector integration tests pass explicit runtime actor/prior-state inputs when they call FeedStore directly."

requirements-completed: [AUD-01, EVT-06, EVT-07, EVT-08, EVT-09, DIAG-02, DIAG-03, ACT-03]

duration: 11 min
completed: 2026-06-19
---

# Phase 04 Plan 02: Async Collector Runtime Actor and Prior-State Wiring Summary

**Collector runtime now passes service:collector-runtime plus leased prior state into storage-owned failure, quarantine, and recovery-capable writes**

## Performance

- **Duration:** 11 min
- **Started:** 2026-06-20T00:01:54Z
- **Completed:** 2026-06-20T00:12:34Z
- **Tasks:** 3
- **Files modified:** 6

## Accomplishments

- Added the `service:collector-runtime` actor constant and wired it into budgeted and non-budgeted failure storage calls.
- Passed `previous_status`, prior `failure_count`, prior `status_reason`, and diagnostic `reason` from leased feeds into storage.
- Wired chunk progress and source-observation success writes with the same actor/prior-state inputs so storage can emit recovery only from prior abnormal state.
- Updated collector integration tests that call `FeedStore` directly to satisfy the explicit runtime signatures without constructing audit rows.

## Task Commits

1. **Task 1: Pass actor and prior state from runtime failure paths** - `be785709` (feat)
2. **Task 2: Pass actor and prior state from runtime success paths** - `2034cca5` (feat)
3. **Task 3: Update async collector integration tests for explicit actor signatures** - `5d7a1bc0` (test)

**Plan metadata:** final docs commit

## Files Created/Modified

- `backend/pipeline/ingestion/collector_runtime.py` - Adds runtime actor constant and passes leased prior-state inputs into failure, non-budgeted failure, chunk progress, and source-observation storage calls.
- `backend/pipeline/ingestion/tests/test_collector_runtime.py` - Adds `previous_status` fixtures and assertions for failure, non-budgeted failure, source observation, and chunk progress call signatures.
- `backend/pipeline/ingestion/tests/test_chunk_ingested.py` - Updates the runtime feed fixture for the new leased prior-state shape.
- `backend/pipeline/ingestion/collectors/tests/test_bcfy_calls_collector_integration.py` - Updates direct progress writes with explicit runtime actor/prior-state inputs.
- `backend/pipeline/ingestion/collectors/tests/test_icecast_collector_integration.py` - Updates direct progress and failure writes with explicit runtime actor/prior-state inputs.
- `backend/pipeline/ingestion/collectors/tests/test_openmhz_collector_integration.py` - Updates direct progress writes with explicit runtime actor/prior-state inputs.

## Verification

- `safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_collector_runtime.py::TestProcessFeedQuarantine backend/pipeline/ingestion/tests/test_collector_runtime.py::TestProcessFeedRetry backend/pipeline/ingestion/tests/test_chunk_ingested.py -q` - Passed, 25 tests and 6 subtests.
- `safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_collector_runtime.py::TestProcessFeedSourceObservation backend/pipeline/ingestion/tests/test_collector_runtime.py::TestProcessFeedResumePosition -q` - Passed, 8 tests.
- `safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_collector_runtime.py backend/pipeline/ingestion/tests/test_chunk_ingested.py -q` - Passed, 90 tests and 6 subtests.
- `python3 -m py_compile` on the three collector integration test files - Passed.
- `rg -n "service:collector-runtime|previous_status|previous_failure_count|previous_status_reason" ...` - Passed for runtime wiring and unit-test assertions.
- `rg -n "actor_id=.*service:collector-runtime|previous_status" backend/pipeline/ingestion/collectors/tests` - Passed for collector integration signatures.
- `rg -n "feed_audit_events|INSERT_FEED_AUDIT|before_values|after_values|feed\\.failure_reported|feed\\.quarantined|feed\\.recovered" ...` - No matches in runtime or collector tests.
- `rg -n "system:" ...` - No matches in touched runtime or test files.
- `git diff --check` - Passed.

## Tests Not Run

- The exact plan commands using host `python3 -m pytest` did not run successfully because the host Python 3.12 environment is missing project dependencies such as `cloudevents`; equivalent scoped tests passed under `uv run python`.
- The exact plan class selectors `TestCollectorRuntimeFailureHandling`, `TestSourceObservation`, and `TestChunkIngested` are stale in the current test file; current equivalent classes were run instead.
- Docker/Testcontainers collector integration suites were not executed locally because AGENTS.md forbids local Docker/testcontainers integration runs without explicit machine-prepared confirmation. The test files were compiled and statically checked.

## Decisions Made

- Used one semantic runtime actor value, `service:collector-runtime`, for async collector runtime audit-capable calls.
- Kept runtime callers limited to causal inputs; no runtime or collector test code constructs audit rows or event payloads.
- Used local retry wrapper coroutines for success writes so storage method calls remain explicit while preserving retry and lease-loss behavior.

## Deviations from Plan

None - plan implementation scope executed as written.

## Issues Encountered

- Local test environment setup required `mise trust .mise.toml` and `safe-run -- mise run generate:protos` so ignored protobuf wrappers existed for imports.
- Host `python3 -m pytest` is not the project dependency environment; `uv run python -m pytest` was required for runnable local tests.
- Plan test selectors were stale relative to current class names, so equivalent current selectors were used.

## Known Stubs

None. Stub scan found only existing test fixtures, optional `None` defaults, empty lists for test collection state, and runtime fields initialized before `_main()`.

## User Setup Required

None - no external service configuration required.

## Threat Flags

None. The touched runtime-to-storage actor/prior-state boundary is the threat surface covered by the plan threat model.

## Next Phase Readiness

Ready for 04-03 Echo/sync-store parity: async runtime now supplies storage with the causal actor and leased prior state needed for storage-owned runtime audit decisions.

## Self-Check: PASSED

- Summary file exists.
- Key modified files exist.
- Task commits found: `be785709`, `2034cca5`, `5d7a1bc0`.

---
*Phase: 04-runtime-event-integration*
*Completed: 2026-06-19*

---
phase: 05-producer-and-runtime-routing-merge
plan: 01
subsystem: ingestion-collectors
tags: [quarantine-policy, collectors, status-reasons, pytest]
requires:
  - phase: 04-strict-policy-table-and-status-vocabulary
    provides: Explicit policy rows and split backend status reason enum values.
provides:
  - Collector producers emit split status reasons for current root causes.
  - Payload helper malformed source payloads use source-payload status.
  - Producer-focused regression tests cover the new split expectations.
affects: [phase-05, phase-06, producer-status-mapping]
tech-stack:
  added: []
  patterns:
    - Direct producer substitutions for status/evidence mapping.
    - Source-payload helper default for malformed successful source payloads.
key-files:
  created:
    - .planning/phases/05-producer-and-runtime-routing-merge/05-01-SUMMARY.md
  modified:
    - backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py
    - backend/pipeline/ingestion/collectors/fire_notifications/collector.py
    - backend/pipeline/ingestion/collectors/icecast/icecast_collector.py
    - backend/pipeline/ingestion/collectors/openmhz/collector.py
    - backend/pipeline/ingestion/collectors/payloads.py
    - backend/pipeline/ingestion/collectors/tests/test_bcfy_calls_collector.py
    - backend/pipeline/ingestion/collectors/tests/test_fire_notifications_collector.py
    - backend/pipeline/ingestion/collectors/tests/test_icecast_collector.py
    - backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py
    - backend/pipeline/ingestion/collectors/tests/test_collector_semantics_helpers.py
    - backend/pipeline/ingestion/collectors/tests/test_collector_failure_semantics_regression.py
key-decisions:
  - "Shared payload helper test expectations were updated with the implementation because the planned helper behavior changed its direct contract."
patterns-established:
  - "Runtime/deploy config producers use system_runtime_configuration_invalid with source-class owner evidence."
  - "Internal credential access producers use system_credential_access_failed with credential-scope owner evidence."
  - "Malformed successful source payloads use system_source_payload_invalid with source-class owner evidence."
requirements-completed: [TEST-15]
duration: 21 min
completed: 2026-06-15
---

# Phase 05 Plan 01: Split Source Producer Mappings Summary

**Collector producers now emit precise runtime-config, credential-access, and source-payload status reasons for current root causes.**

## Performance

- **Duration:** 21 min
- **Started:** 2026-06-15T18:08:00Z
- **Completed:** 2026-06-15T18:28:56Z
- **Tasks:** 3
- **Files modified:** 11

## Accomplishments

- Broadcastify Calls now distinguishes missing JWT env config, Secret Manager access failure, and malformed Calls API payloads.
- Fire Notifications and Icecast shared env/credential config failures now use runtime-configuration status with source-class evidence.
- OpenMHz invalid source-provided media URLs and shared payload-helper malformed source payloads now use source-payload status.

## Task Commits

1. **Task 1: Add failing producer split tests** - `5b5e69c5` (test)
2. **Task 2/3: Implement and verify producer split boundaries** - `386ac4d9` (feat)

## Files Created/Modified

- `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py` - Split Calls JWT config, credential access, and payload statuses.
- `backend/pipeline/ingestion/collectors/fire_notifications/collector.py` - Split Fire runtime config and payload statuses.
- `backend/pipeline/ingestion/collectors/icecast/icecast_collector.py` - Split shared Broadcastify credential config status.
- `backend/pipeline/ingestion/collectors/openmhz/collector.py` - Split invalid media URL status and redact OpenMHz transport diagnostics.
- `backend/pipeline/ingestion/collectors/payloads.py` - Changed malformed successful source payload helper status.
- Collector test files - Added and updated focused producer status expectations.

## Decisions Made

- Kept feed-row configuration failures, provider auth rejection, mixed item failures, and ffmpeg fallback semantics unchanged.
- Updated direct payload-helper tests because the planned helper behavior changed the helper's own contract.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Updated direct payload-helper tests**
- **Found during:** Task 2 verification
- **Issue:** `payloads.extract_optional_item_list(...)` was in plan scope, but two direct helper/regression test files were not listed in `files_modified` and still expected `system_collector_error`.
- **Fix:** Updated helper/regression expectations to `system_source_payload_invalid`.
- **Files modified:** `backend/pipeline/ingestion/collectors/tests/test_collector_semantics_helpers.py`, `backend/pipeline/ingestion/collectors/tests/test_collector_failure_semantics_regression.py`
- **Verification:** `safe-run -- uv run python -m pytest backend/pipeline/ingestion/collectors/tests/test_collector_semantics_helpers.py backend/pipeline/ingestion/collectors/tests/test_collector_failure_semantics_regression.py -q -n 0`
- **Committed in:** `386ac4d9`

**2. [Rule 2 - Missing Critical] Preserved OpenMHz diagnostic redaction**
- **Found during:** Task 2 verification
- **Issue:** The focused producer test slice exposed an existing OpenMHz 403 diagnostic path that preserved `token=secret-value` in the raised reason.
- **Fix:** Added local OpenMHz diagnostic redaction for credential-like key/value fields while keeping raw exception-chain parsing for HTTP status classification.
- **Files modified:** `backend/pipeline/ingestion/collectors/openmhz/collector.py`
- **Verification:** `safe-run -- uv run python -m pytest backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py -q -n 0` as part of the producer slice.
- **Committed in:** `386ac4d9`

---

**Total deviations:** 2 auto-fixed (2 missing critical)
**Impact on plan:** Both fixes were required for the focused verification slice and stayed within backend collector behavior.

## Issues Encountered

- The RED producer slice failed before implementation as expected.
- After implementation, stale helper tests and the existing OpenMHz redaction guard failed; both were fixed and reverified.

## Verification

- `safe-run -- uv run python -m pytest backend/pipeline/ingestion/collectors/tests/test_bcfy_calls_collector.py backend/pipeline/ingestion/collectors/tests/test_fire_notifications_collector.py backend/pipeline/ingestion/collectors/tests/test_icecast_collector.py backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py backend/pipeline/ingestion/collectors/tests/test_aiohttp_requests.py -q -n 0` - 188 passed, 18 subtests passed.
- `safe-run -- uv run python -m pytest backend/pipeline/ingestion/collectors/tests/test_collector_semantics_helpers.py backend/pipeline/ingestion/collectors/tests/test_collector_failure_semantics_regression.py -q -n 0` - 32 passed, 3 subtests passed.
- `safe-run -- uv run python -m pytest backend/pipeline/ingestion/collectors/tests/test_bcfy_calls_collector.py backend/pipeline/ingestion/collectors/tests/test_fire_notifications_collector.py backend/pipeline/ingestion/collectors/tests/test_icecast_collector.py backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py backend/pipeline/ingestion/collectors/tests/test_aiohttp_requests.py backend/pipeline/ingestion/collectors/tests/test_failure_classification.py backend/pipeline/ingestion/tests/test_failure_policy.py -q -n 0` - 207 passed, 41 subtests passed.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Producer split mappings are ready for `05-02` runtime `_PipelineFailure` policy execution work. Phase 6 still owns OpenAPI/frontend/generated compatibility.

## Self-Check: PASSED

Plan tasks are complete, focused verification passed, and deviations are documented.

---
*Phase: 05-producer-and-runtime-routing-merge*
*Completed: 2026-06-15*

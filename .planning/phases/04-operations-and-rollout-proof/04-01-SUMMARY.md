---
phase: 04-operations-and-rollout-proof
plan: 01
subsystem: observability
tags: [python, fastapi, cloud-logging, pubsub, relay, pytest]

requires:
  - phase: 03-webhook-relay-delivery
    provides: Stateless Feed Audit Notification relay and WD client delivery logs
provides:
  - Structured relay warning logs for malformed Pub/Sub messages
  - Structured relay warning logs for missing WD client configuration
  - Caplog contract tests proving relay_event queryability and log sanitization
affects: [operations-and-rollout-proof, feed-audit-webhook, cloud-logging]

tech-stack:
  added: []
  patterns:
    - FastAPI endpoint warnings use extra={"json_fields": ...} for Cloud Logging jsonPayload fields
    - Relay operational log fields stay low-cardinality and omit payload snapshots and API keys

key-files:
  created:
    - .planning/phases/04-operations-and-rollout-proof/04-01-SUMMARY.md
  modified:
    - backend/pipeline/feed_audit_webhook/main.py
    - backend/pipeline/feed_audit_webhook/tests/test_main.py

key-decisions:
  - "Kept malformed Pub/Sub and missing-client paths on existing non-2xx responses so Pub/Sub retry/DLQ behavior is unchanged."
  - "Defined the structured warning dictionaries as module constants so acceptance greps and formatting remain stable."

patterns-established:
  - "Endpoint operational warnings carry relay_event and failure_class through json_fields."
  - "Endpoint log contract tests capture json_fields and assert sensitive payload/API-key markers are absent."

requirements-completed: [OPS-01]

duration: 4min
completed: 2026-06-27
---

# Phase 04 Plan 01: Relay Structured Operational Log Hardening Summary

**Malformed Pub/Sub and missing-client relay failures now emit safe, queryable Cloud Logging jsonPayload fields without changing ACK/NACK behavior.**

## Performance

- **Duration:** 4 min
- **Started:** 2026-06-27T05:17:47Z
- **Completed:** 2026-06-27T05:21:55Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments

- Added RED caplog tests for the two relay endpoint warning paths.
- Added `relay_event` and `failure_class` structured fields for malformed Pub/Sub and missing WD client failures.
- Verified no WD API key, `before_values`, or `after_values` content is exposed by the new endpoint logs.

## Task Commits

Each task was committed atomically:

1. **Task 1: Add endpoint log contract tests** - `f525b6e6` (test)
2. **Task 2: Emit structured warning fields in relay endpoint** - `0dd2290b` (feat)

## Files Created/Modified

- `backend/pipeline/feed_audit_webhook/main.py` - Adds structured `json_fields` for the existing invalid Pub/Sub and missing WD client warnings.
- `backend/pipeline/feed_audit_webhook/tests/test_main.py` - Adds caplog coverage for `relay_event` values and sensitive marker exclusion.
- `.planning/phases/04-operations-and-rollout-proof/04-01-SUMMARY.md` - Captures execution results.

## Verification

- `safe-run -- uv run python -m pytest backend/pipeline/feed_audit_webhook/tests/test_main.py -q` failed in RED with the two expected missing-`relay_event` assertions.
- `safe-run -- uv run python -m pytest backend/pipeline/feed_audit_webhook/tests/test_main.py backend/pipeline/feed_audit_webhook/tests/test_wd_client.py -q` passed: 20 passed, 16 existing Starlette/httpx deprecation warnings.
- `rg -n 'feed_audit_webhook_invalid_pubsub_message|feed_audit_webhook_client_not_initialized|caplog|json_fields' backend/pipeline/feed_audit_webhook/tests/test_main.py` passed.
- `rg -n 'relay_event.*feed_audit_webhook_invalid_pubsub_message|failure_class.*malformed_pubsub_message|relay_event.*feed_audit_webhook_client_not_initialized|failure_class.*configuration_error' backend/pipeline/feed_audit_webhook/main.py` passed.
- `git diff --check` passed.

## TDD Gate Compliance

- RED: `f525b6e6` added failing endpoint log contract tests.
- GREEN: `0dd2290b` added the structured warning fields and made the targeted suite pass.
- REFACTOR: None needed.

## Decisions Made

- Used `relay_event` values exactly as planned: `feed_audit_webhook_invalid_pubsub_message` and `feed_audit_webhook_client_not_initialized`.
- Used low-cardinality `failure_class` values exactly as planned: `malformed_pubsub_message` and `configuration_error`.
- Preserved existing HTTP 400, HTTP 503, and WD client delivery behavior.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- The optional `../.github/instructions/PYTHON_STYLE.instructions.md` path was not present from this worktree. Repository `AGENTS.md` and `.agents/instructions.md` were present and followed.
- The local Node SDK path was not installed, so state context was loaded through the available `gsd-sdk` CLI fallback.

## Known Stubs

None. Stub scan matches were limited to test fixtures, empty payload assertions, and optional `None` defaults.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Ready for Plan 04-02. Relay endpoint logs now cover the non-WD failure classes needed by OPS-01 while keeping routing, retry, and DLQ ownership unchanged.

## Self-Check: PASSED

- Found key files on disk: `backend/pipeline/feed_audit_webhook/main.py`, `backend/pipeline/feed_audit_webhook/tests/test_main.py`, and this summary.
- Found task commits: `f525b6e6` and `0dd2290b`.
- Confirmed `.planning/STATE.md` and `CONTEXT.md` were pre-existing unowned modifications and were not staged.

---
*Phase: 04-operations-and-rollout-proof*
*Completed: 2026-06-27*

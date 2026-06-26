---
phase: 01-audit-contract-and-emission
plan: 01
subsystem: storage
tags: [postgres, sql, audit, jsonb, pytest]

requires: []
provides:
  - Canonical schema version 1 Feed Audit Notification SQL payload builder.
  - Nullable feed_audit_event result column from audited async feed SQL.
  - Nullable feed_audit_event result column from audited sync feed SQL.
  - SQL contract tests for flat payload shape and audited query coverage.
affects:
  - 01-02 shared notification logging helper
  - 01-03 async and sync store integration
  - Phase 2 Cloud Logging and Pub/Sub routing

tech-stack:
  added: []
  patterns:
    - Shared JSONB payload expression returned from write_audit CTEs.
    - LEFT JOIN write_audit ON TRUE to preserve no-op rows with null payloads.

key-files:
  created:
    - .planning/phases/01-audit-contract-and-emission/01-01-SUMMARY.md
  modified:
    - backend/pipeline/storage/feed_audit_sql.py
    - backend/pipeline/storage/feed_queries.py
    - backend/pipeline/storage/sync_feed_queries.py
    - backend/pipeline/storage/tests/test_feed_query_contracts.py

key-decisions:
  - "Build feed_audit_event from write_audit RETURNING values using one shared SQL helper."
  - "Expose feed_audit_event as one nullable JSONB result column on audited async and sync SQL."
  - "Preserve DELETE_FEED_SQL feed_id in write_audit RETURNING for child-delete CTEs while also returning the payload."

patterns-established:
  - "Feed Audit Notification payloads are flat JSONB built from inserted feed_audit_events columns."
  - "Suppressed/no-op audit paths keep their normal result row and surface feed_audit_event as NULL."

requirements-completed:
  - AUDIT-01
  - AUDIT-02
  - AUDIT-05
  - PAYLOAD-01
  - PAYLOAD-02
  - PAYLOAD-03
  - PAYLOAD-04

duration: 6min
completed: 2026-06-26
---

# Phase 1 Plan 1: SQL Payload Contract Summary

**Storage SQL now returns the exact flat Feed Audit Notification v1 JSONB payload as one nullable `feed_audit_event` column.**

## Performance

- **Duration:** 6 min
- **Started:** 2026-06-26T22:31:38Z
- **Completed:** 2026-06-26T22:37:31Z
- **Tasks:** 3
- **Files modified:** 4

## Accomplishments

- Added `feed_audit_event_payload_sql()` with `event_type="radio_transcription.feed_audit_notification"`, `schema_version=1`, and only inserted audit-row fields.
- Wired all audited async feed SQL constants to return `write_audit.feed_audit_event`, including no-op/suppressed rows as NULL.
- Wired all audited sync feed SQL constants to return the same nullable payload while preserving `%s` parameter style.
- Expanded pure SQL contract tests for payload shape, async coverage, sync coverage, and delete child-CTE feed ID preservation.

## Task Commits

1. **Task 1 RED: Payload contract test** - `cb1b9cb9` (test)
2. **Task 1 GREEN: Payload SQL builder** - `b20e4c69` (feat)
3. **Task 2 RED: Async SQL contracts** - `2909b212` (test)
4. **Task 2 GREEN: Async SQL feed_audit_event** - `1c792a43` (feat)
5. **Task 3 RED: Sync SQL contracts** - `9777039e` (test)
6. **Task 3 GREEN: Sync SQL feed_audit_event** - `501c6cd9` (feat)

## Files Created/Modified

- `backend/pipeline/storage/feed_audit_sql.py` - Adds notification constants and the canonical JSONB payload helper.
- `backend/pipeline/storage/feed_queries.py` - Returns nullable `feed_audit_event` from all audited async feed write statements.
- `backend/pipeline/storage/sync_feed_queries.py` - Returns nullable `feed_audit_event` from all audited sync feed write statements.
- `backend/pipeline/storage/tests/test_feed_query_contracts.py` - Pins payload shape and async/sync SQL result-column coverage.

## Decisions Made

- Used `write_audit RETURNING` as the only source for notification payload fields, matching the inserted audit row rather than request-local inputs.
- Used `LEFT JOIN write_audit` in final result selects so suppressed audit paths preserve their normal row with `feed_audit_event = NULL`.
- Kept delete child-delete CTE behavior by returning both `feed_id` and `feed_audit_event` from `write_audit`.

## Verification

- `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_query_contracts.py::TestFeedAuditEventSqlContract -q` - passed, 8 tests and 12 subtests.
- `safe-run -- uv run ruff format --check backend/pipeline/storage/feed_audit_sql.py backend/pipeline/storage/feed_queries.py backend/pipeline/storage/sync_feed_queries.py backend/pipeline/storage/tests/test_feed_query_contracts.py` - passed.
- `git diff --check` - passed.
- Required `rg` acceptance checks for helper existence and `feed_audit_event` result usage passed.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## Known Stubs

None. Stub scan found only pre-existing TODO comments in `feed_queries.py` unrelated to this plan.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Plan 01-02 can consume the nullable `feed_audit_event` column from async and sync store results and emit it through the shared failure-isolated logging helper.

## Self-Check: PASSED

- Verified summary and all key modified files exist.
- Verified all six task commits exist in git history.
- Verified no task commit deleted tracked files unexpectedly.

---
*Phase: 01-audit-contract-and-emission*
*Completed: 2026-06-26*

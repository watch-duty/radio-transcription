---
phase: 02-transactional-storage-writes
plan: "01"
subsystem: database
tags: [alloydb, feed-audit, sql, pytest, storage]

requires:
  - phase: 01-contract-and-schema-foundation
    provides: Feed Audit Event contract, actor vocabulary, and schema foundation
provides:
  - Actor vocabulary cleanup removing the rejected system actor namespace
  - Idempotent actor-constraint replacement migration for already-applied schemas
  - Storage-owned audit SQL primitives for snapshots, sequence allocation, and inserts
affects: [02-transactional-storage-writes, feedstore, audit-writers]

tech-stack:
  added: []
  patterns:
    - Text-level SQL contract tests for audit query invariants
    - Counter-table feed audit sequence allocation with ON CONFLICT

key-files:
  created:
    - terraform/modules/alloydb/sql/ingestion/030_feed_audit_events_actor_constraint.sql
  modified:
    - documentation/feed-audit-events.md
    - terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql
    - backend/pipeline/storage/feed_queries.py
    - backend/pipeline/storage/tests/test_feed_audit_contract.py
    - backend/pipeline/storage/tests/test_feed_store.py

key-decisions:
  - "Removed the rejected system: actor namespace before Phase 2 emits storage audit rows."
  - "Fail closed on legacy system:% audit rows before replacing already-applied actor constraints."
  - "Use feed_audit_event_sequences as the storage-owned sequence allocator instead of deriving order from existing audit rows."

patterns-established:
  - "Audit snapshot SQL projects an explicit maintained allowlist and locks the target feed row."
  - "Audit sequence allocation uses INSERT ... ON CONFLICT against feed_audit_event_sequences."
  - "Text-level storage tests reject raw snapshot projections and race-prone sequence allocation."

requirements-completed: [AUD-04, CON-03, CON-04]

duration: 5 min
completed: 2026-06-19
---

# Phase 02 Plan 01: Contract/Schema Cleanup and Audit SQL Foundation Summary

**Actor vocabulary cleanup plus storage-owned SQL primitives for feed audit snapshots, sequence allocation, and audit inserts**

## Performance

- **Duration:** 5 min
- **Started:** 2026-06-19T14:15:18Z
- **Completed:** 2026-06-19T14:20:36Z
- **Tasks:** 3
- **Files modified:** 6

## Accomplishments

- Removed `system:` from the documented and fresh-schema accepted actor prefixes.
- Added migration `030_feed_audit_events_actor_constraint.sql` to reject legacy `system:%` rows before recreating the actor constraint without that prefix.
- Added `GET_AUDIT_FEED_SNAPSHOT_SQL`, `ALLOCATE_FEED_AUDIT_SEQUENCE_SQL`, and `INSERT_FEED_AUDIT_EVENT_SQL` in storage.
- Added focused contract tests for actor cleanup and feed audit SQL invariants.

## Task Commits

Each task was committed atomically:

1. **Task 1: Remove the rejected actor namespace from contract and schema** - `59be5821` (fix)
2. **Task 2: Add audit SQL primitives for snapshots, sequence allocation, and inserts** - `e3255bb9` (feat)
3. **Task 3: Lock the query contract with focused unit tests** - `a17ec5d3` (test)

**Plan metadata:** Recorded in final docs commit.

## Files Created/Modified

- `documentation/feed-audit-events.md` - Removed the rejected actor prefix and narrowed `gcp-sa:` fallback wording.
- `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql` - Removed `system:%` from the fresh actor constraint.
- `terraform/modules/alloydb/sql/ingestion/030_feed_audit_events_actor_constraint.sql` - Added fail-closed replacement migration for already-applied schemas.
- `backend/pipeline/storage/feed_queries.py` - Added snapshot, sequence allocation, and audit insert SQL constants.
- `backend/pipeline/storage/tests/test_feed_audit_contract.py` - Updated actor vocabulary contract tests and added replacement migration checks.
- `backend/pipeline/storage/tests/test_feed_store.py` - Added text-level audit SQL contract tests.

## Decisions Made

- Removed `system:` from accepted actor forms before any Phase 2 storage writer emits rows.
- Made already-applied schema cleanup fail closed if legacy `system:%` audit rows exist.
- Kept audit SQL construction storage-owned and parameter-based, with no service-supplied event payload shape.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Added missing normalized SQL test helper**
- **Found during:** Task 3 (Lock the query contract with focused unit tests)
- **Issue:** The new `TestFeedAuditSql` tests called `_normalized_sql`, but `test_feed_store.py` only had `_sql_without_comments`.
- **Fix:** Added `_normalized_sql` beside the existing SQL helper.
- **Files modified:** `backend/pipeline/storage/tests/test_feed_store.py`
- **Verification:** `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestFeedAuditSql backend/pipeline/storage/tests/test_feed_audit_contract.py -q`
- **Committed in:** `a17ec5d3`

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** The fix was limited to test infrastructure required by the planned contract checks.

## Issues Encountered

- Initial Task 3 targeted pytest run failed with `NameError: name '_normalized_sql' is not defined`; resolved by the Rule 1 auto-fix above.

## Authentication Gates

None.

## Known Stubs

None. Stub scan found only pre-existing TODO comments and empty-list test fixtures, not placeholders that affect this plan's goal.

## Verification

- `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_audit_contract.py -q` - passed, 8 tests.
- `safe-run -- uv run python -m py_compile backend/pipeline/storage/feed_queries.py` - passed.
- `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestFeedAuditSql backend/pipeline/storage/tests/test_feed_audit_contract.py -q` - passed, 12 tests.
- `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_audit_contract.py backend/pipeline/storage/tests/test_feed_store.py::TestFeedAuditSql -q` - passed, 12 tests.
- `git diff --check -- .planning/phases/02-transactional-storage-writes/02-01-PLAN.md documentation/feed-audit-events.md terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql terraform/modules/alloydb/sql/ingestion/030_feed_audit_events_actor_constraint.sql backend/pipeline/storage/feed_queries.py backend/pipeline/storage/tests/test_feed_audit_contract.py backend/pipeline/storage/tests/test_feed_store.py` - passed.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Ready for `02-02-PLAN.md`. Downstream storage writer work can call the snapshot, sequence allocation, and insert SQL primitives without relying on service-owned audit payload construction.

## Self-Check: PASSED

- Created files exist: `02-01-SUMMARY.md` and `030_feed_audit_events_actor_constraint.sql`.
- Modified task files exist: documentation, migration, SQL query, and storage test files.
- Task commits found: `59be5821`, `e3255bb9`, `a17ec5d3`.

---
*Phase: 02-transactional-storage-writes*
*Completed: 2026-06-19*

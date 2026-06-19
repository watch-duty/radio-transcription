---
phase: 01-contract-and-schema-foundation
plan: 02
subsystem: database
tags: [alloydb, postgresql, sql, audit, hot-protection]

requires: []
provides:
  - ordered feed audit event schema migration
  - bounded current feed diagnostic detail column
  - feed audit event per-feed sequence foundation
  - HOT guard coverage for status_reason_detail
affects:
  - 01-contract-and-schema-foundation
  - 02-transactional-storage-writes
  - 04-runtime-event-integration
  - 05-retention-and-verification-hardening

tech-stack:
  added: []
  patterns:
    - idempotent ordered AlloyDB SQL migration
    - DO-block guarded SQL constraints
    - OID-based HOT protection guard

key-files:
  created:
    - terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql
  modified:
    - terraform/modules/alloydb/sql/ci/hot_protection_check.sql

key-decisions:
  - "feed_audit_events stores feed identity without a cascading feeds foreign key so audit history survives hard delete."
  - "actor_id remains one required namespaced string with exact unknown:unknown fallback and non-empty stable IDs for all namespaced prefixes."
  - "feeds.status_reason_detail is bounded to 2048 characters and remains unindexed as mutable current-state diagnostic data."

patterns-established:
  - "Use feed_audit_event_sequences as the schema foundation for later transactional per-feed sequence allocation."
  - "Guard mutable feeds columns in hot_protection_check.sql instead of indexing them for current-state access."

requirements-completed: [AUD-02, AUD-03, DIAG-01, ACT-01]

duration: 4min
completed: 2026-06-19
---

# Phase 01 Plan 02: SQL Migration and HOT Guard Schema Foundation Summary

**Delete-safe Feed Audit Event schema with bounded diagnostic detail, namespaced actor constraints, per-feed ordering, and HOT guard coverage.**

## Performance

- **Duration:** 4 min
- **Started:** 2026-06-19T04:59:43Z
- **Completed:** 2026-06-19T05:03:29Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments

- Added `029_feed_audit_events.sql` as the ordered migration after `028_initialize_feed_bookmarks.sql`.
- Added `feeds.status_reason_detail` with the `feeds_status_reason_detail_length` 2048-character constraint while preserving `quarantine_reason`.
- Created `feed_audit_event_sequences` and `feed_audit_events` without a `feeds` foreign key or cascade.
- Added action, actor, sequence, detail-length, and `(feed_id, feed_sequence)` uniqueness constraints for audit rows.
- Added timeline/actor audit indexes and extended the HOT guard so `feeds.status_reason_detail` must remain unindexed.

## Task Commits

Each task was committed atomically:

1. **Task 1: Add the ordered feed audit events migration** - `9e8ed4f6` (feat)
2. **Task 2: Extend the HOT guard for status_reason_detail** - `71e34963` (fix)

## Files Created/Modified

- `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql` - Adds bounded current diagnostic detail, sequence-counter foundation, audit table, constraints, and audit indexes.
- `terraform/modules/alloydb/sql/ci/hot_protection_check.sql` - Adds `status_reason_detail` to the guarded mutable `feeds` column list.

## Decisions Made

- Followed the plan's delete-survival requirement by storing audit feed identity as data and not adding `REFERENCES feeds(id)` or `ON DELETE CASCADE`.
- Kept actor attribution to one required `actor_id` string with exact `unknown:unknown` fallback and non-empty suffix checks for every allowed namespace.
- Treated `status_reason_detail` as mutable current-state detail: bounded, nullable, compatibility-preserving, and not indexed.

## Verification

- `test -f terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql`
- `rg -q 'CREATE TABLE IF NOT EXISTS feed_audit_events' terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql`
- `rg -q 'feed_audit_events_feed_sequence_unique' terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql`
- `rg -q 'feed_audit_event_sequences' terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql`
- Verified all actor namespaces have paired non-empty suffix checks.
- Verified non-comment migration SQL contains no `REFERENCES feeds`, `ON DELETE CASCADE`, `pg_cron`, `dispatcher`, `webhook`, or `DROP COLUMN quarantine_reason`.
- `rg -q "'status_reason_detail'" terraform/modules/alloydb/sql/ci/hot_protection_check.sql`
- `rg -q "c\\.oid = x\\.indexrelid" terraform/modules/alloydb/sql/ci/hot_protection_check.sql`
- Verified no `status_reason_detail` HOT guard exception was added.
- `git diff --check -- terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql terraform/modules/alloydb/sql/ci/hot_protection_check.sql`

No live database was required by this plan.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Escaped the actor-prefix verification loop variable**
- **Found during:** Task 1 (Add the ordered feed audit events migration)
- **Issue:** The plan's nested `bash -lc` actor-prefix verification expanded `$prefix` in the outer shell before the inner loop ran, producing an empty search pattern.
- **Fix:** Re-ran the equivalent verification with `\$prefix` escaped so each namespace suffix-length check was verified.
- **Files modified:** None
- **Verification:** Corrected loop passed for `user:google:`, `user-email:`, `service:`, `system:`, `job:`, and `gcp-sa:`.
- **Committed in:** N/A - verification command only

---

**Total deviations:** 1 auto-fixed (Rule 3)
**Impact on plan:** Verification-only adjustment. No schema scope changed.

## Issues Encountered

The actor-prefix verification command needed shell escaping when executed from this environment. The SQL itself already contained the required checks, and the corrected verification passed.

## Known Stubs

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Ready for Plan 01-03 text-level contract verification tests. Later storage/runtime phases can build on the `feed_audit_events` schema and `feed_audit_event_sequences` ordering foundation without changing Phase 1 boundaries.

## Self-Check: PASSED

- Found `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql`.
- Found `.planning/phases/01-contract-and-schema-foundation/01-02-SUMMARY.md`.
- Found task commit `9e8ed4f6`.
- Found task commit `71e34963`.

---
*Phase: 01-contract-and-schema-foundation*
*Completed: 2026-06-19*

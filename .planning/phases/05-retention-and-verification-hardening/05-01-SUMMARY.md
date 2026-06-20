---
phase: 05-retention-and-verification-hardening
plan: "01"
subsystem: database
tags: [alloydb, postgres, pg_cron, retention, audit, pytest]

requires:
  - phase: 04-runtime-event-integration
    provides:
      - Runtime and Echo audit event behavior to retain and verify.
provides:
  - Extension-free feed audit retention procedure.
  - AlloyDB pg_cron daily scheduler registration.
  - Static retention contract tests and present-tense documentation.
affects:
  - phase-05-retention-and-verification-hardening
  - feed-audit-events
  - alloydb-migrations

tech-stack:
  added: []
  patterns:
    - Extension-free SQL helper plus separate pg_cron scheduler migration.
    - Static SQL/documentation contract tests for retention invariants.

key-files:
  created:
    - terraform/modules/alloydb/sql/ingestion/031_feed_audit_event_retention.sql
    - terraform/modules/alloydb/sql/ingestion/032_feed_audit_events_pg_cron_retention.sql
    - .planning/phases/05-retention-and-verification-hardening/05-01-SUMMARY.md
  modified:
    - backend/pipeline/storage/tests/test_feed_audit_contract.py
    - documentation/feed-audit-events.md

key-decisions:
  - "Feed audit retention is enforced by an extension-free procedure scheduled through a separate AlloyDB pg_cron migration."
  - "Retention expires rows by occurred_at, one LIMIT 10000 batch per run, and preserves immutable feed_sequence labels with expected gaps."
  - "Sequence rows are pruned only when there is no current feeds row and no retained feed_audit_events row."

patterns-established:
  - "Retention scheduler split: executable SQL stays outside *pg_cron* files so local schema helpers can apply it."
  - "Contract tests assert both required retention tokens and forbidden archival, tombstone, baseline, cutoff, and sequence-renumbering behavior."

requirements-completed: [AUD-05, VER-04]

duration: 9 min
completed: 2026-06-20
---

# Phase 05 Plan 01: Retention SQL Scheduler and Static Contract Coverage Summary

**18-month Feed Audit Events retention now has a DB-owned bounded procedure, daily AlloyDB pg_cron schedule, and static contract coverage.**

## Performance

- **Duration:** 9 min
- **Started:** 2026-06-20T03:43:44Z
- **Completed:** 2026-06-20T03:52:47Z
- **Tasks:** 3
- **Files modified:** 5

## Accomplishments

- Added `public.prune_feed_audit_events_retention()` as an extension-free retention procedure that deletes expired `feed_audit_events` rows by `occurred_at < NOW() - INTERVAL '18 months'` in one bounded batch.
- Added the AlloyDB-only `feed-audit-events-retention` pg_cron scheduler at `15 3 * * *`, calling the procedure without duplicating delete SQL.
- Updated documentation and static contract tests so retention behavior, expected `feed_sequence` gaps, and safe `feed_audit_event_sequences` cleanup are locked in.

## Task Commits

1. **Task 1: Create extension-free retention procedure** - `fa8027e0` (feat)
2. **Task 2: Register daily AlloyDB pg_cron scheduler** - `c28ef751` (feat)
3. **Task 3: Update retention contract documentation and static tests** - `23d9b402` (test)

## Files Created/Modified

- `terraform/modules/alloydb/sql/ingestion/031_feed_audit_event_retention.sql` - Defines the extension-free bounded retention procedure and sequence pruning.
- `terraform/modules/alloydb/sql/ingestion/032_feed_audit_events_pg_cron_retention.sql` - Registers the daily AlloyDB pg_cron retention job.
- `backend/pipeline/storage/tests/test_feed_audit_contract.py` - Adds static migration and documentation retention tests.
- `documentation/feed-audit-events.md` - Documents enforced Phase 5 retention in present tense.
- `.planning/phases/05-retention-and-verification-hardening/05-01-SUMMARY.md` - Records execution results.

## Verification

- `safe-run -- uv run python -m py_compile backend/pipeline/common/test_schema_helper.py` - passed for Tasks 1 and 2.
- `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_audit_contract.py -q` - passed, `10 passed in 0.59s`.
- `git diff --check` - passed.
- Task acceptance `rg` scans passed for required retention/scheduler tokens and forbidden strings.

## Decisions Made

- Used a two-migration split so local and Testcontainers schema helpers can execute retention semantics while still skipping the AlloyDB-only `pg_cron` scheduler file.
- Kept retention as delete-only SQL; it does not archive, redact, rewrite, synthesize baseline/tombstone events, or renumber `feed_sequence`.
- Skipped shared `STATE.md`, `ROADMAP.md`, and requirements updates because this execution is in a worktree and the orchestrator owns shared tracking after the wave.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed documentation token wrapping for static contract tests**
- **Found during:** Task 3 (Update retention contract documentation and static tests)
- **Issue:** Initial documentation wording split required exact contract phrases across Markdown line breaks, causing the new static documentation test to fail.
- **Fix:** Rewrote the retention sentences so `no retained feed_audit_events rows`, `gaps are expected`, and `oldest non-expired event` are contiguous.
- **Files modified:** `documentation/feed-audit-events.md`
- **Verification:** `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_audit_contract.py -q` passed with `10 passed`.
- **Committed in:** `23d9b402`

---

**Total deviations:** 1 auto-fixed (Rule 1 bug)
**Impact on plan:** The auto-fix made the planned static documentation contract enforceable. No scope expansion.

## Issues Encountered

- A concurrent 05-02 commit briefly changed the tracking state for `032_feed_audit_events_pg_cron_retention.sql` while Task 2 was in progress. Current HEAD was rechecked, the scheduler file was staged by path, and Task 2 was committed atomically as `c28ef751`.

## Known Stubs

None. The stub scan found only existing prose that says a Google subject claim may be "not available"; no placeholder or unwired implementation stubs were introduced.

## User Setup Required

None.

## Next Phase Readiness

The static retention contract is ready for the remaining Phase 5 verification and DB-backed retention semantics plans. The local run intentionally avoided Docker/Testcontainers and broad integration lanes per repository safety rules.

## Self-Check: PASSED

- Created files exist: `031_feed_audit_event_retention.sql`, `032_feed_audit_events_pg_cron_retention.sql`, and `05-01-SUMMARY.md`.
- Task commits found: `fa8027e0`, `c28ef751`, and `23d9b402`.
- `git diff --check` passed before the metadata commit.

---
*Phase: 05-retention-and-verification-hardening*
*Completed: 2026-06-20*

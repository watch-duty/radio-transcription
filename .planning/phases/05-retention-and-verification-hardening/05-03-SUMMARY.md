---
phase: 05-retention-and-verification-hardening
plan: "03"
subsystem: testing
tags: [alloydb, postgres, testcontainers, retention, pg_cron, pytest]

requires:
  - phase: 05-retention-and-verification-hardening
    provides:
      - Extension-free feed audit retention procedure from 05-01.
      - Static retention and scheduler contract coverage from 05-01.
provides:
  - DB-backed retention procedure semantics tests for prepared Testcontainers execution.
  - Explicit pending UAT commands for resource-heavy Testcontainers and AlloyDB pg_cron lanes.
  - Low-resource local verification results for the retention integration test source.
affects:
  - phase-05-retention-and-verification-hardening
  - feed-audit-events
  - storage-integration-tests

tech-stack:
  added: []
  patterns:
    - Manual DB seeding for retention integration tests to avoid extra audit rows from storage writers.
    - Prepared-machine checkpoint lanes recorded as pending UAT when local Docker/credentials are unavailable.

key-files:
  created:
    - .planning/phases/05-retention-and-verification-hardening/05-03-SUMMARY.md
  modified:
    - integration_tests/storage/test_feed_store_integration.py

key-decisions:
  - "Retention integration setup seeds audit rows directly after _insert_feed(...) so the tests avoid current-time store.create_feed audit rows."
  - "Local execution did not start Docker/Testcontainers or prepared AlloyDB pg_cron verification; both lanes remain pending CI/prepared-machine UAT."

patterns-established:
  - "Retention semantics tests assert sequence gaps and sequence-row cleanup without expecting feed_sequence renumbering."
  - "Resource-heavy verification lanes are documented with exact safe-run commands instead of being run in an unapproved local worktree."

requirements-completed: [AUD-05, VER-02, VER-04]

duration: 3 min
completed: 2026-06-20
---

# Phase 05 Plan 03: DB-Backed Retention Semantics and Prepared-Machine Verification Summary

**Retention semantics now have prepared Testcontainers integration coverage for expiry, sequence gaps, live/deleted-feed sequence preservation, and orphan pruning.**

## Performance

- **Duration:** 3 min execution window after context load
- **Started:** 2026-06-20T03:58:42Z
- **Completed:** 2026-06-20T04:01:13Z
- **Tasks:** 1 completed locally, 2 checkpoint lanes pending UAT
- **Files modified:** 2

## Accomplishments

- Added four storage integration tests that call `CALL public.prune_feed_audit_events_retention()` against manually seeded audit rows.
- Covered expired audit-row deletion, retained event survival with a `[2]` sequence gap, live-feed sequence preservation after all events expire, deleted-feed sequence preservation while retained history exists, and orphan sequence pruning after the last audit row expires.
- Kept local verification to low-resource compile/static checks and recorded the resource-heavy lanes as pending prepared-machine or CI UAT.

## Task Commits

1. **Task 1: Add retention procedure integration tests** - `559674de` (test)
2. **Task 2: Verify prepared-machine Testcontainers retention lane** - pending UAT, no local commit
3. **Task 3: Verify prepared AlloyDB pg_cron scheduler lane** - pending UAT, no local commit

## Files Created/Modified

- `integration_tests/storage/test_feed_store_integration.py` - Added DB-backed retention procedure test cases for deletion, sequence gaps, live/deleted feed sequence rows, and orphan sequence cleanup.
- `.planning/phases/05-retention-and-verification-hardening/05-03-SUMMARY.md` - Records local verification, pending UAT lanes, and execution outcome.

## Verification

- `safe-run -- uv run python -m py_compile integration_tests/storage/test_feed_store_integration.py` - passed.
- `rg -n "test_retention_prunes_expired_events_and_preserves_retained_sequence_gap|test_retention_keeps_sequence_for_live_feed_after_all_events_expire|test_retention_keeps_deleted_feed_sequence_while_retained_audit_events_exist|test_retention_prunes_orphan_sequence_after_last_audit_event_expires" integration_tests/storage/test_feed_store_integration.py` - passed; all four tests found.
- `rg -n "CALL public\\.prune_feed_audit_events_retention\\(\\)|INTERVAL '19 months'|INTERVAL '17 months'|next_sequence = 4|next_sequence = 5|\\[2\\]" integration_tests/storage/test_feed_store_integration.py` - passed; required retention tokens found.
- `rg -n "range\\(1, n \\+ 1\\)|MAX\\(feed_sequence\\) \\+ 1|UPDATE feed_audit_events SET feed_sequence" integration_tests/storage/test_feed_store_integration.py` - passed with no matches.
- `sed -n '1902,2066p' integration_tests/storage/test_feed_store_integration.py | rg -n "store\\.create_feed|_insert_feed\\("` - passed; the new block uses `_insert_feed(...)` and has no `store.create_feed(...)`.
- `git diff --check` - passed.

## Pending UAT / Checkpoints

### Task 2: Prepared-Machine Testcontainers Retention Lane

**Status:** Pending CI/prepared-machine verification. Not run locally because this lane starts Docker/Testcontainers and local prepared-machine approval is unavailable in this execution context.

Run only on an approved prepared machine or in CI:

```bash
safe-run -- uv run python -m pytest integration_tests/storage/test_feed_store_integration.py -q -n 0 -k "retention_prunes_expired_events_and_preserves_retained_sequence_gap or retention_keeps_sequence_for_live_feed_after_all_events_expire or retention_keeps_deleted_feed_sequence_while_retained_audit_events_exist or retention_prunes_orphan_sequence_after_last_audit_event_expires or create_feed_audit_failure_rolls_back_feed_and_sequence or update_feed_audit_failure_rolls_back_state_and_sequence or concurrent_same_feed_updates_allocate_contiguous_sequences or delete_feed_succeeds"
```

Expected result: pytest exits 0, output includes `passed`, and no tests fail.

### Task 3: Prepared AlloyDB pg_cron Scheduler Lane

**Status:** Pending CI/prepared-machine verification. Not run locally because an AlloyDB database with `alloydb.enable_pg_cron=on` and `ALLOYDB_PG_CRON_VERIFICATION_DSN` credentials is unavailable in this execution context.

Run only against a disposable/prepared AlloyDB or CI database with pg_cron enabled:

```bash
safe-run -- bash -lc 'set -euo pipefail; : "${ALLOYDB_PG_CRON_VERIFICATION_DSN:?set to a disposable/prepared AlloyDB database with alloydb.enable_pg_cron=on}"; psql "$ALLOYDB_PG_CRON_VERIFICATION_DSN" -v ON_ERROR_STOP=1 -f terraform/modules/alloydb/sql/ingestion/031_feed_audit_event_retention.sql -f terraform/modules/alloydb/sql/ingestion/032_feed_audit_events_pg_cron_retention.sql; test "$(psql "$ALLOYDB_PG_CRON_VERIFICATION_DSN" -v ON_ERROR_STOP=1 -Atc "SELECT count(*) FROM pg_extension WHERE extname = '"'"'pg_cron'"'"';")" = "1"; test "$(psql "$ALLOYDB_PG_CRON_VERIFICATION_DSN" -v ON_ERROR_STOP=1 -Atc "SELECT count(*) FROM cron.job WHERE jobname = '"'"'feed-audit-events-retention'"'"' AND schedule = '"'"'15 3 * * *'"'"' AND command = '"'"'CALL public.prune_feed_audit_events_retention()'"'"';")" = "1"; test "$(psql "$ALLOYDB_PG_CRON_VERIFICATION_DSN" -v ON_ERROR_STOP=1 -Atc "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = '"'"'public'"'"' AND p.proname = '"'"'prune_feed_audit_events_retention'"'"' AND pg_get_functiondef(p.oid) LIKE '"'"'%occurred_at < NOW() - INTERVAL '"'"''"'"'18 months'"'"''"'"'%'"'"' AND pg_get_functiondef(p.oid) LIKE '"'"'%LIMIT 10000%'"'"' AND pg_get_functiondef(p.oid) LIKE '"'"'%FOR UPDATE SKIP LOCKED%'"'"';")" = "1"'
```

Expected result: command exits 0; proof includes `pg_cron`, one `feed-audit-events-retention` job, schedule `15 3 * * *`, command `CALL public.prune_feed_audit_events_retention()`, and the bounded retention procedure body tokens.

## Decisions Made

- Used direct `feed_audit_events` inserts in the new tests so retention scenarios can control exact `occurred_at` and `feed_sequence` values without storage-created current-time audit rows.
- Left Task 2 and Task 3 checkpoints incomplete by design for this run; the plan explicitly allows pending UAT when local approval or prepared AlloyDB verification is unavailable.
- Skipped shared `STATE.md`, `ROADMAP.md`, and requirements updates because this execution is in a worktree and the orchestrator owns shared tracking after the wave.

## Deviations from Plan

None - Task 1 executed as planned. Task 2 and Task 3 were handled according to the run-specific checkpoint instructions and remain pending UAT rather than being marked complete.

## Issues Encountered

- The local `node_modules/@gsd-build/sdk` CLI was not installed, so the read-only state load used the `gsd-sdk` CLI fallback on `PATH`.
- No implementation blockers were encountered for Task 1.

## Authentication Gates

None.

## Known Stubs

None. Stub-pattern scan found no placeholder, TODO, FIXME, empty hardcoded UI data, or unwired mock-data markers in the modified test file.

## Threat Flags

None. The plan modified tests and summary metadata only; no new endpoint, auth path, file access path, schema change, or trust-boundary production surface was introduced.

## Next Phase Readiness

Task 1 is ready for prepared-machine verification. The plan should not be considered fully verified until the Task 2 Testcontainers lane and Task 3 AlloyDB pg_cron scheduler lane pass in CI or an approved prepared environment.

## Self-Check: PASSED

- Summary file exists at `.planning/phases/05-retention-and-verification-hardening/05-03-SUMMARY.md`.
- Task 1 commit found: `559674de`.
- `git diff --check` passed after summary creation.

---
*Phase: 05-retention-and-verification-hardening*
*Completed: 2026-06-20*

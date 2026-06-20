---
phase: 05-retention-and-verification-hardening
verified: 2026-06-20T04:16:09Z
status: human_needed
score: "14/16 must-haves verified"
overrides_applied: 0
human_verification:
  - test: "Prepared-machine Testcontainers retention, rollback, concurrency, and delete-survival lane"
    expected: "The targeted safe-run pytest command from 05-03-SUMMARY exits 0 with passed tests and no failures."
    why_human: "This lane starts Docker/Testcontainers and is explicitly restricted to CI or an approved prepared machine by repository safety rules."
  - test: "Prepared AlloyDB pg_cron scheduler lane"
    expected: "Migrations 031 and 032 apply against prepared AlloyDB with pg_cron enabled; pg_extension contains pg_cron; cron.job has exactly one feed-audit-events-retention job with schedule 15 3 * * * and command CALL public.prune_feed_audit_events_retention(); the installed procedure body contains the bounded retention tokens."
    why_human: "This requires an external prepared AlloyDB database, alloydb.enable_pg_cron=on, and ALLOYDB_PG_CRON_VERIFICATION_DSN credentials."
---

# Phase 5: Retention and Verification Hardening Verification Report

**Phase Goal:** Feed audit events are retained for the required window and the implementation is proven against the v1 behavioral contract.
**Verified:** 2026-06-20T04:16:09Z
**Status:** human_needed
**Re-verification:** No - initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Audit rows are retained for 18 months and expired only through the approved retention mechanism. | VERIFIED | `031_feed_audit_event_retention.sql:1-38` defines only `public.prune_feed_audit_events_retention()`; docs describe DB-owned retention at `documentation/feed-audit-events.md:184-205`. |
| 2 | Retention cutoff uses `feed_audit_events.occurred_at < NOW() - INTERVAL '18 months'`. | VERIFIED | Procedure selects expired events with this exact predicate at `031_feed_audit_event_retention.sql:5-11`; static contract asserts it at `test_feed_audit_contract.py:283-295`. |
| 3 | Each retention run deletes one bounded batch and does not loop through backlog catch-up. | VERIFIED | Procedure uses `LIMIT 10000` and `FOR UPDATE SKIP LOCKED` at lines 10-11 and has no loop construct; docs state one bounded daily batch at `feed-audit-events.md:191-193`. |
| 4 | Retention deletes expired `feed_audit_events` rows only and does not archive, redact, rewrite, synthesize, or renumber events. | VERIFIED | Procedure only deletes from `feed_audit_events` and `feed_audit_event_sequences`; forbidden-token scans found no archive/tombstone/baseline/renumbering patterns. |
| 5 | Retained `feed_sequence` labels remain immutable and may contain gaps. | VERIFIED | Documentation states gaps are expected at `feed-audit-events.md:197-199`; integration test asserts retained sequence `[2]` after expiry at `test_feed_store_integration.py:1902-1953`. |
| 6 | `feed_audit_event_sequences` remains part of the ordering contract and is pruned only when no current feed and no retained audit event exists. | VERIFIED | Procedure guards pruning with `NOT EXISTS` against `public.feeds` and `public.feed_audit_events` at lines 20-29; tests cover live, deleted-with-history, and orphan-after-expiry cases at `test_feed_store_integration.py:1956-2066`. |
| 7 | The AlloyDB scheduler migration registers the named daily pg_cron job and calls the retention procedure. | VERIFIED | `032_feed_audit_events_pg_cron_retention.sql:5-11` creates `pg_cron` and schedules `feed-audit-events-retention` at `15 3 * * *` with `CALL public.prune_feed_audit_events_retention()`. Prepared DB execution remains a human item below. |
| 8 | Automated tests explicitly cover all eight v1 audit actions: create, update, deactivate, reset, delete, failure, quarantine, and recovery. | VERIFIED | Gate test enumerates the exact test names and action tokens at `test_feed_audit_v1_verification_gate.py:17-55`; matching behavioral tests exist in `test_feed_store.py`. |
| 9 | Automated tests explicitly cover diagnostic-detail lifecycle, public API migration away from `quarantine_reason`, and secret/detail bounding. | VERIFIED | Gate test reads lifecycle, service, API, and BFF test files at `test_feed_audit_v1_verification_gate.py:77-99`; grep confirms sanitizer and API/BFF assertions in the referenced files. |
| 10 | Automated tests explicitly cover clean heartbeat, clean progress, clean source-observation, and runtime/Echo no-default-audit paths. | VERIFIED | Gate test covers async and sync no-noise tokens at `test_feed_audit_v1_verification_gate.py:58-73` and `102-123`; matching tests exist in `test_feed_store.py`, `test_sync_feed_store.py`, and `test_feed_query_contracts.py`. |
| 11 | Verification stays in low-resource local lanes and does not add Docker/Testcontainers execution to the static gate. | VERIFIED | Gate file uses pure file reads/asserts only; low-resource spot check `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_audit_contract.py backend/pipeline/storage/tests/test_feed_audit_v1_verification_gate.py -q` passed with 14 tests. |
| 12 | DB-backed tests prove expired audit events are deleted while retained events survive. | VERIFIED | Integration tests seed 19-month and 17-month rows and call `CALL public.prune_feed_audit_events_retention()` at `test_feed_store_integration.py:1902-1953`. Execution of the Testcontainers lane remains a human item below. |
| 13 | DB-backed tests prove retained sequences are not renumbered and sequence rows are preserved/pruned per D-09 through D-11. | VERIFIED | Tests cover retained `[2]`, live-feed sequence preservation, deleted-feed retained-history preservation, and orphan sequence pruning at `test_feed_store_integration.py:1902-2066`. |
| 14 | Rollback and concurrent ordering coverage is part of the prepared-machine verification set. | HUMAN NEEDED | Existing integration tests are present at `test_feed_store_integration.py:1796`, `1835`, and `2072`; 05-03-SUMMARY records the exact prepared-machine command. The command was intentionally not run locally. |
| 15 | Testcontainers execution is explicit and checkpointed per D-13 and repository host-safety rules. | VERIFIED | 05-03-SUMMARY records the Testcontainers lane as pending UAT with exact command and expected result, rather than running it on an unapproved local machine. |
| 16 | Prepared AlloyDB/CI verifies the `*pg_cron*` scheduler migration applies with pg_cron enabled and records the expected job metadata. | HUMAN NEEDED | Source migration is present and wired, but 05-03-SUMMARY records this external AlloyDB lane as pending UAT because prepared credentials/database were unavailable. |

**Score:** 14/16 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `terraform/modules/alloydb/sql/ingestion/031_feed_audit_event_retention.sql` | Extension-free bounded retention procedure | VERIFIED | Exists, substantive, no `pg_cron`, defines procedure, deletes expired events, prunes orphan sequence rows with guards. |
| `terraform/modules/alloydb/sql/ingestion/032_feed_audit_events_pg_cron_retention.sql` | AlloyDB pg_cron scheduler migration | VERIFIED | Exists, creates `pg_cron`, schedules `feed-audit-events-retention`, and contains no retention delete SQL. |
| `backend/pipeline/storage/tests/test_feed_audit_contract.py` | Static retention SQL/documentation contract tests | VERIFIED | Reads both migrations and docs; asserts required tokens and forbidden retention side effects. |
| `documentation/feed-audit-events.md` | Current retention contract | VERIFIED | Present-tense retention section documents 18 months, DB-owned scheduler, bounded batch, gaps, and sequence pruning. |
| `backend/pipeline/storage/tests/test_feed_audit_v1_verification_gate.py` | Low-resource v1 verification gate | VERIFIED | Pure pytest file-read gate over existing behavioral lanes; spot check passed. |
| `integration_tests/storage/test_feed_store_integration.py` | Real database retention semantics tests | VERIFIED | Four retention tests call the production procedure and cover expiry, retained survival, gaps, live/deleted-feed sequence rows, and orphan pruning. Runtime execution pending prepared-machine verification. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `032_feed_audit_events_pg_cron_retention.sql` | `031_feed_audit_event_retention.sql` | `CALL public.prune_feed_audit_events_retention()` | WIRED | Scheduler command at `032...sql:7-11` calls the procedure defined at `031...sql:1`. |
| `backend/pipeline/storage/tests/test_feed_audit_contract.py` | Retention migrations/docs | Static file reads | WIRED | Test reads migration paths at `test_feed_audit_contract.py:263-273` and asserts required/forbidden tokens. |
| `test_feed_audit_v1_verification_gate.py` | Storage/service/BFF behavioral tests | Exact test-name and token registration | WIRED | Gate reads the referenced files and asserts required action, diagnostic, API, and no-noise tokens. |
| `integration_tests/storage/test_feed_store_integration.py` | Retention procedure | `CALL public.prune_feed_audit_events_retention()` | WIRED | Four tests call the production procedure at lines 1945, 1989, 2027, and 2063. |
| `terraform/modules/alloydb/main.tf` | SQL migration files | `fileset("${path.module}/sql/ingestion", "*.sql")` | WIRED | Terraform includes all ingestion SQL files at `main.tf:115-117`; tail of sorted migration list includes 031 then 032. |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|--------------------|--------|
| `031_feed_audit_event_retention.sql` | `expired_events` | `public.feed_audit_events` filtered by `occurred_at` | Yes - deletes selected expired event IDs | VERIFIED |
| `031_feed_audit_event_retention.sql` | `orphaned_sequences` | `public.feed_audit_event_sequences` guarded by `public.feeds` and retained `feed_audit_events` | Yes - deletes only orphaned sequence rows | VERIFIED |
| `test_feed_audit_v1_verification_gate.py` | Required test-token sets | Existing storage, sync, service, API, BFF test files | Yes - reads real tracked test files and fails if names/tokens disappear | VERIFIED |
| `test_feed_store_integration.py` | Seeded audit rows and sequence rows | Testcontainers/Postgres database fixture `db_pool` | Expected yes, but runtime DB lane not run locally | HUMAN NEEDED |
| `032_feed_audit_events_pg_cron_retention.sql` | pg_cron job row | Prepared AlloyDB `cron.job` metadata | Expected yes, but external prepared AlloyDB lane not run locally | HUMAN NEEDED |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Static retention and v1 gate tests execute locally | `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_audit_contract.py backend/pipeline/storage/tests/test_feed_audit_v1_verification_gate.py -q` | `14 passed in 0.58s` | PASS |
| Phase Python files compile | `safe-run -- uv run python -m py_compile integration_tests/storage/test_feed_store_integration.py backend/pipeline/common/test_schema_helper.py backend/pipeline/storage/tests/test_feed_audit_contract.py backend/pipeline/storage/tests/test_feed_audit_v1_verification_gate.py` | exit 0 | PASS |
| Whitespace diff check | `git diff --check` | exit 0 | PASS |
| Focused backend behavioral lane | Orchestrator-observed focused pytest over storage gate/store/service/API files | `212 passed, 45 subtests passed, 16 warnings` | PASS |
| Frontend BFF compatibility lane | Orchestrator-observed `safe-run -- yarn --cwd frontend/api test --run src/feeds/feedsController.test.ts` | `61 passed` | PASS |
| Schema drift check | Orchestrator-observed schema drift check | `drift_detected=false` | PASS |
| Resource-heavy Testcontainers lane | Not run locally by instruction | Pending prepared-machine/CI UAT | HUMAN NEEDED |
| Prepared AlloyDB pg_cron lane | Not run locally by instruction | Pending prepared AlloyDB/CI UAT | HUMAN NEEDED |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-------------|-------------|--------|----------|
| AUD-05 | 05-01, 05-03 | Audit rows are retained for 18 months and expired only by the approved retention mechanism. | HUMAN NEEDED | SQL/docs/tests verify the mechanism; prepared AlloyDB pg_cron execution remains pending. |
| VER-01 | 05-02 | Automated tests verify all v1 audit action paths. | SATISFIED | Static gate and existing behavioral tests cover create, update, deactivate, reset, delete, failure, quarantine, and recovery. |
| VER-02 | 05-03 | Automated tests verify rollback behavior and concurrent per-feed event ordering. | HUMAN NEEDED | Integration tests exist and are included in the prepared-machine command; execution was intentionally deferred. |
| VER-03 | 05-02 | Automated tests verify diagnostic-detail lifecycle, API migration, and bounding. | SATISFIED | Gate and referenced lifecycle/API/service/BFF tests cover redaction, caps, canonical field, and no public `quarantine_reason`. |
| VER-04 | 05-01, 05-03 | Automated tests verify delete-survival and retention behavior. | HUMAN NEEDED | Static and integration tests exist; retention DB execution and delete-survival prepared lane remain pending. |
| VER-05 | 05-02 | Automated tests verify lease churn and clean heartbeat/progress do not emit default audit events. | SATISFIED | Gate ties clean progress, clean heartbeat, clean source observation, no-row mutation, and runtime/Echo storage ownership tests to exact tokens. |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| None | - | No TODO/FIXME/placeholder/unwired mock-data blockers found in phase files. Empty list/dict matches in integration tests are assertions or fixtures, not rendered or production stubs. | INFO | No implementation blocker. |

### Human Verification Required

### 1. Prepared-Machine Testcontainers Retention Lane

**Test:** Run the targeted `safe-run -- uv run python -m pytest integration_tests/storage/test_feed_store_integration.py -q -n 0 -k "retention_prunes_expired_events_and_preserves_retained_sequence_gap or retention_keeps_sequence_for_live_feed_after_all_events_expire or retention_keeps_deleted_feed_sequence_while_retained_audit_events_exist or retention_prunes_orphan_sequence_after_last_audit_event_expires or create_feed_audit_failure_rolls_back_feed_and_sequence or update_feed_audit_failure_rolls_back_state_and_sequence or concurrent_same_feed_updates_allocate_contiguous_sequences or delete_feed_succeeds"` command from 05-03-SUMMARY on CI or an approved prepared machine.
**Expected:** pytest exits 0, output includes passed tests, and there are no failures.
**Why human:** The command starts Docker/Testcontainers and is explicitly disallowed as proactive local verification by repo instructions.

### 2. Prepared AlloyDB pg_cron Scheduler Lane

**Test:** Run the prepared AlloyDB command from 05-03-SUMMARY using `ALLOYDB_PG_CRON_VERIFICATION_DSN` against a disposable/prepared AlloyDB database with `alloydb.enable_pg_cron=on`.
**Expected:** 031 and 032 apply successfully; `pg_extension` contains `pg_cron`; `cron.job` contains exactly one `feed-audit-events-retention` row with schedule `15 3 * * *` and command `CALL public.prune_feed_audit_events_retention()`; installed procedure body contains the bounded retention tokens.
**Why human:** Requires external AlloyDB credentials and instance-level pg_cron enablement that are not available in this local verification context.

### Gaps Summary

No implementation gaps were found. The phase goal is source-verified and low-resource checks pass, but the phase cannot be marked fully passed until the two intentionally deferred prepared-machine/CI lanes above are executed.

---

_Verified: 2026-06-20T04:16:09Z_
_Verifier: the agent (gsd-verifier)_

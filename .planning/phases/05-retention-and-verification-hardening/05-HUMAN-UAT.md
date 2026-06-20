---
status: partial
phase: 05-retention-and-verification-hardening
source: [05-VERIFICATION.md, 05-03-SUMMARY.md]
started: 2026-06-20T04:18:21Z
updated: 2026-06-20T04:18:21Z
---

## Current Test

Awaiting prepared-machine or CI execution for the resource-heavy verification lanes.

## Tests

### 1. Prepared-machine Testcontainers retention, rollback, concurrency, and delete-survival lane

expected: The targeted pytest command exits 0, output includes passed tests, and no tests fail.
result: [pending]

Run only on an approved prepared machine or in CI:

```bash
safe-run -- uv run python -m pytest integration_tests/storage/test_feed_store_integration.py -q -n 0 -k "retention_prunes_expired_events_and_preserves_retained_sequence_gap or retention_keeps_sequence_for_live_feed_after_all_events_expire or retention_keeps_deleted_feed_sequence_while_retained_audit_events_exist or retention_prunes_orphan_sequence_after_last_audit_event_expires or create_feed_audit_failure_rolls_back_feed_and_sequence or update_feed_audit_failure_rolls_back_state_and_sequence or concurrent_same_feed_updates_allocate_contiguous_sequences or delete_feed_succeeds"
```

### 2. Prepared AlloyDB pg_cron scheduler lane

expected: Migrations 031 and 032 apply against prepared AlloyDB with pg_cron enabled; pg_extension contains pg_cron; cron.job has exactly one feed-audit-events-retention job with schedule 15 3 * * * and command CALL public.prune_feed_audit_events_retention(); the installed procedure body contains the bounded retention tokens.
result: [pending]

Run only against a disposable or prepared AlloyDB or CI database with pg_cron enabled:

```bash
safe-run -- bash -lc 'set -euo pipefail; : "${ALLOYDB_PG_CRON_VERIFICATION_DSN:?set to a disposable/prepared AlloyDB database with alloydb.enable_pg_cron=on}"; psql "$ALLOYDB_PG_CRON_VERIFICATION_DSN" -v ON_ERROR_STOP=1 -f terraform/modules/alloydb/sql/ingestion/031_feed_audit_event_retention.sql -f terraform/modules/alloydb/sql/ingestion/032_feed_audit_events_pg_cron_retention.sql; test "$(psql "$ALLOYDB_PG_CRON_VERIFICATION_DSN" -v ON_ERROR_STOP=1 -Atc "SELECT count(*) FROM pg_extension WHERE extname = '"'"'pg_cron'"'"';")" = "1"; test "$(psql "$ALLOYDB_PG_CRON_VERIFICATION_DSN" -v ON_ERROR_STOP=1 -Atc "SELECT count(*) FROM cron.job WHERE jobname = '"'"'feed-audit-events-retention'"'"' AND schedule = '"'"'15 3 * * *'"'"' AND command = '"'"'CALL public.prune_feed_audit_events_retention()'"'"';")" = "1"; test "$(psql "$ALLOYDB_PG_CRON_VERIFICATION_DSN" -v ON_ERROR_STOP=1 -Atc "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = '"'"'public'"'"' AND p.proname = '"'"'prune_feed_audit_events_retention'"'"' AND pg_get_functiondef(p.oid) LIKE '"'"'%occurred_at < NOW() - INTERVAL '"'"''"'"'18 months'"'"''"'"'%'"'"' AND pg_get_functiondef(p.oid) LIKE '"'"'%LIMIT 10000%'"'"' AND pg_get_functiondef(p.oid) LIKE '"'"'%FOR UPDATE SKIP LOCKED%'"'"';")" = "1"'
```

## Summary

total: 2
passed: 0
issues: 0
pending: 2
skipped: 0
blocked: 0

## Gaps

None. These are deferred prepared-machine checks, not implementation gaps.

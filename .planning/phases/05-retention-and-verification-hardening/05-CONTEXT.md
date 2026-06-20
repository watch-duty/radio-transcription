# Phase 5: Retention and Verification Hardening - Context

**Gathered:** 2026-06-20
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 5 enforces the Feed Audit Events V1 retention policy and hardens
verification for the completed v1 audit contract. It covers 18-month retention
for `feed_audit_events`, cleanup of retention-owned sequence bookkeeping, and
focused tests proving retention plus the already-built create/update/delete,
failure/quarantine/recovery, transactionality, diagnostic-detail, delete
survival, and no-noise behaviors.

This phase does not add Watch Duty backend delivery, admin timeline APIs/UI,
full event sourcing, new audit actions, synthetic baseline/tombstone events, or
a redesign of the existing `feed_sequence` ordering contract.

</domain>

<decisions>
## Implementation Decisions

### Retention Mechanism

- **D-01:** Enforce audit retention inside AlloyDB with a DB-owned `pg_cron`
  scheduled job. Add a new ingestion SQL migration whose filename includes
  `pg_cron`, preserving the repository's existing local/CI skip convention for
  Postgres environments without the extension.
- **D-02:** Retention uses `feed_audit_events.occurred_at` as the expiry field.
  The cutoff is `occurred_at < NOW() - INTERVAL '18 months'`, matching the
  event's domain time rather than insert time.
- **D-03:** Run cleanup daily with a bounded delete batch. Do not use an
  unbounded delete that could remove a large paused-backlog in one transaction.
- **D-04:** Backlog catch-up deletes one bounded batch per daily run. Do not
  loop inside one cron invocation until the backlog is gone, and do not switch
  to high-frequency catch-up as part of v1.

### Retention Side Effects

- **D-05:** Retention deletes expired `feed_audit_events` rows only. It must
  not archive rows, rewrite/redact payloads, or create synthetic summary,
  tombstone, or baseline audit events.
- **D-06:** Retention may leave gaps in `feed_sequence` for retained rows. This
  is expected and acceptable; `feed_sequence` values are immutable audit
  labels and must not be renumbered after old rows expire.
- **D-07:** Retained timelines simply start at the oldest non-expired event.
  Consumers should not infer that the first retained `feed_sequence` is the
  first event that ever happened for that feed.

### Sequence Cleanup

- **D-08:** Do not remove `feed_sequence` or `feed_audit_event_sequences` in
  Phase 5. The current contract and tests use them to satisfy stable per-feed
  ordering and concurrent ordering requirements.
- **D-09:** Prune `feed_audit_event_sequences` rows only when both conditions
  are true: the corresponding feed no longer exists in `feeds`, and there are
  no retained `feed_audit_events` rows for that feed.
- **D-10:** Keep sequence rows for live feeds even if all of that feed's older
  audit rows have expired, because future events still need the next
  transactionally allocated `feed_sequence`.
- **D-11:** Keep sequence rows for deleted feeds while any retained audit event
  remains, so retained history and ordering metadata age out together.

### Verification Boundary

- **D-12:** User discussion focused on retention, side effects, and sequence
  cleanup. The planner may choose the exact verification split, but it must
  preserve the Phase 5 roadmap requirements: retention behavior, event
  coverage, rollback/concurrency, diagnostic-detail lifecycle, public API
  compatibility, delete survival, and no-lease-churn behavior.
- **D-13:** Prefer existing verification lanes and patterns: static SQL
  contract tests for migration invariants, focused storage/service/runtime unit
  tests for behavior, and Testcontainers-backed storage integration tests where
  real database transaction or retention semantics matter.

### the agent's Discretion

- Choose the exact daily cron expression, conservative batch size, SQL helper
  shape, and migration number/name, provided the file name includes `pg_cron`
  and the delete is bounded.
- Choose whether retention cleanup uses one statement or a small set of
  statements, provided expired audit rows are deleted before or together with
  safe pruning of orphaned sequence rows.
- Choose the exact automated-test grouping and command set. Keep Docker or
  Testcontainers lanes aligned with repository safety rules and document any
  human/CI-only verification that cannot run locally.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Planning Context

- `.planning/PROJECT.md` - milestone scope, current active Phase 5
  requirements, and project-level constraints.
- `.planning/REQUIREMENTS.md` - Phase 5 requirements AUD-05 and VER-01
  through VER-05.
- `.planning/ROADMAP.md` - Phase 5 goal and success criteria.
- `.planning/STATE.md` - current milestone/session state.
- `.planning/phases/02-transactional-storage-writes/02-CONTEXT.md` -
  existing sequence-ordering and storage-owned audit-write decisions.
- `.planning/phases/03-service-and-compatibility-surface/03-CONTEXT.md` -
  trusted admin actor and diagnostic-detail compatibility decisions.
- `.planning/phases/04-runtime-event-integration/04-CONTEXT.md` - runtime,
  Echo, failure, quarantine, recovery, and no-noise decisions that Phase 5 must
  verify rather than redefine.
- `.planning/phases/04-runtime-event-integration/04-VERIFICATION.md` - latest
  verified Phase 4 behavior and remaining human/CI Echo integration lane.
- `.planning/phases/04-runtime-event-integration/04-HUMAN-UAT.md` - pending
  Docker/Testcontainers UAT item from Phase 4.

### Domain Contract And Schema

- `documentation/feed-audit-events.md` - canonical Feed Audit Event contract,
  retention target, action vocabulary, actor vocabulary, snapshot semantics,
  diagnostic-detail semantics, and phase boundaries.
- `terraform/modules/alloydb/sql/ingestion/019_feeds_pg_cron_jobs.sql` -
  existing pg_cron migration pattern, load-bearing filename convention,
  scheduled job style, and bounded operational cleanup example.
- `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql` -
  `feed_audit_events`, `feed_audit_event_sequences`, `occurred_at`,
  `feed_sequence`, indexes, and constraints.
- `terraform/modules/alloydb/sql/ingestion/030_feed_audit_events_actor_constraint.sql`
  - current actor constraint replacement and sequence backfill pattern.
- `terraform/modules/alloydb/sql/ci/hot_protection_check.sql` - schema guard
  context for feed hot-path columns.

### Test And Verification Context

- `.planning/codebase/TESTING.md` - repository test commands, Testcontainers
  patterns, and unit/integration test organization.
- `.planning/codebase/ARCHITECTURE.md` - storage/service/runtime boundaries
  and existing data flow.
- `.planning/codebase/CONCERNS.md` - fragile FeedStore SQL, pg_cron skip
  behavior, and test coverage gaps.
- `backend/pipeline/storage/tests/test_feed_audit_contract.py` - text-level
  schema/documentation contract tests to extend for retention invariants.
- `backend/pipeline/storage/tests/test_feed_store.py` - focused storage unit
  tests for audit event behavior, transaction inputs, and runtime event gates.
- `backend/pipeline/storage/tests/test_sync_feed_store.py` - sync/Echo audit
  parity tests.
- `backend/pipeline/storage/tests/test_feed_query_contracts.py` - SQL contract
  guards for runtime/storage audit and diagnostic-detail behavior.
- `backend/pipeline/storage/tests/test_feed_lifecycle.py` - diagnostic-detail
  sanitizer and bound tests.
- `backend/services/feeds/tests/test_api.py` and
  `backend/services/feeds/tests/test_service.py` - public service
  compatibility and actor propagation tests.
- `frontend/api/src/feeds/feedsController.test.ts` and frontend feed status
  tests - BFF/frontend compatibility checks around `statusReasonDetail`.
- `integration_tests/storage/test_feed_store_integration.py` -
  Testcontainers-backed storage integration tests for audit rows, rollback,
  concurrent ordering, and delete survival.
- `local_dev/docker_postgres_init.sh` and `.github/workflows/ci.yml` - places
  that intentionally skip `*pg_cron*` migrations outside AlloyDB.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets

- `terraform/modules/alloydb/sql/ingestion/019_feeds_pg_cron_jobs.sql` already
  creates `pg_cron`, schedules named jobs, documents cadence/batch tradeoffs,
  and relies on `*pg_cron*` filename skipping in local/CI Postgres.
- `feed_audit_events.occurred_at` and `idx_feed_audit_events_occurred_at`
  already support retention cutoff scans.
- `feed_audit_event_sequences` already gives a per-feed sequence counter that
  can be pruned independently from retained event rows when no current feed and
  no retained audit events remain.
- `backend/pipeline/storage/tests/test_feed_audit_contract.py` is the right
  place for static guarantees that retention SQL uses `occurred_at`, an
  18-month interval, bounded deletes, the `pg_cron` filename convention, and no
  synthetic event insert.
- `integration_tests/storage/test_feed_store_integration.py` already has
  helpers for fetching audit rows and sequence counters and can be extended for
  DB-backed retention behavior where feasible.

### Established Patterns

- Migrations live under `terraform/modules/alloydb/sql/ingestion/` and execute
  lexically; `pg_cron` migrations must include `pg_cron` in the file name.
- Local docker-compose and CI migration application skip `*pg_cron*` files
  because vanilla Postgres lacks the extension. Tests for pg_cron migrations
  should therefore be static or AlloyDB/prepared-environment specific.
- Storage owns Feed Audit Event creation. Phase 5 retention should operate on
  persisted rows and sequence bookkeeping, not introduce service/runtime audit
  insert paths.
- Audit snapshots are allowlisted domain objects, not raw row dumps. Retention
  should delete rows, not mutate snapshot payloads.
- Testcontainers-backed storage tests are used when behavior depends on real
  database constraints, transactions, and concurrent writes.

### Integration Points

- Add the retention migration after `030_feed_audit_events_actor_constraint.sql`
  with a name that includes `pg_cron`.
- Extend `documentation/feed-audit-events.md` so retention enforcement is no
  longer described as future-only after Phase 5.
- Extend contract tests under `backend/pipeline/storage/tests/` for SQL and doc
  invariants.
- Extend storage integration tests if the retention SQL is factored so it can
  be exercised without requiring the `pg_cron` extension, or document the
  AlloyDB/CI-only lane if not.
- Keep final verification aware of the existing Phase 4 Echo Docker/Testcontainers
  UAT item.

</code_context>

<specifics>
## Specific Ideas

- Retention SQL should be conceptually shaped as "delete expired audit rows by
  `occurred_at`, limited to one daily bounded batch" plus a safe prune for
  sequence rows where `NOT EXISTS` in `feeds` and `NOT EXISTS` in retained
  `feed_audit_events`.
- Gaps in `feed_sequence` after retention are not a bug. They should be
  documented and tested if planner finds a simple contract assertion.

</specifics>

<deferred>
## Deferred Ideas

- Revisit in a future phase whether explicit per-feed `feed_sequence` can be
  removed from the audit contract and replaced by client-side ordering on
  `occurred_at` plus event ID. This is a contract/schema redesign and should
  not be done in Phase 5 retention hardening.

</deferred>

---

*Phase: 5-Retention and Verification Hardening*
*Context gathered: 2026-06-20*

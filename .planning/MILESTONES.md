# Milestones

## v1.0 Feed Audit Events V1 (Shipped: 2026-06-20)

**Phases completed:** 5 phases, 17 plans, 37 tasks

**Audit status:** tech_debt; 0 blockers, 3 pending UAT requirements.

**Known deferred items at close:** 6 audit-open items acknowledged; see `.planning/STATE.md` Deferred Items and `.planning/milestones/v1.0-MILESTONE-AUDIT.md`.

**Key accomplishments:**

- Domain-first Feed Audit Event contract with actor vocabulary, deletion-safe snapshot semantics, capped diagnostic detail, retention target, and repository glossary terms
- Delete-safe Feed Audit Event schema with bounded diagnostic detail, namespaced actor constraints, per-feed ordering, and HOT guard coverage.
- Standard-library pytest contract tests now guard the Feed Audit Event documentation, repository glossary, SQL migration, actor/action constraints, diagnostic-detail bounds, and HOT guard.
- Actor vocabulary cleanup plus storage-owned SQL primitives for feed audit snapshots, sequence allocation, and audit inserts
- FeedStore create/update now write storage-owned feed audit events transactionally with Phase 2 service actor attribution
- FeedStore lifecycle and hard-delete mutations now write storage-owned audit rows transactionally with service actor attribution
- Rollback, concurrent ordering, and persisted-row audit coverage for transactional FeedStore writes, with local Testcontainers execution deferred to CI
- FastAPI feed responses and storage projections now expose canonical status_reason_detail while omitting public quarantine_reason.
- Async FeedStore runtime audit gates with prior-status leasing, sanitized diagnostic detail, and transactional failure/quarantine/recovery events
- Collector runtime now passes service:collector-runtime plus leased prior state into storage-owned failure, quarantine, and recovery-capable writes
- Echo ingestion now uses service:echo-ingestion and SyncFeedStore-owned transactions to emit runtime failure, quarantine, and recovery audit events without noisy clean-heartbeat rows
- Runtime audit contract documentation plus focused async/sync invariant tests for meaningful failure, quarantine, recovery, and no-noise behavior
- 18-month Feed Audit Events retention now has a DB-owned bounded procedure, daily AlloyDB pg_cron schedule, and static contract coverage.
- Static pytest gate tying existing storage, sync, service, API, BFF, and no-noise audit tests to the v1 contract.
- Retention semantics now have prepared Testcontainers integration coverage for expiry, sequence gaps, live/deleted-feed sequence preservation, and orphan pruning.

---

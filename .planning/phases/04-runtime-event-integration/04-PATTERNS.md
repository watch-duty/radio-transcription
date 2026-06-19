# Phase 4 Pattern Map: Runtime Event Integration

**Phase:** 04-runtime-event-integration
**Date:** 2026-06-19

## Existing Patterns To Reuse

| Pattern | Existing Location | Phase 4 Use |
|---------|-------------------|-------------|
| Transactional audit insert owned by storage | `backend/pipeline/storage/feed_store.py` create/update/reset/deactivate/delete paths | Runtime and sync lifecycle writes should append audit rows from storage, not from runtime handlers. |
| Maintained before/after snapshot allowlist | `FeedStore._audit_snapshot`, `feed_queries.GET_AUDIT_FEED_SNAPSHOT_SQL` | Runtime events should use the same feed-row subset so before/after values remain consistent across actions. |
| Per-feed sequence allocation | `feed_queries.ALLOCATE_FEED_AUDIT_SEQUENCE_SQL` | Failure, quarantine, and recovery events must preserve deterministic feed timeline order. |
| SQL contract tests | `backend/pipeline/storage/tests/test_feed_query_contracts.py` | Add checks for previous-status claim fields, diagnostic-detail SQL writes, and async/sync parity. |
| Mocked storage transaction tests | `backend/pipeline/storage/tests/test_feed_store.py`, `tests/connection_util.py` | Extend with runtime event action-selection and rollback/no-event cases. |
| Runtime retry wrapper | `backend/pipeline/ingestion/collector_runtime.py` `retry_with_lease_check` calls | Preserve retry/fencing behavior while passing new audit inputs as keyword arguments. |
| Echo sync store boundary | `backend/pipeline/storage/sync_feed_store.py` | Add psycopg-specific audit helpers with the same domain contract. |
| Echo handler route | `backend/pipeline/ingestion/collectors/echo/main.py` | Pass `service:echo-ingestion` and prior feed state into sync store success/failure calls. |
| Contract documentation | `documentation/feed-audit-events.md` | Update from phase-1 boundary language to include implemented runtime semantics. |

## Closest Analog Mappings

| New/Changed Behavior | Closest Analog | Notes |
|----------------------|----------------|-------|
| Runtime failure audit transaction | `FeedStore.update_feed` audit transaction | Fetch before snapshot, mutate row, fetch after snapshot, allocate sequence, insert event in one transaction. |
| Runtime recovery event | `FeedStore.reset_feed` before/after snapshot | Both clear abnormal state, but recovery is runtime success; admin/manual reset remains `feed.reset`. |
| Sync audit transaction | Async `FeedStore._insert_feed_audit_event` | Same schema and action vocabulary, different DB driver and parameter syntax. |
| Diagnostic-detail persistence boundary | `feed_lifecycle.quarantine_reason_storage_value` | Add canonical detail helper rather than letting runtime write raw exception strings. |
| Runtime actor propagation | Phase 3 explicit `actor_id` service signatures | Require explicit stable actor IDs for audited runtime writes. |

## Anti-Patterns To Avoid

- Do not emit audit rows from `collector_runtime.py` or Echo `main.py`.
- Do not emit an audit event for every failure attempt, heartbeat, lease claim,
  lease release, or retry timing update.
- Do not use `system:` actor IDs.
- Do not encode source type in actor IDs.
- Do not make `quarantine_reason` canonical again; use it only as a temporary
  compatibility mirror where existing flow requires it.
- Do not synthesize audit snapshots from runtime-local objects. Before/after
  values should remain the maintained feed-row subset.


# Phase 1: Audit Contract and Emission - Context

**Gathered:** 2026-06-26
**Status:** Ready for planning

## Phase Boundary

Phase 1 defines and emits the storage-boundary Feed Audit Notification log for every newly inserted `feed_audit_events` row. It does not route logs to Pub/Sub, call the Watch Duty webhook, add delivery state, add database polling, or change feed lifecycle semantics.

The implementation should treat `feed_audit_events` as the canonical audit ledger and structured logs as best-effort notification signals emitted after audited SQL returns the committed event payload.

## Implementation Decisions

### Audit Event Contract
- **D-01:** Emit one structured Feed Audit Notification for every newly inserted `feed_audit_events` row, including admin actions and ingestion/runtime lifecycle actions.
- **D-02:** Emit no notification when an audited SQL statement does not insert an audit row, such as no-op updates or suppressed repeated failure noise.
- **D-03:** Use `event_type="radio_transcription.feed_audit_notification"` and integer `schema_version=1`.
- **D-04:** Keep the v1 payload flat: `event_id`, `action`, `occurred_at`, `actor_id`, `feed_id`, `feed_revision`, `before_values`, and `after_values`, plus `event_type` and `schema_version`.
- **D-05:** Do not add extra fields solely for webhook readability. The Watch Duty endpoint currently requires `feed_id` and preserves unknown fields, so Phase 1 should support the agreed endpoint payload without inventing a broader event schema.

### SQL and Result Shape
- **D-06:** Do not add extra database round trips. Audited SQL should return any notification payload in the same statement that writes the audit row.
- **D-07:** Return a single nullable JSONB column named `feed_audit_event` instead of many scalar `audit_*` columns. This avoids namespace confusion with `feeds.audit_revision` and future feed columns that may begin with `audit_`.
- **D-08:** Build the payload from database-returned audit row values, not request-local guesses, so notifications cannot describe a row that was not inserted.
- **D-09:** Avoid repeated transformation and encode/decode cycles. SQL may build the JSONB payload once; Python should parse only if the DB driver returns a JSON string, then pass the dict to logging with `extra={"json_fields": ...}`.

### Emission Behavior
- **D-10:** Add a shared storage helper for notification preparation and structured log emission, reused by async `FeedStore` and sync `SyncFeedStore`.
- **D-11:** Notification emission failures must never affect ingestion, feed lifecycle writes, or audit row persistence. The helper should catch all local emission exceptions and avoid re-raising.
- **D-12:** Do not import Pub/Sub, webhook clients, Cloud Logging sink clients, or deployment-specific routing code into the feed storage path.
- **D-13:** Remove only storage-layer duplicate failure summary logs once the audit-shaped notification log covers the same event. Keep runtime policy logs, quarantine telemetry, admin/API logs, and unrelated operational logs.

### Verification Boundary
- **D-14:** Phase 1 verification should focus on unit/query-contract tests and storage mock behavior. Integration or E2E tests are not required for the discussion outcome unless the plan finds a small targeted check with clear value.
- **D-15:** Tests should prove actual audit inserts produce one notification payload, suppressed/no-op audit paths produce no notification, helper failures are swallowed, and async/sync stores share the same emitter behavior.

### The Agent's Discretion

The planner may choose exact helper names and SQL helper signatures, but should preserve the single-column `feed_audit_event` result shape and keep shared producer behavior out of routing/relay code.

## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project And Requirements
- `.planning/PROJECT.md` — Defines the Feed Audit Notification milestone, non-critical-path constraint, WD webhook destination, and major decisions.
- `.planning/REQUIREMENTS.md` — Lists Phase 1 requirements `AUDIT-01..05` and `PAYLOAD-01..04`.
- `.planning/ROADMAP.md` — Defines Phase 1 success criteria and later phase boundaries.
- `.planning/research/SUMMARY.md` — Summarizes the chosen log-delivered notification pattern and `event_type`/`schema_version` contract.
- `.planning/research/PITFALLS.md` — Captures failure modes around critical-path coupling, phantom events, broad payloads, and routing assumptions.

### Existing Domain Documentation
- `CONTEXT.md` — Defines Feed Audit Event and Feed Audit Notification terminology. Treat this file as authoritative but note it may have pending local edits unrelated to this GSD artifact.

### Storage And Audit SQL
- `backend/pipeline/storage/feed_audit_sql.py` — Shared audit snapshot allowlist and `insert_feed_audit_event_cte` helper.
- `backend/pipeline/storage/feed_queries.py` — Async audited SQL for progress, source observation, failure, non-budgeted failure, create, update, deactivate, delete, and reset.
- `backend/pipeline/storage/sync_feed_queries.py` — Sync audited SQL for Echo heartbeat, failure, and non-budgeted failure.
- `backend/pipeline/storage/feed_store.py` — Async storage methods that need to emit notifications after audited writes return.
- `backend/pipeline/storage/sync_feed_store.py` — Sync Echo storage methods that currently use `execute()` and need to consume returned notification payloads without extra queries.

### Logging Pattern
- `backend/pipeline/common/log_helper.py` — Existing structured logging pattern uses `extra={"json_fields": ...}` and supports Cloud Logging ingestion.
- `backend/pipeline/common/tracing_utils.py` — Existing trace attribute integration used by structured logging.

### Tests
- `backend/pipeline/storage/tests/test_feed_query_contracts.py` — SQL contract tests for audited queries and CTE generation.
- `backend/pipeline/storage/tests/test_feed_store.py` — Async store unit tests and audit query assertions.
- `backend/pipeline/storage/tests/test_sync_feed_store.py` — Sync store tests for Echo lifecycle writes.
- `backend/pipeline/storage/tests/test_feed_audit_contract.py` — Migration and audit schema contract tests.

## Existing Code Insights

### Reusable Assets
- `feed_audit_sql.audit_snapshot_sql(alias)` already centralizes the allowlisted before/after JSON shape.
- `feed_audit_sql.insert_feed_audit_event_cte(...)` already centralizes audit inserts and has a `returning_sql` hook that can be extended to return the notification JSON payload.
- `backend.pipeline.common.log_helper` already supports structured logs through `json_fields`.

### Established Patterns
- Audited writes use CTEs to lock/read feed state, mutate rows, compute before/after snapshots, insert `feed_audit_events`, and return the normal storage result in one round trip.
- Repeated failures with the same status/status_reason can advance `feeds.audit_revision` without inserting an audit row; notification emission must follow the audit-row insert, not the revision bump.
- Some admin operations return normal feed rows while some sync paths currently discard query results with `execute()`. Phase 1 needs result consumption where an audit row might be inserted.

### Integration Points
- Async `update_feed_progress`, `record_source_observation`, `report_feed_failure`, `release_non_budgeted_failure`, `create_feed`, `update_feed`, `deactivate_feed`, `delete_feed`, and `reset_feed` are the main FeedStore integration points.
- Sync `record_heartbeat`, `record_failure`, and `record_non_budgeted_failure` are the SyncFeedStore integration points.
- Storage-layer duplicate logs currently exist around async failure reporting and sync failure reporting; remove only those duplicates if the new notification covers them.

## Specific Ideas

Prefer a helper shaped around:

```python
emit_feed_audit_notification(row["feed_audit_event"])
```

The helper should no-op for `None`, tolerate asyncpg/psycopg JSON return differences, add `event_type` and `schema_version`, emit with a dedicated logger, and swallow all local exceptions.

## Deferred Ideas

- Cloud Logging sink, Pub/Sub topic/subscription, IAM, retry, and DLQ configuration belong to Phase 2.
- Cloud Run relay, Pub/Sub envelope parsing, WD webhook POST, and API key handling belong to Phase 3.
- Staging/prod rollout proof, operational dashboards, replay tooling, and runbooks belong to Phase 4 or later.
- Durable delivery, outbox tables, database polling, CDC, triggers, and direct webhook calls remain out of scope for this milestone unless requirements change.

---

*Phase: 1-Audit Contract and Emission*
*Context gathered: 2026-06-26*

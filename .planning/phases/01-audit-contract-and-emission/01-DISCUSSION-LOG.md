# Phase 1: Audit Contract and Emission - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-26
**Phase:** 1-Audit Contract and Emission
**Areas discussed:** event source, SQL result shape, payload contract, critical-path isolation, duplicate logs, verification boundary

---

## Event Source

| Option | Description | Selected |
|--------|-------------|----------|
| Emit only health-relevant audit events | Log only failure/quarantine/recovery actions. | |
| Emit every audit row | Log every newly inserted `feed_audit_events` row so the WD backend can decide how to use it. | ✓ |
| Emit feed state changes independent of audit rows | Treat notification as its own lifecycle signal. | |

**User's choice:** Emit every single change to `feed_audit_events`.
**Notes:** The notification stream must follow the audit ledger. State changes that do not insert audit rows should not emit notifications.

---

## SQL Result Shape

| Option | Description | Selected |
|--------|-------------|----------|
| Many scalar audit columns | Return `audit_event_id`, `audit_action`, `audit_feed_revision`, and similar columns next to feed result fields. | |
| Single JSONB notification column | Return one nullable `feed_audit_event` JSONB payload column from audited SQL. | ✓ |
| Extra DB read after write | Query `feed_audit_events` after the audited write completes. | |

**User's choice:** Avoid many schema-detail columns and avoid extra DB round trips.
**Notes:** The single-column shape avoids confusion with `feeds.audit_revision` and future feed columns that may use `audit_` prefixes.

---

## Payload Contract

| Option | Description | Selected |
|--------|-------------|----------|
| Raw audit row | Forward the whole audit row or Cloud Logging entry. | |
| Flat v1 audit payload | Send a flat payload with event identity, action, actor, feed ID, feed revision, and before/after values. | ✓ |
| Endpoint-specific reduced payload | Send only the fields currently required by the WD endpoint. | |

**User's choice:** Use the agreed flat audit payload and keep it minimal.
**Notes:** The endpoint currently requires `feed_id` and preserves unknown fields, but Phase 1 should not add fields solely for readability or future consumers.

---

## Critical-Path Isolation

| Option | Description | Selected |
|--------|-------------|----------|
| Direct webhook call from storage | Call the Watch Duty endpoint after each audited write. | |
| Pub/Sub publish from storage | Publish directly from the feed write path. | |
| Structured log after audit write | Emit a local structured log after the audited SQL returns the inserted audit payload. | ✓ |

**User's choice:** Audit-related event failure must absolutely never affect ingestion.
**Notes:** Phase 1 therefore excludes network delivery code and requires local emission failures to be swallowed.

---

## Duplicate Logs

| Option | Description | Selected |
|--------|-------------|----------|
| Keep all existing storage logs | Add notification logs without removing existing failure summary logs. | |
| Remove only duplicated storage summaries | Remove FeedStore/SyncFeedStore success/failure summaries that duplicate the new audit notification event. | ✓ |
| Remove broader runtime telemetry | Remove collector and quarantine telemetry logs too. | |

**User's choice:** Remove duplicates if present, but preserve useful operational logs.
**Notes:** Keep CollectorRuntime policy logs, quarantine telemetry, admin/API logs, and unrelated diagnostics.

---

## Verification Boundary

| Option | Description | Selected |
|--------|-------------|----------|
| Unit/query-contract focused | Verify SQL shape, helper behavior, and store emission calls without requiring integration/E2E. | ✓ |
| Full DB integration required | Require integration tests to prove each storage path inserts and logs together. | |
| End-to-end local/prod verification | Exercise Cloud Logging, Pub/Sub, relay, and WD endpoint. | |

**User's choice:** No explicit user prompt was needed; this was inferred from prior test preferences and Phase 1 scope.
**Notes:** Later phases own routing and webhook delivery verification.

---

## The Agent's Discretion

- Exact helper/module names are left to the planner and implementer, as long as async and sync stores share the same notification behavior.
- The planner may decide whether a small targeted integration check is worth adding, but Phase 1 should not depend on E2E.

## Deferred Ideas

- Cloud Logging sink, Pub/Sub routing, IAM, DLQ, relay service, webhook auth, and deployment verification are deferred to later phases.
- Durable outbox/delivery tables, database polling, triggers, LISTEN/NOTIFY, CDC, and direct webhook calls were rejected for this milestone boundary.

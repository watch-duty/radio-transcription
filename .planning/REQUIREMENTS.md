# Requirements: Feed Audit Notification Delivery

**Defined:** 2026-06-26
**Core Value:** Feed audit notifications must make feed lifecycle and ingestion
problems visible to Watch Duty quickly without affecting ingestion or feed
lifecycle writes.

## v1 Requirements

### Audit Emission

- [x] **AUDIT-01**: Every newly inserted `feed_audit_events` row emits exactly one best-effort structured Feed Audit Notification log.
- [x] **AUDIT-02**: Feed state changes that do not insert a `feed_audit_events` row emit no Feed Audit Notification.
- [x] **AUDIT-03**: Notification emission never raises to callers and never changes the result of ingestion, feed lifecycle writes, or audit row persistence.
- [x] **AUDIT-04**: Async `FeedStore` and sync `SyncFeedStore` audited write paths use one shared notification helper instead of duplicate payload/logging logic.
- [x] **AUDIT-05**: Storage SQL returns notification payload data from the same audited statement without adding an extra database round trip.

### Payload Contract

- [x] **PAYLOAD-01**: Each notification log includes `event_type="radio_transcription.feed_audit_notification"` and `schema_version=1`.
- [x] **PAYLOAD-02**: Each notification payload is flat and includes `event_id`, `action`, `occurred_at`, `actor_id`, `feed_id`, `feed_revision`, `before_values`, and `after_values`.
- [x] **PAYLOAD-03**: The payload mirrors the existing feed audit snapshot allowlist and does not add raw request bodies, secrets, or extra fields solely for the webhook.
- [x] **PAYLOAD-04**: Payload construction avoids repeated JSON encode/decode cycles; producers pass structured dictionaries to logging.

### Cloud Routing

- [ ] **ROUTE-01**: Cloud Logging routes Feed Audit Notification logs to a dedicated Pub/Sub topic with a filter on `jsonPayload.event_type` and `jsonPayload.schema_version`.
- [ ] **ROUTE-02**: The Log Router sink writer has the minimal Pub/Sub publisher IAM needed for the notification topic.
- [ ] **ROUTE-03**: The Pub/Sub push subscription invokes the relay through authenticated Cloud Run IAM/OIDC.
- [ ] **ROUTE-04**: The Pub/Sub subscription uses retry backoff with 10 second minimum, 60 second maximum, and a dead-letter policy with 10 delivery attempts.

### Webhook Relay

- [ ] **RELAY-01**: A new Cloud Run relay service accepts Pub/Sub push requests at `POST /pubsub/feed-audit-notifications`.
- [ ] **RELAY-02**: The relay decodes the Pub/Sub Cloud Logging `LogEntry`, extracts `jsonPayload`, and shallow-validates only the notification contract required for the WD endpoint.
- [ ] **RELAY-03**: The relay forwards the flat notification payload unchanged to `/api/v1/echo/radio_transcription/internal/audit/webhook/` using `X-Api-Key`.
- [ ] **RELAY-04**: The relay performs two total WD POST attempts for timeout, connection failure, `429`, and `5xx` responses before returning non-2xx to Pub/Sub.
- [ ] **RELAY-05**: The relay returns `204` to Pub/Sub after WD `2xx` responses and returns non-2xx for malformed messages, unsupported schema, auth/config errors, and exhausted transient failures so Pub/Sub can retry and eventually DLQ.
- [ ] **RELAY-06**: Relay request handling is stateless and never reads or writes AlloyDB.

### Observability And Rollout

- [ ] **OPS-01**: Producer, routing, relay success, relay retryable failure, relay permanent/config failure, and DLQ paths emit structured operational logs.
- [ ] **OPS-02**: Deployment configuration documents the required Cloud Logging sink, Pub/Sub topic, DLQ topic, push subscription, relay service, secret/env vars, and IAM bindings.
- [ ] **OPS-03**: Staging verification proves a real feed audit row can produce a Pub/Sub message and a WD webhook call without touching the feed write path.
- [ ] **OPS-04**: Production rollout has a runbook for checking routed logs, Pub/Sub backlog, push failures, and DLQ messages.

## v2 Requirements

### Operations

- **OPS-05**: Operator can replay selected events from `feed_audit_events` by event ID or time range.
- **OPS-06**: Operator can inspect delivery attempt history outside Cloud Logging and Pub/Sub DLQ.
- **OPS-07**: Admin UI or API exposes delivery status for individual feed audit events.

### Security

- **SEC-01**: Relay signs outbound WD webhook requests with HMAC or another stronger verification mechanism if WD endpoint exposure requires it.
- **SEC-02**: Webhook credentials support key rotation without relay downtime.

### Extensibility

- **EXT-01**: Feed audit notifications can support multiple downstream destinations.
- **EXT-02**: Historical feed audit rows can be backfilled to WD after future-event idempotency is proven.

## Out of Scope

| Feature | Reason |
|---------|--------|
| Direct WD webhook call from feed mutation or ingestion code | Couples critical feed writes to downstream latency and availability. |
| Watch Duty backend reading the transcription database | Violates the service boundary and makes WD depend on transcription storage internals. |
| Database trigger, `LISTEN/NOTIFY`, CDC, or DB polling relay for v1 | Adds operational complexity or database coupling not needed for the best-effort notification goal. |
| Full outbox or duplicate payload table | `feed_audit_events` is already the canonical audit ledger; v1 should not duplicate payload storage. |
| Exactly-once delivery | Pub/Sub and HTTP delivery can duplicate; receiver must tolerate duplicates by `event_id`. |
| Adding new audit snapshot fields only for WD readability | Increases schema and snapshot maintenance cost; v1 mirrors the current audit event payload. |
| UI changes | This project is backend and infrastructure notification plumbing. |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| AUDIT-01 | Phase 1 | Complete |
| AUDIT-02 | Phase 1 | Complete |
| AUDIT-03 | Phase 1 | Complete |
| AUDIT-04 | Phase 1 | Complete |
| AUDIT-05 | Phase 1 | Complete |
| PAYLOAD-01 | Phase 1 | Complete |
| PAYLOAD-02 | Phase 1 | Complete |
| PAYLOAD-03 | Phase 1 | Complete |
| PAYLOAD-04 | Phase 1 | Complete |
| ROUTE-01 | Phase 2 | Pending |
| ROUTE-02 | Phase 2 | Pending |
| ROUTE-03 | Phase 2 | Pending |
| ROUTE-04 | Phase 2 | Pending |
| RELAY-01 | Phase 3 | Pending |
| RELAY-02 | Phase 3 | Pending |
| RELAY-03 | Phase 3 | Pending |
| RELAY-04 | Phase 3 | Pending |
| RELAY-05 | Phase 3 | Pending |
| RELAY-06 | Phase 3 | Pending |
| OPS-01 | Phase 4 | Pending |
| OPS-02 | Phase 4 | Pending |
| OPS-03 | Phase 4 | Pending |
| OPS-04 | Phase 4 | Pending |

**Coverage:**
- v1 requirements: 23 total
- Mapped to phases: 23
- Unmapped: 0

---
*Requirements defined: 2026-06-26*
*Last updated: 2026-06-26 after roadmap creation*

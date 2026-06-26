# Roadmap: Feed Audit Notification Delivery

## Overview

This milestone delivers best-effort feed audit notifications without coupling
feed writes to downstream delivery. It first locks the structured audit
notification contract at the storage boundary, then routes matching Cloud
Logging entries to Pub/Sub, adds a stateless Cloud Run relay to call Watch Duty,
and finishes with operational proof, DLQ visibility, and rollout guidance.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [ ] **Phase 1: Audit Contract and Emission** - Feed audit rows produce safe, non-blocking structured notification logs.
- [ ] **Phase 2: Cloud Logging and Pub/Sub Routing** - Matching notification logs route to a dedicated Pub/Sub delivery path with IAM, retry, and DLQ configuration.
- [ ] **Phase 3: Webhook Relay Delivery** - A stateless Cloud Run relay validates Pub/Sub log messages and forwards flat payloads to Watch Duty.
- [ ] **Phase 4: Operations and Rollout Proof** - Delivery is observable, documented, and verified end-to-end in staging before production rollout.

## Phase Details

### Phase 1: Audit Contract and Emission
**Goal**: Feed audit rows produce safe, non-blocking structured notification logs with the exact v1 payload contract.
**Depends on**: Nothing (first phase)
**Requirements**: AUDIT-01, AUDIT-02, AUDIT-03, AUDIT-04, AUDIT-05, PAYLOAD-01, PAYLOAD-02, PAYLOAD-03, PAYLOAD-04
**Success Criteria** (what must be TRUE):
  1. A newly inserted `feed_audit_events` row emits exactly one structured Feed Audit Notification log.
  2. A feed state change that does not insert a `feed_audit_events` row emits no Feed Audit Notification log.
  3. Feed lifecycle writes, ingestion behavior, and audit row persistence still succeed when notification emission fails locally.
  4. Async `FeedStore` and sync `SyncFeedStore` audited write paths expose the same notification payload shape through one shared helper.
  5. A notification log contains `event_type="radio_transcription.feed_audit_notification"`, `schema_version=1`, and the flat allowlisted audit fields without extra database reads or repeated JSON encode/decode cycles.
**Plans**: TBD

### Phase 2: Cloud Logging and Pub/Sub Routing
**Goal**: Matching notification logs reach a dedicated Pub/Sub route with least-privilege publishing, authenticated push, bounded retry, and DLQ configuration.
**Depends on**: Phase 1
**Requirements**: ROUTE-01, ROUTE-02, ROUTE-03, ROUTE-04
**Success Criteria** (what must be TRUE):
  1. Cloud Logging routes only `jsonPayload.event_type="radio_transcription.feed_audit_notification"` and `jsonPayload.schema_version=1` records to the notification Pub/Sub topic.
  2. The Log Router sink writer can publish to the notification topic with only the required Pub/Sub publisher IAM.
  3. The Pub/Sub push subscription invokes the relay endpoint through authenticated Cloud Run IAM/OIDC.
  4. The Pub/Sub subscription has 10 second minimum backoff, 60 second maximum backoff, and a dead-letter policy capped at 10 delivery attempts.
**Plans**: TBD

### Phase 3: Webhook Relay Delivery
**Goal**: A stateless Cloud Run relay turns Pub/Sub-delivered Cloud Logging entries into authenticated Watch Duty webhook calls without touching AlloyDB.
**Depends on**: Phase 2
**Requirements**: RELAY-01, RELAY-02, RELAY-03, RELAY-04, RELAY-05, RELAY-06
**Success Criteria** (what must be TRUE):
  1. Pub/Sub can `POST /pubsub/feed-audit-notifications` with a Cloud Logging `LogEntry`, and the relay extracts `jsonPayload` as the notification payload.
  2. The relay shallow-validates the required notification contract and forwards the flat payload unchanged to `/api/v1/echo/radio_transcription/internal/audit/webhook/` with `X-Api-Key`.
  3. The relay performs two total Watch Duty POST attempts for timeouts, connection failures, `429`, and `5xx` responses.
  4. Watch Duty `2xx` responses produce a `204` response to Pub/Sub, while malformed messages, unsupported schema, auth/config errors, and exhausted transient failures return non-2xx for Pub/Sub retry and eventual DLQ.
  5. Relay request handling is stateless and never reads from or writes to AlloyDB.
**Plans**: TBD

### Phase 4: Operations and Rollout Proof
**Goal**: Operators can verify, deploy, and diagnose the notification path from producer logs through Pub/Sub, relay delivery, Watch Duty response, and DLQ.
**Depends on**: Phase 3
**Requirements**: OPS-01, OPS-02, OPS-03, OPS-04
**Success Criteria** (what must be TRUE):
  1. Producer, routing, relay success, relay retryable failure, relay permanent/config failure, and DLQ paths emit structured operational logs.
  2. Deployment configuration documents the Cloud Logging sink, Pub/Sub topic, DLQ topic, push subscription, relay service, secret/env vars, and IAM bindings required for the path.
  3. A staging verification creates a real feed audit row and observes a Pub/Sub message plus Watch Duty webhook call without modifying the feed write path for delivery.
  4. The production rollout runbook lets an operator check routed logs, Pub/Sub backlog, push failures, and DLQ messages.
**Plans**: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 1 -> 2 -> 3 -> 4

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Audit Contract and Emission | 0/TBD | Not started | - |
| 2. Cloud Logging and Pub/Sub Routing | 0/TBD | Not started | - |
| 3. Webhook Relay Delivery | 0/TBD | Not started | - |
| 4. Operations and Rollout Proof | 0/TBD | Not started | - |

## Coverage

| Requirement Group | Requirements | Phase |
|-------------------|--------------|-------|
| Audit Emission | AUDIT-01, AUDIT-02, AUDIT-03, AUDIT-04, AUDIT-05 | Phase 1 |
| Payload Contract | PAYLOAD-01, PAYLOAD-02, PAYLOAD-03, PAYLOAD-04 | Phase 1 |
| Cloud Routing | ROUTE-01, ROUTE-02, ROUTE-03, ROUTE-04 | Phase 2 |
| Webhook Relay | RELAY-01, RELAY-02, RELAY-03, RELAY-04, RELAY-05, RELAY-06 | Phase 3 |
| Observability And Rollout | OPS-01, OPS-02, OPS-03, OPS-04 | Phase 4 |

Coverage: 23/23 v1 requirements mapped exactly once.

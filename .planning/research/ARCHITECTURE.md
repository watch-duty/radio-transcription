# Architecture Research

**Domain:** Feed audit notification delivery for radio transcription feed audit events
**Researched:** 2026-06-26
**Confidence:** MEDIUM

## Scope and Source Notes

This research covers the follow-up notification path only: storage-layer audit
notification emission, Cloud Logging sink routing, Pub/Sub push delivery, and a
Cloud Run relay. It intentionally does not re-research the transcription,
segmentation, ASR, or evaluated transcript notification pipeline.

The requested `.planning/codebase/ARCHITECTURE.md`,
`.planning/codebase/STRUCTURE.md`, and `.planning/codebase/INTEGRATIONS.md`
files were not available; `.planning/codebase/` existed but was empty during
research. Local conclusions are therefore based on `.planning/PROJECT.md`,
`.planning/ROADMAP.md`, `.planning/REQUIREMENTS.md`, Phase 06 artifacts, and
direct source inspection under `radio-transcription/`.

## Recommended Architecture

Use the existing durable `feed_audit_events` row as the system of record and
emit a best-effort structured log only after the audited storage statement has
succeeded. Route that log through Cloud Logging to Pub/Sub, push it to a small
authenticated Cloud Run relay, and have the relay POST to the downstream Watch
Duty endpoint with idempotency headers.

Do not add a second outbox table in this milestone. Do not publish Pub/Sub or
call external HTTP from inside feed mutation SQL or from the database
transaction. The current project explicitly treats Feed Audit Notifications as
operational signals, not the durable audit record.

### System Overview

```text
Admin/BFF feed mutations
Collector runtime lifecycle writes
Echo ingestion lifecycle writes
        |
        v
FeedStore / SyncFeedStore audited SQL
        |
        +--> AlloyDB feeds + feed_audit_events
        |       source of truth; commit/rollback boundary
        |
        +--> post-success structured log
                event_type="feed_audit_notification"
                schema_version="v1"
                idempotency_key="<feed_id>:<feed_revision>"
                |
                v
        Cloud Logging Log Router sink
                |
                v
        Pub/Sub topic: feed-audit-notifications
                |
                v
        Authenticated push subscription
        retry policy + optional DLQ
                |
                v
        Cloud Run relay: feed-audit-notification-relay
                |
                v
        Downstream Watch Duty receiver
```

All arrows are one-way. The relay must not write feed state, mutate audit rows,
or feed success/failure back into ingestion.

### Component Responsibilities

| Component | Responsibility | Boundary |
|-----------|----------------|----------|
| `feed_audit_events` table | Durable append-only audit history for meaningful feed mutations. | Owns audit truth, not delivery state. |
| `feeds.audit_revision` | Feed-local ordering and idempotency key material. | Gaps are acceptable; global ordering is not required. |
| `feed_audit_sql.py` | Shared CTE construction for before/after snapshots and audit inserts. | Should expose enough returned audit data for notification emission, but not know about Cloud Logging or Pub/Sub. |
| `FeedStore` / `SyncFeedStore` | Storage-layer mutation boundary. After a successful audited write, emit structured notification log. | Must never block ingestion on log routing, Pub/Sub, relay, or receiver failures. |
| Feed audit notification emitter | Convert returned audit row into a versioned JSON payload and log it via existing structured logging conventions. | Owns payload schema and safe logging behavior only. |
| Cloud Logging sink | Filter only `feed_audit_notification` logs and route matching entries to Pub/Sub. | Owns routing configuration and sink IAM, not payload interpretation. |
| Pub/Sub topic/subscription | Buffer push delivery, retry non-acknowledged messages, and optionally forward poison messages to a DLQ. | Owns delivery pressure and retry, not feed state. |
| Cloud Run relay | Decode Pub/Sub and Cloud Logging envelopes, validate the payload, classify receiver errors, and POST downstream. | Owns delivery attempts only; no AlloyDB dependency in v1. |
| Watch Duty receiver | Consume notifications idempotently. | Must tolerate duplicates, gaps, and out-of-order arrival. |

### Event Payload Contract

Emit a compact but self-contained notification payload so the relay does not
need to query AlloyDB:

```json
{
  "event_type": "feed_audit_notification",
  "schema_version": "v1",
  "idempotency_key": "<feed_id>:<feed_revision>",
  "feed_audit_event_id": "<uuid>",
  "feed_id": "<uuid>",
  "action": "feed.quarantined",
  "actor_id": "service_account:gcp:<service-account-unique-id>",
  "occurred_at": "2026-06-26T00:00:00Z",
  "feed_revision": 12,
  "before_values": {},
  "after_values": {}
}
```

Recommended sink filter shape:

```text
jsonPayload.event_type="feed_audit_notification"
jsonPayload.schema_version="v1"
```

The payload may duplicate the durable audit row because Cloud Logging/Pub/Sub is
the delivery path. The durable row remains authoritative if a log, route, or
delivery attempt is missed.

## Recommended Project Structure

```text
radio-transcription/
  backend/pipeline/storage/
    feed_audit_sql.py                  # extend audit CTE return shape
    feed_audit_notifications.py        # payload builder + structured logger
    feed_store.py                      # async storage hook after audited writes
    sync_feed_store.py                 # sync storage hook after audited writes
    tests/
      test_feed_audit_notifications.py # payload/logging behavior
      test_feed_store.py               # audited writes emit expected payloads
      test_sync_feed_store.py          # sync lifecycle emission coverage

  backend/pipeline/feed_audit_relay/
    main.py                            # Cloud Run HTTP endpoint for Pub/Sub push
    models.py                          # Pub/Sub LogEntry + notification schemas
    request_handler.py                 # outbound Watch Duty POST + classification
    tests/
      test_main.py                     # push envelope decode + ack behavior
      test_request_handler.py          # 2xx/4xx/5xx/timeout classification

  terraform/
    modules/                           # only add reusable public modules if topology is generic
    private-deploy-or-environment/     # likely owner for sink/topic/sub/Cloud Run wiring
```

The existing `backend/pipeline/notification/` package is transcript alert
notification logic. Do not overload it with feed audit delivery. Use a separate
package name such as `feed_audit_relay` so ownership and payload contracts stay
clear.

## Architectural Patterns

### Pattern 1: Storage-Truth, Log-Delivered Notification

**What:** Insert `feed_audit_events` in the same audited SQL statement that
mutates `feeds`, return the inserted event fields to the storage method, and
emit a structured log after the statement succeeds.

**When to use:** This matches the current project decision that feed audit
history is durable but notification delivery is best-effort.

**Trade-offs:** A process crash after DB commit but before log emission can
miss a notification. That is acceptable for this milestone. The alternative,
an outbox table plus dispatcher, gives stronger delivery but adds new durable
state and retry semantics that the project has explicitly deferred.

**Example shape:**

```python
row = await pool.fetchrow(audited_mutation_sql, *params)
feed = row_to_feed(row)

notification = feed_audit_notification_from_row(row)
if notification is not None:
    emit_feed_audit_notification(notification)

return feed
```

### Pattern 2: Narrow Log Router Sink

**What:** Use one structured-log event type and one version field, then create a
Cloud Logging sink whose filter matches only that event.

**When to use:** Use this when the delivery mechanism is operational and
best-effort, and when the application already emits logs to Cloud Logging.
This repo already initializes Google Cloud Logging in GCP through
`setup_logging()`.

**Trade-offs:** Log sinks route what reaches Cloud Logging. They are not a
transactional extension of AlloyDB. Sink filters must be verified in Logs
Explorer before enabling downstream delivery.

### Pattern 3: Authenticated Pub/Sub Push to Cloud Run

**What:** Route sink output to a Pub/Sub topic, attach an authenticated push
subscription, and grant the push service account `roles/run.invoker` on the
relay service.

**When to use:** This is the lowest-friction fit for a small serverless relay.
It uses Pub/Sub's push retry/ack model without adding subscriber client loops
to the application.

**Trade-offs:** Push endpoints acknowledge by HTTP status. Returning 2xx means
Pub/Sub stops retrying; returning any other status or timing out causes
redelivery. The relay must be careful to return 2xx for permanent malformed or
non-retryable receiver failures, and non-2xx only for transient retryable
failures.

### Pattern 4: Idempotent Relay

**What:** Treat `feed_audit_event_id` or `<feed_id>:<feed_revision>` as the
idempotency key and forward it to the receiver in headers and body.

**When to use:** Pub/Sub push and Cloud Run retries can duplicate delivery.
Cloud Logging routing does not guarantee application-level exactly-once
semantics.

**Trade-offs:** The relay can stay stateless if the downstream receiver owns
idempotence. If receiver idempotence is unavailable, add a small dedupe store in
the relay as a later hardening phase, not in the storage emission phase.

## Data Flow

### Storage Write Flow

1. Caller invokes a feed mutation through BFF, feeds-service, collector
   runtime, or Echo ingestion.
2. `FeedStore` or `SyncFeedStore` executes the audited SQL statement.
3. SQL updates current feed state and inserts a `feed_audit_events` row in the
   same statement/transaction when the mutation is meaningful.
4. The storage method receives the mutation result plus inserted audit event
   fields.
5. If an audit event was inserted, the storage method emits one structured log.
6. Logging exceptions are swallowed after local diagnostic logging; they do not
   change the feed mutation result.

### Cloud Delivery Flow

1. Cloud Logging receives the structured log.
2. The Log Router sink matches `event_type="feed_audit_notification"` and routes
   the log entry to the Pub/Sub topic.
3. Pub/Sub push sends a wrapped message to the Cloud Run relay endpoint.
4. The relay base64-decodes the `message.data` LogEntry, extracts
   `jsonPayload`, validates `schema_version`, and builds the outbound request.
5. The relay POSTs to the downstream Watch Duty receiver.
6. Receiver success causes relay 2xx and Pub/Sub ack. Retryable receiver
   failures cause relay non-2xx and Pub/Sub retry. Permanent failures are logged
   and acked.

### Relay Error Classification

| Condition | Relay response to Pub/Sub | Reason |
|-----------|---------------------------|--------|
| Valid payload, receiver 2xx | 204 | Successful delivery. |
| Malformed Pub/Sub envelope or missing notification payload | 204 after error log | Poison message should not loop forever. |
| Unsupported schema version | 204 after error log | Permanent until new code exists; route to logs/DLQ manually if needed. |
| Receiver 400/401/403/404/409 | 204 after error log | Non-retryable receiver contract/auth/config issue. |
| Receiver 408/429/5xx or network timeout | 500 | Retryable transient failure. |
| Relay bug/unhandled exception | 500 | Retry, alert, and eventually DLQ. |

## Failure Boundaries

| Boundary | Failure Mode | Effect on Feed Mutation | Recovery Path |
|----------|--------------|-------------------------|---------------|
| AlloyDB audited SQL | Feed update or audit insert fails | Mutation fails or rolls back; no notification should be emitted. | Existing storage error handling. |
| Structured log emission | Logger/client/process fails after commit | Mutation remains committed; notification may be missed. | Operational logs; acceptable best-effort gap. |
| Cloud Logging sink | Filter wrong, sink disabled, sink IAM missing | Mutation unaffected; routed notifications missing. | Validate sink with capture-only Pub/Sub subscription before relay. |
| Pub/Sub push | Relay returns non-2xx or times out | Mutation unaffected; Pub/Sub retries. | Retry policy, push backoff, DLQ for offline debugging. |
| Cloud Run relay | Decode bug or transient crash | Mutation unaffected; message retried. | Deploy fix; replay from subscription/DLQ if retained. |
| Downstream receiver | 5xx/timeout | Mutation unaffected; Pub/Sub retries. | Receiver recovery. |
| Downstream receiver | 4xx/permanent rejection | Mutation unaffected; relay logs and acks to prevent poison loop. | Fix receiver/config; use audit DB for backfill if required. |

## Build Order

1. **Define the contract and storage seam first.**
   Add the `feed_audit_notification` JSON contract, payload builder, and unit
   tests. Extend the audit CTE return shape so storage code can emit from DB
   truth rather than reconstructing event data from caller inputs.

2. **Emit structured logs locally with no cloud route.**
   Wire `FeedStore` and `SyncFeedStore` to emit after successful audited writes.
   Cover create, update, deactivate, reset, delete, failure, quarantine, and
   recovery paths. Verify no log is emitted for unchanged updates or suppressed
   repeated failures.

3. **Provision Cloud Logging sink and Pub/Sub topic in capture-only mode.**
   Create the Pub/Sub topic and sink filter, grant the sink writer identity
   publisher permissions, and validate by pulling messages from a temporary
   subscription. Do not connect the relay yet.

4. **Build the Cloud Run relay against recorded Pub/Sub fixtures.**
   Use sample Cloud Logging-routed Pub/Sub messages from step 3. Implement
   decode, schema validation, idempotency headers, receiver auth, and error
   classification with tests. Keep outbound endpoint configurable and support a
   dry-run/log-only mode for first deploy.

5. **Attach authenticated Pub/Sub push with conservative retry settings.**
   Create a dedicated push invoker service account, grant `roles/run.invoker`
   on the relay, configure push authentication, and add a DLQ if operations
   wants offline inspection of poison/transient failures.

6. **Enable downstream delivery gradually.**
   Start with a narrow sink filter or non-production environment, verify
   receiver idempotence on duplicate `idempotency_key` values, then widen to all
   feed audit notification actions. Backfill remains a separate operator task
   from the durable audit table, not part of the best-effort path.

This order minimizes risk because each layer can be validated independently:
storage truth before logs, logs before routing, routing before push, push before
external side effects.

## Scaling Considerations

| Scale | Architecture Adjustment |
|-------|-------------------------|
| Current/near-term feed audit volume | Structured logging plus Cloud Logging sink, Pub/Sub push, and one small Cloud Run relay is sufficient. |
| Higher admin/runtime mutation volume | Keep sink filter narrow, avoid oversized before/after payloads, tune Cloud Run concurrency, and monitor Pub/Sub backlog/oldest unacked age. |
| Strong delivery requirement | Revisit an outbox table with a dispatcher, delivery attempts, and replay tooling. Do not try to infer guaranteed delivery from Cloud Logging. |

### Scaling Priorities

1. **First bottleneck:** Relay or receiver latency causing Pub/Sub push backlog.
   Fix with relay concurrency, receiver timeout tuning, and DLQ visibility.
2. **Second bottleneck:** Oversized log payloads increasing Cloud Logging and
   Pub/Sub cost. Fix by trimming payload to the fields downstream truly needs.
3. **Third bottleneck:** Need for guaranteed replay/backfill. Fix with an
   explicit outbox or replay command sourced from `feed_audit_events`.

## Anti-Patterns

### Direct HTTP or Pub/Sub Publish Inside Feed Mutation

**What people do:** Call the Watch Duty endpoint or publish Pub/Sub from inside
feed service methods before or during the DB mutation.

**Why it is wrong:** It couples ingestion availability to external delivery and
creates impossible states where delivery succeeds but DB commit fails, or DB
commit succeeds but the process is waiting on a receiver.

**Do this instead:** Commit audited state first, then emit a best-effort
structured log.

### Parallel Outbox Table for the Same V1 Event

**What people do:** Insert both `feed_audit_events` and a separate
notification/outbox row for the same logical event.

**Why it is wrong:** It reintroduces dispatcher state and duplicate durability
semantics before the project needs guaranteed delivery.

**Do this instead:** Treat `feed_audit_events` as truth and Cloud
Logging/Pub/Sub as the best-effort signal path.

### Relay Reads AlloyDB for Enrichment

**What people do:** Make the relay query feeds-service or AlloyDB to rebuild
the notification.

**Why it is wrong:** It makes delivery depend on live feed state after the
event, breaks hard-delete semantics, and adds another production dependency to
the retry path.

**Do this instead:** Put the notification fields needed by the receiver in the
structured log payload.

### Poison Message Infinite Retry

**What people do:** Return 500 for malformed payloads or permanent receiver
contract failures.

**Why it is wrong:** Pub/Sub will redeliver, creating noisy retry loops that do
not repair themselves.

**Do this instead:** Ack permanent failures after logging; reserve non-2xx for
transient failures.

### Broad Sink Filters

**What people do:** Route all application logs from ingestion or feeds-service
to the notification topic.

**Why it is wrong:** It increases cost, complicates relay parsing, and risks
delivering non-audit operational logs downstream.

**Do this instead:** Filter only the stable `event_type` and `schema_version`.

## Integration Points

### External Services

| Service | Integration Pattern | Notes |
|---------|---------------------|-------|
| Cloud Logging | Python structured logs routed by Log Router sink. | Existing repo logging initializes Cloud Logging in GCP. Verify exact `jsonPayload` fields in Logs Explorer. |
| Pub/Sub topic | Sink destination for matching log entries. | Sink writer identity needs publisher permission on the topic. |
| Pub/Sub push subscription | Authenticated HTTPS push to Cloud Run. | 2xx acks; non-2xx/timeouts retry. Configure retry policy and optional DLQ. |
| Cloud Run relay | Auth-required HTTP service invoked by Pub/Sub push service account. | Grant push invoker service account `roles/run.invoker`; keep service private. |
| Watch Duty receiver | Outbound POST from relay. | Must be idempotent by `feed_audit_event_id` or `<feed_id>:<feed_revision>`. |

### Internal Boundaries

| Boundary | Communication | Notes |
|----------|---------------|-------|
| Feed mutation callers -> storage | Python method calls. | Existing callers should not know notification delivery exists. |
| Storage SQL -> notification emitter | Returned audit event fields. | Prefer DB-returned event data over reconstructing from request inputs. |
| Storage emitter -> Cloud Logging | Structured log only. | No Pub/Sub client in storage layer. |
| Relay -> receiver | HTTP POST. | Relay owns retries via Pub/Sub response status, not storage. |

## Roadmap Implications

Suggested phase structure:

1. **Storage notification contract** - Adds payload schema, DB-returned audit
   event data, structured-log emitter, and focused storage tests.
   - Addresses: storage-layer audit emission.
   - Avoids: external delivery coupled to feed writes.

2. **Cloud route capture** - Provisions Cloud Logging sink and Pub/Sub topic,
   validates routed messages through a temporary pull/capture subscription.
   - Addresses: Cloud Logging sink and Pub/Sub topic wiring.
   - Avoids: debugging sink IAM and relay behavior at the same time.

3. **Relay implementation** - Builds Cloud Run relay with fixture-based tests,
   dry-run mode, auth configuration, idempotency headers, and error
   classification.
   - Addresses: Cloud Run relay behavior.
   - Avoids: deploying push delivery before message parsing is proven.

4. **Authenticated push rollout** - Connects Pub/Sub push to Cloud Run, configures
   retry/DLQ, enables downstream POST in non-production or narrow-filter mode,
   then expands.
   - Addresses: end-to-end delivery.
   - Avoids: broad production blast radius on the first route.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Existing audit storage boundary | HIGH | Verified from `feed_audit_sql.py`, `feed_queries.py`, `sync_feed_queries.py`, and audit migration. |
| Logging sink/Pub/Sub/Cloud Run mechanics | HIGH | Verified against Context7 and current official Google Cloud docs. |
| Private deployment/IaC placement | LOW | Private deployment topology is not present in this worktree. |
| Exact receiver contract | LOW | Downstream Watch Duty receiver API was not provided. |
| Overall build order | MEDIUM | Strong local and GCP evidence, but private IaC and receiver details remain unknown. |

## Sources

- Local: `.planning/PROJECT.md`, `.planning/ROADMAP.md`, `.planning/REQUIREMENTS.md`, `.planning/STATE.md`.
- Local: `.planning/phases/06-stable-actor-identity/06-CONTEXT.md`, `06-RESEARCH.md`, and `06-PATTERNS.md`.
- Local: `radio-transcription/CONTEXT.md` for Feed Audit Notification semantics.
- Local: `radio-transcription/backend/pipeline/storage/feed_audit_sql.py` for shared audit insert CTEs and snapshot allowlist.
- Local: `radio-transcription/backend/pipeline/storage/feed_queries.py` and `sync_feed_queries.py` for audited mutation paths.
- Local: `radio-transcription/backend/pipeline/common/log_helper.py` for existing Cloud Logging setup and structured logging pattern.
- Local: `radio-transcription/backend/pipeline/notification/send_notification.py` and `request_handler.py` for existing Pub/Sub-triggered notification/error-classification precedent.
- Context7 CLI: `/googleapis/python-logging`, queried 2026-06-26, confirmed Cloud Logging sinks can route matching logs to Pub/Sub.
- Official Google Cloud Logging docs: https://docs.cloud.google.com/logging/docs/export/configure_export_v2
- Official Google Cloud Logging Pub/Sub export docs: https://docs.cloud.google.com/logging/docs/export/pubsub
- Official Pub/Sub push docs: https://docs.cloud.google.com/pubsub/docs/push
- Official Pub/Sub push authentication docs: https://docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions
- Official Cloud Run Pub/Sub tutorial: https://docs.cloud.google.com/run/docs/tutorials/pubsub
- Official Pub/Sub retry policy docs: https://docs.cloud.google.com/pubsub/docs/subscription-retry-policy
- Official Pub/Sub dead-letter topics docs: https://docs.cloud.google.com/pubsub/docs/dead-letter-topics

---
*Architecture research for: feed audit notification delivery*
*Researched: 2026-06-26*

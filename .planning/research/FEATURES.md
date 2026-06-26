# Feature Research

**Domain:** feed audit notification webhook delivery
**Researched:** 2026-06-26
**Confidence:** MEDIUM

## Feature Landscape

### Table Stakes (Users Expect These)

Features users assume exist. Missing these = product feels incomplete.

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Deliver every committed feed audit event | The milestone goal is every `feed_audit_events` insert reaching the WD backend webhook on a best-effort basis. | HIGH | Delivery must cover all current audit actions: `feed.created`, `feed.updated`, `feed.deactivated`, `feed.reset`, `feed.deleted`, `feed.failure_reported`, `feed.quarantined`, and `feed.recovered`. It should not invent new lifecycle semantics. |
| Non-blocking ingestion and admin mutations | Existing ingestion availability is a hard boundary; webhook outage must not stop feed lifecycle writes. | HIGH | Do not call the WD webhook from the feed mutation transaction, collector hot path, Echo handler, or SQL CTE. Publish or enqueue only after the audit row is committed, or use a separate dispatcher that reads committed audit rows. |
| Durable handoff from audit row to dispatcher | Best-effort delivery still needs a recoverable point between DB commit and HTTP delivery. | HIGH | Prefer `feed_audit_events` as the durable source of truth and enqueue references to event IDs. Avoid duplicating the same logical audit payload into a second outbox table unless a minimal delivery-attempt table is required for operations. |
| Stable idempotency identity | Retries and queue redelivery make duplicates normal, so the WD backend needs a stable key. | MEDIUM | Use `feed_audit_events.id` as the delivery ID and include `feed_id` plus `feed_revision` for feed-local ordering. Send the ID in the JSON body and an idempotency header such as `X-WD-Event-Id`. |
| Versioned webhook payload contract | The receiver needs a stable shape independent of internal SQL details. | MEDIUM | Payload should include `schema_version`, `event_id`, `feed_id`, `action`, `actor_id`, `occurred_at`, `feed_revision`, `before_values`, and `after_values`. Keep current audit JSON allowlists; do not add raw request payloads or secrets. |
| Feed-local ordering metadata | Existing audit semantics are feed-local, not globally ordered. | MEDIUM | Use `feed_revision` as the ordering truth. If Pub/Sub is used, publish with `feed_id` as ordering key where practical, but requirements should still tolerate duplicate/out-of-order HTTP attempts. |
| Bounded retry and backoff | Webhooks fail transiently; best-effort delivery without retry is too weak. | MEDIUM | Retry network errors, timeouts, 429, and 5xx with bounded exponential backoff. Treat most 4xx responses as permanent configuration/contract failures after logging loudly. |
| Dead-letter or stuck-delivery surface | Failed events must not disappear silently after retry exhaustion. | MEDIUM | Use Pub/Sub dead-letter topics or an equivalent stuck-delivery marker. Operators need enough detail to identify event ID, response class, last error, and next action. |
| Manual replay path | "Best effort" needs an operator recovery mechanism when the WD backend was down or misconfigured. | MEDIUM | MVP can be a script/CLI that replays by `event_id`, feed ID, revision range, or time range from `feed_audit_events`. UI/API replay can wait. |
| Delivery observability | Operators need proof that audit delivery is healthy without querying raw logs by hand. | MEDIUM | Emit structured logs/metrics for enqueue success/failure, delivery success, retryable failure, permanent failure, DLQ, backlog age, and per-action counts. Reuse the repo's `record_pipeline_stage` style where it fits. |
| Authentication for WD backend calls | Audit events include actor and feed change data, so outbound calls must not be unauthenticated. | MEDIUM | At minimum match the existing notification pattern of configured endpoint plus secret/API key. HMAC signatures or ID-token auth are stronger but depend on WD backend support. |
| Local/test fake receiver | This project is delivery infrastructure; tests need deterministic webhook behavior. | LOW | Add unit tests around payload conversion/retry classification and integration/component tests with a fake webhook. Use `safe-run --` for heavier suites per repo instructions. |

### Differentiators (Competitive Advantage)

Features that set the product apart. Not required, but valuable.

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| Persisted delivery attempt history | Makes troubleshooting precise without depending only on log retention. | MEDIUM | Defer unless operators need audit-delivery forensics immediately. A compact `feed_audit_event_delivery_attempts` table can store status, attempt count, response code, and timestamps without duplicating payloads. |
| Operator-facing delivery status | Lets admins answer "has WD backend seen this event?" quickly. | HIGH | Useful after MVP, but requires read API/BFF/UI work. Logs, metrics, and DLQ are enough for first delivery milestone. |
| Replay by filter in admin tooling | Reduces operational friction after webhook outages. | HIGH | CLI/script replay is sufficient first. UI replay should wait until delivery semantics and safety checks are proven. |
| HMAC signature with key rotation | Stronger receiver verification than a static API key. | MEDIUM | Valuable if the WD backend endpoint is internet-facing or shared. Defer if private ingress or ID-token auth is the deployment model. |
| Backfill existing historical audit rows | Helps seed WD backend with pre-delivery history. | MEDIUM | Not part of "every future insert." Run only as an explicit one-time migration/tool after receiver idempotency is proven. |
| Adaptive throttling/rate limiting | Protects WD backend during backlog drain or failure recovery. | MEDIUM | Defer until actual throughput/backlog data exists. Include simple concurrency/timeout controls in MVP. |
| Multi-endpoint subscription management | Could support future consumers of audit events. | HIGH | Do not build now; this milestone is specifically WD backend delivery. |

### Anti-Features (Commonly Requested, Often Problematic)

Features that seem good but create problems.

| Feature | Why Requested | Why Problematic | Alternative |
|---------|---------------|-----------------|-------------|
| Synchronous HTTP webhook in feed mutation path | Lowest apparent latency and simple control flow. | Couples ingestion/admin writes to WD backend availability, violates the project boundary, and can turn webhook slowness into feed ingestion incidents. | Commit audit row first, then asynchronously enqueue or dispatch committed event IDs. |
| Treat webhook delivery as the audit source of truth | Downstream systems want a direct event stream. | Existing project explicitly makes `feed_audit_events` the durable ledger; delivery is best-effort follow-up. | Keep AlloyDB audit rows authoritative and make delivery replayable from the ledger. |
| Exactly-once delivery requirement | Sounds safer for audit data. | Pub/Sub and HTTP webhook delivery can redeliver messages; exact end-to-end semantics would add substantial state and still require receiver idempotency. | Define at-least-once delivery attempts plus stable idempotency keys and replay. |
| Audit heartbeat, lease, bookmark, or progress-only writes | More events can seem more complete. | Existing audit scope intentionally excludes noisy runtime churn; adding it would flood WD backend and reduce signal quality. | Deliver only rows that already exist in `feed_audit_events`. |
| Re-research or rework transcription pipeline delivery | Notification code already exists and may look reusable. | The milestone is feed audit webhook delivery, not audio alert notification or transcription pipeline changes. | Reuse patterns where helpful, but keep implementation under feed audit delivery boundaries. |
| Generic webhook platform | Future consumers may want subscriptions, filters, and dashboards. | Overbuilds the first WD backend use case and distracts from reliability of one critical integration. | Ship one configured WD backend destination with clear extension points. |
| Store raw feed/request payloads in delivery records | Makes replay/debugging look easier. | Risks leaking credentials or operational metadata and bypasses the existing audit snapshot allowlist. | Store event IDs and bounded delivery metadata; reconstruct payload from `feed_audit_events`. |
| Duplicate full audit payload into an outbox table | Familiar transactional outbox shape. | Project decisions already avoided duplicating the logical event into separate audit and outbox rows. | Use `feed_audit_events` as the event table; if needed, add minimal delivery state keyed by audit event ID. |

## Feature Dependencies

```text
Existing audited feed mutation SQL
    └──requires──> committed feed_audit_events row
                       └──requires──> durable non-blocking handoff
                                          └──requires──> delivery worker/function
                                                             └──requires──> WD webhook auth/config
                                                             └──requires──> retry/backoff policy
                                                             └──requires──> structured observability

Stable event_id + feed_revision
    └──enables──> idempotent receiver behavior
    └──enables──> replay by event/range
    └──enables──> feed-local ordering checks

Dead-letter/stuck-delivery handling
    └──requires──> retry classification
    └──requires──> manual replay path

Synchronous HTTP in mutation path
    └──conflicts──> non-blocking ingestion/admin writes

Exactly-once delivery promise
    └──conflicts──> Pub/Sub/webhook duplicate delivery reality
```

### Dependency Notes

- **Committed audit rows before delivery:** the current SQL writes audit rows inside feed mutation statements; delivery must observe committed rows so webhook failure cannot roll back feed state.
- **Handoff before webhook worker:** without a durable handoff or dispatcher cursor, a process crash after commit can lose the notification even though the audit row exists.
- **Idempotency before retry/replay:** retries, Pub/Sub redelivery, and manual replay all require the WD backend to recognize duplicate `event_id` values.
- **Observability before launch:** "best effort" is only acceptable if delivery gaps are visible through metrics, logs, DLQ/stuck-event counts, and replay tooling.
- **Receiver contract before payload finalization:** exact auth header names, accepted response codes, timeout budget, and payload versioning need WD backend confirmation before implementation.

## MVP Definition

### Launch With (v1)

Minimum viable product - what's needed to validate the concept.

- [ ] Deliver all future committed `feed_audit_events` rows to one configured WD backend webhook destination.
- [ ] Keep feed mutations and ingestion available when the webhook, queue, or delivery worker is down.
- [ ] Use a versioned JSON payload with `event_id`, `feed_id`, `feed_revision`, action, actor, timestamps, and before/after snapshots.
- [ ] Include a stable idempotency key/header and require receiver idempotency in the contract.
- [ ] Retry transient HTTP/network failures with bounded backoff and classify permanent 4xx failures.
- [ ] Surface exhausted/stuck deliveries through DLQ or equivalent state plus structured logs/metrics.
- [ ] Provide operator replay by event ID/range from `feed_audit_events`.
- [ ] Cover payload conversion, enqueue/dispatch failure behavior, retry classification, and fake-webhook integration tests.

### Add After Validation (v1.x)

Features to add once core is working.

- [ ] Persist compact delivery attempt history - add when logs/DLQ are insufficient for operations.
- [ ] Delivery status read API - add when operators need to inspect delivery state outside logs.
- [ ] Admin/BFF replay controls - add after CLI replay has a proven safety model.
- [ ] HMAC signature/key rotation - add if endpoint exposure or receiver requirements need stronger request verification.
- [ ] Adaptive rate limiting - add after measuring backlog drain behavior and WD backend limits.

### Future Consideration (v2+)

Features to defer until product-market fit is established.

- [ ] Multi-destination audit event subscriptions - defer until there is a second real consumer.
- [ ] Historical backfill of all existing audit rows - defer until future-event delivery and idempotency are proven.
- [ ] Operator UI timeline merged with delivery status - defer until audit read APIs are part of a separate visibility milestone.
- [ ] Retention/archive strategy for audit and delivery metadata - defer unless delivery state materially changes storage growth.

## Feature Prioritization Matrix

| Feature | User Value | Implementation Cost | Priority |
|---------|------------|---------------------|----------|
| Non-blocking committed-row delivery | HIGH | HIGH | P1 |
| Complete coverage of current audit actions | HIGH | MEDIUM | P1 |
| Versioned payload contract | HIGH | MEDIUM | P1 |
| Idempotency key/header | HIGH | LOW | P1 |
| Retry/backoff classification | HIGH | MEDIUM | P1 |
| DLQ/stuck-delivery signal | HIGH | MEDIUM | P1 |
| Manual replay script/CLI | HIGH | MEDIUM | P1 |
| Structured delivery metrics/logs | HIGH | MEDIUM | P1 |
| Persisted attempt history | MEDIUM | MEDIUM | P2 |
| Delivery status API/UI | MEDIUM | HIGH | P2 |
| HMAC signing/key rotation | MEDIUM | MEDIUM | P2 |
| Historical backfill | MEDIUM | MEDIUM | P3 |
| Multi-endpoint subscriptions | LOW | HIGH | P3 |

**Priority key:**
- P1: Must have for launch
- P2: Should have, add when possible
- P3: Nice to have, future consideration

## Competitor Feature Analysis

This is not a competitive product surface. The relevant comparison is against proven webhook and queue-delivery platform behavior.

| Feature | Platform Pattern | Our Approach |
|---------|------------------|--------------|
| Fast non-blocking webhook processing | GitHub recommends responding quickly and using a queue for asynchronous processing when webhook work could exceed timeout. | Do not block ingestion/admin writes on outbound WD webhook calls; use an asynchronous dispatcher. |
| Retry and redelivery | Stripe retries failed webhook deliveries with exponential backoff; Pub/Sub redelivers unacknowledged messages. | Use bounded retry/backoff, classify failures, and expect duplicates. |
| Idempotency | Stripe documents idempotency keys for safe retries; Pub/Sub docs require idempotent subscribers because messages can be redelivered. | Use `feed_audit_events.id` as the idempotency key and require WD backend duplicate handling. |
| Dead-letter handling | Pub/Sub supports dead-letter topics with configurable maximum delivery attempts. | Use DLQ or equivalent stuck-event state for exhausted deliveries. |
| Manual recovery | GitHub supports manual redelivery for recent webhook deliveries. | Provide replay from the durable audit ledger by event ID/range; UI can wait. |

## Sources

- `.planning/PROJECT.md` - local project scope, delivery boundary, audit ledger decisions. Confidence: HIGH.
- `radio-transcription/.planning/codebase/ARCHITECTURE.md` - existing pipeline, services, storage, notification component boundaries. Confidence: HIGH.
- `radio-transcription/.planning/codebase/TESTING.md` - repo test patterns and verification commands. Confidence: HIGH.
- `radio-transcription/.planning/codebase/CONCERNS.md` - known risks, missing feed audit delivery, audit table growth. Confidence: HIGH.
- `radio-transcription/terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql` - audit event schema/actions/indexes. Confidence: HIGH.
- `radio-transcription/backend/pipeline/storage/feed_audit_sql.py` and `feed_queries.py` - audited mutation SQL and snapshot allowlist. Confidence: HIGH.
- `radio-transcription/backend/pipeline/notification/send_notification.py` and `request_handler.py` - existing outbound notification retry/auth pattern. Confidence: HIGH.
- Context7 `/googleapis/python-pubsub` docs for publisher `publish`, ordering keys, futures, and dead-letter policy fields: https://github.com/googleapis/python-pubsub/blob/main/docs/pubsub/publisher/api/client.md and https://github.com/googleapis/python-pubsub/blob/main/docs/pubsub/types.md. Confidence: HIGH.
- Google Cloud Pub/Sub subscription overview: https://docs.cloud.google.com/pubsub/docs/subscription-overview. Confidence: HIGH.
- Google Cloud Pub/Sub push troubleshooting: https://docs.cloud.google.com/pubsub/docs/push-troubleshooting. Confidence: HIGH.
- Google Cloud Pub/Sub retry policy: https://docs.cloud.google.com/pubsub/docs/subscription-retry-policy. Confidence: HIGH.
- Google Cloud Pub/Sub dead-letter topics: https://docs.cloud.google.com/pubsub/docs/dead-letter-topics. Confidence: HIGH.
- GitHub webhook troubleshooting/redelivery docs: https://docs.github.com/en/webhooks/testing-and-troubleshooting-webhooks/troubleshooting-webhooks and https://docs.github.com/en/webhooks/testing-and-troubleshooting-webhooks/redelivering-webhooks. Confidence: MEDIUM for general webhook delivery patterns.
- Stripe webhook delivery/idempotency docs: https://docs.stripe.com/webhooks and https://docs.stripe.com/api/idempotent_requests. Confidence: MEDIUM for general webhook delivery patterns.

## Open Questions For Requirements

- What exact WD backend auth mechanism is required: API key, HMAC signature, Google ID token, private ingress, or a combination?
- What response codes should be treated as permanent versus retryable for the WD backend? Confirm handling for 400, 401/403, 404, 409, 408, 429, and 5xx.
- Does WD backend require strict per-feed ordering, or is idempotent upsert by `event_id` plus `feed_revision` sufficient?
- What is the acceptable delivery latency target for normal operation and backlog replay?
- Should delivery attempt state be persisted in the database for v1, or are logs/metrics/DLQ plus replay enough for the first milestone?

---
*Feature research for: feed audit notification webhook delivery*
*Researched: 2026-06-26*

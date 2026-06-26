# Pitfalls Research

**Domain:** Best-effort feed audit notification delivery via Cloud Logging log sinks, Pub/Sub, and webhook dispatch in a brownfield radio transcription ingestion system
**Researched:** 2026-06-26
**Confidence:** HIGH for Google Cloud and repo-specific pitfalls; MEDIUM for phase names because the follow-up roadmap is not created yet

## Recommended Phase Vocabulary

Use these phase names when turning this research into the next roadmap:

| Proposed Phase | Purpose |
|----------------|---------|
| Phase 1: Delivery Contract | Define the exact committed audit event shape, log schema, sink filter, redaction rules, idempotency key, and non-blocking guarantees. |
| Phase 2: Logging and Pub/Sub Infrastructure | Add Terraform for the log sink, Pub/Sub topic/subscription, dead-letter topic, IAM, metrics, and alerts. |
| Phase 3: Webhook Dispatcher | Build the parser/dispatcher with explicit ack/retry/permanent-failure behavior, idempotency handling, authentication, and endpoint timeout policy. |
| Phase 4: Verification and Rollout | Run staging smoke tests, replay/miss detection checks, dashboards, alert validation, and gradual production rollout. |

## Critical Pitfalls

### Pitfall 1: Turning Best-Effort Delivery Into a Feed Mutation Dependency

**What goes wrong:**
Feed create/update/failure/quarantine writes start waiting on Cloud Logging, Pub/Sub, webhook calls, endpoint auth, or delivery state. A downstream notification outage then slows or breaks ingestion, feed leasing, recovery, or admin mutation paths.

**Why it happens:**
Audit delivery is tempting to implement like an outbox or transactional webhook because the existing audit ledger is transactionally coupled to feed writes. The follow-up delivery requirement is different: the durable ledger is the system of record, and notification is explicitly best-effort.

**How to avoid:**
Phase 1 must state that feed writes only commit durable audit rows and optionally emit a post-commit structured log. No feed mutation path should call a webhook or wait for Pub/Sub publish confirmation. If the design emits a log from application code, emit only after the storage operation returns committed audit data, and include the committed audit event ID or `(feed_id, feed_revision)` tuple so the dispatcher never depends on speculative request state.

**Warning signs:**
- Feed store methods import Pub/Sub, webhook, or Cloud Logging sink-specific clients.
- Unit tests for feed mutations mock network delivery.
- A failed notification causes a feed mutation transaction rollback.
- Ingestion code adds retries around audit notification work instead of around feed storage work.

**Phase to address:**
Phase 1: Delivery Contract.

---

### Pitfall 2: Emitting Phantom Notifications Before the Audit Transaction Commits

**What goes wrong:**
The dispatcher sends a webhook for a feed audit event that never committed, or sends a before/after snapshot that does not match the persisted `feed_audit_events` row.

**Why it happens:**
Structured logs are easy to emit near the same code that prepares a mutation. In this repo, audited feed writes are built from raw SQL CTEs and rollback tests already prove that rejected audit writes roll back feed state. A log emitted inside or before that transaction has weaker consistency than the ledger.

**How to avoid:**
Phase 1 must require notification logs to carry committed ledger identifiers and DB-derived values only. Prefer emitting from a single storage/service boundary after the audited write returns. If the implementation cannot reliably log after commit, make the dispatched payload minimal and have the dispatcher re-read `feed_audit_events` by ID before sending.

**Warning signs:**
- Notification payloads are assembled from API request bodies or caller-provided "before" values.
- Logs include `action` and `after_values` before the store call returns.
- Tests cover successful notification emission but not storage rollback with no notification.

**Phase to address:**
Phase 1: Delivery Contract, with rollback regression tests in Phase 3.

---

### Pitfall 3: Treating Log Routing as a Durable Outbox

**What goes wrong:**
Operators assume every audit event will eventually notify. In reality, Cloud Logging sinks route only matching log entries that arrive after the sink is configured correctly; misconfigured sinks do not route entries until fixed. There is no automatic backfill from the log sink path.

**Why it happens:**
The phrase "log-routed Pub/Sub" sounds durable because Pub/Sub is durable once a message is on the topic. The weak point is before Pub/Sub: log emission, sink filtering, sink IAM, and sink configuration.

**How to avoid:**
Phase 1 must document that the audit table remains the source of truth and notification is a convenience signal. Phase 4 must include a reconciliation check that compares recent `feed_audit_events` counts to routed notification log counts. Do not market this as complete delivery until a real DB outbox/replay path exists.

**Warning signs:**
- Roadmap success criteria say "all audit events are delivered" without a reconciliation or replay mechanism.
- No operator-visible distinction between "audit recorded" and "notification attempted".
- Missing notifications are investigated only in Pub/Sub, not in Cloud Logging sink metrics or sink error logs.

**Phase to address:**
Phase 1: Delivery Contract and Phase 4: Verification and Rollout.

---

### Pitfall 4: Sink Filter Drift Sends Too Much, Too Little, or the Wrong Data

**What goes wrong:**
The log sink routes unrelated pipeline logs, misses valid audit notification logs, sends duplicate events from multiple matching sinks, or routes sensitive diagnostic fields because the filter targets broad message text instead of a stable structured field.

**Why it happens:**
The repo already uses structured logs through `extra={"json_fields": ...}` for pipeline metrics and ingestion telemetry. Adding another event type without a strict schema and golden tests invites filter drift. Cloud Logging also routes every sink independently, so overlapping sinks can fan out the same log entry.

**How to avoid:**
Phase 1 must define a dedicated event type such as `feed_audit_notification_requested`, a dedicated logger or log name if practical, and a minimal allowlisted payload. Phase 2 must create a sink filter that matches only stable structured fields, preview the filter in Logs Explorer, and include an exclusion strategy for test/local noise.

**Warning signs:**
- Sink filter uses only free-form `message:` text.
- Filter omits `jsonPayload.event_type` or equivalent structured match.
- Preview returns heartbeat, chunk ingestion, alert notification, or unrelated service logs.
- Test fixtures assert human-readable log messages rather than structured keys.

**Phase to address:**
Phase 1: Delivery Contract and Phase 2: Logging and Pub/Sub Infrastructure.

---

### Pitfall 5: Missing Sink Writer IAM or Destination Terraform

**What goes wrong:**
The sink exists but routes nothing to Pub/Sub because the destination topic does not exist, the sink writer identity lacks `roles/pubsub.publisher`, or Terraform later removes an out-of-band IAM grant. Operators only notice when notifications are absent.

**Why it happens:**
Cloud Logging creates or reports a sink writer identity that must be granted destination permissions. Cross-project destinations and Terraform-managed IAM make this easy to miss. The current Terraform has a generic Cloud Function module with Pub/Sub triggers but no observed log sink module.

**How to avoid:**
Phase 2 must provision the Pub/Sub topic before the sink, capture the sink writer identity, grant it publisher access in Terraform, and add an alert on Logging export error metrics and `logging_sink` error logs. Terraform should own all IAM required for log routing, not a console step.

**Warning signs:**
- Terraform creates `google_logging_project_sink` but no IAM binding for the sink writer identity.
- Sink destination is typed as a bare topic name instead of `pubsub.googleapis.com/projects/.../topics/...`.
- Sink error logs show `topic_not_found`, permission errors, or increasing `exports/error_count`.
- Deployment instructions include manual console IAM fixes.

**Phase to address:**
Phase 2: Logging and Pub/Sub Infrastructure.

---

### Pitfall 6: Duplicate and Out-of-Order Webhook Delivery

**What goes wrong:**
External receivers see duplicate notifications for the same audit event, or receive `feed.updated` after a later `feed.quarantined` event. If the receiver treats each webhook as unique truth, dashboards, tickets, or downstream incident workflows become noisy or wrong.

**Why it happens:**
Pub/Sub defaults to at-least-once delivery and no ordering guarantee. Push and export subscriptions do not support exactly-once delivery. Publish-side duplicates can still occur even when exactly-once is enabled for eligible pull subscriptions. Dead-letter forwarding is best-effort and can also affect ordering.

**How to avoid:**
Phase 1 must include an idempotency key based on committed audit identity, preferably `feed_audit_event.id`, with `(feed_id, feed_revision)` as a defensible fallback. Phase 3 must send that key in the payload and/or an `Idempotency-Key` header, treat receiver duplicate acceptance as success, and never depend on Pub/Sub `messageId` as the business identity.

**Warning signs:**
- Webhook payload lacks `audit_event_id` and `feed_revision`.
- Dispatcher dedupes only on Pub/Sub `messageId` or Cloud Logging `insertId`.
- Tests assert exact send count without duplicate-message cases.
- Product language implies chronological external delivery.

**Phase to address:**
Phase 1: Delivery Contract and Phase 3: Webhook Dispatcher.

---

### Pitfall 7: Poison Messages Create Retry Storms or Silent Drops

**What goes wrong:**
Malformed log entries, schema drift, missing fields, 4xx webhook errors, or endpoint auth failures either retry forever and grow Pub/Sub backlog or get acknowledged without any durable operational signal.

**Why it happens:**
The existing codebase already has mixed Pub/Sub failure semantics in `backend/pipeline/evaluation/processor.py`, and the current notification Cloud Function module uses `RETRY_POLICY_DO_NOT_RETRY`. The alert notification sender catches 4xx as non-retryable but retries selected 5xx errors inside urllib3. Feed audit delivery needs its own explicit policy.

**How to avoid:**
Phase 3 must define an ack policy before coding:
- Invalid envelope or unsupported schema version: ack, count, and log a permanent parse failure.
- Valid event with missing committed audit identity: ack to avoid poison-looping, count as contract failure.
- Receiver 400/401/403/404: classify intentionally. Bad configuration should alert; bad payload should be permanent.
- Receiver 429/5xx/timeouts: retry with bounded backoff and a subscription dead-letter topic.

Phase 2 must configure the dead-letter topic and the Pub/Sub service account IAM required for dead lettering. Phase 4 must verify dead-letter metrics and run a bad-message smoke test.

**Warning signs:**
- Error handling says only `raise` or `return` without naming ack/retry outcome.
- No dead-letter subscription exists.
- 4xx responses are retried indefinitely.
- Malformed messages disappear with only a debug log.

**Phase to address:**
Phase 2: Logging and Pub/Sub Infrastructure, Phase 3: Webhook Dispatcher, and Phase 4: Verification and Rollout.

---

### Pitfall 8: Push Endpoint Backpressure Blocks Delivery for Healthy Events

**What goes wrong:**
A slow or failing webhook endpoint causes push backoff, growing backlog, old audit notifications, and delayed alerts for unrelated feed events. Endpoint latency becomes the throughput limiter.

**Why it happens:**
Pub/Sub push acknowledgments are HTTP status codes. Non-success responses and acknowledgment deadline expiry cause redelivery. Push subscriptions back off when they see too many failures, and endpoint latency directly affects throughput. Ordered delivery can further reduce push throughput when the same ordering key is hot.

**How to avoid:**
Phase 3 should keep the dispatcher fast and bounded: short connect/read timeouts, no long feed API fanout per event, limited payload transformations, and explicit max body size. Avoid ordered push delivery unless the receiver truly requires it; if ordering is required, use feed ID as the key and document the latency tradeoff. Phase 4 should alert on push response classes, latency, oldest unacked age, and outstanding messages.

**Warning signs:**
- Dispatcher fetches full feed history or all tags before every send.
- Push subscription enables ordering without a receiver requirement.
- Webhook timeout exceeds the ack deadline or Cloud Function timeout.
- `push_request_count` error classes rise while backlog age grows.

**Phase to address:**
Phase 3: Webhook Dispatcher and Phase 4: Verification and Rollout.

---

### Pitfall 9: Leaking Audit Snapshots, Diagnostics, or Secrets Through Logs

**What goes wrong:**
The routed log entry or webhook payload exposes credentials, source URLs, operational diagnostics, internal actor details, or full before/after JSON snapshots to a destination with broader access than AlloyDB.

**Why it happens:**
Cloud Logging sinks can route any matching log entry, and the current alert notification sender logs full outbound payloads. Feed audit snapshots are allowlisted today, but they still include operational metadata such as status reasons, retry times, source feed IDs, and tags. Future fields could be sensitive if added without review.

**How to avoid:**
Phase 1 must define a notification-specific allowlist that is narrower than `feed_audit_events.before_values/after_values`. Include event identity, action, feed ID/name if approved, revision, timestamp, actor class or actor ID policy, and minimal changed-field summary. Do not log external credentials, raw request bodies, access tokens, API keys, full endpoint responses, or full audit snapshots by default. Phase 3 must redact request/response logs.

**Warning signs:**
- Dispatcher logs `Sending payload: ...` with the full audit event.
- Sink filter matches broad logs that might include exceptions and request bodies.
- New audit fields automatically appear in the webhook payload.
- Security review is deferred until after infrastructure exists.

**Phase to address:**
Phase 1: Delivery Contract and Phase 3: Webhook Dispatcher.

---

### Pitfall 10: Reusing Alert Notification Dedupe and Payload Semantics Blindly

**What goes wrong:**
Feed audit notifications inherit alert-specific behavior: Redis dedupe keyed by `segment_id`, app transcript URLs, full payload logging, tag fetches from feeds API, and 4xx handling designed for evaluated audio alerts. The result is incorrect idempotency and unnecessary dependencies.

**Why it happens:**
There is an existing notification package and tests, so copying it looks cheaper than defining an audit-specific dispatcher. But alert notifications are segment-centric; feed audit events are ledger-centric.

**How to avoid:**
Phase 3 can reuse low-level HTTP helpers only after separating domain-neutral behavior from alert-specific payload construction. Audit delivery should have its own payload model, idempotency key, tests, and failure policy. Avoid feed API calls unless the committed audit event lacks required display fields and the contract explicitly permits enrichment.

**Warning signs:**
- Audit dispatcher imports `AlertNotification` or uses `segment_id`.
- Redis dedupe key does not include audit event identity.
- Audit webhook URL points users to transcripts rather than feed/audit context.
- Tests are copied from `test_send_notification.py` with only names changed.

**Phase to address:**
Phase 3: Webhook Dispatcher.

---

## Technical Debt Patterns

Shortcuts that seem reasonable but create long-term problems.

| Shortcut | Immediate Benefit | Long-term Cost | When Acceptable |
|----------|-------------------|----------------|-----------------|
| Emit the whole audit row as the log/webhook payload | Fast implementation | Leaks fields as the audit table evolves and couples external contract to storage schema | Never for production delivery |
| Filter on free-form log message text | Easy sink setup | Fragile routing, accidental fanout, hard-to-test filters | Only for one-off manual debugging, not roadmap work |
| Use Pub/Sub `messageId` as business idempotency | Avoids adding a payload field | Fails on publish-side duplicates and replay/re-emission | Never; use audit identity |
| Skip dead-letter topic because delivery is best-effort | Fewer resources | Poison messages either vanish or retry indefinitely with no inspection path | Only in a local prototype |
| Console-create sink IAM | Unblocks staging quickly | Terraform drift can remove or hide permissions | Acceptable only as a documented temporary break-glass action |
| Reuse alert notification code unchanged | Saves boilerplate | Segment-centric semantics leak into feed audit delivery | Never; only reuse extracted HTTP primitives |
| No reconciliation because the ledger is durable | Less operational work | Missing notifications go unnoticed | Never for production rollout |

## Integration Gotchas

Common mistakes when connecting to external services.

| Integration | Common Mistake | Correct Approach |
|-------------|----------------|------------------|
| Cloud Logging Log Router | Assuming sinks backfill old log entries after creation or repair | Treat sinks as forward-only; add reconciliation and, if required later, a separate replay/backfill tool from `feed_audit_events` |
| Cloud Logging sink filters | Omitting a filter or matching broad text | Match a dedicated structured `event_type` and preview the filter before deploy |
| Cloud Logging sink IAM | Creating a sink but not granting its writer identity destination permissions | Terraform the sink, capture writer identity, grant `roles/pubsub.publisher` on the topic |
| Pub/Sub topic/subscription | Configuring dead-letter on the topic instead of the subscription | Dead-letter policy belongs on the subscription; also grant required Pub/Sub service account roles |
| Pub/Sub push | Returning non-2xx for permanent bad messages | Ack permanent poison messages after logging/counting them; reserve retries for transient failures |
| Pub/Sub push auth | Leaving webhook public or using a shared static secret only | Use authenticated push/OIDC where possible, plus receiver-side issuer/audience validation or a narrowly scoped API key if external receiver cannot verify OIDC |
| Webhook endpoint | Treating any 4xx as retryable | Split endpoint misconfiguration from bad payload; alert loudly on auth/config errors |
| Existing notification service | Copying payload logging and tag lookup | Build audit-specific payloads and redact logs |

## Performance Traps

Patterns that work at small scale but fail as usage grows.

| Trap | Symptoms | Prevention | When It Breaks |
|------|----------|------------|----------------|
| Broad sink routes too many logs | Pub/Sub topic volume spikes; webhook receives unrelated data | Dedicated event type and sink filter preview | Immediately after deploy if filter is broad or omitted |
| Per-event enrichment calls feeds API | Dispatcher latency and error rate track feeds API latency | Put required fields in the committed log event; avoid enrichment on hot path | During feed incident bursts or feeds-service degradation |
| Ordered push on a low-cardinality key | One bad event delays many later events | Avoid ordering, or use high-cardinality `feed_id` only when required | Any repeated events on the same key |
| Slow receiver timeouts | Backlog age grows; push backoff activates | Short timeouts, bounded retries, DLQ, latency alerts | Endpoint p95 approaches ack deadline or function timeout |
| No retention/cleanup planning for delivery logs | Log and Pub/Sub cost grows invisibly | Define retention for delivery attempt logs and DLQ review cadence | After rollout when audit volume grows |

## Security Mistakes

Domain-specific security issues beyond general web security.

| Mistake | Risk | Prevention |
|---------|------|------------|
| Routing full snapshots | Audit metadata and future sensitive fields reach broader destinations | Maintain a notification allowlist separate from storage allowlist |
| Trusting caller-provided actor fields | Spoofed actor identity in external notifications | Use the committed `actor_id` from the audit ledger; keep BFF actor propagation controls intact |
| Public unauthenticated webhook | Anyone can submit fake audit notifications | Use authenticated push/OIDC or receiver-side secret validation, and do not expose internal feed service routes |
| Logging full outbound payloads and responses | Secrets or operational details land in Cloud Logging and then can be re-routed | Log event IDs, status class, receiver name, and error category only |
| Sink filter includes exception logs | Stack traces and accidental request bodies route to Pub/Sub | Filter only on structured audit notification event type |
| Cross-project destination with broad IAM | Other systems can publish/read audit notifications | Grant least-privilege publisher to sink writer and subscriber to dispatcher/receiver only |

## UX Pitfalls

Common operator experience mistakes in this domain.

| Pitfall | User Impact | Better Approach |
|---------|-------------|-----------------|
| Notification status is not distinguishable from audit status | Operators think a missing webhook means no audit event happened | Use clear wording: "audit recorded" versus "notification attempted" |
| No way to inspect failed delivery | Operators cannot explain missing external signals | DLQ review query, dashboard, and runbook in Phase 4 |
| Duplicate notifications have no stable ID | Receivers cannot collapse repeated deliveries | Include `audit_event_id` and `feed_revision` in every payload |
| Alerts fire on every transient receiver blip | Alert fatigue during downstream outages | Alert on sustained error rate/backlog age, not single failures |
| Notification payload lacks feed/action context | External receiver cannot route the message usefully | Include minimal feed identity, action, actor class/policy, revision, and changed-field summary |

## "Looks Done But Isn't" Checklist

Things that appear complete but are missing critical pieces.

- [ ] **Log event contract:** Structured `event_type`, schema version, committed audit identity, idempotency key, and redacted payload are documented and tested.
- [ ] **Transaction safety:** Rollback tests prove failed audit writes do not emit/send notifications.
- [ ] **Sink filter:** Logs Explorer preview matches only intended audit notification events.
- [ ] **Sink IAM:** Terraform grants the sink writer identity Pub/Sub publisher rights.
- [ ] **Dead letter path:** Subscription has a DLQ, required IAM, metrics, and an inspection runbook.
- [ ] **Ack policy:** Parser, permanent failures, transient failures, 4xx, 429, 5xx, and timeout behavior are explicitly tested.
- [ ] **Idempotency:** Dispatcher and receiver contract handle duplicate Pub/Sub delivery.
- [ ] **Observability:** Dashboards cover sink export counts/errors, Pub/Sub backlog, push response classes, latency, DLQ count, and dispatcher error categories.
- [ ] **Security:** Payload and logs exclude secrets, raw request bodies, full snapshots, and endpoint response bodies.
- [ ] **Rollout:** Staging smoke test emits a real audit event and verifies sink -> Pub/Sub -> dispatcher -> webhook path before production.

## Recovery Strategies

When pitfalls occur despite prevention, how to recover.

| Pitfall | Recovery Cost | Recovery Steps |
|---------|---------------|----------------|
| Delivery blocks ingestion | HIGH | Revert synchronous delivery code, restore feed write path to ledger-only, then reintroduce post-commit best-effort logging behind a feature flag |
| Phantom notifications | MEDIUM | Disable sink/dispatcher, identify affected `audit_event_id` values, notify receiver to ignore unmatched IDs, add post-commit/re-read guard |
| Misconfigured sink drops entries | MEDIUM | Fix topic/IAM/filter, inspect sink error logs and export metrics, document missed window, optionally build manual replay from `feed_audit_events` if product requires it |
| Broad filter floods Pub/Sub | LOW/MEDIUM | Disable sink, narrow filter, purge or drain subscription, validate with preview before reenabling |
| Poison message backlog | MEDIUM | Pause push delivery or switch subscription type, inspect bad message, patch parser or contract, ack/drop or DLQ affected messages, add regression test |
| Receiver outage backlog | LOW/MEDIUM | Let retry/DLQ policy operate, tune timeouts/backoff, communicate delayed best-effort status, replay DLQ only after receiver recovery |
| Sensitive payload routed | HIGH | Disable sink/dispatcher immediately, rotate affected secrets if any, narrow payload allowlist, review destination access, purge retained messages where possible |
| Duplicate receiver actions | MEDIUM | Add idempotency key support, ask receiver to dedupe by audit identity, replay or reconcile affected external records |

## Pitfall-to-Phase Mapping

How roadmap phases should address these pitfalls.

| Pitfall | Prevention Phase | Verification |
|---------|------------------|--------------|
| Delivery blocks ingestion | Phase 1 | Feed mutation tests have no network mocks; failure injection proves notification outage does not affect feed write success |
| Phantom notifications | Phase 1 and Phase 3 | Rollback test verifies no notification log/send for failed transaction; dispatcher can re-read committed audit event |
| Log routing treated as durable outbox | Phase 1 and Phase 4 | Docs say ledger is source of truth; reconciliation dashboard compares audit rows to routed events |
| Sink filter drift | Phase 1 and Phase 2 | Golden structured log key test plus Logs Explorer preview query saved in Terraform/docs |
| Sink IAM/destination failure | Phase 2 | Terraform plan includes sink, topic, writer identity IAM, and export error alerts |
| Duplicate/out-of-order delivery | Phase 1 and Phase 3 | Duplicate Pub/Sub message test sends one logical webhook identity; payload contains audit id and revision |
| Poison messages | Phase 2 and Phase 3 | Unit tests cover malformed, missing field, 4xx, 429, 5xx, timeout, and DLQ behavior |
| Push backpressure | Phase 3 and Phase 4 | Load/smoke test verifies bounded latency; alerts cover push error class, latency, backlog age, outstanding messages |
| Sensitive data leakage | Phase 1 and Phase 3 | Payload allowlist review; tests assert excluded fields and redacted logs |
| Alert notification semantics copied blindly | Phase 3 | Audit dispatcher tests do not import alert proto/segment ID assumptions and use audit-specific fixtures |

## Sources

- Local project context: `.planning/PROJECT.md` (feed audit scope, delivery boundary, actor identity constraints).
- Local codebase concerns: `radio-transcription/.planning/codebase/CONCERNS.md` (audit growth, unresolved actors, Pub/Sub failure-semantics gaps, fragile feed store and collector runtime).
- Local testing patterns: `radio-transcription/.planning/codebase/TESTING.md` (pytest/Vitest patterns, integration test structure, host-stability constraints).
- Local integrations: `radio-transcription/.planning/codebase/INTEGRATIONS.md` (existing Pub/Sub, Cloud Logging, notification endpoint, Terraform module, and environment contracts).
- Local code references: `radio-transcription/backend/pipeline/common/clients/pubsub_client.py`, `radio-transcription/backend/pipeline/notification/send_notification.py`, `radio-transcription/backend/pipeline/notification/request_handler.py`, `radio-transcription/backend/pipeline/storage/feed_audit_sql.py`, `radio-transcription/backend/pipeline/common/log_helper.py`, `radio-transcription/terraform/modules/cloud_function/main.tf`.
- Context7: `/googleapis/python-pubsub` docs for publisher futures, ordering keys, retry configuration, push wrapper shape, and dead-letter policy fields.
- Context7: `/websites/cloud_google_sdk` docs for `gcloud logging` sink management and writer identity behavior.
- Google Cloud Logging, route log entries: https://docs.cloud.google.com/logging/docs/routing/overview
- Google Cloud Logging, configure sinks: https://docs.cloud.google.com/logging/docs/export/configure_export_v2
- Google Cloud Logging, Pub/Sub routed logs: https://docs.cloud.google.com/logging/docs/export/pubsub
- Google Cloud Logging, troubleshoot routing and sink errors: https://docs.cloud.google.com/logging/docs/export/troubleshoot
- Google Cloud Pub/Sub, subscription overview: https://docs.cloud.google.com/pubsub/docs/subscription-overview
- Google Cloud Pub/Sub, push subscriptions: https://docs.cloud.google.com/pubsub/docs/push
- Google Cloud Pub/Sub, monitoring push subscriptions and dead letters: https://docs.cloud.google.com/pubsub/docs/monitoring
- Google Cloud Pub/Sub, dead-letter topics: https://docs.cloud.google.com/pubsub/docs/dead-letter-topics
- Google Cloud Pub/Sub, exactly-once delivery limits: https://docs.cloud.google.com/pubsub/docs/exactly-once-delivery
- Google Cloud Pub/Sub, ordering: https://docs.cloud.google.com/pubsub/docs/ordering
- Google Cloud Pub/Sub, subscription retry policy: https://docs.cloud.google.com/pubsub/docs/subscription-retry-policy
- Google Cloud Pub/Sub, retry requests: https://docs.cloud.google.com/pubsub/docs/retry-requests
- Google Cloud Pub/Sub, subscription troubleshooting and duplicate handling: https://docs.cloud.google.com/pubsub/docs/pull-troubleshooting

---
*Pitfalls research for: feed audit notification delivery*
*Researched: 2026-06-26*

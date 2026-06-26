# Project Research Summary

**Project:** Feed Audit Notification Delivery
**Domain:** Best-effort backend notification delivery for feed audit events
**Researched:** 2026-06-26
**Confidence:** MEDIUM

## Executive Summary

This project is backend and infrastructure delivery plumbing for an existing radio transcription system. The durable product record is already the AlloyDB `feed_audit_events` ledger; this milestone should add a best-effort notification path that makes newly committed audit events visible to the Watch Duty backend without making feed mutation or ingestion paths depend on downstream availability.

The recommended approach is to emit one tightly scoped structured Cloud Logging event after audited storage writes return committed audit data, route only those events to Pub/Sub through a Cloud Logging sink, and push them to a small authenticated Cloud Run relay. The relay should decode the routed LogEntry, validate the flat audit payload, forward it to `/api/v1/echo/radio_transcription/internal/audit/webhook/` with the configured API key, and rely on Pub/Sub retry plus DLQ for transient failures.

The main risks are coupling notification delivery to feed writes, emitting phantom or sensitive events, misconfiguring sink filters or IAM, and assuming log routing is a durable outbox. Mitigate these by freezing the payload contract first, using DB-returned audit identifiers only, keeping sink filters narrow on `event_type="radio_transcription.feed_audit_notification"` and `schema_version=1`, adding explicit ack/retry classification, and verifying each layer before connecting the next.

## Key Findings

### Recommended Stack

Use the existing Python 3.13 backend stack and GCP primitives already present in the repo. Add a small FastAPI/Uvicorn Cloud Run service under `backend/pipeline/feed_audit_relay`, keep dependencies scoped through the uv workspace, and reuse shared logging and HTTP patterns rather than introducing a new runtime, queue worker framework, or database dispatcher.

The infrastructure path should be Cloud Logging structured logs to Log Router sink to Pub/Sub topic to authenticated Pub/Sub push to Cloud Run. This matches the project requirement for best-effort delivery and avoids custom delivery tables or Watch Duty database access.

**Core technologies:**
- Python 3.13: relay runtime - matches repo-wide backend runtime and Docker base images.
- uv workspace: package and lock management - keeps the new relay aligned with existing backend packages.
- FastAPI + Uvicorn: Cloud Run HTTP endpoint - matches existing service style and is straightforward to test.
- Cloud Logging structured logs: post-commit audit notification signal - integrates with existing `setup_logging()` and `json_fields` conventions.
- Cloud Logging Log Router sink: routes only matching notification logs - decouples producers from Pub/Sub clients.
- Pub/Sub topic, push subscription, retry policy, and DLQ: buffering and redelivery - gives bounded best-effort behavior without a custom dispatcher table.
- Cloud Run v2: webhook relay runtime - private, authenticated, low-operational-overhead service for outbound delivery.
- Terraform: infrastructure ownership - use existing IaC patterns and avoid console-managed sink IAM.
- urllib3: outbound webhook client - aligns with existing notification request handling and supports explicit retries/timeouts.
- Pydantic: payload validation - validates Pub/Sub, LogEntry, and outbound webhook models while tolerating extra GCP fields.

### Expected Features

The product should deliver every future committed feed audit event on a best-effort basis while preserving ingestion availability. The Watch Duty receiver must dedupe by a stable event identity because Pub/Sub and HTTP retries make duplicates normal.

**Must have (table stakes):**
- Emit one notification for every newly inserted `feed_audit_events` row - future audit events need near-real-time visibility.
- Keep delivery non-blocking - webhook, Pub/Sub, or relay failures must not fail or delay feed writes.
- Use a versioned flat payload contract - include `event_type`, `schema_version`, event ID, feed ID, action, actor, timestamp, revision, and bounded before/after fields.
- Include idempotency identity - use `feed_audit_events.id` and include `feed_revision` for feed-local ordering context.
- Retry transient failures with bounded backoff - retry 408, 429, 5xx, timeouts, and connection errors.
- Surface exhausted failures - DLQ, structured logs, metrics, and a runbook are required so failures are not silent.
- Authenticate both legs - Pub/Sub push should use Cloud Run IAM/OIDC and the relay should authenticate to Watch Duty with the configured API key.
- Cover sync and async storage paths - Echo collector and VM/admin paths must emit consistently.

**Should have (differentiators):**
- Compact delivery attempt visibility - useful if DLQ/logs are not enough, but avoid a v1 custom delivery table unless operations requires it.
- Operator replay by event/range from `feed_audit_events` - valuable recovery tool; CLI/runbook can precede API or UI.
- HMAC signing or stronger receiver verification - defer until receiver requirements or exposure model demand it.
- Adaptive throttling - defer until real webhook limits and backlog behavior are measured.

**Defer (v2+):**
- Multi-destination webhook subscription platform - overbuilds the one Watch Duty backend destination.
- Historical backfill of old audit rows - only after future-event idempotency is proven.
- Operator UI delivery timeline - separate visibility product work.
- Exactly-once delivery - unrealistic for this architecture; define at-least-once attempts plus receiver idempotency.
- Database triggers, CDC, polling cursors, outbox payload tables, or LISTEN/NOTIFY - out of scope for v1 and contrary to the best-effort requirement.

### Architecture Approach

Use a storage-truth, log-delivered notification pattern. `feed_audit_events` remains the append-only source of truth. Storage methods emit a structured notification log only after audited SQL returns committed audit data. Cloud Logging and Pub/Sub own routing, buffering, redelivery, and DLQ. The Cloud Run relay owns envelope parsing, payload validation, outbound Watch Duty POST, and retry classification, but it must not read AlloyDB or mutate feed state.

**Major components:**
1. `feed_audit_events` table - durable audit ledger and source of committed event identity.
2. `FeedStore` and `SyncFeedStore` - audited mutation boundaries that call a shared post-success notification emitter.
3. Feed audit notification emitter - builds the flat v1 payload from DB-returned fields and logs it through structured logging.
4. Cloud Logging sink - filters only `radio_transcription.feed_audit_notification` events with `schema_version=1`.
5. Pub/Sub topic, push subscription, and DLQ - buffers routed logs, retries retryable failures, and preserves exhausted messages.
6. Cloud Run relay - decodes Pub/Sub/LogEntry envelopes, validates payloads, sends authenticated webhook requests, and maps failures to ack or retry.
7. Watch Duty receiver - consumes duplicate-tolerant notifications keyed by committed audit identity.

### Critical Pitfalls

1. **Turning best-effort delivery into a feed mutation dependency** - keep network delivery out of feed mutation and ingestion paths; emit only after storage success and swallow logging failures.
2. **Emitting phantom notifications before commit** - build payloads from committed DB-returned audit fields, not request bodies or speculative before/after values.
3. **Treating log routing as a durable outbox** - document that the ledger is authoritative and add reconciliation checks for recent audit rows versus routed notifications.
4. **Sink filter drift or IAM gaps** - match stable structured fields only, preview filters, Terraform the sink writer `roles/pubsub.publisher` grant, and alert on export errors.
5. **Duplicate, out-of-order, or poison-message behavior** - include audit idempotency keys, avoid Pub/Sub business identity, ack permanent malformed/4xx cases, retry transient failures, and configure a DLQ.
6. **Sensitive data leakage** - maintain a notification-specific allowlist and redact outbound request/response logs.
7. **Blindly reusing alert notification semantics** - reuse only neutral HTTP helper patterns; keep audit payloads, IDs, and tests separate from segment alert notification logic.

## Implications for Roadmap

Based on research, suggested phase structure:

### Phase 1: Delivery Contract and Storage Emission

**Rationale:** The payload, idempotency key, redaction boundary, and non-blocking guarantee determine every downstream component. This phase must settle the naming conflict in research by using the project contract: `event_type="radio_transcription.feed_audit_notification"` and `schema_version=1`.

**Delivers:** Shared payload builder, structured-log emitter, audit CTE return extensions if needed, async and sync storage hooks, and unit tests proving emission occurs only for committed audit rows and never for suppressed/no-op/rolled-back mutations.

**Addresses:** Complete audit action coverage, non-blocking emission, versioned flat payload, idempotency identity, safe before/after allowlist.

**Avoids:** Feed write coupling, phantom notifications, sensitive payload leakage, broad log contracts.

### Phase 2: Logging and Pub/Sub Infrastructure

**Rationale:** Routing and IAM should be validated before a webhook relay creates external side effects. Capture-only Pub/Sub validation isolates sink filter and sink writer failures.

**Delivers:** Terraform-managed Pub/Sub topic, DLQ topic, log sink, sink writer IAM, temporary/capture subscription or validation path, export/backlog/error metrics, and saved filter preview/runbook.

**Uses:** Cloud Logging Log Router, Pub/Sub, Terraform, Cloud Monitoring alerts, least-privilege IAM.

**Implements:** Cloud route from structured notification logs to Pub/Sub without connecting the Watch Duty endpoint yet.

**Avoids:** Sink filter drift, missing publisher IAM, silent export failures, broad production blast radius.

### Phase 3: Cloud Run Webhook Relay

**Rationale:** The relay can be built and verified against recorded Pub/Sub LogEntry fixtures from Phase 2 before authenticated push is enabled.

**Delivers:** `feed_audit_relay` FastAPI service, Pydantic models, envelope decode and schema validation, API-key authenticated outbound POST, explicit timeout/retry classification, idempotency headers, dry-run/log-only mode, and fake-webhook tests.

**Uses:** Python 3.13, uv, FastAPI, Uvicorn, urllib3, Pydantic, Cloud Run.

**Implements:** Stateless delivery attempts from routed log payloads to the Watch Duty webhook.

**Avoids:** Poison retry storms, alert-notification semantic leakage, relay database reads, slow receiver backpressure, public unauthenticated endpoints.

### Phase 4: Authenticated Push, DLQ, and Rollout

**Rationale:** End-to-end delivery should be enabled gradually after storage emission, cloud routing, and relay parsing are independently proven.

**Delivers:** Authenticated Pub/Sub push subscription, Cloud Run invoker service account/IAM, DLQ policy with 10 delivery attempts, staging smoke test, duplicate delivery test, receiver idempotency validation, dashboard/alerts, reconciliation check, and production rollout plan.

**Addresses:** Retry/backoff, exhausted delivery visibility, receiver authentication, duplicate/out-of-order tolerance, operational proof.

**Avoids:** Unbounded retry loops, silent drops, unauthenticated ingress, assumptions that log routing guarantees delivery.

### Phase Ordering Rationale

- Contract first: downstream routing, relay parsing, receiver idempotency, and redaction all depend on stable structured fields.
- Storage before infrastructure: local tests can prove the critical non-blocking and post-commit guarantees before any cloud wiring exists.
- Capture route before relay: sink filter and IAM failures are easier to debug without external webhook side effects.
- Relay before push rollout: fixture-based tests establish ack/retry behavior before Pub/Sub can create retry pressure.
- Rollout last: staging, DLQ, metrics, and reconciliation are the evidence that best-effort delivery is observable enough for production.

### Research Flags

Phases likely needing deeper research during planning:
- **Phase 1:** Confirm exact Watch Duty receiver payload contract, accepted action names, actor field policy, and whether full before/after snapshots are allowed.
- **Phase 2:** Research private deployment topology and Terraform ownership because public modules were visible but the final environment composition was not.
- **Phase 3:** Confirm WD response-code semantics, timeout budget, API-key header expectations, and whether stronger signing or OIDC is required.
- **Phase 4:** Validate operational thresholds for latency, backlog age, DLQ count, and acceptable reconciliation gaps.

Phases with standard patterns (skip research-phase unless project details changed):
- **Phase 1 storage tests:** Existing feed audit SQL and storage patterns are well documented locally.
- **Phase 2 GCP primitives:** Log sinks, Pub/Sub push, DLQ, and Cloud Run IAM are covered by official Google docs.
- **Phase 3 service implementation:** FastAPI/uv/urllib3 patterns already exist in the repo; targeted implementation planning should be enough after receiver contract validation.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | Python 3.13, uv, FastAPI, Cloud Logging, Pub/Sub, Cloud Run, and Terraform align with repo patterns and official docs. Exact Terraform placement is medium confidence because private deployment composition was not visible. |
| Features | MEDIUM | Core table stakes are clear from PROJECT.md and existing audit semantics, but receiver contract, replay expectations, and persisted attempt visibility need validation. |
| Architecture | MEDIUM | The storage-truth/log-delivered pattern fits the stated best-effort requirement. Confidence is reduced by unknown private IaC layout and downstream API details. |
| Pitfalls | HIGH | Risks are strongly supported by repo-specific code paths and official Google Cloud delivery semantics. |

**Overall confidence:** MEDIUM

### Gaps to Address

- Receiver contract: validate exact JSON fields, required `feed_id` behavior, idempotency expectations, response-code handling, and authentication before implementation.
- Event type and schema version: standardize all planning around `radio_transcription.feed_audit_notification` and integer `schema_version=1`.
- Private Terraform/deployment root: locate the environment composition that owns Cloud Run, sinks, Pub/Sub, IAM, secrets, and monitoring.
- Delivery observability threshold: define alert thresholds for export errors, push error class, oldest unacked age, DLQ count, and reconciliation drift.
- Replay/backfill scope: decide whether v1 only needs DLQ inspection and runbook recovery or also a small CLI replay from `feed_audit_events`.
- Payload redaction: confirm the allowlist for actor, before/after values, feed names, diagnostic reasons, and response logging before external routing.

## Sources

### Primary (HIGH confidence)
- `.planning/PROJECT.md` - project scope, active requirements, out-of-scope items, endpoint path, auth notes, and delivery constraints.
- `.planning/research/STACK.md` - recommended service/runtime stack, GCP primitives, package shape, and version compatibility.
- `.planning/research/FEATURES.md` - table-stakes delivery behavior, differentiators, anti-features, and MVP criteria.
- `.planning/research/ARCHITECTURE.md` - storage-truth/log-delivered architecture, component boundaries, data flow, and build order.
- `.planning/research/PITFALLS.md` - phase vocabulary, critical pitfalls, prevention strategies, and looks-done checklist.
- Local code references cited by research - `feed_audit_sql.py`, `feed_queries.py`, `sync_feed_queries.py`, `log_helper.py`, existing notification request handlers, and Terraform modules.

### Secondary (MEDIUM confidence)
- Context7 `/googleapis/python-logging` - structured Cloud Logging and sink behavior.
- Context7 `/googleapis/python-pubsub` - Pub/Sub publish, ordering, push wrapper, retry, and DLQ behavior.
- Context7 `/websites/cloud_google_sdk` - `gcloud logging` sink management and writer identity behavior.
- Official Google Cloud Logging docs - structured logging, query language, log routing, Pub/Sub export, sink configuration, and routing troubleshooting.
- Official Google Cloud Pub/Sub docs - push subscriptions, push authentication, retry policy, dead-letter topics, monitoring, ordering, duplicate handling, and exactly-once limits.
- Official Cloud Run docs - Pub/Sub push tutorial, service IAM, and request timeout behavior.

### Tertiary (LOW confidence)
- HashiCorp Google provider release notes - current provider availability; exact upgrade suitability depends on private deployment constraints.
- General webhook platform docs from GitHub and Stripe - useful delivery/idempotency patterns but not project-specific requirements.

---
*Research completed: 2026-06-26*
*Ready for roadmap: yes*

# Feed Audit Notification Delivery

## What This Is

This project adds a best-effort notification path for radio transcription feed
audit events. The transcription engine remains the owner of feed audit history,
while Watch Duty backend receives near-real-time event notifications through a
webhook for reporter/channel alerting and operational response.

## Core Value

Feed audit notifications must make feed lifecycle and ingestion problems visible
to Watch Duty quickly without affecting ingestion or feed lifecycle writes.

## Requirements

### Validated

- ✓ Radio transcription already runs as a Google Cloud audio pipeline with VM
  collectors, Cloud Run/functions, Pub/Sub claim-check messages, GCS audio
  artifacts, and AlloyDB-backed state — existing.
- ✓ Feed lifecycle state is already centralized in AlloyDB through async and
  sync storage layers, including feed statuses, status reasons, diagnostic
  detail, and feed-local audit revisions — existing.
- ✓ `feed_audit_events` is the canonical append-only feed audit ledger for
  audited lifecycle mutations — existing.
- ✓ Internal FastAPI services, TypeScript BFF routes, Google OIDC/service-account
  auth, Cloud Logging, Pub/Sub, and Terraform modules are established deployment
  patterns — existing.

### Active

- [ ] Emit a structured Feed Audit Notification for every newly inserted
  `feed_audit_events` row.
- [ ] Ensure notification emission is best-effort and can never fail or delay
  ingestion, feed lifecycle writes, or audit row persistence.
- [ ] Route notification logs through Cloud Logging to Pub/Sub using
  `event_type="radio_transcription.feed_audit_notification"` and
  `schema_version=1`.
- [ ] Add a Cloud Run webhook relay subscriber that consumes Pub/Sub push
  messages and forwards the flat audit event payload to the Watch Duty endpoint
  `/api/v1/echo/radio_transcription/internal/audit/webhook/`.
- [ ] Configure downstream retry and dead-letter behavior so transient WD
  failures retry without creating custom delivery tables or coupling delivery
  to the write path.

### Out of Scope

- Durable audit replication — `feed_audit_events` remains the durable audit
  system of record; webhook delivery is an operational notification signal.
- Exactly-once notification delivery — duplicates are expected and downstream
  systems must dedupe by `event_id` where needed.
- Watch Duty backend reading the transcription database — that crosses the
  service boundary and was explicitly rejected.
- Database triggers, `LISTEN/NOTIFY`, CDC, polling cursors, outbox payload
  tables, or extra write-path delivery state for v1 — they add coupling or
  operational complexity not needed for this alerting use case.
- UI changes — this project is backend/infrastructure delivery plumbing.

## Context

- The current codebase is a Python/TypeScript monorepo for a radio transcription
  pipeline. Python 3.13 services and workers handle ingestion, segmentation,
  normalization, transcription, evaluation, notification, and backend APIs.
  TypeScript powers the BFF and React operator UI.
- Feed mutations are owned by storage-layer SQL in
  `backend/pipeline/storage`, with both async `FeedStore` and sync
  `SyncFeedStore` paths. The Echo collector uses the sync store; VM collectors
  use the async store.
- The WD backend endpoint exists at
  `/api/v1/echo/radio_transcription/internal/audit/webhook/`, authenticates with
  `X-Api-Key`, currently requires `feed_id`, and preserves unknown fields.
- Leadership wants notification delivery to be an engine implementation detail:
  WD should consume webhook events, not connect to the transcription database.
- The target operational latency is roughly under one minute, but the ingestion
  path must remain protected from notification failures.

## Constraints

- **Critical path**: Notification logging, routing, and webhook delivery must not
  add synchronous network calls, extra database reads, or failure coupling to
  ingestion and feed lifecycle writes.
- **Payload**: The WD webhook receives the flat feed audit event payload with
  `event_type` and `schema_version`; avoid nested wrapper formats and avoid
  duplicate encode/decode work.
- **Reliability**: Use short local subscriber retry plus Pub/Sub redelivery and
  DLQ. Do not implement a custom delivery table in v1.
- **Security**: Pub/Sub push to the relay uses Cloud Run IAM/OIDC, and the relay
  authenticates to WD with the configured radio-transcription API key.
- **Maintainability**: Reuse shared helpers across async and sync feed storage
  paths, and do not duplicate feed audit payload construction logic.

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Emit after audited SQL returns a new audit row | Keeps notifications tied to committed audit events and avoids notifying suppressed no-op updates | — Pending |
| Use Cloud Logging → Pub/Sub → Cloud Run relay | Decouples WD delivery from ingestion without polling the database or adding write-path delivery tables | — Pending |
| Forward flat audit event payload | Matches the existing WD endpoint contract and avoids transformation-heavy relay code | — Pending |
| Add Pub/Sub DLQ with 10 delivery attempts | Bounds poison/config retry loops while preserving Pub/Sub redelivery for transient failures and crashes | — Pending |
| Keep `feed_audit_events` as the only durable audit ledger | Prevents duplicate payload storage and keeps audit history ownership in the transcription engine | — Pending |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `$gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `$gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-06-26 after initialization*

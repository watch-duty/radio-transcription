# Evidence-Based Quarantine Policy

## What This Is

This project redesigns radio transcription feed quarantine so quarantine means
only a narrow, high-confidence, feed-actionable state. The existing brownfield
system already has a collector/runtime contract, feed lifecycle storage,
source-specific collectors, and operator-facing feed status, but post-capture
pipeline failures and non-feed-actionable source/system failures can still
consume the same persisted feed quarantine budget.

The v1 goal is to make the runtime decide from structured policy evidence,
route non-quarantine conditions through a non-budgeted suppressed retry path,
and preserve current schema compatibility with minimal code changes.

## Core Value

On-call should be alerted only when the quarantined feed is likely something a
human can fix at feed scope.

## Requirements

### Validated

- ✓ VM ingestion collectors expose a typed runtime boundary with
  `CapturedChunk`, `SourceObservation`, and `FeedFailure` — existing.
- ✓ Runtime owns leases, heartbeats, GCS upload, Pub/Sub publish, bookmark
  writes, failure counting, quarantine telemetry, and lease release —
  existing.
- ✓ Feed lifecycle is persisted through AlloyDB-backed `FeedStore` methods and
  SQL in `backend/pipeline/storage/` — existing.
- ✓ `SourceObservation` and successful chunk progress can clear stale persisted
  failure state — existing.
- ✓ `feeds.status_reason` is a canonical abnormal-condition label, while
  `quarantine_reason` is raw forensic text — existing domain guidance.
- ✓ Frontend status mapping already treats `failing` and `quarantined` as UI
  `error`, so v1 can use `status='failing'` without introducing a new
  lifecycle status — existing.

### Active

- [ ] Add structured failure policy evidence for runtime decisions:
  `owner_scope`, `failure_scope`, `endpoint_kind`, `policy_intent`,
  `executed_action`, and `pipeline_stage` where needed.
- [ ] Keep `quarantine_reason` as raw forensic detail only; do not use it for
  policy, alert routing, or state transitions.
- [ ] Add a non-budgeted storage path for suppressed retry states that writes
  `status='failing'`, `failure_count=0`, `retry_after`, `status_reason`, and
  releases the lease without writing `quarantine_reason`.
- [ ] Restrict `report_feed_failure(...)` quarantine-budget increments to
  policy decisions with `owner_scope=feed` and
  `policy_intent=quarantine_feed`.
- [ ] Route post-capture Pub/Sub publish failures after bookmark through
  `policy_intent=hold_for_replay` and
  `executed_action=suppress_feed_quarantine_record_publish_gap`.
- [ ] Add `FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED` for the
  v1 post-bookmark publish gap state.
- [ ] Emit `feed_failure_policy_decision` telemetry for every routed failure.
- [ ] Emit `post_bookmark_publish_failure` telemetry for known publish gaps
  with `replay_missing=true` and `data_gap_known=true`.
- [ ] Treat unannotated `FeedFailure` as a non-budgeted telemetry gap with
  `failure_count=0`, not legacy quarantine.
- [ ] Preserve automatic stale-state clearing through successful chunk progress
  and `SourceObservation`.
- [ ] Add focused storage and runtime tests proving non-budgeted paths never
  increment quarantine budget or emit `feed_quarantined`.

### Out of Scope

- Full durable publish outbox, DLQ, or replay worker — important follow-up, but
  v1 explicitly records that replay is missing for post-bookmark publish gaps.
- Actual fleet-wide source-class or pipeline circuit breaker state — v1 emits
  policy intent only.
- Database migration for a persistent structured audit/event table — logs carry
  the structured policy object in v1.
- New feed lifecycle status — v1 reuses `failing` as a scheduler-compatible
  suppressed retry state.
- Broad UI redesign for status reasons — only update shared types/status
  handling if required by new backend enum values.
- Echo ingestion parity — v1 targets VM ingestion first.
- Parsing or whitelisting `quarantine_reason` values — explicitly forbidden.

## Context

The codebase map lives in `.planning/codebase/`:

- `ARCHITECTURE.md` documents the event-driven pipeline, feed lifecycle store,
  VM collector runtime, management APIs, and frontend layers.
- `CONCERNS.md` captures the current quarantine-budget mismatch as a primary
  technical concern.
- `TESTING.md` documents narrow local validation expectations and warns against
  broad local integration/E2E runs by default.

Incident and design context from the conversation:

- GOO-613 showed overly aggressive quarantine behavior, including repeated
  mass feed quarantine after shared/system failures.
- GOO-618 showed transient auth-related feed quarantine and inconsistent
  catch-up after reset.
- GOO-557 summarized retained quarantine categories; many were not truly
  feed-actionable.
- Pub/Sub schema validation failures and paused ordering keys are downstream
  pipeline failures, not source/feed health.
- Broadcastify Calls and Fire Notifications 401 failures are usually shared
  credential/source-class problems, not per-feed problems.
- Source offline, provider 404, transient transport failures, item-scoped
  403/404, malformed upstream responses, and telemetry gaps should not share
  a quarantine-driving feed budget.

The agreed v1 compatibility decision:

- Use `status='failing'` with `failure_count=0` and `retry_after` as a
  scheduler-compatible suppressed retry state.
- For Pub/Sub publish failure after bookmark, future ingestion may resume after
  `retry_after`, but v1 does not replay the already-bookmarked message.
- Logs must explicitly mark `replay_missing=true` and `data_gap_known=true`.

## Constraints

- **Minimal Code Change**: Prefer runtime policy routing, one storage method,
  focused enum additions, and focused tests over broad schema or architecture
  changes — the user requested minimal code changes.
- **Current Schema Compatibility**: Do not add DB migrations for v1 unless a
  code path cannot be implemented safely without one — v1 persists only
  current-schema fields.
- **Operational Semantics**: Do not alert on feed quarantine unless on-call can
  take meaningful feed-specific action — this is the core policy constraint.
- **Data Integrity**: Post-bookmark publish gaps must be explicitly logged as
  known data gaps; do not silently treat delayed future ingestion as replay.
- **Test Safety**: Use narrow local tests and `safe-run --`; avoid broad local
  E2E/integration stacks unless explicitly requested.
- **Source of Truth**: `quarantine_reason` is forensic only; policy decisions
  must use canonical structured fields.

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Keep quarantine narrow and feed-actionable | Prevents shared system/provider/pipeline incidents from becoming many feed-level pages | — Pending |
| Use structured policy evidence, not raw reason strings | Raw reason text is for investigation and is not stable enough for policy | — Pending |
| Reuse `status='failing'` for v1 suppressed retry | Avoids DB lifecycle migration and keeps scheduler compatibility | — Pending |
| Add a non-budgeted release path | Most non-actionable failures need retry/backoff without consuming quarantine budget | — Pending |
| Post-bookmark publish failure records a data gap, not replay | Bookmark already advanced, and v1 has no durable outbox | — Pending |
| No v1 source-class breaker state | Breakers are needed later, but v1 should first stop feed-budget damage | — Pending |
| No ADR for the `failing` compatibility choice | User explicitly chose no ADR for this v1 decision | — Pending |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `$gsd-transition`):
1. Requirements invalidated? -> Move to Out of Scope with reason
2. Requirements validated? -> Move to Validated with phase reference
3. New requirements emerged? -> Add to Active
4. Decisions to log? -> Add to Key Decisions
5. "What This Is" still accurate? -> Update if drifted

**After each milestone** (via `$gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-06-14 after initialization*

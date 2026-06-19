# Feed Audit Events V1

## What This Is

Feed Audit Events V1 adds durable, queryable history for meaningful feed
mutations in the radio transcription backend. It is for Watch Duty engineers
and future admin tooling that need to answer what happened to a feed, when it
happened, what changed, and whether the cause was a human action or system
runtime behavior.

This project is not full event sourcing. The current `feeds` row remains the
authoritative current-state model; the new work adds an append-only audit
history and a cleaner current diagnostic detail field.

## Core Value

Operators can reconstruct meaningful feed lifecycle and configuration changes
from durable backend data instead of relying on short-lived logs.

## Requirements

### Validated

- ✓ Existing ingestion runtime leases feeds and manages current feed status
  through `unclaimed`, `active`, `failing`, `quarantined`, and `deactivated`
  states — existing
- ✓ Existing feed failure policy records retryable failures, failure counts,
  retry windows, status reasons, and quarantine transitions in AlloyDB —
  existing
- ✓ Existing feed service supports feed create, update, deactivate, reset,
  delete, and list/read operations through FastAPI and storage-layer methods —
  existing
- ✓ Existing UI and BFF consume feed status and status reason fields for
  operator-facing feed views — existing
- ✓ Existing quarantine telemetry emits structured logs and optional metrics
  when feeds transition to `quarantined` — existing
- ✓ Existing AlloyDB migration workflow supports ordered ingestion schema
  changes and pg_cron maintenance jobs — existing
- ✓ Feed Audit Event contract defines domain meaning, action vocabulary, actor
  ID vocabulary, deletion snapshot semantics, raw capped diagnostic-detail
  tradeoff, retention target, v1 boundaries, and future consumer derivation —
  validated in Phase 1
- ✓ AlloyDB schema foundation adds `feeds.status_reason_detail`,
  `feed_audit_events`, `feed_audit_event_sequences`, delete-safe feed identity,
  occurred time, per-feed sequence, actor/action constraints, JSON object
  checks, and HOT guard coverage — validated in Phase 1
- ✓ Text-level contract tests protect the Phase 1 docs, SQL migration,
  actor/action constraints, delete-survival semantics, diagnostic-detail bounds,
  and HOT guard behavior — validated in Phase 1
- ✓ Storage-owned feed create, meaningful update, deactivate, reset, and delete
  mutations persist audit events transactionally with their current-state
  changes — validated in Phase 2
- ✓ Phase 2 audit rows capture feed identity, actor ID, per-feed sequence,
  action, status/status reason/detail, and maintained before/after snapshots
  for storage lifecycle mutations — validated in Phase 2
- ✓ Feed Audit Event sequence allocation uses the
  `feed_audit_event_sequences` table inside the same storage transaction, with
  rollback and concurrent ordering coverage assigned to CI for DB execution —
  validated in Phase 2

### Active

- [ ] Persist audit events for the remaining runtime outcomes: failure
  reported, quarantined, and recovered.
- [ ] Wire storage, service, and runtime paths to populate
  `status_reason_detail` as the canonical bounded diagnostic detail while
  keeping `quarantine_reason` populated as a compatibility alias for one
  release.
- [ ] Preserve authenticated admin identity at the trusted service boundary
  while keeping untrusted request bodies from spoofing actors.
- [ ] Retain audit history for 18 months and enforce retention in v1.
- [ ] Verify service/API compatibility, failure/quarantine, recovery, retention,
  and no-lease-churn behavior with focused automated tests.

### Out of Scope

- Watch Duty backend webhook delivery — GOO-431/GOO-629 need the contract, but
  delivery workers, signatures, retries, and receiver integration are deferred.
- Admin timeline read APIs and frontend UI — GOO-574 can consume the durable
  data later, but v1 is write-only.
- Full feed event sourcing — the current `feeds` table remains the source of
  current state.
- Routine worker lease churn — `active`/`unclaimed` lease handoffs are
  scheduler mechanics and should not pollute the default audit history.
- Historical baseline/backfill events for existing feeds — no synthetic
  `snapshot_initialized` events in v1.
- Reworking quarantine policy thresholds or source-specific failure
  classification — v1 records the existing policy outcomes.

## Context

The Linear tickets describe one broader product problem rather than three
separate implementation tasks. GOO-557 asks for per-feed history that survives
log retention and includes who changed fields, not just status-machine
transitions. GOO-431 and GOO-629 point toward downstream propagation to the
Watch Duty backend, but the first useful step is to establish a durable backend
event contract that can later be delivered.

The current implementation stores the latest feed state in AlloyDB and emits
quarantine telemetry only when a feed crosses into `quarantined`. That is useful
for monitoring but insufficient for support and forensic questions because logs
are short-lived, not feed-history-shaped, and not available to future admin UI
queries.

The feed lifecycle also mixes product-significant state with worker scheduling.
`active` and `unclaimed` are lease states; `failing`, `quarantined`, and
`deactivated` are more useful as operator-facing lifecycle outcomes. Audit
events should therefore focus on meaningful changes and explicit causes, while
leaving heartbeat-scale scheduler noise out of the default event stream.

The most important modeling decision from the design review is to avoid storing
an HTTP-shaped webhook payload as canonical data. V1 should store domain audit
event data. Future delivery can derive webhook payloads from the same durable
events without duplicating the audit ledger.

## Constraints

- **Brownfield architecture**: Preserve the existing current-state `feeds`
  model, storage-layer SQL patterns, and FastAPI service boundaries — the
  ingestion runtime already depends on current-state lease queries and fenced
  writes.
- **Database consistency**: Feed mutations and audit inserts must commit or
  roll back together — audit history is only useful if it cannot drift from
  successful state changes.
- **Compatibility**: Existing consumers of `quarantine_reason` must keep
  working during the v1 rollout — add `status_reason_detail` without removing
  the old field immediately.
- **Signal quality**: Do not audit routine heartbeat or lease churn by default
  — the audit table must stay understandable and affordable.
- **Retention**: Keep feed audit events for 18 months — this is the v1 product
  target and should be enforced, not just documented.
- **Security**: Do not persist secrets, tokens, raw credential-bearing
  exception strings, or unbounded provider responses in diagnostic detail —
  persisted reason text must be bounded and scrubbed where needed.
- **Delivery boundary**: WD backend delivery is a later phase — v1 schema should
  support it without introducing dispatcher state or webhook attempts yet.

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Use the canonical term Feed Audit Event | Linear asks for audit history broader than lifecycle webhook payloads | Validated in Phase 1 |
| Name the durable table `feed_audit_events` | Keeps v1 focused on auditability, not delivery mechanics | Validated in Phase 1 |
| Keep `feeds` as current-state source of truth | Avoids an invasive event-sourcing rewrite of lease and failure paths | Validated in Phase 1 |
| Store `before_values` and `after_values` JSON objects | Directly answers what changed without forcing consumers to diff rows | Validated in Phase 1 |
| Add `status_reason_detail` and keep `quarantine_reason` as an alias for one release | Generalizes diagnostic detail beyond quarantine while preserving compatibility | Schema validated in Phase 1; compatibility behavior pending |
| Keep audit creation inside existing `FeedStore` mutation methods | Prevents service/runtime callers from constructing divergent state and audit history | Validated in Phase 2 |
| Use `feed_audit_event_sequences` for per-feed ordering | Avoids racy `MAX(feed_sequence) + 1` allocation under concurrent writes | Validated in Phase 2 |
| Use `service:feeds-service` as the Phase 2 actor fallback | Avoids nullable or spoofable user actors until trusted admin identity forwarding lands | Validated in Phase 2; human actor forwarding pending |
| Emit one `feed.quarantined` event when threshold crossing occurs | Avoids duplicate `failure_reported` plus `quarantined` events for the same outcome | — Pending |
| Treat all persisted non-quarantine failures as audit-worthy | Operators need repeated failure context, not only terminal quarantine state | — Pending |
| Do not audit routine lease churn in v1 | Lease handoffs are high-noise scheduler mechanics, not admin-facing history | — Pending |
| Do not create synthetic baseline events for existing feeds | Avoids misleading history that did not actually happen | — Pending |
| Defer WD webhook delivery and admin timeline reads | Establish durable data first; downstream propagation and UI can build on it later | — Pending |

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
*Last updated: 2026-06-19 after Phase 2 completion*

# Feed Audit Events

## Domain Contract

A Feed Audit Event is durable backend history for a meaningful feed mutation.
It answers what happened to a feed, when it happened, what changed, and which
causal actor produced the change.

Feed Audit Events are not full event sourcing. The current `feeds` row remains
the authoritative current feed state, and `feed_audit_events` is the append-only
audit history that future storage writers, runtime emitters, Watch Duty
delivery, and admin timelines derive from.

Every Feed Audit Event has one action from the action vocabulary, one required
`actor_id`, an `occurred_at` event time, a stable per-feed `feed_sequence`, and
allowlisted `before_values` and `after_values` snapshots. Schema details support
that domain meaning; they do not define it by themselves.

Phase 1 does not implement runtime event emission, storage write integration,
admin read APIs/UI, Watch Duty delivery dispatch, Watch Duty receivers, or
retention enforcement jobs.

Decision coverage: D-18, D-19, and D-20.

## Terminology

`feeds` is the current-state table. It answers what the system currently knows
about a configured feed and remains the source of truth for leasing, status,
failure counters, retry windows, and current diagnostic state.

`feed_audit_events` is audit history. It answers what meaningful feed changes
occurred over time and must remain useful even when the current `feeds` row is
later hard-deleted.

`status_reason` is the typed machine-readable abnormal-condition reason. It is
the stable value future policy and routing logic can use.

`status_reason_detail` is bounded explanatory text. It gives operators raw
diagnostic context and must not drive control flow.

`quarantine_reason` is a deprecated legacy field. New service, BFF, and
frontend contracts should use `status_reason_detail`; existing database cleanup
can happen separately from the public API migration.

## Action Vocabulary

The Feed Audit Event action vocabulary has exactly these action names:

- `feed.created`: a configured feed was created.
- `feed.updated`: meaningful feed configuration changed.
- `feed.deactivated`: a feed was intentionally deactivated.
- `feed.reset`: a feed was reset for future processing.
- `feed.deleted`: a feed was removed from current-state storage.
- `feed.failure_reported`: a non-terminal abnormal failure was persisted.
- `feed.quarantined`: a failure episode crossed the quarantine threshold.
- `feed.recovered`: successful activity cleared previously persisted abnormal
  state.

Routine worker lease churn, heartbeats, and clean progress updates are not part
of this default action vocabulary.

## Actor ID Vocabulary

Each Feed Audit Event has one required `actor_id` string. The value is
namespaced and stable enough for filtering. V1 does not add separate
`actor_type`, `actor_display`, or transport-service actor columns.

Canonical actor forms are:

- `user:google:<sub>` for a human admin identified by the Google subject claim.
- `user-email:<normalized_email>` for future trusted human identity paths where
  a Google subject claim is not available and policy explicitly allows an email
  actor. Phase 3 Google admin mutations require `user:google:<sub>` and must
  reject missing or invalid `sub` values.
- `service:<service_name>` for a semantic service actor.
- `job:<job_name>` for scheduled or maintenance job actors.
- `gcp-sa:<service_account_email>` only when the authenticated workload
  principal is the best available actor and no semantic service or job actor is
  known.
- `unknown:unknown` as the explicit fallback. It should be rare and visible in
  later tests and monitoring.

Admin actions represent the causal human actor, not the BFF or feeds-service
transport service account, per D-13. Future implementation must derive admin
actors from trusted authentication context, not from an untrusted request body
field.

The schema foundation caps `actor_id` at 512 characters and rejects empty or
whitespace-containing stable-id suffixes for every namespaced actor form.
`unknown:unknown` is the only non-namespaced value.

Decision coverage: D-06 through D-13.

## Before And After Values

`before_values` and `after_values` are allowlisted JSON snapshots of meaningful
domain values before and after an audited mutation. They are not raw row dumps
and should exclude high-noise operational fields unless a later phase proves
they are needed.

The schema stores `before_values`, `after_values`, and `metadata` as JSON
objects. Arrays, scalars, booleans, and JSON null are not valid audit snapshot
shapes.

`feed.deleted` uses normal `before_values` as the self-contained deleted-feed
snapshot. It does not get a delete-specific identity blob. It uses the same
maintained allowlist mechanism as other audit events, and audit history must
not depend on the current `feeds` row after hard delete.

The initial deletion snapshot allowlist is:

- `id`
- `name`
- `source_type`
- `status`
- `failure_count`
- `retry_after`
- `status_reason`
- `status_reason_updated_at`
- `status_reason_detail`
- `quarantine_reason`
- `last_bookmark_time`
- `created_at`
- `feed_properties.source_feed_id`
- `feed_properties.tags`

The default deletion snapshot explicitly excludes:

- `worker_id`
- `fencing_token`
- `last_heartbeat`
- `last_processed_filename`
- `unclaimed_since`

Decision coverage: D-01 through D-05.

## Diagnostic Detail

`status_reason` is the typed machine-readable reason. `status_reason_detail` is
explanatory text and does not drive control flow.

`status_reason_detail` is the canonical bounded raw diagnostic detail for
current feed state and Feed Audit Events. Phase 1 preserves emitted text with no
redaction or transformation beyond the 2048-character cap.

The security tradeoff from D-16 is accepted for Phase 1: capped raw detail
preserves debugging value, but it can persist sensitive text if upstream
failure strings contain it. Later hardening can revise the contract to add
redaction or transformation before persistence.

`quarantine_reason` is not the canonical diagnostic-detail API. It may remain
in storage or audit snapshots during migration, but new code should not add
public alias behavior around it.

Decision coverage: D-14, D-15, D-16, and D-17.

## Retention

Feed Audit Events are retained for 18 months. Phase 5 owns retention
enforcement, including any scheduled deletion job or retention verification.

Phase 1 only defines the retention target and the fields future enforcement can
use. It does not add retention enforcement jobs.

## Phase Boundaries

Phase 1 defines the contract and terminology only. It does not add:

- runtime event emission
- storage write integration
- admin read APIs or UI
- Watch Duty delivery dispatcher state
- Watch Duty webhook attempts, signatures, or receiver behavior
- retention enforcement jobs

Future phases can implement those behaviors, but they must preserve the Feed
Audit Event meaning defined here.

## Schema Reference

Schema names are supporting details for the domain contract:

| Concept | Canonical schema name | Meaning |
|---------|-----------------------|---------|
| Audit history table | `feed_audit_events` | Append-only history of meaningful feed mutations |
| Current-state table | `feeds` | Authoritative current feed state |
| Event time | `occurred_at` | When the domain event occurred |
| Per-feed order | `feed_sequence` | Stable ordering value for one feed timeline |
| Before snapshot | `before_values` | Allowlisted values before the mutation |
| After snapshot | `after_values` | Allowlisted values after the mutation |

The schema foundation must let an audit event identify the affected feed after
the current `feeds` row is hard-deleted. Future DDL must not use cascading feed
foreign keys that remove audit history on feed delete.

The canonical table is `feed_audit_events`, and the current-state table remains
`feeds`. The canonical ordering fields are `occurred_at` and `feed_sequence`.
The canonical snapshot fields are `before_values` and `after_values`.

## Consumer Payload Derivation

Future Watch Duty delivery and admin timelines derive payloads from this domain
contract without changing Feed Audit Event meaning.

Watch Duty delivery can map domain events into outbound payloads later without
turning `feed_audit_events` into delivery dispatcher state. Admin timelines can
query the audit history later without changing action names, actor semantics,
snapshot semantics, diagnostic-detail semantics, or retention meaning.

# Phase 4: Runtime Event Integration - Context

**Gathered:** 2026-06-19
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 4 makes runtime and Echo feed paths produce the meaningful failure,
quarantine, and recovery audit behavior promised by Feed Audit Events V1. It
covers async collector runtime failures/successes, non-budgeted failure release
paths, source-observation success paths, Echo/sync ingestion parity, runtime
actor IDs, and runtime diagnostic-detail lifecycle.

This phase does not add Watch Duty backend delivery, admin timeline read APIs,
retention jobs, new quarantine policy, new admin actor propagation, full event
sourcing, or routine lease/heartbeat history.

</domain>

<decisions>
## Implementation Decisions

### Failure Outcome Audit Boundary

- **D-01:** A runtime failure write creates a Feed Audit Event only when the
  persisted feed state changes to a new `(status, status_reason)` combination.
- **D-02:** Diagnostic-detail churn alone does not emit another audit event.
  If `status` and `status_reason` are unchanged, updating
  `status_reason_detail`, retry timing, or failure counters is current-state
  maintenance only.
- **D-03:** A non-terminal abnormal state change emits `feed.failure_reported`.
  Example: `active` or `unclaimed` to `failing` with a reason, or
  `failing/system_authentication_failed` to
  `failing/system_configuration_invalid`.
- **D-04:** A failure write that crosses the quarantine threshold emits one
  `feed.quarantined` outcome event. Do not emit both `feed.failure_reported`
  and `feed.quarantined` for the same write.
- **D-05:** The event before/after snapshots should make repeated attempts
  understandable, but repeated attempts with the same persisted
  `(status, status_reason)` are intentionally not new audit events.

### Recovery Event Boundary

- **D-06:** Emit `feed.recovered` when successful runtime activity clears a
  previously persisted abnormal lifecycle status and returns the feed to a
  normal runtime status.
- **D-07:** The abnormal statuses for recovery are `failing` and
  `quarantined`. Admin/manual reset from those states remains `feed.reset`,
  not `feed.recovered`.
- **D-08:** Claiming or leasing a previously failing feed is not recovery by
  itself. Emit recovery only after successful runtime progress, source
  observation, or Echo success proves the feed has resumed healthy processing.
- **D-09:** If a success write clears only stale `status_reason_detail` while
  the lifecycle status was already normal, do not emit `feed.recovered`.
- **D-10:** If a success write clears abnormal state and a later lease release
  changes `active` to `unclaimed`, the recovery event represents the immediate
  success write. The later release remains unaudited lease churn.

### Echo And Sync Store Parity

- **D-11:** Echo/sync ingestion gets full v1 audit parity now. Its failure and
  success paths must emit the same `feed.failure_reported`,
  `feed.quarantined`, and `feed.recovered` semantics as async runtime paths.
- **D-12:** Echo may use sync-specific SQL/helper mechanics because it uses
  psycopg, but the contract, action selection, snapshots, actor IDs, and
  diagnostic lifecycle must match `FeedStore`.
- **D-13:** Echo events skipped because the feed is already quarantined or
  deactivated do not create audit events when they do not mutate feed state.
  Those are logs/metrics only.
- **D-14:** Echo successful recording for a failing feed emits
  `feed.recovered` if that write moves the feed from abnormal status to normal
  status.

### Runtime Actor And Auth Provenance

- **D-15:** Runtime-generated audit events use semantic service actors as the
  canonical `actor_id`: `service:collector-runtime` for async runtime paths,
  `service:echo-ingestion` for Echo paths, and `service:feeds-service` only for
  service-originated non-human mutations.
- **D-16:** Do not use the removed `system:` actor prefix in Phase 4. Phase 2
  superseded the older Phase 1 mention of `system:<component_name>`.
- **D-17:** Do not encode source type into `actor_id`. Keep actor cardinality
  stable and put source-specific details such as `source_type`, collector name,
  worker ID, or runtime path in audit metadata or snapshots where useful.
- **D-18:** `gcp-sa:<service_account_email>` remains a fallback actor form only
  when the authenticated workload principal is the best available identity and
  no semantic service/job actor is known. It is not the normal actor for
  collector or Echo events.
- **D-19:** If the program needs GCP provenance in production, it may read the
  attached service account email from the metadata server at startup and store
  or log it as metadata/config validation. Do not replace the semantic
  `service:*` actor with the service-account email.
- **D-20:** For service-to-service admin flows from Phase 3, the incoming Google
  ID-token claims can identify the caller service (`email`, `sub`, `azp`) and
  should be used to trust the BFF boundary. They do not identify the human
  admin who clicked the action; human causal actor still comes from trusted BFF
  actor context.

### Diagnostic Detail Lifecycle

- **D-21:** Runtime abnormal writes set canonical `status_reason_detail` with
  bounded diagnostic text for both non-terminal failures and quarantines.
- **D-22:** `status_reason_detail` follows `status_reason`: set with abnormal
  state, updated as current-state detail when useful, and cleared when abnormal
  state is cleared.
- **D-23:** Persisted detail must be bounded and must not retain secrets,
  credential values, or unbounded provider responses. Phase 4 should add a
  small persistence-boundary sanitizer before storing runtime exception/provider
  text, while preserving useful operator context.
- **D-24:** `quarantine_reason` is legacy and noncanonical. New runtime code
  must not depend on it for public API behavior. If a temporary internal mirror
  is required to avoid breaking an existing flow, keep it compatibility-only
  and continue clearing it with recovery/reset.

### Noise Suppression

- **D-25:** Routine lease acquisition, release, heartbeat renewal, clean
  progress, and clean source observations do not emit default audit events.
- **D-26:** A clean success write may update current-state fields and clear
  stale diagnostics without audit when the feed was already in normal status.
- **D-27:** If implementation discovers a runtime write that is ambiguous
  between scheduler mechanics and feed lifecycle, default to no audit event
  unless it changes persisted abnormal lifecycle state.

### the agent's Discretion

- Choose the exact helper names, SQL statement layout, and sync/async sharing
  structure, provided storage remains the owner of audit row creation.
- Choose the practical metadata shape for runtime provenance, as long as it
  avoids secrets and does not turn source-specific metadata into actor ID
  cardinality.
- Choose whether temporary `quarantine_reason` mirroring is needed based on
  tests and existing app flow. The canonical field remains
  `status_reason_detail`.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Planning Context

- `.planning/PROJECT.md` - project scope, current milestone decisions, and
  auditability problem framing.
- `.planning/REQUIREMENTS.md` - Phase 4 requirements AUD-01, EVT-06,
  EVT-07, EVT-08, EVT-09, DIAG-02, DIAG-03, ACT-03, and COMP-04.
- `.planning/ROADMAP.md` - Phase 4 goal and success criteria.
- `.planning/STATE.md` - current milestone/session state.
- `.planning/phases/01-contract-and-schema-foundation/01-CONTEXT.md` -
  original domain contract, action vocabulary, and diagnostic-detail
  background. Note that its `system:` actor mention was superseded by Phase 2.
- `.planning/phases/02-transactional-storage-writes/02-CONTEXT.md` -
  storage-owned audit writes, required actor IDs, full allowlisted snapshots,
  sequence allocation, and removal of `system:`.
- `.planning/phases/03-service-and-compatibility-surface/03-CONTEXT.md` -
  trusted admin actor propagation and public `status_reason_detail`
  compatibility decisions.

### Domain Contract And Schema

- `documentation/feed-audit-events.md` - canonical action vocabulary, actor
  vocabulary, `status_reason_detail` semantics, no-lease-churn boundary, and
  before/after snapshot meaning.
- `terraform/modules/alloydb/sql/ingestion/003_feeds.sql` - current feed row
  fields and lifecycle/status columns.
- `terraform/modules/alloydb/sql/ingestion/024_feeds_status_reason.sql` -
  typed `status_reason` and canonical `status_reason_detail` schema.
- `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql` -
  audit table and sequence schema foundation.
- `terraform/modules/alloydb/sql/ingestion/030_feed_audit_events_actor_constraint.sql`
  - current actor constraint vocabulary.

### Runtime And Storage Code

- `backend/pipeline/storage/feed_store.py` - async `FeedStore`, audit helper,
  snapshot allowlist, runtime failure methods, source observation handling, and
  progress/update methods.
- `backend/pipeline/storage/feed_queries.py` - SQL for runtime progress,
  source observation, failure/quarantine, non-budgeted failure, audit insert,
  and current feed projections.
- `backend/pipeline/storage/sync_feed_store.py` - Echo/sync persistence path
  that needs audit parity.
- `backend/pipeline/storage/tests/test_feed_store.py` - storage tests to extend
  for runtime events, recovery, and diagnostic lifecycle.
- `backend/pipeline/storage/tests/test_sync_feed_store.py` - sync store tests
  to extend for Echo parity.

### Collector Runtime And Echo

- `backend/pipeline/ingestion/collector_runtime.py` - failure handling,
  non-budgeted failure handling, source observation success, and quarantine
  telemetry integration points.
- `backend/pipeline/ingestion/models.py` - `FeedFailure` and
  `SourceObservation` data used by runtime success/failure paths.
- `backend/pipeline/ingestion/failure_policy.py` - budgeted versus
  non-budgeted failure classification.
- `backend/pipeline/ingestion/quarantine_reason.py` - existing exception text
  and cap behavior to generalize for `status_reason_detail`.
- `backend/pipeline/ingestion/quarantine_telemetry.py` - existing telemetry
  that remains separate from durable audit history.
- `backend/pipeline/ingestion/tests/test_collector_runtime.py` - runtime tests
  to extend for actor and event-trigger boundaries.
- `backend/pipeline/ingestion/collectors/echo/main.py` - Echo status,
  heartbeat/success, and failure paths.
- `backend/pipeline/ingestion/collectors/echo/tests/test_main.py` and
  `backend/pipeline/ingestion/collectors/echo/tests/test_echo_collector_integration.py`
  - Echo tests to extend for sync parity.

### GCP Auth And Runtime Identity

- `backend/pipeline/common/auth.py` - current backend OIDC verification helper
  that returns incoming token claims.
- `backend/pipeline/common/auth_client.py` - existing helper that mints Google
  ID tokens from Cloud Run/GCE metadata server credentials.
- `https://docs.cloud.google.com/run/docs/authenticating/service-to-service` -
  Cloud Run service-to-service ID-token model and receiver parsing pattern.
- `https://docs.cloud.google.com/docs/authentication/token-types` - Google ID
  token claim fields; service account ID tokens include `email`, `sub`, and
  `azp`.
- `https://docs.cloud.google.com/docs/authentication/get-id-token` - metadata
  server ID-token generation and the fact that metadata-server tokens are for
  the attached service account, not end users.
- `https://docs.cloud.google.com/run/docs/securing/service-identity` - Cloud
  Run service identity model and metadata-server token behavior.
- `https://docs.cloud.google.com/run/docs/configuring/services/service-identity`
  - Cloud Run service-account attachment/configuration reference.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets

- `FeedStore._insert_feed_audit_event`: existing storage-owned audit insert
  helper to reuse or extend for runtime events.
- `FeedStore._audit_snapshot_from_row`: existing allowlisted snapshot logic
  that should continue to define before/after payloads.
- `feed_queries.INSERT_FEED_AUDIT_EVENT_SQL` and sequence SQL: existing
  per-feed ordering and insert foundation.
- `quarantine_reason.cap_quarantine_reason_for_storage` and
  `quarantine_reason.exception_text`: existing bounded diagnostic helpers that
  can be generalized for canonical `status_reason_detail`.
- `auth.verify_oidc_token`: already returns decoded incoming OIDC token claims
  and can identify trusted caller services for admin paths.
- `auth_client.get_id_token`: existing pattern for Cloud Run service identity
  when this service calls another authenticated service.

### Established Patterns

- Storage owns audit row creation. Runtime and service callers should pass
  causal inputs, status reason, diagnostic text, and actor ID; they should not
  construct or insert audit rows directly.
- Current feed state remains authoritative. Runtime changes update `feeds`,
  and audit rows describe meaningful state changes; no event replay is
  introduced.
- Lease mechanics are intentionally noisy and optimized around fencing,
  heartbeat, and `SKIP LOCKED`. Audit logic must stay out of routine claim,
  heartbeat, release, and clean progress paths unless those writes clear
  abnormal lifecycle state.
- Echo is a sync store path and should match domain semantics even if it cannot
  literally call async `FeedStore` helpers.
- Quarantine telemetry remains useful but is not durable audit history. Phase 4
  should keep telemetry behavior while adding durable audit rows at storage
  mutation points.

### Integration Points

- `FeedStore.report_feed_failure`: emit `feed.failure_reported` or
  `feed.quarantined` only when the persisted `(status, status_reason)` combo
  changes.
- `FeedStore.release_non_budgeted_failure`: emit `feed.failure_reported` only
  when it creates a new non-terminal abnormal combo.
- `FeedStore.record_source_observation` and the progress/update path using
  `UPDATE_PROGRESS_SQL`: emit `feed.recovered` only when success clears
  abnormal lifecycle status.
- `SyncFeedStore.record_failure`: add sync audit parity for failure/quarantine
  decisions and canonical diagnostic detail.
- `SyncFeedStore.record_heartbeat` / Echo success path: emit `feed.recovered`
  only for abnormal-to-normal success.
- `collector_runtime._record_feed_failure`,
  `collector_runtime._record_non_budgeted_failure`, and
  `collector_runtime._process_source_observation`: pass the correct semantic
  actor and bounded diagnostic inputs into storage.
- `ingestion/collectors/echo/main.py`: pass `service:echo-ingestion` and avoid
  emitting skipped-feed audit rows when no feed mutation occurs.

</code_context>

<specifics>
## Specific Ideas

- The user explicitly chose the `(status, status_reason)` combination as the
  runtime failure audit boundary.
- The user clarified that clearing only `status_reason_detail` is not
  `feed.recovered` if lifecycle status does not change.
- The user proposed recovery as abnormal status, including `failing` or
  `quarantined`, moving to normal status; Phase 4 should preserve that product
  meaning even if implementation needs to account for claim-then-success
  mechanics.
- The user accepted the GCP auth distinction: incoming auth can identify the
  caller service account, while human identity still requires trusted BFF actor
  propagation.
- The best actor design is semantic runtime actors plus optional GCP
  service-account provenance metadata, not service-account email as the normal
  audit actor.

</specifics>

<deferred>
## Deferred Ideas

- Watch Duty backend delivery, delivery retries, dead-letter state, and webhook
  signatures remain v2/out of scope for Phase 4.
- Admin timeline read APIs and UI remain out of scope.
- Retention enforcement and broad verification hardening remain Phase 5.
- Routine lease/heartbeat history can be revisited only if a later product or
  operational requirement justifies an internal event stream.
- Signed actor-context JWT/HMAC for admin actor propagation is deferred unless
  the trusted service boundary becomes more complex.
- Making `gcp-sa:<email>` the normal runtime actor is rejected for v1; keep it
  as a fallback for genuinely unknown semantic service identity.

</deferred>

---

*Phase: 4-Runtime Event Integration*
*Context gathered: 2026-06-19*

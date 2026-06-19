# Phase 1: Contract and Schema Foundation - Context

**Gathered:** 2026-06-19
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 1 defines the Feed Audit Event contract and database foundation that later
storage, service, runtime, delivery, and admin timeline work must use. It covers
the domain event meaning, required audit fields, actor ID contract,
`status_reason_detail` semantics, deletion snapshot semantics, per-feed
ordering foundation, and documentation shape. It does not implement audited
storage writes, runtime event emission, WD delivery, admin read APIs, or UI.

</domain>

<decisions>
## Implementation Decisions

### Delete Identity

- **D-01:** Do not create a delete-specific identity blob for
  `feed.deleted`. The event's `before_values` is the self-contained deletion
  snapshot.
- **D-02:** `feed.deleted.before_values` should use the same maintained
  allowlist mechanism as other audit events.
- **D-03:** The deletion snapshot allowlist should be derived from `feeds` row
  fields or a long-term-maintainable subset of that row.
- **D-04:** Exclude noisy worker/heartbeat lease fields from the default delete
  snapshot unless a later phase explicitly proves they are needed.
- **D-05:** Audit history must not rely on the deleted `feeds` row continuing to
  exist and must not use a cascading FK that removes audit events on feed
  delete.

### Actor ID

- **D-06:** Store one required `actor_id` string on every Feed Audit Event.
  Do not add separate `actor_type` or `actor_display` columns in v1.
- **D-07:** `actor_id` must be namespaced and stable enough for filtering:
  `<namespace>:<stable-id>`.
- **D-08:** For human admin actions, prefer `user:google:<sub>` using the
  Google subject claim already present on the BFF `GoogleUser`.
- **D-09:** Use `user-email:<normalized_email>` only as a fallback when a Google
  subject is unavailable.
- **D-10:** Use semantic non-human actor IDs for system-originated events:
  `service:<service_name>`, `system:<component_name>`, and `job:<job_name>`.
- **D-11:** Reserve `gcp-sa:<service_account_email>` for cases where only the
  authenticated workload principal is known and no semantic service/system actor
  can be determined.
- **D-12:** Use `unknown:unknown` as an explicit fallback; it should be rare and
  visible in tests/monitoring later.
- **D-13:** For admin actions, `actor_id` should represent the causal human
  actor, not the BFF or feeds-service transport service account. Trusted actor
  forwarding from BFF to FastAPI is a later phase detail, but the Phase 1
  contract must support it.

### Diagnostic Detail

- **D-14:** `status_reason_detail` follows current `quarantine_reason` behavior
  for persisted text in v1: preserve the emitted detail text and cap its length.
- **D-15:** Do not require redaction or transformation of
  `status_reason_detail` in Phase 1 beyond the length cap.
- **D-16:** Record the security tradeoff explicitly: raw capped detail is easier
  to implement and preserves debugging value, but it can persist sensitive text
  if upstream failure strings contain it. Later hardening can add redaction as a
  contract revision.
- **D-17:** `status_reason` remains the typed machine-readable reason;
  `status_reason_detail` is explanatory text and must not become control flow.

### Contract Documentation

- **D-18:** Write the Phase 1 documentation as a domain contract first, with
  storage schema details second.
- **D-19:** The contract must define Feed Audit Event meaning, action
  vocabulary, actor ID vocabulary, before/after semantics,
  `status_reason_detail`, retention policy, and v1 boundaries.
- **D-20:** Storage columns, indexes, migration names, and table layout are
  supporting details. They should be documented enough for implementation, but
  future WD delivery/admin timeline consumers should not have to reverse
  engineer domain meaning from the table schema.

### the agent's Discretion

The user chose high-level contract semantics and delegated exact allowlist
membership to maintainability. Downstream planning may choose the initial
`feeds` field allowlist, sequence allocator mechanism, migration file names,
and schema constraints as long as the decisions above are preserved.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Planning Context

- `.planning/PROJECT.md` — project scope, out-of-scope boundaries, and prior
  design decisions.
- `.planning/REQUIREMENTS.md` — Phase 1 requirements: AUD-02, AUD-03, DIAG-01,
  ACT-01, DOC-01, DOC-02, DOC-03.
- `.planning/ROADMAP.md` — Phase 1 goal and success criteria.
- `.planning/research/SUMMARY.md` — researched stack, architecture, pitfalls,
  and phase ordering.

### Codebase Maps

- `.planning/codebase/STACK.md` — toolchain, migration, storage, API, and test
  context.
- `.planning/codebase/ARCHITECTURE.md` — storage/service/runtime boundaries and
  current feed state architecture.
- `.planning/codebase/CONCERNS.md` — HOT/index risks, feed-store fragility,
  auth-boundary risks, and testing concerns.

### Current Schema And Helpers

- `terraform/modules/alloydb/sql/ingestion/003_feeds.sql` — current `feeds`
  table fields that inform the deletion snapshot allowlist.
- `terraform/modules/alloydb/sql/ingestion/020_quarantine_reason.sql` —
  current `quarantine_reason` compatibility field.
- `terraform/modules/alloydb/sql/ingestion/024_feeds_status_reason.sql` —
  current typed status-reason schema.
- `backend/pipeline/storage/quarantine_reason.py` — existing 2048-character cap
  behavior to mirror for `status_reason_detail` in v1.

### Auth And Actor Context

- `frontend/common/src/types/auth.ts` — current BFF user claims include
  `email`, `email_verified`, `sub`, `aud`, and `iss`.
- `frontend/api/src/authentication.ts` — BFF authentication populates
  `request.user` and admin status.
- `frontend/api/src/utils.ts` — BFF calls backend services with Google
  service-to-service ID tokens.
- `backend/pipeline/common/auth.py` — FastAPI service OIDC verification and
  current service-principal view.
- `backend/services/feeds/main.py` — feed route boundary where actor context
  will later be passed into service/store calls.
- `backend/services/feeds/service.py` — feed service methods that currently
  call store mutations without actor context.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets

- `backend/pipeline/storage/quarantine_reason.py`: reuse or generalize the
  existing cap behavior for `status_reason_detail`; Phase 1 does not require
  redaction.
- `frontend/common/src/types/auth.ts`: use `GoogleUser.sub` as the preferred
  human actor stable ID source for future admin events.
- `backend/pipeline/common/auth.py`: backend services currently see OIDC claims
  from the calling service token, not necessarily the original human user.

### Established Patterns

- Database schema changes belong under
  `terraform/modules/alloydb/sql/ingestion/` using ordered idempotent SQL
  migrations.
- Feed current state is owned by `feeds` plus related storage-layer query
  modules; Phase 1 should preserve `feeds` as current state and avoid event
  sourcing.
- Store classes and query modules own persistence behavior; later phases should
  avoid direct audit inserts from controllers or runtime code.
- Existing architecture treats free-text diagnostic strings as explanatory, not
  policy inputs.

### Integration Points

- New schema must integrate with `feeds.status_reason` and
  `feeds.quarantine_reason` compatibility.
- Future actor propagation crosses the BFF-to-FastAPI boundary:
  `frontend/api/src/feeds/feedsController.ts` -> `backend/services/feeds/main.py`
  -> `backend/services/feeds/service.py` -> `FeedStore`.
- Future storage writers must support both async `FeedStore` and sync
  `SyncFeedStore`, but Phase 1 only defines the shared contract/schema
  foundation.

</code_context>

<specifics>
## Specific Ideas

- User explicitly clarified that after hard delete, the `feeds` row is removed.
  The audit event must not depend on that row.
- User prefers keeping delete identity simple: use `before_values`, not extra
  delete-only duplicated fields.
- User prefers long-term maintainability over archiving every possible field in
  delete events.
- User explicitly chose raw capped diagnostic detail over redacted summaries,
  despite the known security tradeoff.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope.

</deferred>

---

*Phase: 1-Contract and Schema Foundation*
*Context gathered: 2026-06-19*

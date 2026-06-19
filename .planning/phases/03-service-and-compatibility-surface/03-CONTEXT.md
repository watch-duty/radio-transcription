# Phase 3: Service and Compatibility Surface - Context

**Gathered:** 2026-06-19
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 3 delivers the service/API/BFF/frontend compatibility surface for Feed
Audit Events V1. It must make admin feed mutations carry trusted Google admin
identity into the storage-owned audit writer, expose `status_reason_detail` as
the public diagnostic-detail field, and keep existing feed UI flows working.

This phase does not add runtime failure/quarantine/recovery event emission,
Watch Duty webhook delivery, admin timeline read APIs, retention jobs, or a UI
redesign.

</domain>

<decisions>
## Implementation Decisions

### Trusted Admin Actor Handoff

- **D-01:** Admin mutations use trusted identity propagation. The BFF derives
  `actor_id` from the authenticated `request.user` after `isAdmin` has passed,
  then forwards that actor context to feeds-service on the authenticated
  service-to-service call.
- **D-02:** Feeds-service accepts forwarded actor context only from the trusted
  BFF service identity. Actor context from request bodies or untrusted/direct
  callers must not be accepted.
- **D-03:** The audit event persists the existing single causal `actor_id`,
  e.g. `user:google:<sub>`. Do not add canonical `causal_actor`,
  `committing_actor`, `caller_service`, `actor_type`, or `actor_display` core
  fields in v1. Caller/committer provenance can be logged or stored as metadata
  if useful, but it is not the audit identity.

### Missing Identity Policy

- **D-04:** Phase 3 Google admin mutations require a valid non-empty Google
  `sub`; the actor is `user:google:<sub>`.
- **D-05:** Missing, empty, or malformed `sub` on an admin mutation is a
  security risk and must fail closed before mutating feed state. Do not fall
  back to `user-email:<email>`, `unknown:unknown`, `user:null`, or an empty
  actor suffix for admin actions.
- **D-06:** `user-email:<normalized_email>` remains an allowed actor namespace
  for future trusted human identity paths, but it is not the Phase 3 fallback
  for Google admin mutations.

### Diagnostic Detail API Shape

- **D-07:** Public feed responses expose canonical `status_reason_detail`.
  `quarantine_reason` is deprecated and should not remain a public alias if
  removing it does not break the app flow.
- **D-08:** Compatibility means existing feed API/BFF/frontend flows continue
  working, not that `quarantine_reason` keeps carrying equivalent text.
- **D-09:** Phase 3 should update service/BFF/frontend contracts to consume
  `status_reason_detail` and avoid adding new alias or mirroring behavior.
  Runtime-owned cleanup of existing `quarantine_reason` writes belongs to
  Phase 4 unless Phase 3 is explicitly expanded.
- **D-10:** Project requirements and domain documentation have been updated so
  downstream planning does not preserve the older `quarantine_reason`
  compatibility-alias requirement.

### Frontend Compatibility Behavior

- **D-11:** The frontend should display `statusReasonDetail` in the same places
  that previously displayed optional `quarantineReason`: status indicator
  tooltip/search/table/header flows.
- **D-12:** Do not add new visible UI sections or redesign feed status display
  in Phase 3.
- **D-13:** TypeScript/public frontend naming is `statusReasonDetail`, the
  camelCase mapping of backend `status_reason_detail`.

### the agent's Discretion

- Choose the exact internal header name and parsing helper, provided it is
  explicitly internal and never accepted from untrusted request bodies.
- Choose whether to retain caller-service provenance in logs or audit metadata;
  the core audit identity remains the single `actor_id`.
- Choose the smallest frontend changes that preserve the current status display
  flow while migrating the detail field.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project Scope And Requirements

- `.planning/PROJECT.md` — current project context, active requirements, and
  updated `quarantine_reason` deprecation decision.
- `.planning/REQUIREMENTS.md` — Phase 3 requirements DIAG-04, ACT-02,
  COMP-01, COMP-02, and COMP-03 after the diagnostic-detail compatibility
  update.
- `.planning/ROADMAP.md` — Phase 3 goal and success criteria.
- `.planning/STATE.md` — current milestone/session state.

### Prior Phase Decisions

- `.planning/phases/01-contract-and-schema-foundation/01-CONTEXT.md` —
  single `actor_id`, actor namespace vocabulary, diagnostic-detail semantics,
  deletion snapshot semantics, and non-event-sourcing boundary.
- `.planning/phases/02-transactional-storage-writes/02-CONTEXT.md` —
  storage-owned audit writes, `service:feeds-service` Phase 2 fallback, and
  Phase 3 ownership of trusted admin actor forwarding.
- `documentation/feed-audit-events.md` — canonical Feed Audit Event contract,
  actor vocabulary, and updated diagnostic-detail terminology.

### Codebase Context

- `.planning/codebase/ARCHITECTURE.md` — BFF/FastAPI/service/store layering and
  feed management flow.
- `.planning/codebase/INTEGRATIONS.md` — GCP auth, API Gateway/Endpoints
  userinfo, service-to-service auth, and internal HTTP integration context.
- `.planning/codebase/STACK.md` — FastAPI/Pydantic, Express/tsoa, React, and
  testing stack.

### Auth And Service Boundary

- `frontend/api/src/authentication.ts` — current BFF user authentication,
  gateway userinfo parsing, and `isAdmin` population.
- `frontend/common/src/types/auth.ts` — `GoogleUser` shape with `sub`, email,
  and `isAdmin`.
- `frontend/api/src/utils.ts` — Google ID-token service client used for BFF to
  backend service calls.
- `backend/pipeline/common/auth.py` — FastAPI OIDC verification dependency for
  backend services.
- `backend/services/feeds/main.py` — feed routes where trusted actor context
  should enter the service layer.
- `backend/services/feeds/service.py` — current `service:feeds-service`
  fallback that Phase 3 must replace for admin mutations.
- `backend/services/feeds/models.py` — feed request/response models; request
  bodies must remain actor-field-free.

### Diagnostic Detail And Frontend Surface

- `backend/pipeline/storage/feed_store.py` — current feed row mapping and audit
  snapshot handling for `status_reason_detail` and `quarantine_reason`.
- `backend/pipeline/storage/feed_queries.py` — current feed list/get/reset
  projections and remaining runtime/storage SQL references.
- `frontend/api/src/feeds/feedsController.ts` — BFF feed response conversion
  and admin mutation proxy calls.
- `frontend/common/src/types/feeds.ts` — shared `Feed` type where
  `statusReasonDetail` belongs.
- `frontend/transcription-ui/src/components/common/FeedStatusIndicator.tsx` —
  status tooltip that should display `statusReasonDetail`.
- `frontend/transcription-ui/src/components/feeds/FeedSearchView.tsx`,
  `frontend/transcription-ui/src/components/feeds/FeedTable.tsx`,
  `frontend/transcription-ui/src/components/feeds/FeedConfigurationTable.tsx`,
  and `frontend/transcription-ui/src/components/transcripts/FeedHeader.tsx` —
  existing status indicator call sites.

### External Auth References

- `https://cloud.google.com/run/docs/authenticating/service-to-service` —
  Cloud Run service-to-service ID-token model for authenticating the BFF
  service identity to feeds-service.
- `https://cloud.google.com/api-gateway/docs/authenticate-service-account` —
  API Gateway pattern for forwarding verified user claims to a backend through
  userinfo headers.
- `https://cloud.google.com/endpoints/docs/openapi/authenticating-users-custom`
  — Cloud Endpoints userinfo header pattern.
- `https://cloud.google.com/iap/docs/signed-headers-howto` — signed upstream
  identity header precedent and validation model.
- `https://www.rfc-editor.org/rfc/rfc8693.html` — OAuth token exchange
  delegation model; useful context but full token exchange is not required in
  Phase 3.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets

- `frontend/api/src/authentication.ts`: already decodes trusted gateway
  userinfo or bearer JWT payloads and populates `request.user.isAdmin`.
- `frontend/api/src/utils.ts`: already creates Google ID-token clients for
  internal service calls.
- `backend/pipeline/common/auth.py`: already verifies backend service OIDC
  tokens for FastAPI apps.
- `FeedStore` audited mutation methods: already require explicit `actor_id`,
  so Phase 3 should feed them the trusted admin actor through the existing
  service/storage call chain.
- `FeedStatusIndicator`: already formats optional detail text in status
  tooltips; migrate its prop from `quarantineReason` to `statusReasonDetail`
  rather than creating a new UI surface.

### Established Patterns

- Backend services are thin FastAPI routes, service classes, and store calls.
  Keep actor extraction at the trusted route/service boundary and audit writes
  in storage.
- BFF controllers proxy to backend services with typed conversions and
  generated tsoa metadata. Update source controllers/types; do not hand-edit
  generated routes/specs except through the established generation path.
- Feed request bodies currently forbid or omit `actor_id`; preserve that
  anti-spoofing boundary.
- Storage owns audit row construction. Service/runtime callers pass only causal
  actor input and feed mutation intent.

### Integration Points

- BFF admin mutation methods in `feedsController.ts` must derive and forward
  `actor_id` only after admin authorization succeeds.
- Feeds-service routes in `main.py` must validate trusted caller context before
  accepting forwarded actor identity.
- `FeedService` should accept/preserve explicit actor IDs for admin mutations
  and keep service/system actor behavior for non-human service-originated
  paths.
- Backend `Feed` response models, BFF backend/frontend conversion, shared
  frontend types, and status indicator call sites must all move from
  `quarantine_reason` / `quarantineReason` to
  `status_reason_detail` / `statusReasonDetail`.

</code_context>

<specifics>
## Specific Ideas

- Treat the actor propagation as a trusted identity assertion paired with
  service-to-service authentication. It is not a loose, client-controllable
  header.
- The durable audit row should answer who caused the change; for admin changes
  that is the Google user subject, not the BFF or feeds-service transport
  account.
- Existing app flow compatibility is the goal for diagnostic detail. The
  deprecated field does not need to stay populated or public.

</specifics>

<deferred>
## Deferred Ideas

- Signed actor-context JWT or HMAC header can be added later if more trusted
  callers are introduced or the service boundary becomes less direct.
- Full OAuth token exchange / on-behalf-of token flow is not needed for Phase
  3; the current GCP service identity plus trusted actor context pattern is
  sufficient.
- Runtime-owned cleanup of existing `quarantine_reason` writes belongs to Phase
  4 runtime diagnostic lifecycle work.
- Separate `causal_actor`, `committing_actor`, `caller_service`,
  `actor_type`, or `actor_display` fields remain out of scope for v1 core
  audit persistence.

</deferred>

---

*Phase: 3-Service and Compatibility Surface*
*Context gathered: 2026-06-19*

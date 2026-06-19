# Phase 3: Service and Compatibility Surface - Research

**Researched:** 2026-06-19
**Domain:** trusted admin actor propagation, FastAPI feed API
compatibility, BFF/frontend diagnostic-detail migration
**Confidence:** HIGH

<user_constraints>
## User Constraints From Context

Phase 3 is bounded to the service/API/BFF/frontend compatibility surface.
It does not add runtime failure/quarantine/recovery event emission, Watch
Duty webhook delivery, admin timeline read APIs, retention jobs, or a UI
redesign. [VERIFIED: .planning/phases/03-service-and-compatibility-surface/03-CONTEXT.md]

Admin mutations must use trusted identity propagation: the BFF derives the
human actor from authenticated `request.user` after `isAdmin` passes, calls the
feeds service as the BFF service account, and forwards internal actor context
only to that trusted service boundary. [VERIFIED: 03-CONTEXT.md]

The persisted audit identity remains one required `actor_id`; do not introduce
core `causal_actor`, `committing_actor`, `caller_service`, `actor_type`, or
`actor_display` fields in v1. [VERIFIED: 03-CONTEXT.md; VERIFIED:
documentation/feed-audit-events.md]

Google admin mutations require a valid non-empty Google `sub`; the actor is
`user:google:<sub>`. Missing, empty, or malformed `sub` is a security risk and
must fail closed before mutation. Do not use `user:null`, `user:`, empty
suffixes, `unknown:unknown`, or an email fallback for Phase 3 Google admin
mutations. [VERIFIED: 03-CONTEXT.md]

Public feed responses should expose canonical `status_reason_detail`. The
deprecated `quarantine_reason` field should not remain a public alias if
removal does not break the app flow. The frontend should display
`statusReasonDetail` in the same existing status indicator/search/table/header
places that previously displayed `quarantineReason`. [VERIFIED:
03-CONTEXT.md]
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| DIAG-04 | Existing feed API/BFF/frontend flows continue working while consumers migrate from `quarantine_reason` to `status_reason_detail`. | Backend storage already has `status_reason_detail`; service/BFF/frontend response types and mappings still expose `quarantine_reason` / `quarantineReason` and need migration. [VERIFIED: backend/pipeline/storage/feed_queries.py; VERIFIED: backend/services/feeds/models.py; VERIFIED: frontend/api/src/feeds/feedsController.ts; VERIFIED: frontend/common/src/types/feeds.ts] |
| ACT-02 | Admin-initiated feed mutations preserve authenticated admin identity at the trusted boundary. | BFF already authenticates users and computes `isAdmin`; feeds-service currently only sees service-to-service OIDC and uses `service:feeds-service` fallback. [VERIFIED: frontend/api/src/authentication.ts; VERIFIED: backend/pipeline/common/auth.py; VERIFIED: backend/services/feeds/service.py] |
| COMP-01 | Existing feed API callers continue receiving fields they depend on during the compatibility window. | Current app flow does not branch on `quarantineReason`; it is optional display text. Migration should preserve feed status/statusReason/status display and remove only the deprecated detail alias. [VERIFIED: rg quarantineReason; VERIFIED: frontend/transcription-ui/src/components/common/FeedStatusIndicator.tsx] |
| COMP-02 | Feed API responses expose canonical diagnostic detail without breaking clients. | Add `status_reason_detail` to storage projections, storage `Feed` typed dict, FastAPI `Feed`, BFF backend shape, shared frontend `Feed`, and generated OpenAPI. [VERIFIED: backend/pipeline/storage/feed_store.py; VERIFIED: backend/pipeline/storage/feed_queries.py; VERIFIED: backend/services/feeds/models.py; VERIFIED: frontend/api/openapi.yaml] |
| COMP-03 | Existing frontend/BFF feed status and status-reason behavior remains compatible. | Keep the existing status indicator and call sites; rename the optional detail prop and assertions from quarantine to status reason detail. [VERIFIED: frontend/transcription-ui/src/components/common/FeedStatusIndicator.tsx; VERIFIED: frontend/transcription-ui/src/components/feeds/FeedSearchView.tsx; VERIFIED: frontend/transcription-ui/src/components/feeds/FeedTable.tsx; VERIFIED: frontend/transcription-ui/src/components/feeds/FeedConfigurationTable.tsx; VERIFIED: frontend/transcription-ui/src/components/transcripts/FeedHeader.tsx] |
</phase_requirements>

## Current System Findings

### BFF Authentication And Admin Boundary

`frontend/api/src/authentication.ts` parses trusted Google gateway userinfo
headers or bearer JWT payloads into `GoogleUser`, then sets `isAdmin` by calling
`checkIsAdmin(email)`. [VERIFIED: frontend/api/src/authentication.ts] The
shared `GoogleUser` type already contains `sub`, `email`, `email_verified`,
`aud`, and `iss`. [VERIFIED: frontend/common/src/types/auth.ts]

The BFF feed controller already receives `@Request() request` for admin
mutations and rejects non-admin users with `HttpError(403, 'Forbidden')`.
[VERIFIED: frontend/api/src/feeds/feedsController.ts] It currently forwards
only the feed mutation body to feeds-service; it does not forward actor context
and tests assert those backend requests have no actor-related headers.
[VERIFIED: frontend/api/src/feeds/feedsController.test.ts]

The BFF's downstream client uses `google-auth-library` `getIdTokenClient` for
service-to-service calls unless local/test auth disables it. [VERIFIED:
frontend/api/src/utils.ts] This gives feeds-service an authenticated service
caller identity separately from the human actor context that Phase 3 must add.

### Feeds-Service Auth And Actor Fallback

`backend/pipeline/common/auth.py` verifies Google OIDC tokens in GCP and
returns token claims; outside GCP it returns local-dev claims. [VERIFIED:
backend/pipeline/common/auth.py] `backend/services/feeds/main.py` installs that
dependency globally on the FastAPI app, so the service is authenticated, but
route handlers do not currently receive the decoded claims. [VERIFIED:
backend/services/feeds/main.py]

`backend/services/feeds/service.py` currently uses `_FEEDS_SERVICE_ACTOR_ID =
"service:feeds-service"` for create, update, deactivate, delete, and reset.
[VERIFIED: backend/services/feeds/service.py] Phase 2 deliberately used that
fallback until trusted admin forwarding landed in Phase 3. [VERIFIED:
.planning/phases/02-transactional-storage-writes/02-02-SUMMARY.md; VERIFIED:
.planning/phases/02-transactional-storage-writes/02-03-SUMMARY.md]

Storage methods already require explicit keyword-only `actor_id`; the Phase 3
change should feed trusted admin actor IDs through the existing service/store
chain rather than adding audit insert behavior above storage. [VERIFIED:
backend/pipeline/storage/feed_store.py; VERIFIED:
backend/pipeline/storage/tests/test_feed_store.py]

### Diagnostic Detail Compatibility

Phase 1 added `feeds.status_reason_detail`, and Phase 2 included it in audit
snapshot primitives, but normal public feed projections still omit it in
`CREATE_FEED_SQL`, `GET_FEED_SQL`, `LIST_FEEDS_*`, and `UPDATE_FEED_SQL`.
[VERIFIED: backend/pipeline/storage/feed_queries.py] `RESET_FEED_SQL` returns
`status_reason_detail`, but `_row_to_feed` ignores it, so current service
responses cannot expose it consistently. [VERIFIED:
backend/pipeline/storage/feed_store.py; VERIFIED:
backend/pipeline/storage/feed_queries.py]

The FastAPI `Feed` model currently exposes `quarantine_reason` and
`status_reason`, but not `status_reason_detail`. [VERIFIED:
backend/services/feeds/models.py] The BFF backend shape maps
`quarantine_reason` to frontend `quarantineReason`. [VERIFIED:
frontend/api/src/feeds/feedsController.ts] The shared frontend `Feed` type has
optional `quarantineReason` and no `statusReasonDetail`. [VERIFIED:
frontend/common/src/types/feeds.ts]

UI usage is display-only. `quarantineReason` is passed to
`FeedStatusIndicator` in search, feed tables, configuration tables, and feed
headers. The component appends it to tooltip text; no control flow depends on
it. [VERIFIED: rg quarantineReason; VERIFIED:
frontend/transcription-ui/src/components/common/FeedStatusIndicator.tsx]

### Generated API Surface

`frontend/api/openapi.yaml` is generated from source and currently exposes
`quarantineReason` on the public `Feed` schema. [VERIFIED:
frontend/api/openapi.yaml; VERIFIED: frontend/api/package.json] Phase 3 should
update source types/controllers and regenerate or verify the spec through
`yarn generate-spec` / `yarn verify-spec`, not hand-edit the contract in
isolation. [VERIFIED: frontend/api/package.json]

## Recommended Design

Use a small internal actor-context header from BFF to feeds-service, paired
with service-to-service authentication:

- BFF validates admin authorization exactly where it already does.
- BFF derives `actor_id = user:google:<request.user.sub>`.
- BFF fails closed before a service call if `sub` is missing, empty, or
  whitespace-bearing.
- BFF sends the actor only as an internal header, recommended name
  `X-WD-Actor-Id`, on admin mutation calls.
- Feed request bodies remain actor-field-free.
- Feeds-service reads decoded OIDC claims on admin mutation routes.
- Feeds-service accepts the forwarded actor header only when the authenticated
  caller is trusted. In production, trust is a configured allowlist of BFF
  service account emails. In local non-GCP mode, the existing local-dev claims
  may be treated as trusted for developer flow and tests.
- `FeedService` admin mutation methods require explicit `actor_id`; they no
  longer own a silent `service:feeds-service` fallback for admin routes.
- Storage remains the only layer that constructs and inserts audit events.

For diagnostic detail, migrate the public response contract from
`quarantine_reason` / `quarantineReason` to
`status_reason_detail` / `statusReasonDetail` across backend storage
projections, service models, BFF conversion, shared types, generated OpenAPI,
and existing UI call sites. Do not add a new UI surface.

## Security Domain

| Threat | Risk | Mitigation |
|--------|------|------------|
| Forged human actor from request body | External or untrusted caller attributes feed mutation to another user | Keep request models actor-field-free; derive actors only from BFF auth context and internal header. |
| Forged internal actor header | Direct caller sends `X-WD-Actor-Id` to feeds-service | Feeds-service validates authenticated caller claims against trusted BFF service accounts before accepting the header. |
| Missing admin Google `sub` | Audit row becomes ambiguous or fake human identity is stored | BFF fails closed before mutation and tests verify no downstream call is made. |
| Silent fallback to `service:feeds-service` | Human admin actions lose attribution | Make `actor_id` explicit in `FeedService` admin methods and update route tests to assert human actor propagation. |
| Diagnostic detail field drift | Backend and frontend display different status detail concepts | Move public contract to `status_reason_detail` in one phase and test absence of public `quarantine_reason` alias. |

## UI Gate

Phase 3 has a roadmap UI hint because it touches React feed status display.
It does not introduce a new screen, layout, visual hierarchy, or interaction
model. A separate UI-SPEC is not required for this phase; plans should preserve
the existing `FeedStatusIndicator` behavior and update tests for the renamed
detail prop. [VERIFIED: .planning/ROADMAP.md; VERIFIED:
.planning/phases/03-service-and-compatibility-surface/03-CONTEXT.md]

## Open Questions (RESOLVED)

1. **RESOLVED: Should public responses keep `quarantine_reason` as an alias?**
   No. The user chose the canonical `status_reason_detail` path if app flow is
   not broken. Code search shows the old field is optional display text only,
   so Phase 3 should remove it from the public service/BFF/frontend contract
   and migrate display to `statusReasonDetail`. [VERIFIED:
   03-DISCUSSION-LOG.md; VERIFIED: rg quarantineReason]

2. **RESOLVED: How should `actor_id` be constructed for admin mutations?**
   Use `user:google:<sub>` from the authenticated Google user after BFF admin
   authorization. Missing or invalid `sub` is a security failure and must
   reject before feed mutation. [VERIFIED: 03-CONTEXT.md; VERIFIED:
   frontend/common/src/types/auth.ts]

3. **RESOLVED: How should feeds-service distinguish trusted BFF context from a
   forged header?** Use service-to-service OIDC for caller identity and an
   explicit trusted BFF service-account allowlist for accepting actor headers.
   The header is actor context, not authentication. [VERIFIED:
   backend/pipeline/common/auth.py; VERIFIED: frontend/api/src/utils.ts;
   VERIFIED: 03-CONTEXT.md]

4. **RESOLVED: Does Phase 3 include runtime cleanup of `quarantine_reason`
   writes?** No. Runtime-owned write cleanup belongs to Phase 4 unless the
   phase is expanded. Phase 3 only changes public service/BFF/frontend
   contracts and current feed projections. [VERIFIED: 03-CONTEXT.md]

## Verification Strategy

Use narrow local checks only:

```bash
safe-run -- uv run python -m pytest backend/services/feeds/tests/test_api.py backend/services/feeds/tests/test_service.py -q
safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestFeedQueryProjection backend/pipeline/storage/tests/test_feed_store.py::TestFeedAuditStorageBoundary -q
safe-run -- yarn --cwd frontend/api test --run src/feeds/feedsController.test.ts
safe-run -- yarn --cwd frontend/api typecheck
safe-run -- yarn --cwd frontend/api verify-spec
safe-run -- yarn --cwd frontend/transcription-ui test --run src/components/common/FeedStatusIndicator.test.tsx src/components/feeds/FeedSearchView.test.tsx
safe-run -- yarn --cwd frontend/transcription-ui typecheck
safe-run -- yarn --cwd frontend/common build
```

Avoid broad local API/component/E2E/Docker suites per repository instructions.
CI should cover broader build/test lanes. [VERIFIED: AGENTS.md; VERIFIED:
.agents/instructions.md]

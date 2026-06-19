---
phase: 03-service-and-compatibility-surface
verified: 2026-06-19T18:38:04Z
status: human_needed
score: "18/18 must-haves verified"
overrides_applied: 0
human_verification:
  - test: "Production trusted actor forwarding configuration"
    expected: "In GCP, TRUSTED_ACTOR_FORWARDING_SERVICE_ACCOUNTS includes the BFF service account and admin feed mutations persist audit actor_id as user:google:<sub>."
    why_human: "The repository proves fail-closed code paths and tests allowlisted claims, but the real deployed service account email and environment variable are external configuration."
  - test: "Real browser feed status detail display"
    expected: "Feed search, feed table, configuration table, and transcript header status tooltips display statusReasonDetail with no visible regression."
    why_human: "Component tests cover tooltip text, but visual hover behavior and user-flow feel require browser inspection."
---

# Phase 3: Service and Compatibility Surface Verification Report

**Phase Goal:** Preserve existing API/BFF/frontend flows while carrying trusted admin actor context and moving public diagnostic detail to `status_reason_detail`.
**Verified:** 2026-06-19T18:38:04Z
**Status:** human_needed
**Re-verification:** No - initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Admin-initiated feed mutations preserve the authenticated admin identity when it is available at the trusted service boundary. | VERIFIED | BFF derives `user:google:<sub>` from `request.user.sub` only after `isAdmin` passes in `frontend/api/src/feeds/feedsController.ts:172-188`; create/update/reset/deactivate/delete forward `X-WD-Actor-Id` at lines 320-328, 353-361, 384-392, 419-427, and 453-461. Feeds-service resolves that header and routes pass `actor_id` into service methods at `backend/services/feeds/main.py:101-118`, `134-136`, `182-184`, `297-340`. |
| 2 | Existing feed API callers continue receiving the current fields they depend on during the compatibility window. | VERIFIED | FastAPI `Feed` still exposes id/name/source/status/heartbeat/status reason/source feed/tags/speech timestamp at `backend/services/feeds/models.py:83-90`; BFF conversion preserves source URLs, archive URLs, status, substatus, heartbeat, tags, and status reason at `frontend/api/src/feeds/feedsController.ts:139-153`; OpenAPI `Feed` keeps the corresponding public schema at `frontend/api/openapi.yaml:305-340`. |
| 3 | Feed API responses expose `status_reason_detail` without breaking existing clients. | VERIFIED | Public storage projections select `status_reason_detail` for create/get/list/reset/update in `backend/pipeline/storage/feed_queries.py:397-405`, `418-423`, `500-505`, `526-531`, `573-592`, and `607-617`; `_row_to_feed` maps it at `backend/pipeline/storage/feed_store.py:187-198` and `290-298`; FastAPI exposes it at `backend/services/feeds/models.py:83-90`; BFF maps it to `statusReasonDetail` at `frontend/api/src/feeds/feedsController.ts:45-53` and `139-153`. |
| 4 | Existing BFF/frontend feed status and status-reason behavior remains compatible while diagnostic-detail display moves from `quarantine_reason` to `status_reason_detail`. | VERIFIED | Shared type exposes `statusReasonDetail` at `frontend/common/src/types/feeds.ts:44-55`; `FeedStatusIndicator` formats that prop into the existing tooltip path at `frontend/transcription-ui/src/components/common/FeedStatusIndicator.tsx:50-81`; existing search/table/config/header call sites pass the renamed prop at `FeedSearchView.tsx:117-123`, `FeedTable.tsx:319-324`, `FeedConfigurationTable.tsx:288-293`, and `FeedHeader.tsx:89-94`. |
| 5 | Actor attribution cannot be forged through untrusted request body fields. | VERIFIED | BFF actor derivation ignores mutation bodies and uses only authenticated request user state at `frontend/api/src/feeds/feedsController.ts:172-188`; backend request models have no `actor_id` field at `backend/services/feeds/models.py:64-90`; feeds-service resolves actor context from trusted caller claims plus `X-WD-Actor-Id`, not body fields, at `backend/services/feeds/main.py:86-118`; API tests assert actor fields are absent from create/update request models and responses at `backend/services/feeds/tests/test_api.py:138-169` and `785-815`. |
| 6 | Public FastAPI feed responses expose `status_reason_detail` as the canonical diagnostic detail field. | VERIFIED | `Feed.status_reason_detail` is present in the response model at `backend/services/feeds/models.py:83-90`; API test asserts it and rejects `quarantine_reason` as a model field at `backend/services/feeds/tests/test_api.py:81-87`. |
| 7 | Public FastAPI feed responses no longer expose `quarantine_reason` after this compatibility migration. | VERIFIED | FastAPI `Feed` has no `quarantine_reason` field at `backend/services/feeds/models.py:83-90`; API response test asserts `status_reason_detail` is returned and `quarantine_reason` is absent at `backend/services/feeds/tests/test_api.py:428-448`. |
| 8 | Storage public feed projections return `status_reason_detail` consistently for create, get, list, update, and reset paths. | VERIFIED | All full-feed projection SQL constants include `status_reason_detail` at `backend/pipeline/storage/feed_queries.py:397-405`, `418-423`, `500-505`, `526-531`, `573-592`, and `607-617`; `TestStatusReasonSqlProjection` enforces that coverage at `backend/pipeline/storage/tests/test_feed_store.py:414-428`. |
| 9 | Audit snapshots may continue to include `quarantine_reason` internally; this plan changes the public service contract, not historical audit snapshot content. | VERIFIED | The domain contract allows internal/audit snapshot migration overlap at `documentation/feed-audit-events.md:110-122` and `151-153`; audit snapshot SQL still selects `f.quarantine_reason` at `backend/pipeline/storage/feed_queries.py:438-449`; `_audit_snapshot` preserves it internally at `backend/pipeline/storage/feed_store.py:357-369`. |
| 10 | The TypeScript public feed contract uses `statusReasonDetail`, not `quarantineReason`. | VERIFIED | `Feed` declares `statusReasonDetail?: string` at `frontend/common/src/types/feeds.ts:44-55`; `rg -n "quarantineReason|quarantine_reason" frontend/common frontend/api/src/feeds frontend/api/openapi.yaml frontend/transcription-ui/src` returned no matches. |
| 11 | BFF conversion maps backend `status_reason_detail` to frontend `statusReasonDetail`. | VERIFIED | `FeedBackend` accepts `status_reason_detail` at `frontend/api/src/feeds/feedsController.ts:45-53`; `convertFeedBackend` returns `statusReasonDetail: response.status_reason_detail ?? undefined` at lines 139-153; BFF test asserts the mapping at `frontend/api/src/feeds/feedsController.test.ts:253-261`. |
| 12 | Generated OpenAPI exposes `statusReasonDetail` and no longer exposes `quarantineReason` on `Feed`. | VERIFIED | OpenAPI `Feed` schema includes `statusReasonDetail` at `frontend/api/openapi.yaml:305-340`; BFF test asserts presence of `statusReasonDetail` and absence of the legacy field at `frontend/api/src/feeds/feedsController.test.ts:265-281`; `safe-run -- yarn --cwd frontend/api verify-spec` passed with no generated diff. |
| 13 | Existing status tooltip/search/table/header behavior remains visually and functionally the same, with only the optional detail prop renamed. | VERIFIED | Tooltip formatter still joins substatus, status reason, and detail text in the same order at `FeedStatusIndicator.tsx:50-81`; tests assert tooltip text in the indicator, condensed search, and header at `FeedStatusIndicator.test.tsx:90-103`, `FeedSearchView.test.tsx:52-84`, and `FeedHeader.test.tsx:68-98`. Real visual inspection remains a human item. |
| 14 | Admin `actor_id` is derived only from authenticated BFF `request.user.sub` after `isAdmin` passes. | VERIFIED | `getAdminActorId` rejects non-admin requests, requires a string `sub`, rejects empty or whitespace-bearing values, and returns `user:google:<sub>` at `frontend/api/src/feeds/feedsController.ts:172-188`. |
| 15 | Missing, empty, or malformed Google `sub` fails before any downstream mutation call. | VERIFIED | BFF helper throws before service client work at `frontend/api/src/feeds/feedsController.ts:172-188`; tests cover missing, empty, whitespace-only, containing-whitespace, and leading-whitespace `sub` values and assert `mockRequest` is not called at `frontend/api/src/feeds/feedsController.test.ts:355-381`. |
| 16 | Feeds-service accepts forwarded actor context only from trusted caller identity and never from request bodies. | VERIFIED | In GCP mode `_caller_can_forward_actor` requires a non-empty allowlist and matching caller email at `backend/services/feeds/main.py:86-98`; `_resolve_admin_actor_id` also requires a well-formed `X-WD-Actor-Id` at lines 101-118. API tests cover missing/malformed actor headers and untrusted/trusted GCP callers at `backend/services/feeds/tests/test_api.py:251-360`. |
| 17 | `FeedService` admin mutation methods require explicit `actor_id` and do not silently fall back to `service:feeds-service` for admin routes. | VERIFIED | `FeedService` create/update/deactivate/delete/reset signatures require keyword-only `actor_id` and pass it through to storage at `backend/services/feeds/service.py:27-37`, `40-60`, `104-113`, `126-135`, and `148-160`; signature tests enforce required keyword-only actor IDs at `backend/services/feeds/tests/test_service.py:37-56`; negative grep over `backend/services/feeds frontend/api/src/feeds` found no `service:feeds-service`, `user:null`, `user: `, or `system:` matches. |
| 18 | Storage remains the only layer constructing `feed_audit_events` rows. | VERIFIED | Storage owns `_insert_feed_audit_event` and `INSERT_FEED_AUDIT_EVENT_SQL` execution at `backend/pipeline/storage/feed_store.py:395-433`; service/route hardening tests assert no `feed_audit_events`, `INSERT_FEED_AUDIT_EVENT_SQL`, or `_insert_feed_audit_event` references in feeds-service code at `backend/pipeline/storage/tests/test_feed_store.py:583-593`. |

**Score:** 18/18 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `backend/pipeline/storage/feed_queries.py` | Public feed projection SQL includes `status_reason_detail` | VERIFIED | Exists, substantive, and all full-feed projections include `status_reason_detail`. GSD artifact check passed for Plan 03-01. |
| `backend/pipeline/storage/feed_store.py` | Feed TypedDict and row mapper include `status_reason_detail` | VERIFIED | `Feed` TypedDict and `_row_to_feed` map `status_reason_detail`; audit helper remains storage-owned. GSD artifact check passed for Plan 03-01. |
| `backend/services/feeds/models.py` | FastAPI response model exposes `status_reason_detail` and omits `quarantine_reason` | VERIFIED | Response model has `status_reason_detail` and no public `quarantine_reason`. GSD artifact check passed for Plan 03-01. |
| `backend/services/feeds/tests/test_api.py` | Service response and actor-boundary compatibility tests | VERIFIED | Tests cover canonical detail, absent public legacy field, actor keyword propagation, missing/malformed actor rejection, and trusted/untrusted GCP callers. |
| `backend/services/feeds/tests/test_service.py` | Service model and explicit actor propagation tests | VERIFIED | Tests enforce required keyword-only `actor_id` and exact storage actor forwarding. |
| `frontend/common/src/types/feeds.ts` | Shared `Feed` type with `statusReasonDetail` | VERIFIED | Shared type declares `statusReasonDetail` and is re-exported from `frontend/common/src/index.ts:1-7`. GSD artifact check passed for Plan 03-02. |
| `frontend/api/src/feeds/feedsController.ts` | BFF diagnostic mapping and admin actor forwarding | VERIFIED | Maps backend diagnostic detail and forwards actor headers on mutation calls only. GSD artifact checks passed for Plans 03-02 and 03-03. |
| `frontend/api/openapi.yaml` | Generated public API schema | VERIFIED | `Feed` schema includes `statusReasonDetail`; `verify-spec` passed with no diff. GSD artifact check passed for Plan 03-02. |
| `frontend/transcription-ui/src/components/common/FeedStatusIndicator.tsx` | Existing tooltip behavior under canonical prop name | VERIFIED | Formats `statusReasonDetail` in existing tooltip path. GSD artifact check passed for Plan 03-02. |
| `backend/services/feeds/main.py` | Trusted caller validation and route-level actor resolution | VERIFIED | Validates caller trust and actor header, then passes actor to service methods. GSD artifact check passed for Plan 03-03. |
| `backend/services/feeds/service.py` | Explicit actor_id service method signatures | VERIFIED | Admin mutations require explicit actor_id and pass it to storage. GSD artifact check passed for Plan 03-03. |
| `frontend/api/src/feeds/feedsController.test.ts` | BFF missing-sub and actor header forwarding tests | VERIFIED | Tests cover canonical detail mapping, OpenAPI field naming, actor header forwarding, and missing/malformed admin sub rejection. GSD artifact check passed for Plan 03-03. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `backend/pipeline/storage/feed_queries.py` | `backend/pipeline/storage/feed_store.py` | `_row_to_feed` consumes projected columns | WIRED | GSD key-link check for Plan 03-01 passed; SQL projects `status_reason_detail` and `_row_to_feed` reads it. |
| `backend/services/feeds/models.py` | `frontend/api/src/feeds/feedsController.ts` | downstream BFF response shape | WIRED | GSD key-link check for Plan 03-01 passed; backend and BFF agree on `status_reason_detail`. |
| `backend/services/feeds/models.py` | `frontend/api/src/feeds/feedsController.ts` | `FeedBackend.status_reason_detail` | WIRED | GSD key-link check for Plan 03-02 passed. |
| `frontend/api/src/feeds/feedsController.ts` | `frontend/common/src/types/feeds.ts` | `convertFeedBackend` returns `statusReasonDetail` | WIRED | GSD key-link check for Plan 03-02 passed. |
| `frontend/common/src/types/feeds.ts` | `frontend/transcription-ui/src/components/common/FeedStatusIndicator.tsx` | `Feed.statusReasonDetail` prop is displayed | WIRED | GSD key-link check for Plan 03-02 passed; call sites pass the shared type property into the indicator. |
| `frontend/api/src/authentication.ts` | `frontend/api/src/feeds/feedsController.ts` | `AuthenticatedRequest.user.sub` | WIRED | GSD key-link check for Plan 03-03 passed; BFF uses `request.user.sub`. |
| `frontend/api/src/feeds/feedsController.ts` | `backend/services/feeds/main.py` | `X-WD-Actor-Id` internal header | WIRED | GSD key-link check for Plan 03-03 passed; source sends and target reads the same header. |
| `backend/services/feeds/main.py` | `backend/services/feeds/service.py` | route passes `actor_id` keyword | WIRED | GSD key-link check for Plan 03-03 passed. |
| `backend/services/feeds/service.py` | `backend/pipeline/storage/feed_store.py` | store mutation `actor_id` keyword | WIRED | GSD key-link check for Plan 03-03 passed; storage remains the audit writer. |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|--------------------|--------|
| Storage and FastAPI feed response | `status_reason_detail` | AlloyDB `feeds.status_reason_detail` selected by public feed SQL, mapped by `_row_to_feed`, validated by FastAPI `Feed` | Yes | FLOWING - selected in all full-feed projections and exposed through the FastAPI response model. |
| BFF and shared frontend feed contract | `statusReasonDetail` | Backend `status_reason_detail` in `FeedBackend` converted by `convertFeedBackend` | Yes | FLOWING - BFF maps the backend value to the shared `Feed.statusReasonDetail` property. |
| React status tooltip surfaces | `statusReasonDetail` prop | `listFeeds` / `getFeed` query data consumed by search/table/header components | Yes | FLOWING - `FeedSearchView` fetches feeds at `FeedSearchView.tsx:236-261`; `TranscriptView` fetches list/get feed data at `TranscriptView.tsx:174-194`; call sites pass detail into `FeedStatusIndicator`. |
| Admin actor attribution | `actor_id` | Authenticated BFF `request.user.sub` -> `X-WD-Actor-Id` -> feeds-service trusted route helper -> `FeedService` -> `FeedStore` | Yes, in code and tests | FLOWING - code traces all layers, but deployed service-account allowlist remains human verification. |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Phase 3 backend tests | User-provided: `safe-run -- uv run python -m pytest ... -q` | 77 passed, 30 subtests | PASS |
| BFF feed controller tests | User-provided: `safe-run -- yarn --cwd frontend/api test --run src/feeds/feedsController.test.ts` | 61 passed | PASS |
| Frontend status/header tests | User-provided: `safe-run -- yarn --cwd frontend/transcription-ui test --run ...` | 19 passed | PASS |
| Python touched files compile | `safe-run -- uv run python -m py_compile backend/pipeline/storage/feed_queries.py backend/pipeline/storage/feed_store.py backend/services/feeds/main.py backend/services/feeds/service.py` | Exit 0 | PASS |
| Generated OpenAPI has no drift | `safe-run -- yarn --cwd frontend/api verify-spec` | Generated spec and `git diff --exit-code openapi.yaml` passed | PASS |
| Frontend API typecheck | `safe-run -- yarn --cwd frontend/api typecheck` | `tsc --noEmit` passed | PASS |
| Frontend UI typecheck | `safe-run -- yarn --cwd frontend/transcription-ui typecheck` | `tsc --noEmit` passed | PASS |
| Legacy public diagnostic names removed | `rg -n "quarantineReason|quarantine_reason" frontend/common frontend/api/src/feeds frontend/api/openapi.yaml frontend/transcription-ui/src` | No matches | PASS |
| Banned actor placeholders absent from service/BFF source | `rg -n "service:feeds-service|user:null|user: |system:" backend/services/feeds frontend/api/src/feeds` | No matches | PASS |
| Whitespace diff check | `git diff --check` | Exit 0 | PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-------------|-------------|--------|----------|
| DIAG-04 | 03-01, 03-02 | Existing feed API/BFF/frontend flows continue working while consumers migrate from `quarantine_reason` to `status_reason_detail`. | SATISFIED | Backend, BFF, shared types, OpenAPI, and UI call sites expose/use `status_reason_detail` / `statusReasonDetail`; negative grep found no legacy public references in the specified source paths. |
| ACT-02 | 03-03 | Admin-initiated feed mutations preserve authenticated admin identity when available at the trusted boundary. | SATISFIED | BFF derives `user:google:<sub>`, forwards `X-WD-Actor-Id`, feeds-service validates trusted caller context, and service passes `actor_id` to storage. |
| COMP-01 | 03-01, 03-02, 03-03 | Existing feed API callers continue receiving current fields during the compatibility window. | SATISFIED | Feed response schema/model and BFF conversion preserve existing status, status reason, source, heartbeat, tags, URL, and list/get behavior while changing only the diagnostic detail field name. |
| COMP-02 | 03-01, 03-02 | Feed API responses expose the new canonical diagnostic detail without breaking existing clients. | SATISFIED | Storage projections, FastAPI model, BFF conversion, shared type, and OpenAPI all expose canonical detail; tests verify field mapping and legacy field absence. |
| COMP-03 | 03-02, 03-03 | Existing frontend/BFF feed status and status-reason behavior remains compatible with the v1 backend change. | SATISFIED | Existing status indicator, condensed search, feed table, configuration table, and transcript header use the same tooltip path with the renamed detail prop; component tests verify tooltip text. |

No Phase 3 requirement IDs are orphaned. `.planning/REQUIREMENTS.md` maps exactly DIAG-04, ACT-02, COMP-01, COMP-02, and COMP-03 to Phase 3, and all five appear in Phase 3 plan frontmatter.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `backend/pipeline/storage/feed_queries.py` | 298 | Existing TODO for recovery-path performance index | INFO | Runtime recovery performance follow-up, unrelated to Phase 3 service/BFF/frontend compatibility. |
| `backend/pipeline/storage/feed_queries.py` | 558 | Existing hard-delete cleanup TODO | INFO | Legacy cleanup note; current delete/audit behavior remains storage-owned and tested. |
| `frontend/transcription-ui/src/components/feeds/FeedSearchView.tsx` | 270, 280 | Existing TODOs for backend-computed tags | INFO | Existing filter data-source workaround, not a `statusReasonDetail` or actor propagation stub. |
| `frontend/transcription-ui/src/components/feeds/FeedConfigurationTable.tsx` | 154 | Existing pagination TODO | INFO | Existing frontend pagination limitation, unrelated to Phase 3 must-haves. |

No blocker anti-patterns found. `return null`, `=[]`, `={}`, and `= null` scan matches were reviewed as legitimate React guard states, test fixtures, reset SQL, or normal initialization, not stubs.

### Human Verification Required

### 1. Production Trusted Actor Forwarding Configuration

**Test:** In a deployed GCP environment, verify `TRUSTED_ACTOR_FORWARDING_SERVICE_ACCOUNTS` includes the BFF service account email, then perform representative admin create/update/reset/deactivate/delete feed actions.
**Expected:** Mutations succeed through the BFF and resulting audit rows use `actor_id = user:google:<authenticated admin sub>`.
**Why human:** The repo proves fail-closed code behavior and mocked trusted/untrusted claim handling, but actual deployed service-account claims and environment configuration are outside the codebase.

### 2. Real Browser Feed Status Detail Display

**Test:** In the browser, inspect feed search, feed table, configuration table, and transcript header status tooltips for a feed with `statusReasonDetail`.
**Expected:** Existing status UI still works and tooltip text displays the canonical detail, for example `Quarantined (System Unexpected Error): unsupported audio format`.
**Why human:** Component tests verify text generation, but visual hover behavior and user-flow feel require browser inspection.

### Gaps Summary

No codebase gaps found. All roadmap success criteria and plan frontmatter must-haves are implemented, substantive, wired, and covered by focused tests or static checks. Status is `human_needed` only because deployed GCP trust configuration and real-browser UI behavior cannot be fully verified from repository evidence.

---

_Verified: 2026-06-19T18:38:04Z_
_Verifier: the agent (gsd-verifier)_

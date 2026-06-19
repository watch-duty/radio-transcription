# Phase 3: Service and Compatibility Surface - Pattern Map

**Mapped:** 2026-06-19
**Files analyzed:** 16 implementation and test targets
**Analogs found:** 16 / 16

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `backend/pipeline/storage/feed_store.py` | storage model mapper | SQL row -> domain dict | Existing `_row_to_feed`, `_audit_snapshot` | exact role |
| `backend/pipeline/storage/feed_queries.py` | query utility | feed projections | Existing `RESET_FEED_SQL` return shape and list/get projections | exact role |
| `backend/pipeline/storage/tests/test_feed_store.py` | storage tests | text-level SQL/model contracts | Existing `TestFeedQueryProjection`, `TestFeedAuditStorageBoundary` | exact role |
| `backend/services/feeds/models.py` | FastAPI contract | Pydantic response model | Existing `Feed` model | exact role |
| `backend/services/feeds/service.py` | service layer | route -> store delegation | Existing Phase 2 actor tests | exact role |
| `backend/services/feeds/main.py` | HTTP route layer | request -> service call | Existing route error handling and dependency usage | exact role |
| `backend/services/feeds/tests/test_api.py` | API tests | TestClient + mock service | Existing create/update contract tests | exact role |
| `backend/services/feeds/tests/test_service.py` | service tests | AsyncMock store assertions | Existing Phase 2 actor propagation tests | exact role |
| `frontend/common/src/types/feeds.ts` | shared TS contract | BFF/UI feed type | Existing `Feed` interface | exact role |
| `frontend/api/src/feeds/feedsController.ts` | BFF controller | backend response -> frontend response; admin proxy | Existing conversion helpers and admin checks | exact role |
| `frontend/api/src/feeds/feedsController.test.ts` | BFF tests | mocked service client assertions | Existing create/update/reset/deactivate/delete tests | exact role |
| `frontend/api/openapi.yaml` | generated contract | tsoa generated schema | Existing generated `Feed` schema | generated artifact |
| `frontend/transcription-ui/src/components/common/FeedStatusIndicator.tsx` | UI component | feed status/detail display | Existing tooltip formatter | exact role |
| `frontend/transcription-ui/src/components/common/FeedStatusIndicator.test.tsx` | UI component tests | tooltip assertions | Existing quarantineReason tooltip test | exact role |
| `frontend/transcription-ui/src/components/feeds/*.tsx` | UI call sites | feed props -> status indicator | Existing status indicator usage | exact role |
| `frontend/transcription-ui/src/components/transcripts/FeedHeader.tsx` | UI call site | selected feed -> status indicator | Existing header status display | exact role |

## Pattern Assignments

### Backend storage projections

Use `backend/pipeline/storage/feed_queries.py` as the single source of feed
projection fields. Public return queries already select the fields consumed by
`_row_to_feed`; Phase 3 should add `status_reason_detail` alongside
`quarantine_reason` in:

- `CREATE_FEED_SQL`
- `GET_FEED_SQL`
- `LIST_FEEDS_DESC_SQL`
- `LIST_FEEDS_ASC_SQL`
- `UPDATE_FEED_SQL`

`RESET_FEED_SQL` already returns `status_reason_detail`, so the main storage
mapper change is to add `status_reason_detail` to the `Feed` `TypedDict` and
`_row_to_feed` return value. Keep `_audit_snapshot` unchanged except for
normal test adjustments; audit snapshots still include `quarantine_reason` as
an internal/storage historical value.

### FastAPI route/service patterns

`backend/services/feeds/main.py` converts service errors to `HTTPException`
at the route boundary. Preserve that pattern when adding trusted actor
resolution:

- route helper returns an `actor_id` string or raises `HTTPException`
- create/update/deactivate/delete/reset routes pass `actor_id` to
  `FeedService`
- list/get routes remain read-only and do not require actor context

The app currently installs `Depends(verify_oidc_token)` globally. To inspect
caller claims for trusted actor forwarding, add the same dependency as a
route parameter on mutation routes. This follows the existing auth dependency
and avoids parsing authorization headers manually in routes.

`backend/services/feeds/service.py` is a thin delegator. Keep it thin:
admin mutation methods should accept `actor_id` and pass it to the storage
method. Do not build audit rows or inspect request headers in the service
class.

### BFF controller patterns

`frontend/api/src/feeds/feedsController.ts` already has these helpers:

- `convertFeedBackend`
- `convertFeedCreate`
- `convertFeedUpdate`

Follow the existing shape by adding a small helper near the conversion helpers
for admin actor derivation and header construction. The helper should use the
already-injected `AuthenticatedRequest`, not request body fields.

Use Axios/Gaxios request config `headers` on only admin mutation calls:

- `POST /v1/feeds`
- `PUT /v1/feeds/{feedId}`
- `POST /v1/feeds/{feedId}/reset`
- `POST /v1/feeds/{feedId}/deactivate`
- `DELETE /v1/feeds/{feedId}`

Do not add actor headers to list/get calls.

### Frontend UI patterns

`FeedStatusIndicator` already owns tooltip formatting for substatus,
`statusReason`, and optional detail text. Rename the optional detail prop to
`statusReasonDetail` and keep the same formatter behavior. Call sites should
replace:

```tsx
quarantineReason={feed.quarantineReason}
```

with:

```tsx
statusReasonDetail={feed.statusReasonDetail}
```

No new UI section, copy, state, or layout is needed.

### Generated API contract pattern

`frontend/api/package.json` defines:

```json
"generate-spec": "yarn run tsoa spec --yaml && node scripts/post-process-spec.js",
"verify-spec": "yarn generate-spec && git diff --exit-code openapi.yaml"
```

After source type/controller updates, run `safe-run -- yarn --cwd frontend/api
verify-spec`. If it updates `frontend/api/openapi.yaml`, include that generated
file in the execution summary and commit.

## Verification Patterns

| Area | Existing Local Check | Phase 3 Target |
|------|----------------------|----------------|
| FastAPI routes | `backend/services/feeds/tests/test_api.py` | actor route helper, `status_reason_detail` response, no body actor field |
| Feed service | `backend/services/feeds/tests/test_service.py` | explicit actor ID from route to storage |
| Storage query contract | `backend/pipeline/storage/tests/test_feed_store.py` | public projection includes `status_reason_detail`; service does not build audit rows |
| BFF controller | `frontend/api/src/feeds/feedsController.test.ts` | actor header forwarding, missing sub failure, detail mapping |
| Generated OpenAPI | `frontend/api/openapi.yaml` | `statusReasonDetail` present and `quarantineReason` absent |
| UI tooltip | `FeedStatusIndicator.test.tsx`, `FeedSearchView.test.tsx` | status detail tooltip remains the same behavior under new prop |

## Anti-Patterns To Avoid

- Do not accept `actor_id` from feed request bodies.
- Do not leave `FeedService` with a silent `service:feeds-service` fallback for
  admin mutation routes.
- Do not trust `X-WD-Actor-Id` unless the caller service identity is trusted.
- Do not add `quarantine_reason` as a public alias in service/BFF/frontend
  models after migrating to `status_reason_detail`.
- Do not redesign feed status UI or add a new timeline/admin screen in this
  phase.
- Do not add runtime failure/quarantine/recovery writers in this phase.

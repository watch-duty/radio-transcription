# Testing Patterns

**Analysis Date:** 2026-06-26

## Test Framework

**Runner:**
- Python uses `pytest` from `pyproject.toml` dev dependencies with `pytest-asyncio`, `pytest-cov`, and `pytest-xdist`; root config lives in `pyproject.toml` `[tool.pytest.ini_options]`.
- Python unit tests often use standard-library `unittest.TestCase` and `unittest.IsolatedAsyncioTestCase` while still running under pytest, as in `backend/services/feeds/tests/test_service.py`, `backend/pipeline/ingestion/tests/test_router.py`, and `model/tests/gemini_sft/test_config.py`.
- TypeScript API tests use Vitest from `frontend/api/package.json`; no package-specific Vitest config file is present for `frontend/api`.
- React UI tests use Vitest plus Testing Library from `frontend/transcription-ui/package.json`; config lives in `frontend/transcription-ui/vitest.config.js` and setup lives in `frontend/transcription-ui/src/test/setup.ts`.
- Model package tests use package-local pytest config in `model/pyproject.toml` with `testpaths = ["tests"]`.

**Assertion Library:**
- Python `unittest` assertions are common in class-based tests, as in `backend/services/feeds/tests/test_api.py`, `backend/pipeline/storage/tests/test_feed_store.py`, and `model/tests/common/tests/test_manifest.py`.
- Python `pytest` assertions, `pytest.raises`, `pytest.mark.parametrize`, `caplog`, and async fixtures are used in pytest-style tests, as in `backend/pipeline/common/tests/test_actor_identity.py`, `backend/pipeline/segmentation/tests/test_orchestration.py`, and `integration_tests/storage/test_feed_store_integration.py`.
- TypeScript uses Vitest `expect`, `vi`, `describe`, `it`, and Testing Library queries/events, as in `frontend/api/src/feeds/feedsController.test.ts`, `frontend/transcription-ui/src/components/transcripts/TranscriptRow.test.tsx`, and `frontend/transcription-ui/src/hooks/useAudioSegments.test.tsx`.

**Run Commands:**
```bash
safe-run -- mise run test              # Python backend unit tests via .mise.toml test:unit
safe-run -- mise run test:model        # Model package tests from model/pyproject.toml
safe-run -- mise run test:component    # Docker/testcontainers storage component tests
safe-run -- mise run test:api          # HTTP API integration tests under integration_tests/api
safe-run -- mise run test:e2e          # Docker Compose end-to-end pipeline tests
yarn --cwd frontend/api test           # Vitest API proxy tests
yarn --cwd frontend/transcription-ui test # Vitest React UI tests
```

## Test File Organization

**Location:**
- Put backend package unit tests under a local `tests/` directory next to the package, as in `backend/services/feeds/tests/test_api.py`, `backend/pipeline/storage/tests/test_feed_store.py`, and `backend/pipeline/ingestion/tests/test_router.py`.
- Keep notification function tests at the package root where that package already does so, as in `backend/pipeline/notification/test_send_notification.py`, `backend/pipeline/notification/test_request_handler.py`, and `backend/pipeline/notification/test_notification_deduplication.py`.
- Put model package tests under `model/tests/`, with subtrees matching packages such as `model/tests/gemini_sft/test_config.py` and `model/tests/common/tests/test_manifest.py`.
- Put integration tests under `integration_tests/`, grouped by scope: `integration_tests/storage/`, `integration_tests/api/`, and `integration_tests/e2e/`.
- Co-locate TypeScript tests beside the module under test, as in `frontend/api/src/authentication.test.ts`, `frontend/api/src/feeds/feedsController.test.ts`, `frontend/transcription-ui/src/service/listFeeds.test.ts`, and `frontend/transcription-ui/src/components/common/RequireAdmin.test.tsx`.

**Naming:**
- Use `test_*.py` for Python files, as in `backend/pipeline/ingestion/tests/test_slo_contract_lint.py` and `integration_tests/e2e/test_transcription_pipeline.py`.
- Use `.test.ts` and `.test.tsx` for TypeScript files, as in `frontend/api/src/config.test.ts`, `frontend/transcription-ui/src/hooks/useAudioSegments.test.tsx`, and `frontend/transcription-ui/src/audio/WebAudioPlayer.test.ts`.
- Name helper files without `test_` when they are imported by tests but not collected as tests, as in `backend/pipeline/storage/tests/connection_util.py`, `integration_tests/feed_utils.py`, and `integration_tests/utils.py`.

**Structure:**
```text
backend/services/<service>/tests/test_*.py
backend/pipeline/<package>/tests/test_*.py
backend/pipeline/notification/test_*.py
integration_tests/{storage,api,e2e}/test_*.py
model/tests/**/test_*.py
frontend/api/src/**/*.test.ts
frontend/transcription-ui/src/**/*.test.{ts,tsx}
```

## Test Structure

**Suite Organization:**
```python
class TestFeedServiceAuditActor(unittest.IsolatedAsyncioTestCase):
    async def test_update_feed_passes_admin_actor_to_store(self) -> None:
        store = mock.AsyncMock()
        service = FeedService(store)
        result = await service.update_feed("...", feed_in, actor_id=actor_id)
        assert result is not None
        store.update_feed.assert_awaited_once_with(...)
```

```typescript
describe('FeedsController', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should return converted feeds on success', async () => {
    mockRequest.mockResolvedValueOnce({ data: [mockBackendFeed] });
    const result = await new FeedsController().listFeeds();
    expect(result).toEqual([expectedFrontendFeed]);
  });
});
```

**Patterns:**
- Use class-based `unittest` suites when checking method signatures, service behavior, or async methods with `IsolatedAsyncioTestCase`, as in `backend/services/feeds/tests/test_service.py` and `backend/pipeline/common/clients/tests/test_pubsub_client.py`.
- Use pytest function tests for lightweight contracts, parametrization, and fixtures, as in `backend/pipeline/common/tests/test_actor_identity.py`, `backend/pipeline/segmentation/tests/test_orchestration.py`, and `integration_tests/storage/test_feed_store_integration.py`.
- Use `setUp` and `tearDown` to manage FastAPI dependency overrides and reusable clients in unittest suites, as in `backend/services/feeds/tests/test_api.py`.
- Use `beforeEach` and `afterEach` for Vitest mocks and DOM cleanup, as in `frontend/api/src/feeds/feedsController.test.ts`, `frontend/transcription-ui/src/components/common/RequireAdmin.test.tsx`, and `frontend/transcription-ui/src/components/transcripts/TranscriptRow.test.tsx`.
- Keep tests contract-oriented: verify public response shapes, audit actor propagation, schema/golden stability, and route/query conversion in `backend/services/feeds/tests/test_api.py`, `backend/pipeline/storage/tests/test_feed_store.py`, `backend/pipeline/ingestion/tests/test_slo_contract_lint.py`, and `frontend/api/src/feeds/feedsController.test.ts`.

## Mocking

**Framework:** Python `unittest.mock`, pytest `monkeypatch`, Vitest `vi`

**Patterns:**
```python
store = mock.AsyncMock()
store.update_feed.return_value = _store_feed(name="Updated Feed")
service = FeedService(store)
result = await service.update_feed(str(_FEED_ID), feed_in, actor_id=actor_id)
store.update_feed.assert_awaited_once_with(feed_id=_FEED_ID, ...)
```

```typescript
vi.mock('google-auth-library', () => {
  class MockGoogleAuth {
    getIdTokenClient = vi.fn().mockResolvedValue({ request: mockRequest });
  }
  return { GoogleAuth: MockGoogleAuth };
});
```

**What to Mock:**
- Mock external cloud SDK clients and network clients in unit tests, as in `backend/pipeline/common/clients/tests/test_pubsub_client.py`, `backend/pipeline/notification/test_send_notification.py`, and `frontend/api/src/feeds/feedsController.test.ts`.
- Mock async storage pools with reusable helpers instead of connecting to a database in unit tests; use `backend/pipeline/storage/tests/connection_util.py` `make_mock_pool`.
- Mock FastAPI auth dependencies through `app.dependency_overrides`, as in `backend/services/feeds/tests/test_api.py`.
- Mock environment variables with `mock.patch.dict` or `vi.hoisted` before import-time config runs, as in `backend/pipeline/notification/test_send_notification.py` and `frontend/api/src/config.test.ts`.
- Mock browser and DOM APIs that JSDOM lacks or cannot safely exercise, as in `frontend/transcription-ui/src/test/setup.ts` stubbing media methods and `frontend/transcription-ui/src/components/transcripts/TranscriptRow.test.tsx` stubbing `navigator.clipboard`.
- Mock child UI components when the parent contract is prop wiring, as in `frontend/transcription-ui/src/components/transcripts/TranscriptRow.test.tsx` mocking `TranscriptPlayControl`.

**What NOT to Mock:**
- Do not mock pure conversion and validation helpers when the contract depends on them; tests exercise real conversion in `frontend/api/src/feeds/feedsController.test.ts`, `model/tests/common/tests/test_manifest.py`, and `backend/services/feeds/tests/test_api.py`.
- Do not mock database behavior in component storage tests; use Docker-backed AlloyDB Omni via `integration_tests/conftest.py` and tests such as `integration_tests/storage/test_feed_store_integration.py`.
- Do not mock full service graphs in E2E tests; publish Pub/Sub messages and verify API-visible results through `integration_tests/e2e/test_transcription_pipeline.py` and `integration_tests/test_utils.py`.

## Fixtures and Factories

**Test Data:**
```python
def _store_feed(**overrides: object) -> dict[str, object]:
    feed: dict[str, object] = {
        "id": _FEED_ID,
        "name": "Test Feed",
        "source_type": SourceType.BCFY_FEEDS,
        "status": FeedStatus.UNCLAIMED,
    }
    feed.update(overrides)
    return feed
```

```typescript
const mockAudioSegment: AudioSegment = {
  id: 'tx-123',
  feedId: 'feed-123',
  classification: AudioClassification.SPEECH,
  annotations: [{ type: AnnotationType.TRANSCRIPT, data: { text: '...', errors: [] } }],
};
```

**Location:**
- Keep Python helper factories near the tests that own them, as in `backend/services/feeds/tests/test_service.py` `_store_feed`, `backend/pipeline/storage/tests/test_feed_store.py` `_full_feed_row`, and `backend/pipeline/ingestion/tests/test_chunk_ingested.py` `_make_chunk`.
- Put reusable pytest fixtures in local `conftest.py` files, as in `integration_tests/conftest.py`, `backend/pipeline/segmentation/tests/conftest.py`, and `backend/pipeline/ingestion/collectors/tests/conftest.py`.
- Store golden files under package-local `golden/` directories when asserting serialized output, as in `backend/pipeline/ingestion/tests/golden/chunk_ingested.json` and related files.
- Store media fixtures under package-local `test_data/` directories, as in `backend/pipeline/segmentation/tests/test_data/test_bcfy.flac`.
- Put shared React render wrappers in `frontend/transcription-ui/src/test/testUtils.tsx`; use `renderWithQueryClient` for React Query and `renderWithRouter` for route-dependent components.

## Coverage

**Requirements:** No explicit coverage percentage is enforced in `pyproject.toml`, `.mise.toml`, `frontend/api/package.json`, or `frontend/transcription-ui/package.json`.

**View Coverage:**
```bash
safe-run -- uv run pytest backend/ --cov=backend       # Ad hoc Python coverage; pytest-cov is in pyproject.toml
yarn --cwd frontend/api test --coverage                # Ad hoc Vitest coverage; coverage-v8 is installed
yarn --cwd frontend/transcription-ui test --coverage   # Ad hoc UI coverage; coverage-v8 is installed
```

**Configuration:**
- Python coverage omits tests and fixtures through `pyproject.toml` `[tool.coverage.run]` patterns `**/*test*`, `**/tests/**`, and `**/conftest.py`.
- Vitest coverage provider packages are present in `frontend/api/package.json` and `frontend/transcription-ui/package.json`, but no named `coverage` script is defined in either package.

## Test Types

**Unit Tests:**
- Backend unit tests under `backend/` run through `.mise.toml` `test:unit`, which executes `uv run python -m pytest backend/ -q --ignore-glob='**/test_*_integration.py'`.
- Model unit tests run through `.mise.toml` `test:model`, which executes package-local tests under `model/tests/` with `model/pyproject.toml` extras.
- API proxy and React UI unit tests run through `frontend/api/package.json` `test` and `frontend/transcription-ui/package.json` `test`.

**Integration Tests:**
- Storage component tests use pytest, asyncpg, and testcontainers under `integration_tests/storage/`, with shared container setup in `integration_tests/conftest.py`.
- API integration tests use `httpx.AsyncClient` against running backend/proxy services under `integration_tests/api/`, as in `integration_tests/api/test_feeds_api.py`.
- E2E tests use Docker Compose and Pub/Sub/API verification under `integration_tests/e2e/`, as in `integration_tests/e2e/test_transcription_pipeline.py`.

**E2E Tests:**
- Docker-managed E2E tests run through `.mise.toml` `test:e2e`, which stops existing compose services, builds `integration-tests`, and runs that container.
- Host-side E2E tests against a running environment run through `.mise.toml` `test:e2e:local`, which executes `uv run pytest integration_tests/e2e/`.

**Contract Tests:**
- Use tests to pin cross-file or generated contracts, as in `backend/pipeline/ingestion/tests/test_slo_contract_lint.py`, `backend/pipeline/segmentation/tests/test_orchestration.py`, `backend/pipeline/storage/tests/test_feed_query_contracts.py`, and `frontend/api/package.json` `verify-spec`.
- Use OpenAPI generation verification through `.pre-commit-config.yaml` `verify-openapi-spec` and `frontend/api/package.json` `verify-spec`.

## Common Patterns

**Async Testing:**
```python
class TestPubSubClient(unittest.IsolatedAsyncioTestCase):
    async def test_close_stops_initialized_publisher(self) -> None:
        client = PubSubClient()
        await client.close()
```

```python
@pytest.mark.asyncio
async def test_feeds_api_proxy(proxy_client: httpx.AsyncClient, test_bcfy_feed: tuple[str, str]) -> None:
    response = await proxy_client.get("/feeds", timeout=10.0)
    assert response.status_code == 200
```

```typescript
const { result } = renderHook(() => useAudioSegments(options), { wrapper });
await waitFor(() => {
  expect(result.current.isAudioSegmentsSuccess).toBe(true);
});
```

**Error Testing:**
```python
with self.assertRaisesRegex(RunConfigError, "validation_manifest_uri"):
    load_run_config(path)

with pytest.raises(ValueError, match="Google user email"):
    actor_id_from_google_email(email)
```

```typescript
await expect(controller.listFeeds({ tags: ['not-json'] })).rejects.toMatchObject({
  status: 400,
  message: 'Invalid tags query parameter',
});
```

**Log Testing:**
- Use `self.assertLogs` for logger-level records when checking emitted structured payloads, as in `backend/pipeline/ingestion/tests/test_chunk_ingested.py`.
- Use pytest `caplog` when checking log records and redaction, as in `backend/pipeline/common/tests/test_actor_identity.py`.

**Parallel Safety:**
- Keep integration tests parallel-safe because root pytest config in `pyproject.toml` sets `addopts = "-n auto"`; `integration_tests/conftest.py` documents unique IDs and `integration_tests/feed_utils.py` creates UUID-based feed names.
- Disable xdist only for shared Docker-backed storage suites through `.mise.toml` component tasks using `-n0`, as in `test:component`, `test:component:feeds`, and `test:component:audio_segments`.

---

*Testing analysis: 2026-06-26*

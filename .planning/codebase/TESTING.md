# Testing Patterns

**Analysis Date:** 2026-05-24

## Test Framework

**Runner:**
- Python backend unit tests use `unittest` classes and are also collected by `pytest` in CI. Config: `[tool.pytest.ini_options]` in `pyproject.toml`.
- Python model common-library tests use `pytest` from the `model/` workspace. Config: `model/pyproject.toml`.
- TypeScript API tests use Vitest. Scripts: `frontend/api/package.json`.
- React UI tests use Vitest plus React Testing Library. Config: `frontend/transcription-ui/vitest.config.js`; setup file: `frontend/transcription-ui/src/test/setup.ts`.

**Assertion Library:**
- Python backend tests primarily use `unittest.TestCase` assertions. Examples: `backend/pipeline/ingestion/tests/test_router.py`, `backend/pipeline/storage/tests/test_feed_store.py`, `backend/services/feeds/tests/test_api.py`.
- Python integration/component tests use `pytest` fixtures and `pytest.raises`. Examples: `integration_tests/conftest.py`, `integration_tests/storage/test_feed_store_integration.py`.
- TypeScript tests use Vitest `expect`. Examples: `frontend/api/src/feeds/feedsController.test.ts`, `frontend/transcription-ui/src/service/listTranscripts.test.ts`.
- React UI tests use `@testing-library/react`, `@testing-library/jest-dom/vitest`, and Vitest assertions. Examples: `frontend/transcription-ui/src/components/transcripts/TranscriptView.test.tsx`, `frontend/transcription-ui/src/test/setup.ts`.

**Run Commands:**
```bash
mise run test              # Alias for Python backend unit tests from `.mise.toml`
mise run test:unit         # Runs `uv run python -m unittest discover backend/pipeline`
uv run python -m pytest backend/ -q  # CI backend unit-test command from `.github/workflows/ci.yml`
yarn --cwd frontend/api test --watch=false  # Frontend API unit tests from `.github/workflows/ci.yml`
yarn --cwd frontend/transcription-ui test --watch=false  # React UI unit tests from `.github/workflows/ci.yml`
cd model && uv run --extra dev --extra scoring pytest colabs/common/tests/  # Model common tests from `.mise.toml`
```

## Test File Organization

**Location:**
- Backend Python tests are colocated under package-local `tests/` directories. Examples: `backend/pipeline/ingestion/tests/`, `backend/pipeline/storage/tests/`, `backend/pipeline/transcription/tests/`.
- Some small backend modules keep tests directly beside implementation files. Examples: `backend/pipeline/notification/test_request_handler.py`, `backend/pipeline/common/storage/test_redis_service.py`.
- Backend service API tests live under each service package. Examples: `backend/services/feeds/tests/test_api.py`, `backend/services/rules/tests/test_api.py`, `backend/services/transcripts/tests/test_api.py`.
- Frontend API tests are colocated with controllers. Examples: `frontend/api/src/auth/authController.test.ts`, `frontend/api/src/feeds/feedsController.test.ts`.
- React UI tests are colocated with components, services, and utilities. Examples: `frontend/transcription-ui/src/components/audio/AudioPlayer.test.tsx`, `frontend/transcription-ui/src/service/listFeeds.test.ts`, `frontend/transcription-ui/src/utils/timeUtils.test.ts`.
- Cross-service non-unit tests live in `integration_tests/`, with component tests in `integration_tests/storage/`, API tests in `integration_tests/api/`, and full-flow tests in `integration_tests/e2e/`.
- Model common tests live in `model/colabs/common/tests/` and are scoped by `model/pyproject.toml`.

**Naming:**
- Python: `test_*.py`. Examples: `backend/pipeline/common/tests/test_audio.py`, `integration_tests/e2e/test_transcription_pipeline.py`, `model/colabs/common/tests/test_scoring.py`.
- TypeScript/TSX: `*.test.ts` and `*.test.tsx`. Examples: `frontend/api/src/rules/rulesController.test.ts`, `frontend/transcription-ui/src/components/Login.test.tsx`.
- Shared test helpers should not use a test suffix unless they contain tests. Examples: `backend/pipeline/storage/tests/connection_util.py`, `frontend/transcription-ui/src/test/testUtils.tsx`, `integration_tests/test_utils.py`.

**Structure:**
```text
backend/pipeline/<area>/tests/test_<module>.py
backend/services/<service>/tests/test_api.py
frontend/api/src/<feature>/<feature>Controller.test.ts
frontend/transcription-ui/src/components/<area>/<Component>.test.tsx
frontend/transcription-ui/src/service/<serviceFunction>.test.ts
integration_tests/{storage,api,e2e}/test_*.py
model/colabs/common/tests/test_*.py
```

## Test Structure

**Suite Organization:**
```python
class TestRouteCapturerRegistered(unittest.TestCase):
    def test_each_registered_source_type_routes_correctly(self) -> None:
        for source_type, (capture_fn, url_base) in _COLLECTORS.items():
            with self.subTest(source_type=source_type.value):
                ...
```
Pattern source: `backend/pipeline/ingestion/tests/test_router.py`.

```python
class TestUpdateFeedProgress(unittest.IsolatedAsyncioTestCase):
    async def test_returns_true_when_lease_held(self) -> None:
        pool = make_mock_pool(execute_result="UPDATE 1")
        store = FeedStore(pool)
        result = await store.update_feed_progress(...)
        self.assertTrue(result)
```
Pattern source: `backend/pipeline/storage/tests/test_feed_store.py`.

```typescript
describe('FeedsController', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('listFeeds', () => {
    it('should return converted feeds on success', async () => {
      ...
    });
  });
});
```
Pattern source: `frontend/api/src/feeds/feedsController.test.ts`.

```tsx
renderTranscriptView(
  <MemoryRouter>
    <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />
  </MemoryRouter>
);
await waitFor(() => {
  expect(screen.getByText('Hello')).toBeTruthy();
});
```
Pattern source: `frontend/transcription-ui/src/components/transcripts/TranscriptView.test.tsx`.

**Patterns:**
- Use one test class per behavior group in Python. Examples: `TestRouteCapturerRegistered` and `TestSupportedSourceTypes` in `backend/pipeline/ingestion/tests/test_router.py`.
- Use `unittest.IsolatedAsyncioTestCase` for async backend APIs. Example: `backend/pipeline/storage/tests/test_feed_store.py`.
- Use `setUp()` and `tearDown()` for mutable global framework state such as FastAPI dependency overrides. Example: `backend/services/feeds/tests/test_api.py`.
- Use nested `describe()` blocks for TypeScript controller methods and feature cases. Example: `frontend/api/src/feeds/feedsController.test.ts`.
- Use Testing Library queries by label, role, and text for UI behavior. Example: `frontend/transcription-ui/src/components/transcripts/TranscriptView.test.tsx`.

## Mocking

**Framework:** Python `unittest.mock`, FastAPI `TestClient`, pytest fixtures, Vitest `vi`, React Testing Library, and Testcontainers for component-level database tests.

**Patterns:**
```python
with mock.patch.dict(
    "backend.pipeline.ingestion.router._COLLECTORS",
    {source_type: (mock_fn, url_base)},
):
    result = route_capturer(feed, shutdown_event, resources)
```
Pattern source: `backend/pipeline/ingestion/tests/test_router.py`.

```python
pool = make_mock_pool(execute_result="UPDATE 1")
store = FeedStore(pool)
result = await store.update_feed_progress(...)
```
Pattern source: `backend/pipeline/storage/tests/test_feed_store.py`; helper: `backend/pipeline/storage/tests/connection_util.py`.

```python
app.dependency_overrides[verify_oidc_token] = skip_auth
self.client = TestClient(app)
```
Pattern source: `backend/services/feeds/tests/test_api.py`.

```typescript
vi.mock('../config.js', () => ({
  FEEDS_STORE_API_URL: 'http://feeds-api.example.com',
}));

vi.mock('google-auth-library', () => ({
  GoogleAuth: vi.fn().mockImplementation(() => ({
    getIdTokenClient: vi.fn().mockResolvedValue({ request: mockRequest }),
  })),
}));
```
Pattern source: `frontend/api/src/feeds/feedsController.test.ts`.

```typescript
beforeEach(() => {
  mockFetch.mockClear();
  vi.stubGlobal('fetch', mockFetch);
});
```
Pattern source: `frontend/transcription-ui/src/service/listTranscripts.test.ts`.

**What to Mock:**
- Mock GCP clients, HTTP clients, Pub/Sub publishers, OAuth clients, and storage pools in unit tests. Examples: `frontend/api/src/feeds/feedsController.test.ts`, `backend/pipeline/storage/tests/test_feed_store.py`, `backend/pipeline/evaluation/tests/test_processor.py`.
- Mock FastAPI authentication dependencies in service API tests. Example: `backend/services/feeds/tests/test_api.py`.
- Mock browser globals, API services, auth context, and heavy visual/audio dependencies in UI tests. Examples: `frontend/transcription-ui/src/components/transcripts/TranscriptView.test.tsx`, `frontend/transcription-ui/src/service/listTranscripts.test.ts`.
- Use Testcontainers only for component tests under `integration_tests/storage/`, not backend unit tests. Fixture source: `integration_tests/conftest.py`.

**What NOT to Mock:**
- Do not open real sockets or instantiate real `aiohttp.ClientSession` in unit tests; use `CaptureResources` with `mock.AsyncMock`. Example: `backend/pipeline/ingestion/collectors/tests/conftest.py`.
- Do not depend on live GCP services in unit tests; controller tests mock `google-auth-library` and config modules in `frontend/api/src/feeds/feedsController.test.ts`.
- Do not use Docker/Testcontainers in default backend unit tests; keep those under `integration_tests/storage/` and `mise run test:component`.
- Do not exercise full Docker Compose flows from unit tests; E2E tests stay under `integration_tests/e2e/`.

## Fixtures and Factories

**Test Data:**
```python
def _make_feed(source_type: SourceType) -> LeasedFeed:
    return LeasedFeed(
        id=uuid.uuid4(),
        name=f"test-{source_type}",
        external_id="ext-id",
        source_type=source_type,
        ...
    )
```
Pattern source: `backend/pipeline/ingestion/tests/test_router.py`.

```python
def make_mock_pool(
    *,
    fetchrow_result: dict | None = None,
    execute_result: str = "UPDATE 0",
    fetch_result: list | None = None,
) -> mock.AsyncMock:
    ...
```
Pattern source: `backend/pipeline/storage/tests/connection_util.py`.

```tsx
export const renderWithQueryClient = (ui: React.ReactElement) => {
  const testQueryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={testQueryClient}>{ui}</QueryClientProvider>
  );
};
```
Pattern source: `frontend/transcription-ui/src/test/testUtils.tsx`.

**Location:**
- Python local factories live in the test module when only one suite uses them. Example: `_make_feed()` in `backend/pipeline/ingestion/tests/test_router.py`.
- Python shared backend test helpers live beside the relevant package tests. Example: `backend/pipeline/storage/tests/connection_util.py`.
- Collector test helpers live in local `conftest.py` files when several collector suites need the same no-op resources. Example: `backend/pipeline/ingestion/collectors/tests/conftest.py`.
- Integration fixtures live in `integration_tests/conftest.py` and feature fixture modules such as `integration_tests/feed_utils.py`.
- React rendering helpers live in `frontend/transcription-ui/src/test/testUtils.tsx`.

## Coverage

**Requirements:** No coverage threshold is enforced in the inspected configs. `pytest-cov` is present in `pyproject.toml`, but `.mise.toml`, `pyproject.toml`, and frontend package scripts do not define a coverage command or minimum.

**View Coverage:**
```bash
uv run pytest --cov=backend backend/  # Available because `pytest-cov` is in `pyproject.toml`; not a configured project gate
```

## Test Types

**Unit Tests:**
- Backend Python unit tests cover pure logic, storage SQL call behavior through mocked pools, FastAPI route behavior with mocked services, pipeline processors, and transcriber factories. Examples: `backend/pipeline/ingestion/tests/test_router.py`, `backend/pipeline/storage/tests/test_feed_store.py`, `backend/services/feeds/tests/test_api.py`, `backend/pipeline/transcription/tests/test_transcribers.py`.
- Frontend API unit tests cover backend response conversion, URL construction, status mapping, auth cookie behavior, and backend error handling. Examples: `frontend/api/src/auth/authController.test.ts`, `frontend/api/src/feeds/feedsController.test.ts`.
- React UI unit tests cover service requests, component rendering, loading/error states, and user interactions. Examples: `frontend/transcription-ui/src/service/listTranscripts.test.ts`, `frontend/transcription-ui/src/components/transcripts/TranscriptView.test.tsx`.
- Model common unit tests cover manifest merge behavior, scoring policies, and GCS utility edge cases. Examples: `model/colabs/common/tests/test_manifest.py`, `model/colabs/common/tests/test_scoring.py`, `model/colabs/common/tests/test_gcs_utils.py`.

**Integration Tests:**
- Component-level database tests live in `integration_tests/storage/` and use Testcontainers from `integration_tests/conftest.py`. Run through `mise run test:component`.
- API integration tests live in `integration_tests/api/` and target running services over HTTP. Run through `mise run test:api`.
- Backend collector tests with `_integration` in the filename exist under `backend/pipeline/ingestion/collectors/tests/`; keep them separated from unit-only assumptions when selecting test commands.

**E2E Tests:**
- Full pipeline E2E tests live in `integration_tests/e2e/` and exercise Docker Compose services, Pub/Sub emulator, GCS emulator, and pipeline flows. Run through `mise run test:e2e` or `mise run test:e2e:local`.
- CI runs E2E tests through `.github/workflows/integration-tests.yml`; do not run these as part of ordinary unit verification.

## Common Patterns

**Async Testing:**
```python
class TestRenewHeartbeatsBatchDiagnostic(unittest.IsolatedAsyncioTestCase):
    async def test_short_circuits_on_empty_input(self) -> None:
        pool = mock.AsyncMock()
        store = FeedStore(pool)
        result = await store.renew_heartbeats_batch_diagnostic([], _WORKER_ID)
        self.assertEqual(result, [])
        pool.fetch.assert_not_called()
```
Pattern source: `backend/pipeline/storage/tests/test_feed_store.py`.

```typescript
mockRequest.mockResolvedValueOnce({ data: [mockBackendFeed] });
const controller = new FeedsController();
const result = await controller.listFeeds();
expect(result).toEqual([expectedFrontendFeed]);
```
Pattern source: `frontend/api/src/feeds/feedsController.test.ts`.

```tsx
await waitFor(() => {
  expect(screen.getByText('Hello')).toBeTruthy();
});
```
Pattern source: `frontend/transcription-ui/src/components/transcripts/TranscriptView.test.tsx`.

**Error Testing:**
```python
with self.assertRaises(ValueError) as ctx:
    route_capturer(feed, shutdown_event, resources)
self.assertIn("bcfy_calls", str(ctx.exception))
```
Pattern source: `backend/pipeline/ingestion/tests/test_router.py`.

```python
response = self.client.post("/v1/feeds", json=payload)
self.assertEqual(response.status_code, status.HTTP_422_UNPROCESSABLE_CONTENT)
```
Pattern source: `backend/services/feeds/tests/test_api.py`.

```typescript
mockRequest.mockRejectedValueOnce(new Error('Network Error'));
await expect(controller.listFeeds()).rejects.toThrow(/Network Error/);
```
Pattern source: `frontend/api/src/feeds/feedsController.test.ts`.

```typescript
mockFetch.mockResolvedValueOnce({
  ok: false,
  status: 403,
  statusText: 'Forbidden',
  text: async () => 'Forbidden',
});
await expect(listTranscripts('feed123', 'tokenXYZ')).rejects.toThrow(
  /403.*Forbidden/
);
```
Pattern source: `frontend/transcription-ui/src/service/listTranscripts.test.ts`.

---

*Testing analysis: 2026-05-24*

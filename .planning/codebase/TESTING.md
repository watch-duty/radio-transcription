# Testing Patterns

**Analysis Date:** 2026-05-27

## Test Framework

**Runner:**
- Python unit tests primarily use `unittest` and run through `uv run python -m unittest discover backend/pipeline` via `mise run test:unit` in `.mise.toml`.
- Python async/component/integration tests use `pytest` `>=9.0.2`, `pytest-asyncio`, `pytest-xdist`, and `testcontainers[postgres]` configured in `pyproject.toml`.
- Root pytest uses `asyncio_mode = "auto"`, function-scoped async fixture loops, and `addopts = "-n auto"` in `pyproject.toml`.
- Model common tests use a separate pytest config in `model/pyproject.toml` with `testpaths = ["colabs/common/tests"]`.
- Frontend API tests use Vitest from `frontend/api/package.json`.
- Frontend UI tests use Vitest plus React Testing Library and JSDOM setup from `frontend/transcription-ui/vitest.config.js` and `frontend/transcription-ui/src/test/setup.ts`.

**Assertion Library:**
- Python `unittest.TestCase` assertions are common in backend unit tests: `backend/pipeline/storage/tests/test_feed_store.py`, `backend/pipeline/ingestion/tests/test_retry.py`, `backend/services/feeds/tests/test_api.py`.
- Python pytest tests use bare `assert` in integration and async component tests: `integration_tests/storage/test_feed_store_integration.py`, `integration_tests/api/test_transcripts_api.py`.
- Apache Beam tests use `apache_beam.testing.util.assert_that` and `equal_to` in `backend/pipeline/normalization/tests/test_transforms.py`.
- TypeScript tests use Vitest `expect`, `vi`, and Testing Library queries/events: `frontend/api/src/transcripts/transcriptsController.test.ts`, `frontend/transcription-ui/src/components/feeds/FeedTable.test.tsx`.

**Run Commands:**
```bash
mise run test                 # Alias for Python backend unit tests
mise run test:unit            # uv run python -m unittest discover backend/pipeline
mise run test:model           # Model common-library pytest tests from model/
mise run test:component       # Storage component tests with testcontainers
mise run test:api             # HTTP API integration tests under integration_tests/api/
mise run test:e2e             # Full Docker Compose E2E test environment
mise run test:e2e:local       # E2E tests against an already-running environment
yarn --cwd frontend/api test  # Vitest for frontend proxy API
yarn --cwd frontend/transcription-ui test # Vitest for React UI
```

## Test File Organization

**Location:**
- Backend Python unit tests are colocated inside module-level `tests/` packages: `backend/pipeline/storage/tests/`, `backend/pipeline/evaluation/tests/`, `backend/pipeline/ingestion/tests/`, `backend/pipeline/normalization/tests/`.
- Some small service tests live directly next to their module: `backend/pipeline/notification/test_send_notification.py`, `backend/pipeline/notification/test_request_handler.py`.
- FastAPI service tests live under each service package: `backend/services/feeds/tests/test_api.py`, `backend/services/rules/tests/test_api.py`, `backend/services/transcripts/tests/test_api.py`.
- Integration tests live only under `integration_tests/`, split into `storage/`, `api/`, and `e2e/`.
- TypeScript tests are colocated with feature source in `frontend/api/src/**` and `frontend/transcription-ui/src/**`.
- Model common tests live under `model/colabs/common/tests/`; SFT CLI regression tests live under `model/scripts/sft/tests/`.

**Naming:**
- Python tests use `test_*.py` files and `test_*` methods/functions: `backend/pipeline/ingestion/tests/test_runtime.py`, `integration_tests/storage/test_feed_store_integration.py`.
- TypeScript tests use `*.test.ts` and `*.test.tsx`: `frontend/api/src/auth/authController.test.ts`, `frontend/transcription-ui/src/components/Login.test.tsx`.
- Integration test filenames include the scope in the name: `test_feed_store_integration.py`, `test_transcription_pipeline.py`.

**Structure:**
```text
backend/pipeline/<domain>/tests/test_<behavior>.py
backend/services/<service>/tests/test_api.py
integration_tests/{storage,api,e2e}/test_<flow>.py
frontend/api/src/<domain>/<domain>Controller.test.ts
frontend/transcription-ui/src/{components,service,utils}/**/*.test.{ts,tsx}
model/colabs/common/tests/test_<module>.py
model/scripts/sft/tests/test_<contract>.py
```

## Test Structure

**Suite Organization:**
```python
# backend/pipeline/ingestion/tests/test_retry.py
class TestRetryOnRetryable(unittest.IsolatedAsyncioTestCase):
    async def test_retries_on_retryable_then_succeeds(self) -> None:
        fn = mock.AsyncMock(side_effect=[OSError("fail"), "ok"])
        result = await retry_with_lease_check(
            fn,
            lease_lost=asyncio.Event(),
            shutdown=asyncio.Event(),
            max_retries=2,
            base_delay_sec=0.0,
            retryable=(OSError,),
        )
        self.assertEqual(result, "ok")
        self.assertEqual(fn.await_count, 2)
```

```python
# integration_tests/storage/test_feed_store_integration.py
@pytest.fixture
async def store(db_pool: asyncpg.Pool) -> FeedStore:
    await db_pool.execute("TRUNCATE feeds CASCADE")
    return FeedStore(db_pool)

async def test_primary_cte_sets_status_to_active(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    feed_id = await _insert_feed(db_pool, "bf-0", source_type="bcfy_feeds")
    worker = uuid.uuid4()
    result = await store.acquire_feeds_batch(worker, limits={...})
    assert len(result) == 1
```

```typescript
// frontend/api/src/transcripts/transcriptsController.test.ts
describe('listTranscripts', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should return converted data on success', async () => {
    mockRequest.mockResolvedValueOnce({ data: mockBackendResponse });
    const controller = new TranscriptsController();
    const result = await controller.listTranscripts('test', { limit: 100 });
    expect(result).toEqual(expectedResult);
  });
});
```

**Patterns:**
- Group `unittest` suites by behavior or state machine branch, not just by method name: `backend/pipeline/ingestion/tests/test_retry.py`, `backend/pipeline/ingestion/tests/test_runtime.py`, `model/colabs/common/tests/test_manifest.py`.
- Use helper builders for repeated domain payloads: `_make_runtime()` and `_make_settings()` in `backend/pipeline/ingestion/tests/test_runtime.py`, `_insert_feed()` in `integration_tests/storage/test_feed_store_integration.py`.
- Use deterministic UUIDs and fixed timestamps in backend tests when object identity/order matters: `backend/pipeline/storage/tests/test_feed_store.py`, `backend/pipeline/transcription/tests/test_processor.py`.
- Keep API contract tests focused on HTTP status codes, response JSON, and service calls: `backend/services/feeds/tests/test_api.py`.
- React tests render through small local helper functions that install providers: `renderTranscriptView()` in `frontend/transcription-ui/src/components/transcripts/TranscriptView.test.tsx`, `renderWithQueryClient()` in `frontend/transcription-ui/src/test/testUtils.tsx`.
- Use `waitFor()` for UI state that depends on async query resolution: `frontend/transcription-ui/src/components/transcripts/TranscriptView.test.tsx`.

## Mocking

**Framework:** Python `unittest.mock`, pytest fixtures/monkeypatch patterns, `testcontainers`, Vitest `vi`, React Testing Library utilities.

**Patterns:**
```python
# backend/pipeline/storage/tests/connection_util.py
def make_mock_pool(
    *,
    fetchrow_result: dict | None = None,
    execute_result: str = "UPDATE 0",
    fetch_result: list | None = None,
) -> mock.AsyncMock:
    pool = mock.AsyncMock()
    pool.fetchrow.return_value = fetchrow_result
    pool.execute.return_value = execute_result
    pool.fetch.return_value = fetch_result or []
    return pool
```

```python
# backend/services/feeds/tests/test_api.py
async def skip_auth() -> dict[str, str]:
    return {"sub": "test@example.com", "email": "test@example.com"}

class TestFeedsAPI(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_service = AsyncMock()
        app.state.feed_service = self.mock_service
        app.dependency_overrides[verify_oidc_token] = skip_auth
        self.client = TestClient(app)
```

```typescript
// frontend/api/src/auth/authController.test.ts
const { mockGetToken, mockRefreshAccessToken, mockSetCredentials, mockState } =
  vi.hoisted(() => ({
    mockGetToken: vi.fn(),
    mockRefreshAccessToken: vi.fn(),
    mockSetCredentials: vi.fn(),
    mockState: { allowedOrigin: 'http://localhost:5173' },
  }));

vi.mock('google-auth-library', () => {
  const OAuth2Client = vi.fn().mockImplementation(() => ({
    getToken: mockGetToken,
    refreshAccessToken: mockRefreshAccessToken,
    setCredentials: mockSetCredentials,
  }));
  return { OAuth2Client };
});
```

```typescript
// frontend/transcription-ui/src/components/transcripts/TranscriptView.test.tsx
vi.mock('../../service/listTranscripts', () => ({
  listTranscripts: vi.fn(),
}));

vi.mock('../../context/AuthContext', () => ({
  useAuth: () => ({ token: 'fake-token' }),
}));
```

**What to Mock:**
- Mock external cloud clients and network calls in unit tests: Google Auth in `frontend/api/src/auth/authController.test.ts`, Pub/Sub in `backend/pipeline/common/clients/tests/test_pubsub_client.py`, GCS upload/publish boundaries in `backend/pipeline/ingestion/tests/test_runtime.py`.
- Mock FastAPI auth dependencies and service state for API unit tests: `backend/services/feeds/tests/test_api.py`.
- Mock React service modules and auth context for component tests: `frontend/transcription-ui/src/components/transcripts/TranscriptView.test.tsx`.
- Mock browser/platform APIs absent in JSDOM: `HTMLMediaElement` methods in `frontend/transcription-ui/src/test/setup.ts`, Wavesurfer in `frontend/transcription-ui/src/components/transcripts/TranscriptView.test.tsx`.
- Use testcontainers for database component tests instead of mocking SQL behavior when testing real store semantics: `integration_tests/conftest.py`, `integration_tests/storage/test_feed_store_integration.py`.

**What NOT to Mock:**
- Do not mock SQL when the behavior under test is lease, fencing token, retry, or schema interaction; use `integration_tests/storage/` with an AlloyDB Omni container.
- Do not mock shared type conversions at controller boundaries; assert the real conversion output as in `frontend/api/src/transcripts/transcriptsController.test.ts`.
- Do not construct real `aiohttp.ClientSession` objects in unit tests; use `mock.AsyncMock(spec=aiohttp.ClientSession)` as in `backend/pipeline/ingestion/tests/test_runtime.py`.
- Do not let unit tests depend on live Google Cloud state; model/SFT preflight tests pass `storage_client=None` or fake clients in `model/scripts/sft/tests/test_pipeline_build.py`.

## Fixtures and Factories

**Test Data:**
```python
# integration_tests/conftest.py
@pytest.fixture(scope="session")
def postgres_container() -> Generator[dict[str, Any]]:
    if not _docker_available():
        pytest.skip("Docker is not available")
    container = PostgresContainer(
        image="google/alloydbomni:15",
        username="postgres",
        password="postgres",
        dbname="postgres",
        driver=None,
    )
    container.with_command("postgres -c 'max_connections=100'")
    container.start()
    ...
```

```typescript
// frontend/transcription-ui/src/test/testUtils.tsx
export const renderWithQueryClient = (ui: React.ReactElement) => {
  const testQueryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={testQueryClient}>{ui}</QueryClientProvider>
  );
};
```

```python
# model/scripts/sft/tests/test_pipeline_build.py
with tempfile.TemporaryDirectory() as tmp:
    train_path = Path(tmp) / "train.jsonl"
    report_path = Path(tmp) / "preflight_report.json"
    train_path.write_text(json.dumps(self._make_bad_example()) + "\n")
    report = run_preflight(
        train_jsonl_path=train_path,
        val_jsonl_path=None,
        storage_client=None,
        report_path=report_path,
    )
```

**Location:**
- Shared integration fixtures live in `integration_tests/conftest.py` and `integration_tests/feed_utils.py`.
- Storage mock helpers live in `backend/pipeline/storage/tests/connection_util.py`.
- UI render helpers live in `frontend/transcription-ui/src/test/testUtils.tsx`; global JSDOM setup lives in `frontend/transcription-ui/src/test/setup.ts`.
- Domain-specific builders generally stay in the test file that uses them: `_make_settings()` in `backend/pipeline/ingestion/tests/test_runtime.py`, `_make_good_example()` in `model/scripts/sft/tests/test_pipeline_build.py`.
- Golden or fixture data for normalization tests lives under `backend/pipeline/normalization/tests/test_data/` and `backend/pipeline/ingestion/tests/golden/`.

## Coverage

**Requirements:** No enforced coverage percentage is detected. `pytest-cov` is listed in `pyproject.toml`, but no `--cov` addopts or coverage threshold is configured. Vitest coverage settings are not configured in `frontend/api/package.json`, `frontend/transcription-ui/package.json`, or `frontend/transcription-ui/vitest.config.js`.

**View Coverage:**
```bash
uv run pytest --cov=backend --cov-report=term-missing backend/pipeline
uv run pytest --cov=model/colabs/common --cov-report=term-missing model/colabs/common/tests
yarn --cwd frontend/api test --coverage
yarn --cwd frontend/transcription-ui test --coverage
```

Coverage commands above are available patterns, not configured project gates. Add explicit config before treating them as required CI signals.

## Test Types

**Unit Tests:**
- Backend unit tests isolate storage, retry loops, processors, API services, and transformations with mocks: `backend/pipeline/storage/tests/test_feed_store.py`, `backend/pipeline/evaluation/tests/test_service.py`, `backend/pipeline/ingestion/tests/test_retry.py`.
- FastAPI service unit tests use `TestClient`, dependency overrides, and mocked service state: `backend/services/feeds/tests/test_api.py`.
- Frontend API unit tests instantiate controllers and mock Google Auth/backend requests: `frontend/api/src/transcripts/transcriptsController.test.ts`, `frontend/api/src/auth/authController.test.ts`.
- React UI unit/component tests render components with MemoryRouter, QueryClient, and service mocks: `frontend/transcription-ui/src/components/feeds/FeedTable.test.tsx`, `frontend/transcription-ui/src/components/transcripts/TranscriptView.test.tsx`.
- Model unit/regression tests validate manifest merging, scoring, Vertex request construction, SFT CLI preflight, README contracts, and Docker runtime expectations: `model/colabs/common/tests/test_manifest.py`, `model/colabs/common/tests/test_scoring.py`, `model/scripts/sft/tests/test_pipeline_build.py`.

**Integration Tests:**
- Storage component tests use `testcontainers.postgres.PostgresContainer` with `google/alloydbomni:15`, apply SQL from `terraform/modules/alloydb/sql/ingestion/`, and exercise real `asyncpg` store behavior in `integration_tests/storage/`.
- API integration tests use `httpx.AsyncClient` against running services configured by environment variables like `TRANSCRIPTS_API_HOST` in `integration_tests/api/test_transcripts_api.py`.
- Collector integration tests that require Docker or ffmpeg use `unittest.skipUnless()` gates: `backend/pipeline/ingestion/collectors/tests/test_icecast_collector_integration.py`, `backend/pipeline/ingestion/collectors/echo/tests/test_echo_collector_integration.py`.

**E2E Tests:**
- Full system E2E tests live in `integration_tests/e2e/` and run through Docker Compose with Pub/Sub emulator, GCS emulator, mock audio server, pipeline services, APIs, and notification services via `mise run test:e2e`.
- Host-run E2E uses `mise run test:e2e:local`, which generates protos first and runs `uv run pytest integration_tests/e2e/` against an existing environment.

## Common Patterns

**Async Testing:**
```python
# unittest async style in backend/pipeline/ingestion/tests/test_retry.py
class TestRetryShutdown(unittest.IsolatedAsyncioTestCase):
    async def test_aborts_before_first_attempt_if_shutdown(self) -> None:
        fn = mock.AsyncMock(return_value="ok")
        shutdown = asyncio.Event()
        shutdown.set()
        with self.assertRaises(asyncio.CancelledError):
            await retry_with_lease_check(
                fn,
                lease_lost=asyncio.Event(),
                shutdown=shutdown,
            )
```

```python
# pytest async style in integration_tests/api/test_transcripts_api.py
@pytest.mark.asyncio
async def test_transcripts_api(
    api_client: httpx.AsyncClient, test_bcfy_feed: tuple[str, str]
) -> None:
    response = await api_client.post("/transcripts", json=payload, timeout=10.0)
    assert response.status_code == 201, f"Failed to create: {response.text}"
```

**Error Testing:**
```python
# backend/services/feeds/tests/test_api.py
self.mock_service.create_feed.side_effect = FeedAlreadyExistsError(
    "bcfy_feeds", "123"
)
response = self.client.post("/v1/feeds", json=payload)
self.assertEqual(response.status_code, status.HTTP_409_CONFLICT)
```

```typescript
// frontend/transcription-ui/src/service/listTranscripts.test.ts
mockFetch.mockResolvedValueOnce({
  ok: false,
  status: 403,
  statusText: 'Forbidden',
  headers: { get: () => null },
  text: async () => 'Forbidden',
});

await expect(listTranscripts('feed123', 'tokenXYZ')).rejects.toThrow(
  /403.*Forbidden/
);
```

**Regression Tests:**
- Capture specific known invariants in test names and assertions. Examples include stale prediction clearing in `model/colabs/common/tests/test_manifest.py`, source-type recovery filtering in `integration_tests/storage/test_feed_store_integration.py`, RSS watchdog behavior in `backend/pipeline/ingestion/tests/test_runtime.py`, and virtualized scroller preservation in `frontend/transcription-ui/src/components/feeds/FeedTable.test.tsx`.
- Documentation and runtime contract drift tests are acceptable when docs drive operation. `model/scripts/sft/tests/test_readme_docs.py` asserts SFT README command descriptions and cost basis; `model/scripts/sft/tests/test_docker_runtime.py` asserts ASR Docker runtime wiring.

---

*Testing analysis: 2026-05-27*

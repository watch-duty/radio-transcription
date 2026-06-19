# Testing Patterns

**Analysis Date:** 2026-06-19

## Test Framework

**Runner:**
- Python: `pytest>=9.0.2` from `pyproject.toml`, with `pytest-asyncio`, `pytest-xdist`, `pytest-cov`, `testcontainers[postgres]`, and `docker` in the root dev dependency group.
- Python tests include both pytest function style and `unittest.TestCase` / `unittest.IsolatedAsyncioTestCase`, as in `backend/pipeline/segmentation/tests/test_orchestration.py`, `backend/services/feeds/tests/test_api.py`, and `backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py`.
- Root pytest config lives in `pyproject.toml`: `asyncio_mode = "auto"`, function-scoped async fixture loops, `addopts = "-n auto"`, and warning filters for selected async/thread warnings.
- Model package tests use `model/pyproject.toml` with `testpaths = ["tests"]`.
- Frontend API and UI use Vitest from `frontend/api/package.json` and `frontend/transcription-ui/package.json`; UI setup is configured in `frontend/transcription-ui/vitest.config.js`.

**Assertion Library:**
- Python uses `unittest` assertions (`self.assertEqual`, `self.assertRaisesRegex`) in many backend/model tests, as in `backend/services/feeds/tests/test_api.py` and `model/tests/gemini_sft/test_config.py`.
- Python pytest-style tests use bare `assert` and `pytest.raises`, as in `backend/pipeline/segmentation/tests/test_orchestration.py`.
- TypeScript uses Vitest `expect` and React Testing Library assertions, with jest-dom installed via `frontend/transcription-ui/src/test/setup.ts`.

**Run Commands:**
```bash
mise run test:unit                                      # Backend unit tests from `.mise.toml`
mise run test:model                                     # Model package tests from `.mise.toml`
yarn --cwd frontend/api test --watch=false              # API proxy Vitest tests from `frontend/api/package.json`
yarn --cwd frontend/transcription-ui test --watch=false # UI Vitest tests from `frontend/transcription-ui/package.json`
mise run test:component                                 # Storage component tests from `.mise.toml`
mise run test:api                                       # API integration tests from `.mise.toml`
mise run test:e2e                                       # Docker Compose E2E tests from `.mise.toml`
```

Use `safe-run -- <command>` for agent-run tests that may consume substantial CPU, memory, or process count, following `AGENTS.md`.

## Test File Organization

**Location:**
- Backend unit tests are colocated under package-local `tests/` directories, for example `backend/services/feeds/tests/test_api.py`, `backend/pipeline/storage/tests/test_feed_store.py`, and `backend/pipeline/ingestion/tests/test_router.py`.
- Some Cloud Function style tests live next to implementation files, as in `backend/pipeline/notification/test_send_notification.py` and `backend/pipeline/notification/test_request_handler.py`.
- Model tests live under `model/tests/`, including `model/tests/gemini_sft/test_config.py`, `model/tests/gemini_sft/test_workflow.py`, and `model/tests/common/tests/test_manifest.py`.
- Cross-service tests live under `integration_tests/`, split into `integration_tests/storage/`, `integration_tests/api/`, and `integration_tests/e2e/`.
- Frontend tests are colocated beside implementation files with `.test.ts` or `.test.tsx`, as in `frontend/api/src/feeds/feedsController.test.ts`, `frontend/transcription-ui/src/service/listFeeds.test.ts`, and `frontend/transcription-ui/src/components/feeds/FeedTable.test.tsx`.

**Naming:**
- Python tests use `test_*.py`, as in `backend/pipeline/ingestion/tests/test_collector_runtime.py` and `integration_tests/storage/test_feed_store_integration.py`.
- TypeScript tests use `*.test.ts` and `*.test.tsx`, as in `frontend/api/src/authentication.test.ts` and `frontend/transcription-ui/src/context/AuthProvider.test.tsx`.
- Test helper files omit test-case naming when they are support modules, as in `backend/pipeline/storage/tests/connection_util.py`, `integration_tests/feed_utils.py`, and `model/tests/fake_gcs.py`.

**Structure:**
```text
backend/<area>/<module>/tests/test_*.py        # Backend package unit tests
backend/pipeline/notification/test_*.py        # Function-local tests
model/tests/**/test_*.py                       # Model package tests
integration_tests/storage/test_*_integration.py # Testcontainers-backed store tests
integration_tests/api/test_*.py                # Running-service API tests
integration_tests/e2e/test_*.py                # Full pipeline tests
frontend/api/src/**/*.test.ts                  # API proxy Vitest tests
frontend/transcription-ui/src/**/*.test.tsx    # UI component Vitest tests
```

## Test Structure

**Suite Organization:**
```python
# `backend/services/feeds/tests/test_api.py`
class TestFeedsAPI(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_service = AsyncMock()
        app.state.feed_service = self.mock_service
        app.dependency_overrides[verify_oidc_token] = skip_auth
        self.client = TestClient(app)

    def tearDown(self) -> None:
        app.dependency_overrides.clear()
```

```python
# `backend/pipeline/segmentation/tests/test_orchestration.py`
def test_pipeline_invalid_timeout() -> None:
    options = SegmentationOptions(flags=[...])
    with pytest.raises(ValueError, match=r"stale_timeout_ms .*"):
        get_pipeline(options)
```

```typescript
// `frontend/api/src/feeds/feedsController.test.ts`
describe('FeedsController', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should return converted feeds on success', async () => {
    mockRequest.mockResolvedValueOnce({ data: [mockBackendFeed] });
    const controller = new FeedsController();
    const result = await controller.listFeeds();
    expect(result).toEqual([expectedFrontendFeed]);
  });
});
```

**Patterns:**
- Group related behavior with `unittest.TestCase` classes for service/store contracts, as in `backend/pipeline/storage/tests/test_feed_store.py`.
- Use pure pytest functions for lightweight behavior and contract checks, as in `backend/pipeline/segmentation/tests/test_orchestration.py`.
- Use `describe`/`it` for TypeScript units and components, as in `frontend/transcription-ui/src/service/listFeeds.test.ts` and `frontend/transcription-ui/src/components/feeds/FeedTable.test.tsx`.
- Put reusable setup in `setUp`/`tearDown`, pytest fixtures, or local `render*` helpers, as in `backend/services/feeds/tests/test_api.py`, `integration_tests/conftest.py`, and `frontend/transcription-ui/src/components/feeds/FeedTable.test.tsx`.

## Mocking

**Framework:** Python `unittest.mock`; pytest `monkeypatch`; Vitest `vi`.

**Patterns:**
```python
# `backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py`
@patch(f"{_COL_MOD}.control_flow.sleep_or_cancel", new_callable=AsyncMock)
async def test_retryable_4xx_retry_success(self, mock_sleep: AsyncMock) -> None:
    mock_sleep.return_value = False
    self.session.get = AsyncMock(side_effect=[resp_retryable, resp200])
```

```python
# `backend/services/feeds/tests/test_api.py`
app.dependency_overrides[verify_oidc_token] = skip_auth
self.mock_service.create_feed.side_effect = FeedAlreadyExistsError(
    "bcfy_feeds", "123"
)
```

```typescript
// `frontend/transcription-ui/src/service/listFeeds.test.ts`
beforeEach(() => {
  mockFetch.mockClear();
  vi.stubGlobal('fetch', mockFetch);
});
```

```typescript
// `frontend/api/src/feeds/feedsController.test.ts`
vi.mock('google-auth-library', () => {
  class MockGoogleAuth {
    getIdTokenClient = vi.fn().mockResolvedValue({ request: mockRequest });
  }
  return { GoogleAuth: MockGoogleAuth };
});
```

**What to Mock:**
- Mock external HTTP, Google Auth, Pub/Sub, GCS, and downstream service clients in unit tests, as in `frontend/api/src/feeds/feedsController.test.ts`, `backend/pipeline/notification/test_send_notification.py`, and `backend/pipeline/normalization/tests/test_processor.py`.
- Mock FastAPI authentication with dependency overrides, as in `backend/services/feeds/tests/test_api.py`.
- Mock timers, browser globals, `fetch`, `navigator`, and service modules in frontend tests, as in `frontend/transcription-ui/src/context/AuthProvider.test.tsx`, `frontend/transcription-ui/src/components/Login.test.tsx`, and `frontend/transcription-ui/src/service/authSession.test.ts`.
- Use fake in-memory clients when testing model workflows, as in `model/tests/fake_gcs.py` and `model/tests/gemini_sft/test_workflow.py`.

**What NOT to Mock:**
- Do not mock SQL/schema behavior in component tests under `integration_tests/storage/`; use the `PostgresContainer` fixture in `integration_tests/conftest.py`.
- Do not mock route/service serialization in FastAPI API tests where `TestClient` can exercise request validation and response models, as in `backend/services/feeds/tests/test_api.py`.
- Do not mock React component DOM output when user-visible behavior can be asserted through React Testing Library, as in `frontend/transcription-ui/src/components/feeds/FeedTable.test.tsx`.

## Fixtures and Factories

**Test Data:**
```python
# `backend/pipeline/storage/tests/test_feed_store.py`
def _full_feed_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "id": _FEED_ID,
        "name": "My Feed",
        "source_type": "bcfy_feeds",
        "status": "unclaimed",
    }
    row.update(overrides)
    return row
```

```python
# `integration_tests/conftest.py`
@pytest.fixture(scope="session")
def postgres_container() -> Generator[dict[str, Any]]:
    container = PostgresContainer(
        image="google/alloydbomni:15",
        username="postgres",
        password="postgres",
        dbname="postgres",
        driver=None,
    )
```

```typescript
// `frontend/transcription-ui/src/components/feeds/FeedTable.test.tsx`
const renderFeedTable = (
  props: Partial<React.ComponentProps<typeof FeedTable>> = {}
) => {
  return render(
    <MemoryRouter>
      <VirtuosoMockContext.Provider value={{ viewportHeight: 1000, itemHeight: 100 }}>
        <FeedTable {...finalProps} />
      </VirtuosoMockContext.Provider>
    </MemoryRouter>
  );
};
```

**Location:**
- Backend test helpers: `backend/pipeline/storage/tests/connection_util.py`, `backend/pipeline/common/test_schema_helper.py`, and package-local `conftest.py` files.
- Integration fixtures: `integration_tests/conftest.py`, `integration_tests/feed_utils.py`, and `integration_tests/utils.py`.
- Model fakes: `model/tests/fake_gcs.py`, `model/tests/conftest.py`, and `model/tests/common/tests/conftest.py`.
- Frontend render helpers: `frontend/transcription-ui/src/test/testUtils.tsx`; feature-specific render helpers are also colocated in tests such as `frontend/transcription-ui/src/components/feeds/FeedTable.test.tsx`.
- Golden files: `backend/pipeline/ingestion/tests/golden/*.json` and audio fixtures under `backend/pipeline/segmentation/tests/test_data/`.

## Coverage

**Requirements:** None enforced. `pytest-cov` is present in `pyproject.toml`, but no coverage threshold, coverage config, or default coverage task is defined in `pyproject.toml`, `.mise.toml`, `frontend/api/package.json`, `frontend/transcription-ui/package.json`, or `frontend/transcription-ui/vitest.config.js`.

**View Coverage:**
```bash
uv run pytest --cov=backend backend/                    # Ad hoc Python coverage; pytest-cov is installed in `pyproject.toml`
yarn --cwd frontend/transcription-ui test --coverage    # Ad hoc Vitest coverage if coverage provider dependencies are added
```

## Test Types

**Unit Tests:**
- Backend unit tests run with `mise run test:unit` from `.mise.toml` and cover `backend/` while ignoring `**/test_*_integration.py`.
- Model unit tests run with `mise run test:model` from `.mise.toml` and install model extras declared in `model/pyproject.toml`.
- Frontend API and UI unit tests run through Vitest scripts in `frontend/api/package.json` and `frontend/transcription-ui/package.json`.

**Integration Tests:**
- Component/store integration tests under `integration_tests/storage/` use Testcontainers and an AlloyDB Omni image configured in `integration_tests/conftest.py`.
- API integration tests under `integration_tests/api/` target running services via HTTP clients and are invoked by `mise run test:api` in `.mise.toml`.
- Collector integration-style tests exist inside backend collector suites, such as `backend/pipeline/ingestion/collectors/tests/test_bcfy_calls_collector_integration.py` and `backend/pipeline/ingestion/collectors/tests/test_openmhz_collector_integration.py`.

**E2E Tests:**
- E2E tests under `integration_tests/e2e/` run full pipeline flows with Docker Compose and the Pub/Sub emulator, documented in `CONTRIBUTING.md` and invoked by `mise run test:e2e`.
- GitHub Actions runs non-storage integration tests through `.github/workflows/integration-tests.yml` with `uv run pytest integration_tests/ --ignore=integration_tests/storage/ -n auto -v -s --log-cli-level=INFO`.

## Common Patterns

**Async Testing:**
```python
# `backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py`
class TestDownloadM4a(unittest.IsolatedAsyncioTestCase):
    async def test_valid_download_disables_redirects(self) -> None:
        self.session.get = AsyncMock(return_value=resp)
        result = await _download_m4a(self.session, url, self.shutdown)
        self.assertEqual(result, b"m4a")
```

```python
# `integration_tests/api/test_feeds_api.py`
@pytest.mark.asyncio
async def test_create_feed(...):
    response = await proxy_client.post(...)
```

```typescript
// `frontend/transcription-ui/src/service/listFeeds.test.ts`
it('should throw error if response not ok', async () => {
  mockFetch.mockResolvedValueOnce({ ok: false, status: 500, text: async () => 'Internal Server Error' });
  await expect(listFeeds('tokenXYZ')).rejects.toThrow(/500.*Internal Server Error/);
});
```

**Error Testing:**
```python
# `model/tests/gemini_sft/test_config.py`
with self.assertRaisesRegex(RunConfigError, "validation_manifest_uri"):
    load_run_config(self._write_config(body))
```

```python
# `backend/pipeline/segmentation/tests/test_orchestration.py`
with pytest.raises(ValueError, match=r"stale_timeout_ms .*"):
    get_pipeline(options)
```

```typescript
// `frontend/api/src/feeds/feedsController.test.ts`
const error = new Error('Not Found') as Error & { response?: { status: number } };
error.response = { status: 404 };
mockRequest.mockRejectedValueOnce(error);
await expect(controller.getFeed('feed_123')).rejects.toThrow(/Not Found/);
```

---

*Testing analysis: 2026-06-19*

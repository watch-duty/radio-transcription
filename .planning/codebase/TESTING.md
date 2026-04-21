# Testing Patterns

**Analysis Date:** 2026-04-21

Two test surfaces: Python backend (pytest + stdlib `unittest` test cases) and TypeScript frontend (Vitest + React Testing Library). Integration tests share a dedicated `integration_tests/` tree and use `testcontainers` to spin up real AlloyDB Omni and fake-GCS containers.

## Test Framework

**Runner:**
- Python: **pytest** `>=9.0.2` with **pytest-asyncio** `>=1.3.0`. Configured in `pyproject.toml:231-240` under `[tool.pytest.ini_options]`.
  - `asyncio_mode = "auto"` — every `async def test_*` is automatically an async test; no `@pytest.mark.asyncio` decorator required.
  - `asyncio_default_fixture_loop_scope = "function"` — a fresh event loop per test.
  - `filterwarnings` suppresses two known-noisy deprecations from `httplib2` and `pydub`.
- Frontend: **Vitest** `^4.1.4` (`frontend/transcription-ui/package.json:62`). Environment declared per-file via the header comment `// @vitest-environment jsdom` (see `frontend/transcription-ui/src/components/audio/AudioPlayer.test.tsx:1`).

**Assertion Library:**
- Python: stdlib `unittest.TestCase` assertions (`assertEqual`, `assertTrue`, `assertRaises`, `assertRaisesRegex`, `assertLogs`, `assertIsNone`, `subTest`). Ruff rules `PT009` and `PT027` are ignored in `pyproject.toml:132-133` — unittest-style is the house style, not pytest `assert`.
- Frontend: Vitest `expect` + `@testing-library/react` matchers (`toEqual`, `toHaveBeenCalledWith`, `rejects.toThrow`).

**Run Commands:**
```bash
# Python — all unit tests (backend/pipeline/**)
uv run python -m unittest discover backend/pipeline          # canonical, via mise task 'test:unit'
mise run test:unit                                           # same thing
uv run pytest backend/                                       # alt: pytest auto-discovery

# Single file
uv run pytest backend/pipeline/ingestion/tests/test_retry.py
.venv/bin/python -m pytest backend/pipeline/ingestion/tests/test_retry.py  # alternative

# Integration tests (require Docker — skip if unavailable)
mise run test:component                   # all testcontainer-backed store tests
mise run test:component:feeds             # FeedStore only
mise run test:component:rules             # RulesStore only
mise run test:api                         # API integration via HTTP
mise run test:e2e                         # full E2E in Docker Compose
mise run test:e2e:local                   # E2E against running stack

# Coverage
uv run pytest --cov=backend backend/      # pytest-cov is in dev deps (pyproject.toml:61)

# Frontend — from frontend/transcription-ui/
yarn test                                 # Vitest (interactive watch by default)
yarn test --run                           # one-shot
```

## Test File Organization

**Location:**
- Python: **co-located** under `tests/` sibling directory to the source module it exercises. Every backend submodule has its own `tests/` folder:
  - `backend/pipeline/ingestion/tests/`
  - `backend/pipeline/ingestion/collectors/tests/`
  - `backend/pipeline/ingestion/collectors/echo/tests/`
  - `backend/pipeline/ingestion/broadcastify_credential_rotation/tests/`
  - `backend/pipeline/storage/tests/`
  - `backend/pipeline/common/tests/`, `backend/pipeline/common/clients/tests/`, `backend/pipeline/common/storage/tests/`
  - `backend/pipeline/transcription/tests/`, `backend/pipeline/rules/tests/`, `backend/pipeline/evaluation/tests/`
  - `backend/services/feeds/tests/`, `backend/services/transcripts/tests/`
- Python integration tests that touch external systems live at the repo root in `integration_tests/` (NOT `backend/…/tests/`). Split into `integration_tests/storage/`, `integration_tests/api/`, `integration_tests/e2e/`.
- Frontend: test files sit **next to the source** file with the same stem (`<Name>.test.tsx` beside `<Name>.tsx`, or `<name>.test.ts` beside `<name>.ts`). No separate `__tests__/` tree.

**Naming:**
- Python unit tests: `test_<module_under_test>.py`. Example: `test_retry.py` tests `retry.py`.
- Python integration tests: `test_<thing>_integration.py` inside the collector `tests/` folder, or co-located in `integration_tests/` (e.g., `integration_tests/storage/test_feed_store_integration.py`).
- Python test classes: `Test<Behavior>` — each scenario gets its own class with a docstring describing the scope (e.g., `TestRetrySuccessFirstAttempt`, `TestRetryOnRetryable`, `TestProcessFeedFenceViolation`).
- Python test methods: `test_<behavior>_<expected>` (`test_returns_result_on_first_attempt`, `test_exhausts_max_retries_and_raises`, `test_shutdown_skips_individual_release`).
- Frontend tests: `describe('<ComponentOrFunction>', () => { it('<behavior>', ...) })` nested structure.

**Structure:**
```
backend/pipeline/ingestion/
├── retry.py
├── router.py
├── normalizer_runtime.py
├── settings.py
├── quarantine_telemetry.py
├── tests/                            # unit tests — pure, no external I/O
│   ├── test_retry.py
│   ├── test_router.py
│   ├── test_runtime.py
│   ├── test_settings.py
│   └── test_quarantine_telemetry.py
└── collectors/
    ├── icecast/icecast_collector.py
    └── tests/
        ├── test_icecast_collector.py              # unit
        └── test_icecast_collector_integration.py  # testcontainers

integration_tests/
├── conftest.py                       # shared session-scoped postgres_container + db_pool fixtures
├── storage/                          # component tests against real AlloyDB Omni
├── api/                              # HTTP tests against running services
└── e2e/                              # full Docker Compose flow
```

## Test Structure

**Suite Organization:**
Python tests use `unittest.TestCase` (sync) or `unittest.IsolatedAsyncioTestCase` (async), grouped one class per scenario. From `backend/pipeline/ingestion/tests/test_retry.py:13-24`:

```python
class TestRetrySuccessFirstAttempt(unittest.IsolatedAsyncioTestCase):
    """Tests for retry_with_lease_check when fn succeeds immediately."""

    async def test_returns_result_on_first_attempt(self) -> None:
        fn = mock.AsyncMock(return_value="ok")
        result = await retry_with_lease_check(
            fn,
            lease_lost=asyncio.Event(),
            shutdown=asyncio.Event(),
        )
        self.assertEqual(result, "ok")
        fn.assert_awaited_once()
```

For HTTP handlers built on aiohttp, inherit `aiohttp.test_utils.AioHTTPTestCase` and override `get_application()`. From `backend/pipeline/ingestion/tests/test_health_server.py:30-47`:

```python
class HealthzHandlerTests(AioHTTPTestCase):
    async def get_application(self) -> web.Application:
        self.settings = _fake_settings()
        self.state = HealthState()
        return build_app(self.settings, self.state)

    async def _get_healthz(self) -> tuple[int, dict]:
        resp = await self.client.request("GET", "/healthz")
        body = await resp.json()
        return resp.status, body
```

For FastAPI handlers, use `fastapi.testclient.TestClient` with dependency overrides (`backend/services/feeds/tests/test_api.py:19-30`):

```python
class TestFeedsAPI(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_service = AsyncMock()
        app.state.feed_service = self.mock_service
        app.dependency_overrides[verify_oidc_token] = skip_auth
        self.client = TestClient(app)

    def tearDown(self) -> None:
        app.dependency_overrides.clear()
```

Integration tests (component-level) use pytest-style `async def` functions with fixtures from `integration_tests/conftest.py`.

**Patterns:**
- **Setup pattern:** `setUp`/`setUpClass` for unittest-style; pytest `@pytest.fixture` for integration tests (`integration_tests/storage/test_feed_store_integration.py:15-19`).
- **Teardown pattern:** `tearDown` to reset module state (e.g., `quarantine_telemetry._client = None` in `test_quarantine_telemetry.py:13-15`) and to clear FastAPI `dependency_overrides`.
- **Docstrings on every test method** state the contract being verified — grep for the pattern, virtually every test has one.
- **`subTest` for table-driven variants** (`test_router.py:32-48`): iterate over the registry and assert each entry routes correctly with a unique `subTest(source_type=...)` label.
- **Deterministic IDs** as module-level constants so assertions are readable: `_WORKER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")`, `_FEED_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")` in `test_runtime.py:24-25`.

## Mocking

**Framework:** `unittest.mock` — `MagicMock`, `AsyncMock`, and `mock.patch` / `mock.patch.object` / `mock.patch.dict`. Frontend uses `vi.mock` and `vi.fn`.

**Patterns:**

Patch context managers at the call site (not at the definition site). From `backend/pipeline/ingestion/tests/test_runtime.py:49-64`:
```python
def _mock_pubsub_publish(message_id: str = "test-message-id") -> mock._patch:
    """Patch publish_audio_chunk to return a fixed message id (at call site)."""
    return mock.patch(
        "backend.pipeline.ingestion.normalizer_runtime.gcp_helper.publish_audio_chunk",
        new_callable=mock.AsyncMock,
        return_value=message_id,
    )
```

Patch a registry dict for scoped substitution (`test_router.py:41-48`):
```python
with mock.patch.dict(
    "backend.pipeline.ingestion.router._COLLECTORS",
    {source_type: (mock_fn, url_base)},
):
    result = route_capturer(feed, shutdown_event)
mock_fn.assert_called_once_with(feed, shutdown_event, url_base)
```

Use `AsyncMock` with `side_effect` lists for multi-call sequences (`test_retry.py:41-52`):
```python
fn = mock.AsyncMock(side_effect=[OSError("fail"), "ok"])
result = await retry_with_lease_check(
    fn, lease_lost=asyncio.Event(), shutdown=asyncio.Event(),
    max_retries=2, base_delay_sec=0.0, retryable=(OSError,),
)
self.assertEqual(fn.await_count, 2)
```

Frontend module-level mock using `vi.mock` (`frontend/transcription-ui/src/components/audio/AudioPlayer.test.tsx:29-34`):
```typescript
vi.mock('howler', () => ({
  Howl: function (opts: MockHowlOptions) {
    mockCapturedOptions = opts;
    return mockHowlInstance;
  },
}));
```

Frontend global fetch stubbing (`frontend/transcription-ui/src/service/listFeeds.test.ts:8-11`):
```typescript
beforeEach(() => {
  mockFetch.mockClear();
  vi.stubGlobal('fetch', mockFetch);
});
```

**What to Mock:**
- Outbound GCP clients (`gcs_client`, `pubsub_client`, `MonitoringClient`) and any I/O that crosses a process boundary.
- AlloyDB in unit tests via a mock `asyncpg.Pool` (see `_make_pool` helper in `backend/pipeline/storage/tests/test_feed_store.py:31-42`) — real pools are used only in `integration_tests/`.
- Time/sleep behavior is controlled by setting `base_delay_sec=0.0` on retry helpers, NOT by patching `asyncio.sleep` (keeps event-loop semantics intact).
- `os._exit` when a test needs to observe fence violation without tearing the interpreter down (`test_runtime.py:249-251`).

**What NOT to Mock:**
- `asyncio.Event` — create real events; `Event` is cheap and faithful.
- The module-under-test's own internal helpers — drive them via their public interface.
- Pure data types (`CapturedChunk`, `LeasedFeed`) — build real instances via `_make_*` helpers.
- Loggers — use `self.assertLogs("module.name", level=logging.ERROR)` instead (`test_quarantine_telemetry.py:21-24`).

## Fixtures and Factories

**Test Data:**
In-file factory functions named `_make_*` sit at the top of each test module. From `backend/pipeline/ingestion/tests/test_runtime.py:28-46`:
```python
def _make_captured_chunk(audio_bytes: bytes) -> CapturedChunk:
    """Build a CapturedChunk with a current timestamp and a 15-second window."""
    now = datetime.datetime.now(datetime.UTC)
    return CapturedChunk(
        audio_bytes=audio_bytes,
        chunk_start_time=now,
        chunk_end_time=now + datetime.timedelta(seconds=CHUNK_DURATION_SECONDS),
    )

_FEED = LeasedFeed(
    id=_FEED_ID, name="Test Feed", source_type=SourceType.BCFY_FEEDS,
    last_processed_filename=None, last_bookmark_time=None,
    fencing_token=1, source_feed_id="123",
)
```

Settings factories accept `**overrides` and merge over a real defaults dict (`test_runtime.py:67-108`):
```python
def _make_settings(**overrides) -> mock.MagicMock:
    defaults = {"worker_id": _WORKER_ID, "max_feeds_per_worker": 250, ...}
    defaults.update(overrides)
    m = mock.MagicMock()
    m.configure_mock(**defaults)
    return m
```

Integration-test pool fixture — session-scoped container + per-test pool (`integration_tests/conftest.py:31-97`):
```python
@pytest.fixture(scope="session")
def postgres_container() -> Generator[dict[str, Any]]:
    container = PostgresContainer(image="google/alloydbomni:15", ...)
    container.start()
    # ... run schema SQL files from terraform/modules/alloydb/sql/ingestion
    yield {...}
    container.stop()

@pytest.fixture
async def db_pool(postgres_container) -> AsyncIterator[asyncpg.Pool]:
    pool = await create_pool(...)
    yield pool
    await pool.close()
```

Frontend fixture — React Query provider helper (`frontend/transcription-ui/src/test/testUtils.tsx:4-15`):
```typescript
export const renderWithQueryClient = (ui: React.ReactElement) => {
  const testQueryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={testQueryClient}>{ui}</QueryClientProvider>
  );
};
```

**Location:**
- Python unit-test factories: inline at the top of each test file, private (leading underscore).
- Python integration-test fixtures: `integration_tests/conftest.py` (session-scoped container), per-test fixtures inline in each test file.
- Frontend shared test utilities: `frontend/transcription-ui/src/test/testUtils.tsx`.
- No backend `conftest.py` — unit tests avoid shared fixtures; each file is self-contained.

## Coverage

**Requirements:** None enforced. `pytest-cov` is available as a dev dependency (`pyproject.toml:61`) but no `--cov-fail-under` threshold is configured and CI does not gate on coverage.

**View Coverage:**
```bash
uv run pytest --cov=backend --cov-report=term-missing backend/
uv run pytest --cov=backend --cov-report=html backend/      # writes htmlcov/
```

## Test Types

**Unit Tests:**
- Scope: single module, no external I/O. Network, DB, GCS, Pub/Sub are mocked.
- Located co-located under `tests/` (see "Test File Organization").
- Discovered by `python -m unittest discover backend/pipeline` (mise `test:unit` task).
- Fast — safe to run the full backend unit suite during development.

**Integration Tests:**
- Three tiers, all in `integration_tests/`:
  1. **Component tests** (`integration_tests/storage/`) — real AlloyDB Omni container via `testcontainers.postgres.PostgresContainer(image="google/alloydbomni:15")`. Schema SQL from `terraform/modules/alloydb/sql/ingestion/*.sql` applied once per session.
  2. **API tests** (`integration_tests/api/`) — HTTP calls against running services.
  3. **E2E tests** (`integration_tests/e2e/`) — full pipeline under Docker Compose, exercising Pub/Sub emulator + Rules + Notification + Mock server.
- Collector-level integration tests also live next to their collector (e.g., `backend/pipeline/ingestion/collectors/tests/test_icecast_collector_integration.py`) because they need the collector's internal fixtures. They use `PostgresContainer` plus `fsouza/fake-gcs-server` via `testcontainers.core.container.DockerContainer` (see that file, lines 56-100).
- Every integration test class is guarded by `@unittest.skipUnless(_docker_available(), "Docker is not available")` so suites pass on machines without Docker.

**E2E Tests:**
- Pytest-driven under `integration_tests/e2e/` (`test_rules_creation_evaluation_publish.py`, `test_notification_redis.py`). Orchestrated by `docker-compose.yml` and `mise run test:e2e` which runs `docker compose run --rm integration-tests` and then tears down the stack.
- The frontend does not use Playwright/Cypress — no browser-level E2E tests at present.

## Common Patterns

**Async Testing:**
Subclass `unittest.IsolatedAsyncioTestCase` and write `async def test_*`. No decorator. From `backend/pipeline/ingestion/tests/test_retry.py:54-66`:
```python
class TestRetryOnRetryable(unittest.IsolatedAsyncioTestCase):
    async def test_exhausts_max_retries_and_raises(self) -> None:
        fn = mock.AsyncMock(side_effect=OSError("fail"))
        with self.assertRaises(OSError):
            await retry_with_lease_check(
                fn, lease_lost=asyncio.Event(), shutdown=asyncio.Event(),
                max_retries=2, base_delay_sec=0.0, retryable=(OSError,),
            )
        self.assertEqual(fn.await_count, 3)      # 1 initial + 2 retries
```

For pytest-style integration tests, `asyncio_mode = "auto"` means `async def test_*` just works (`integration_tests/storage/test_feed_store_integration.py:15-19`).

**Error Testing:**
`with self.assertRaises(Exc):` for type-only checks, `assertRaisesRegex(Exc, r"pattern")` when the message matters. From `test_router.py:62-66`:
```python
with self.assertRaises(ValueError) as ctx:
    route_capturer(feed, shutdown_event)
self.assertIn("bcfy_calls", str(ctx.exception))
```

For asserting a log line was emitted, use `assertLogs` (`test_quarantine_telemetry.py:21-37`):
```python
with self.assertLogs(
    "backend.pipeline.ingestion.quarantine_telemetry",
    level=logging.ERROR,
) as cm:
    await quarantine_telemetry.emit_quarantine_event(
        feed_id="abc-123", feed_name="Test Feed", source_type="bcfy_feeds",
    )
self.assertEqual(len(cm.records), 1)
record = cm.records[0]
self.assertEqual(getattr(record, "event_type"), "feed_quarantined")
```

Frontend error testing via `rejects.toThrow` (`listFeeds.test.ts:38-48`):
```typescript
mockFetch.mockResolvedValueOnce({ ok: false, status: 500, statusText: 'Internal Server Error' });
await expect(listFeeds('tokenXYZ')).rejects.toThrow('Error: 500 Internal Server Error');
```

---

*Testing analysis: 2026-04-21*

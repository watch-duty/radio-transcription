# Testing Patterns

**Analysis Date:** 2026-06-14

## Test Framework

**Python Runner:**
- `pytest` is the main test runner.
- `pytest-asyncio` supports async runtime/storage tests.
- `pytest-xdist`, `pytest-cov`, `testcontainers`, `docker`, and `fakeredis`
  are available for broader or integration-oriented tests.

**Frontend Runner:**
- Vitest is used in `frontend/api` and `frontend/transcription-ui`.
- Testing Library and jsdom are used for React component tests.
- Frontend tests live beside components/services.

**Assertion Libraries:**
- Python uses pytest assertions and monkeypatch/fakes.
- Frontend uses Vitest `expect` and Testing Library matchers.

## Run Commands

```bash
safe-run -- python3 -m pytest <target> -q
safe-run -- uv run python -m pytest <target> -q
safe-run -- yarn --cwd frontend/api test <target>
safe-run -- yarn --cwd frontend/transcription-ui test <target>
git diff --check
```

For docs-only changes, prefer `git diff --check`.

Do not run broad local E2E, API, component, Docker, testcontainers, or full
integration-stack commands unless the user explicitly asks and confirms the
machine is prepared.

## Test File Organization

**Python:**
- Backend tests usually live under `tests/` beside the package:
  - `backend/pipeline/ingestion/tests/`
  - `backend/pipeline/ingestion/collectors/tests/`
  - `backend/pipeline/storage/tests/`
  - `backend/services/feeds/tests/`
- Some packages keep test files directly beside code, such as
  `backend/pipeline/notification/test_*.py`.
- Golden log/event payloads live under
  `backend/pipeline/ingestion/tests/golden/`.

**Frontend:**
- Component and service tests sit beside source files as `*.test.ts` or
  `*.test.tsx`, for example
  `frontend/transcription-ui/src/components/feeds/FeedTable.test.tsx`.
- API proxy controller tests live beside controllers under
  `frontend/api/src/{domain}/`.

**Integration:**
- Resource-heavy integration tests live under `integration_tests/`.
- Local stack tests depend on Docker Compose services, emulators, Postgres,
  Redis, mock servers, and generated protos.

## Test Structure

**Python Patterns:**
- Tests use direct function/method calls with fake dependencies where possible.
- Async tests use `pytest.mark.asyncio` or async fixtures.
- Storage tests use helper utilities under
  `backend/pipeline/storage/tests/connection_util.py`.
- Runtime tests isolate `CollectorRuntime` helpers and mocked store/client
  behavior rather than starting the full worker stack.

**Frontend Patterns:**
- Component tests render components through Testing Library.
- Service tests validate request construction and response/error handling.
- API controller tests exercise Express/tsoa route behavior with mocked
  upstream calls.

## Mocking

**Python:**
- Use monkeypatch, fake clients, and small test doubles for GCS, Pub/Sub,
  storage, HTTP responses, and source endpoints.
- Avoid mocking pure helpers when a direct input/output test is practical.
- Integration tests may use real local containers/emulators, but those are not
  the default validation path.

**Frontend:**
- Use Vitest mocks for API calls, auth/session helpers, and browser APIs.
- Shared test helpers live in `frontend/transcription-ui/src/test/`.

## Fixtures and Factories

**Python:**
- Collector tests define local fake responses and feed dictionaries close to
  the tests.
- Golden structured-log payloads live in
  `backend/pipeline/ingestion/tests/golden/`.
- Local mock audio files live in `local_dev/mock_audio/` and
  `backend/pipeline/normalization/tests/test_data/`.

**Frontend:**
- UI tests use local fixtures/factories inside test files and shared helpers in
  `frontend/transcription-ui/src/test/testUtils.tsx`.

## Coverage

**Requirements:**
- No single global coverage threshold was observed in repo config.
- The practical standard is focused coverage for changed behavior, especially
  around runtime state transitions, SQL/store effects, API responses, and
  frontend status mappings.

**High-Value Areas:**
- Feed lifecycle and quarantine policy in storage/runtime tests.
- Collector classification and item-to-feed promotion tests.
- Protobuf compatibility and generated schema consumers.
- API status mapping and frontend status rendering.

## Test Types

**Unit Tests:**
- Scope: pure helpers, service classes, runtime policy helpers, classifiers,
  and API controller logic.
- Default choice for agent-run validation.

**Integration Tests:**
- Scope: storage, local APIs, and end-to-end pipeline behavior.
- Location: `integration_tests/` and selected `*_integration.py` collector
  tests.
- Require explicit user approval before local runs.

**E2E / Local Stack:**
- Scope: Docker Compose stack with Pub/Sub, GCS, Postgres, Redis, mock audio,
  ingestion, normalization, transcription, evaluation, notification, APIs, and
  frontend proxy.
- Command surfaces are in `.mise.toml` and `docker-compose.yml`.
- Prefer CI for full-stack validation.

## Common Patterns

**Async Testing:**
```python
async def test_record_source_observation_clears_failure_state(...):
    result = await store.record_source_observation(...)
    assert result["recorded"] is True
```

**Error Testing:**
```python
with pytest.raises(ValueError):
    FeedFailure("unknown", "reason")
```

**Runtime Policy Tests:**
- Assert which store method is called.
- Assert structured log fields where event contracts matter.
- Assert `feed_quarantined` is emitted only for real quarantine transitions.

**Storage Tests:**
- Verify database state after each store operation, not just return values.
- Include `failure_count`, `status`, `retry_after`, `status_reason`,
  `worker_id`, and fencing token behavior when relevant.

## Local Test Safety

The repo explicitly warns that broad local tests can exhaust developer
machines. Agent defaults:

- For docs-only changes, run `git diff --check`.
- For code changes, run the narrowest relevant tests.
- Avoid unscoped `uv run pytest`, `uv run pytest integration_tests/`,
  `mise run test:e2e`, `mise run test:component`, `mise run test:api`, and
  Docker Compose integration-test runs unless explicitly approved.

---

*Testing analysis: 2026-06-14*
*Update when test patterns change*

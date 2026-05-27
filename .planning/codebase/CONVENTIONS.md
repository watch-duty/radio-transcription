# Coding Conventions

**Analysis Date:** 2026-05-27

## Naming Patterns

**Files:**
- Use `snake_case.py` for Python implementation and test modules: `backend/pipeline/storage/feed_store.py`, `backend/pipeline/ingestion/normalizer_runtime.py`, `backend/pipeline/storage/tests/test_feed_store.py`.
- Use package-local `tests/` directories for most Python unit tests: `backend/pipeline/ingestion/tests/`, `backend/pipeline/normalization/tests/`, `backend/services/feeds/tests/`.
- Keep integration tests under the top-level `integration_tests/` tree with purpose-specific subdirectories: `integration_tests/storage/test_feed_store_integration.py`, `integration_tests/api/test_transcripts_api.py`, `integration_tests/e2e/test_transcription_pipeline.py`.
- Use `PascalCase.tsx` for React components and matching `.test.tsx` files: `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`, `frontend/transcription-ui/src/components/transcripts/TranscriptView.test.tsx`.
- Use `camelCase.ts` for TypeScript service/util modules and matching `.test.ts`: `frontend/transcription-ui/src/service/listTranscripts.ts`, `frontend/transcription-ui/src/service/listTranscripts.test.ts`.
- Use `*Controller.ts` for TSOA controllers and co-located `*Controller.test.ts` tests: `frontend/api/src/transcripts/transcriptsController.ts`, `frontend/api/src/transcripts/transcriptsController.test.ts`.
- Keep shared TypeScript contracts in plural domain files under `frontend/common/src/types/`: `frontend/common/src/types/transcripts.ts`, `frontend/common/src/types/feeds.ts`.
- Keep model/SFT CLI code under `model/scripts/sft/` with small adapter modules under `model/scripts/sft/adapters/`: `model/scripts/sft/pipeline.py`, `model/scripts/sft/preflight.py`, `model/scripts/sft/adapters/gcs_manifest.py`.

**Functions:**
- Use `snake_case` for Python functions and private helpers; prefix module-private helpers with `_`: `backend/pipeline/ingestion/router.py` uses `supported_source_types()`, `resolve_topic_path()`, and `route_capturer()`, while `model/scripts/sft/pipeline.py` uses `_load_registry()`, `_make_adapter()`, and `_build_split_jsonl()`.
- Use `async def` for I/O and pipeline coordination in Python when the called dependencies are async: `backend/pipeline/storage/feed_store.py`, `backend/pipeline/storage/connection.py`, `backend/pipeline/ingestion/retry.py`.
- Use `camelCase` for TypeScript functions and component props: `frontend/api/src/transcripts/transcriptsController.ts` uses `convertTranscriptResponse()`, `frontend/transcription-ui/src/service/listTranscripts.ts` exports `listTranscripts()`.
- Name React components in `PascalCase` and export the component function directly: `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`, `frontend/transcription-ui/src/components/feeds/FeedTable.tsx`.
- For tests, name Python methods/functions by behavior with `test_...`: `backend/pipeline/ingestion/tests/test_retry.py`, `integration_tests/storage/test_feed_store_integration.py`. TypeScript uses `describe()` groups and `it('should ...')` behavior strings in `frontend/api/src/transcripts/transcriptsController.test.ts`.

**Variables:**
- Use `snake_case` for Python locals, arguments, and fields: `feed_id`, `worker_id`, `last_bookmark_time` in `backend/pipeline/storage/feed_store.py`.
- Use `_UPPER_CASE` or `UPPER_CASE` module constants for stable configuration and test fixtures: `_FEED_ID` in `backend/pipeline/storage/tests/test_feed_store.py`, `DEFAULT_REFRESH_INTERVAL` in `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`, `PREFLIGHT_TOKEN_CAP` in `model/scripts/sft/preflight.py`.
- Prefix internal Python instance attributes with `_`: `FeedStore._pool` in `backend/pipeline/storage/feed_store.py`, `NormalizerRuntime._shutdown` in `backend/pipeline/ingestion/normalizer_runtime.py`.
- Use `camelCase` for TypeScript locals and serialized API-facing model names in TS: `queryParams`, `isAlert`, `startTimestamp` in `frontend/api/src/transcripts/transcriptsController.ts` and `frontend/common/src/types/transcripts.ts`.
- Preserve backend wire formats at API boundaries, then convert them into frontend `camelCase`: `TranscriptResponse` uses `feed_id`, `transmission_id`, and `start_timestamp`, while `Transcript` uses `feedId`, `transmissionId`, and `startTimestamp` in `frontend/api/src/transcripts/transcriptsController.ts`.

**Types:**
- Use Python `enum.StrEnum` for string-valued domain enums: `SourceType` and `FeedStatus` in `backend/pipeline/storage/feed_store.py`, `AudioMimeType` in `backend/pipeline/ingestion/models.py`.
- Use `TypedDict` for dict-shaped records returned by storage and evaluators: `LeasedFeed`, `HeartbeatResult`, and `Feed` in `backend/pipeline/storage/feed_store.py`; `EvaluationResult` in `backend/pipeline/evaluation/rules_evaluation/evaluator.py`.
- Use frozen dataclasses for immutable domain payloads and configuration: `CapturedChunk` and `CaptureResources` in `backend/pipeline/ingestion/models.py`, `AudioChunkData` and `NormalizeAudioConfig` in `backend/pipeline/normalization/common/datatypes.py`, `CanonicalRow` in `model/colabs/common/manifest.py`.
- Use Pydantic models for FastAPI request/response bodies: `backend/services/feeds/models.py`, `backend/services/transcripts/models.py`, `backend/services/audio_segments/models.py`.
- Use TypeScript `interface` for object contracts and `type` for unions/compositions: `frontend/common/src/types/transcripts.ts`, `frontend/common/src/types/feeds.ts`, `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`.
- Use `TYPE_CHECKING` imports to avoid runtime dependency costs and circular imports in Python: `backend/pipeline/ingestion/router.py`, `backend/services/feeds/service.py`, `integration_tests/conftest.py`.

## Code Style

**Formatting:**
- Python formatting is Ruff-managed with `line-length = 80` and `target-version = "py313"` in `pyproject.toml`.
- Run Python formatting with `uv run ruff format` or `mise run format:ruff` from `.mise.toml`.
- TypeScript formatting is Prettier-managed by `.prettierrc`: semicolons enabled, single quotes, trailing commas where valid in ES5, `printWidth` 80, and `tabWidth` 2.
- Frontend imports are sorted by `@trivago/prettier-plugin-sort-imports` using per-tree orders in `.prettierrc`.
- Notebook formatting is handled by `mise run format:notebooks` and validated by `mise run lint:notebooks` in `.mise.toml`; notebooks under `model/colabs/**/*.ipynb` are excluded from Ruff source linting in `pyproject.toml`.
- Terraform formatting is part of `mise run format` via `terraform fmt -recursive` in `.mise.toml`.

**Linting:**
- Python linting uses Ruff `select = ["ALL"]` with a curated ignore list in `pyproject.toml`; future Python code should satisfy Ruff unless the surrounding subtree has a specific per-file ignore.
- Ruff import sorting treats `backend` and `local_dev` as first-party in `pyproject.toml`.
- `ty check` is the Python type-checker in `.mise.toml` and `.pre-commit-config.yaml`; `tool.ty.src.exclude = ["model/"]` keeps model code outside the root Ty check.
- Model colab Python code is intentionally exempt from root Ruff rules via `pyproject.toml`, while `model/pyproject.toml` defines a separate `common` package and pytest config for `model/colabs/common/tests`.
- `model/scripts/sft/**.py` has a relaxed Ruff profile in `pyproject.toml`; keep CLI scripts readable and tested, but do not assume the stricter backend annotation and branch-count rules apply there.
- `frontend/api/eslint.config.js` uses ESLint flat config with `@eslint/js`, `typescript-eslint`, Node globals, and `eslint-config-prettier`.
- `frontend/transcription-ui/eslint.config.js` adds React hooks, React Refresh, TanStack Query, browser globals, CSS linting, and Prettier compatibility.
- Pre-commit runs proto generation, Ruff check/format, Ty, notebook linting, API/UI ESLint, Prettier, TypeScript checks, TSOA route generation, and OpenAPI spec verification via `.pre-commit-config.yaml`.

## Import Organization

**Order:**
1. Standard library imports first. Python examples: `backend/pipeline/storage/feed_store.py`, `backend/pipeline/ingestion/models.py`. TypeScript examples: `frontend/api/src/auth/authController.ts` uses type-only `express` import before third-party imports.
2. Third-party imports next. Python examples: `asyncpg`, `pytest`, `aiohttp`, `google.cloud` in `backend/pipeline/storage/feed_store.py`, `integration_tests/conftest.py`, and `backend/pipeline/normalization/audio/audio_processor.py`. TypeScript examples: `express`, `google-auth-library`, `tsoa`, React, MUI, and TanStack Query imports in `frontend/api/src/transcripts/transcriptsController.ts` and `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`.
3. First-party imports last. Python examples import from `backend.pipeline...` after third-party imports in `backend/pipeline/storage/feed_store.py`; TypeScript uses `@transcription/common` then relative imports in `frontend/api/src/transcripts/transcriptsController.ts`.
4. Relative imports are last in TypeScript, with local `./` before `../` in `frontend/api` according to `.prettierrc`; React UI groups React, third-party modules, `@...` aliases, relative non-CSS imports, then CSS.

**Path Aliases:**
- Python uses repository-root imports such as `backend.pipeline.storage.feed_store` and `integration_tests.feed_utils`; `.mise.toml` sets `PYTHONPATH = "."`.
- Model common code is imported as `common.*` when running under `model/`; `model/pyproject.toml` maps package `common` to `model/colabs/common`.
- Frontend shared package is imported as `@transcription/common` from both `frontend/api/src/` and `frontend/transcription-ui/src/`.
- The frontend UI ESLint/Prettier config reserves the `^@` import group in `.prettierrc`, but source currently uses package imports like `@tanstack/react-query`, `@mui/material`, and `@transcription/common` rather than a local `@/` alias.

## Error Handling

**Patterns:**
- Raise typed domain exceptions when callers need semantic handling. Use `FeedAlreadyExistsError`, `FeedNameAlreadyExistsError`, and `AlreadyExistsError` from `backend/pipeline/common/exceptions.py`.
- Convert storage/domain exceptions to HTTP errors at FastAPI boundaries. `backend/services/feeds/main.py` maps `ValueError` to `400`, duplicate feed exceptions to `409`, missing resources to `404`, and successful deletes to `204`.
- Convert backend proxy failures to `HttpError` at TypeScript controller boundaries. `frontend/api/src/utils.ts` normalizes `GaxiosError` and unknown errors; controllers like `frontend/api/src/transcripts/transcriptsController.ts` catch `unknown`, call `handleBackendError()`, and throw `HttpError`.
- Validate external or persisted enum strings before constructing domain records. `backend/pipeline/storage/feed_store.py` converts row strings to `SourceType` and `FeedStatus`, then raises `ValueError` with context if an unknown value appears.
- Prefer fail-loud behavior for malformed model/evaluation inputs. `model/colabs/common/manifest.py` raises `ValueError` for missing prediction and ground-truth keys in `merge_predictions_to_manifest()`, while `load_manifest()` soft-fails unreadable or malformed manifest files to `[]` with logs.
- Async runtime retry loops should preserve cancellation and lease-loss semantics. `backend/pipeline/ingestion/retry.py` raises `LeaseExpiredError` when heartbeat state is lost and raises `asyncio.CancelledError` when shutdown is set.
- CLI workflows should return integer status codes and log clean user-facing messages for expected failures. `model/scripts/sft/pipeline.py` returns `1` for missing prompt override files and unknown dataset config rather than printing tracebacks.
- Do not suppress `CancelledError` in collectors; `backend/pipeline/ingestion/models.py` documents that capture functions must use `try/finally` and never suppress cancellation.

## Logging

**Framework:** Python `logging`, Google Cloud Logging, OpenTelemetry trace helpers, and TypeScript `console` JSON logs.

**Patterns:**
- Use `logger = logging.getLogger(__name__)` for normal Python modules: `backend/pipeline/storage/feed_store.py`, `backend/pipeline/evaluation/processor.py`, `model/colabs/common/manifest.py`, `model/scripts/sft/preflight.py`.
- Initialize backend logging once with `setup_logging()` from `backend/pipeline/common/logging.py`; it uses Cloud Logging and tracing in GCP and `logging.basicConfig(..., force=True)` locally.
- Use structured JSON Dataflow logs with contextual `LoggerAdapter`s from `backend/pipeline/normalization/common/logging.py`. Use `get_task_logger(__name__, {"system": "...", "component": "..."})` as in `backend/pipeline/normalization/audio/audio_processor.py`.
- Use `%s`-style logging in stricter backend code to satisfy logging lint rules, as in `backend/pipeline/storage/connection.py`.
- Some relaxed model/SFT files use f-string logging because `model/scripts/**.py` and `model/colabs/**/*.py` have per-file Ruff ignores in `pyproject.toml`; do not copy that style into strict backend modules.
- In TypeScript proxy code, log backend failures as JSON through `console.error(JSON.stringify(...))` in `frontend/api/src/utils.ts`; the Express fallback logs raw errors in `frontend/api/src/index.ts`.
- In the React UI, keep console logging limited to exceptional client-side failures such as auth/session refresh errors in `frontend/transcription-ui/src/context/AuthProvider.tsx`.

## Comments

**When to Comment:**
- Comment domain invariants and operational contracts where future changes can silently break production behavior. Examples: the capture/runtime contract in `backend/pipeline/ingestion/models.py`, the `SourceType` three-place change warning in `backend/pipeline/storage/feed_store.py`, and the SFT hard-gate contract in `model/scripts/sft/preflight.py`.
- Use comments to document non-obvious platform constraints. Examples: PgBouncer transaction-mode limitations in `backend/pipeline/storage/connection.py`, JSDOM media stubs in `frontend/transcription-ui/src/test/setup.ts`, and single-dataset JSONL reuse in `model/scripts/sft/pipeline.py`.
- Keep comments near the code they constrain; do not duplicate broad architecture prose in implementation files unless it prevents a known class of regression.
- Tests may include regression comments when they encode a previously fragile invariant, as in `model/colabs/common/tests/test_manifest.py` and `backend/pipeline/ingestion/tests/test_runtime.py`.

**JSDoc/TSDoc:**
- Use TSOA decorators and class/interface comments for API contract generation, especially in `frontend/api/src/*/*Controller.ts`.
- Use short JSDoc comments for request/query classes when decorators need metadata, such as `ListTranscriptsQueryParams.limit` in `frontend/api/src/transcripts/transcriptsController.ts`.
- React component files mostly avoid exported TSDoc and rely on prop interfaces, clear component names, and tests: `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`.
- Python public functions/classes often include Google-style docstrings, and Ruff uses `pydocstyle` with `convention = "google"` in `pyproject.toml`; docstring-required rules are ignored, but new complex public APIs should still document args, returns, raises, and invariants.

## Function Design

**Size:** Prefer small boundary functions and helpers for routine code, but expect larger orchestrators in pipeline/runtime areas. `backend/pipeline/ingestion/router.py` is a compact registry module, while `backend/pipeline/ingestion/normalizer_runtime.py` and `backend/pipeline/normalization/transforms/stateful.py` contain large stateful orchestration functions with explicit `noqa` complexity exemptions.

**Parameters:** Use typed parameters throughout strict backend code. Group related configuration into dataclasses or settings objects instead of long untyped argument lists: `StitchAudioConfig` in `backend/pipeline/normalization/common/datatypes.py`, `CaptureResources` in `backend/pipeline/ingestion/models.py`, `AlloyDBSettings` in `backend/pipeline/storage/settings.py`.

**Return Values:** Use explicit domain return values. Storage methods return `bool` for fenced success/failure (`backend/pipeline/storage/feed_store.py`), `TypedDict` rows for record payloads, and `None` for not-found service results (`backend/services/feeds/service.py`). API handlers return Pydantic models or raise `HTTPException` (`backend/services/feeds/main.py`). TypeScript service wrappers return typed shared contracts such as `ListTranscriptsResponse` in `frontend/transcription-ui/src/service/listTranscripts.ts`.

## Module Design

**Exports:** Keep modules domain-focused and avoid broad barrels except for intentionally shared TypeScript types. `frontend/common/src/index.ts` exports shared API contracts; Python packages generally import concrete modules directly, such as `backend.pipeline.storage.feed_store` or `backend.pipeline.ingestion.router`.

**Barrel Files:** Minimal Python `__init__.py` files mark package boundaries and are usually empty. TypeScript has a real shared barrel in `frontend/common/src/index.ts`; do not add barrels in feature folders unless they simplify a shared package boundary.

**Dependency Injection:** Prefer injecting external clients and factories at boundaries to keep tests isolated. Examples include `AudioProcessor` factories in `backend/pipeline/normalization/audio/audio_processor.py`, `FeedService(store)` in `backend/services/feeds/service.py`, and mocked Google clients in `frontend/api/src/auth/authController.test.ts`.

**Generated Code:** Do not hand-edit generated protobuf or TSOA output. Protobuf output lives under `backend/pipeline/schema_types/` and is generated by `mise run generate:protos`; TSOA routes are generated into `frontend/api/src/generated/` by `yarn --cwd frontend/api generate-routes`.

---

*Convention analysis: 2026-05-27*

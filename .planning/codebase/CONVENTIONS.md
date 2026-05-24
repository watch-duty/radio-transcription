# Coding Conventions

**Analysis Date:** 2026-05-24

## Naming Patterns

**Files:**
- Python modules use `snake_case.py` and packages use lower-case directories. Examples: `backend/pipeline/ingestion/settings.py`, `backend/pipeline/storage/feed_store.py`, `backend/pipeline/normalization/transforms/stitcher_engine.py`.
- Python tests use `test_*.py` under nearby `tests/` packages or alongside the implementation for small modules. Examples: `backend/pipeline/ingestion/tests/test_router.py`, `backend/pipeline/storage/tests/test_feed_store.py`, `backend/pipeline/notification/test_send_notification.py`.
- TypeScript API controller files use feature directories and camelCase controller names. Examples: `frontend/api/src/feeds/feedsController.ts`, `frontend/api/src/rules/rulesController.ts`, `frontend/api/src/transcripts/transcriptsController.ts`.
- React components use PascalCase filenames. Examples: `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`, `frontend/transcription-ui/src/components/audio/AudioDisplay.tsx`.
- Frontend service utilities use camelCase filenames. Examples: `frontend/transcription-ui/src/service/listTranscripts.ts`, `frontend/transcription-ui/src/utils/apiUtils.ts`.
- TypeScript tests use `.test.ts` and `.test.tsx` colocated with the source. Examples: `frontend/api/src/feeds/feedsController.test.ts`, `frontend/transcription-ui/src/components/transcripts/TranscriptView.test.tsx`.
- Generated files are excluded from formatting/linting and should not be hand-edited. Examples: `backend/pipeline/schema_types/*_pb2.py`, `backend/pipeline/schema_types/*_pb2.pyi`, `frontend/api/src/generated/routes.js`, `frontend/api/openapi.yaml`.

**Functions:**
- Python functions and methods use `snake_case` with explicit return annotations for production code. Examples: `supported_source_types()` and `resolve_topic_path()` in `backend/pipeline/ingestion/router.py`, `create_pool_with_retry()` in `backend/pipeline/storage/connection.py`.
- Python internal helpers use a leading underscore. Examples: `_require_env()` and `_load_caps_from_env()` in `backend/pipeline/ingestion/settings.py`, `_log_retry()` in `backend/pipeline/storage/connection.py`.
- Async Python APIs use `async def` all the way through the storage/service boundary. Examples: `create_pool()` in `backend/pipeline/storage/connection.py`, `FeedService.create_feed()` in `backend/services/feeds/service.py`, FastAPI handlers in `backend/services/feeds/main.py`.
- TypeScript functions use `camelCase`; React components use PascalCase named exports. Examples: `getSourceUrl()` and `convertFeedBackend()` in `frontend/api/src/feeds/feedsController.ts`, `listTranscripts()` in `frontend/transcription-ui/src/service/listTranscripts.ts`, `TranscriptView()` in `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`.
- TypeScript controller methods use public `camelCase` methods decorated by TSOA. Examples: `listFeeds()`, `getFeed()`, `createFeed()`, `resetFeed()`, and `deactivateFeed()` in `frontend/api/src/feeds/feedsController.ts`.

**Variables:**
- Python constants use upper-case names at module scope. Examples: `_DEFAULT_CAPS` in `backend/pipeline/ingestion/settings.py`, `BCFY_FEEDS_URL_BASE` and `OPENMHZ_URL_BASE` in `backend/pipeline/ingestion/router.py`.
- Python module loggers use `logger` or `_logger` from `logging.getLogger(__name__)`. Examples: `logger` in `backend/pipeline/common/logging.py`, `_logger` in `backend/pipeline/storage/connection.py`.
- Python instance internals use leading underscores. Example: `self._store` in `backend/services/feeds/service.py`.
- TypeScript values use `camelCase` and constants use upper-case for module-level fixed values. Examples: `allowedOrigin` in `frontend/api/src/config.ts`, `DEFAULT_REFRESH_INTERVAL` and `FEED_POLLING_INTERVAL_MS` in `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`.
- React state follows `[value, setValue]` naming. Examples: `feedId/setFeedId`, `searchedFeedId/setSearchedFeedId`, and `isAudioPlaying/setIsAudioPlaying` in `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`.

**Types:**
- Python classes, dataclasses, exceptions, and `TypedDict`-style models use PascalCase. Examples: `NormalizerSettings` in `backend/pipeline/ingestion/settings.py`, `AlloyDBSettings` in `backend/pipeline/storage/settings.py`, `FeedAlreadyExistsError` in `backend/pipeline/common/exceptions.py`.
- Python dataclass settings are frozen and keyword-only when representing environment-backed configuration. Examples: `@dataclass(frozen=True, kw_only=True)` in `backend/pipeline/ingestion/settings.py` and `backend/pipeline/storage/settings.py`.
- Python Pydantic/FastAPI request and response models use PascalCase nouns. Examples: `Feed`, `FeedCreate`, and `Tag` in `backend/services/feeds/models.py`.
- TypeScript interfaces and type aliases use PascalCase. Examples: `FeedBackend`, `FeedCreateBackend`, and `BaseFeedBackend` in `frontend/api/src/feeds/feedsController.ts`; `ListTranscriptsPage` and `ListTranscriptsData` in `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`.
- Shared frontend API types live in `frontend/common/src/types/` and are imported from `@transcription/common`. Examples: `frontend/common/src/types/feeds.ts`, `frontend/common/src/types/transcripts.ts`.

## Code Style

**Formatting:**
- Use `mise run format` from the repo root for repository-wide formatting; the task is defined in `.mise.toml` and delegates to Ruff, Terraform fmt, and frontend formatters.
- Python formatting uses Ruff with `line-length = 80`, `target-version = "py313"`, and generated protobuf exclusions in `pyproject.toml`.
- TypeScript/JavaScript formatting uses Prettier with semicolons, single quotes, trailing commas where valid in ES5, `printWidth: 80`, `tabWidth: 2`, and sorted imports through `@trivago/prettier-plugin-sort-imports` in `.prettierrc`.
- Prettier ignores generated/openapi output via `.prettierignore`: `frontend/api/openapi.yaml`, `**/generated/*`, and `dist/`.
- Notebooks under `model/colabs/` are formatted by `mise run format:notebooks`; the task in `.mise.toml` repairs notebook schema fields and runs `uv run ruff format model/colabs`.

**Linting:**
- Use `mise run lint` from the repo root for repository-wide checks; `.agents/instructions.md` makes `mise` the project-standard task runner.
- Python linting uses Ruff `select = ["ALL"]` with a curated ignore list in `pyproject.toml`; type checking uses `ty check` from `.mise.toml`.
- Ruff treats `backend` and `local_dev` as first-party imports through `[tool.ruff.lint.isort]` in `pyproject.toml`.
- Python test files have relaxed annotation/assertion/security rules through `[tool.ruff.lint.per-file-ignores]` for `"**/**/test*.py"` in `pyproject.toml`.
- Model notebooks and Colab helper scripts under `model/colabs/**/*.ipynb` and `model/colabs/**/*.py` are excluded from Ruff lint rules in `pyproject.toml`; use the notebook-specific `mise` tasks for those files.
- Frontend API linting uses ESLint flat config with `@eslint/js`, `typescript-eslint`, and `eslint-config-prettier` in `frontend/api/eslint.config.js`.
- React UI linting uses ESLint flat config with `typescript-eslint`, `@tanstack/eslint-plugin-query`, React Hooks, React Refresh, CSS linting, and Prettier compatibility in `frontend/transcription-ui/eslint.config.js`.
- TypeScript strictness is enforced through `tsc --noEmit` scripts in `frontend/api/package.json` and `frontend/transcription-ui/package.json`; UI app strict options are in `frontend/transcription-ui/tsconfig.app.json`.

## Import Organization

**Order:**
1. Python imports follow future imports, standard library, third-party libraries, then local `backend`/`local_dev` imports. Examples: `backend/pipeline/ingestion/router.py`, `backend/services/feeds/main.py`, `backend/pipeline/storage/connection.py`.
2. TypeScript API imports are sorted by `.prettierrc`: `node:` built-ins, `@google-cloud/`, `express`, third-party modules, then `./` and `../` relative imports. Example: `frontend/api/src/feeds/feedsController.ts`.
3. React UI imports are sorted by `.prettierrc`: React, third-party modules, `@` imports, relative non-CSS imports, then CSS imports. Example: `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`.
4. Type-only imports should use `import type` in TypeScript. Examples: `frontend/api/src/feeds/feedsController.ts`, `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`.
5. Heavy optional model dependencies stay behind function-local imports. Examples: `model/colabs/common/scoring.py`, `model/colabs/common/audio_utils.py`, `model/colabs/common/public_dataset_evaluation.py`.

**Path Aliases:**
- Python code imports repo packages by absolute path from repo root, with `PYTHONPATH = "."` configured in `.mise.toml`. Examples: `from backend.pipeline.storage.feed_store import SourceType` in `backend/pipeline/ingestion/settings.py`.
- Python model package code imports the local model library as `common.*` from `model/colabs/common`; packaging is configured in `model/pyproject.toml`.
- TypeScript frontend packages import shared types from `@transcription/common`, backed by `frontend/common/package.json` and local `file:../common` dependencies in `frontend/api/package.json` and `frontend/transcription-ui/package.json`.
- Frontend API ESM imports include `.js` on relative TypeScript source imports. Examples: `../config.js` and `../utils.js` in `frontend/api/src/feeds/feedsController.ts`.

## Error Handling

**Patterns:**
- Raise `ValueError` for invalid configuration or user-controlled inputs in Python, with a local `msg` variable before raising. Examples: `_require_env()` in `backend/pipeline/ingestion/settings.py`, `resolve_topic_path()` in `backend/pipeline/ingestion/router.py`, `AlloyDBSettings` helpers in `backend/pipeline/storage/settings.py`.
- Chain infrastructure exceptions when converting them to domain-level errors. Example: `create_pool()` in `backend/pipeline/storage/connection.py` raises `TimeoutError` or `ConnectionError` from the original exception.
- Use focused exception handlers around boundary calls, then return simple domain values from service methods. Example: `FeedService.get_feed()` catches invalid UUID strings and returns `None` in `backend/services/feeds/service.py`.
- FastAPI handlers translate service/domain errors to HTTP errors at the route boundary. Example: `create_feed()` converts `ValueError` to 400 and `FeedAlreadyExistsError` to 409 in `backend/services/feeds/main.py`.
- TypeScript API controllers catch `unknown`, normalize backend failures through `handleBackendError()`, and throw `HttpError`. Examples: `frontend/api/src/feeds/feedsController.ts`, `frontend/api/src/rules/rulesController.ts`, `frontend/api/src/transcripts/transcriptsController.ts`.
- Frontend browser service utilities throw typed API errors after reading response bodies. Example: `apiFetch()` in `frontend/transcription-ui/src/utils/apiUtils.ts`.
- Environment validation is fail-fast for required backend URLs and UI/API origins. Examples: `frontend/api/src/config.ts`, `_require_env()` in `backend/pipeline/ingestion/settings.py`.

## Logging

**Framework:** Python `logging`, Google Cloud Logging integration, and frontend/API `console` for boundary diagnostics.

**Patterns:**
- Initialize Python module loggers with `logging.getLogger(__name__)`. Examples: `backend/pipeline/common/logging.py`, `backend/pipeline/evaluation/service.py`, `backend/pipeline/transcription/processor.py`.
- Call `setup_logging()` from backend process entry points so local runs use `logging.basicConfig()` and GCP runs use `google.cloud.logging.Client().setup_logging()`. Implementation: `backend/pipeline/common/logging.py`.
- Prefer lazy logging interpolation (`logger.info("...", value)`) in production Python. Examples: `backend/pipeline/evaluation/service.py`, `backend/pipeline/storage/connection.py`.
- Use `logger.exception()` at isolation points where the stack trace is needed. Examples: `backend/pipeline/evaluation/service.py`, `backend/pipeline/normalization/main.py`, `backend/pipeline/notification/send_notification.py`.
- Frontend API error logs are JSON-shaped in `handleBackendError()` at `frontend/api/src/utils.ts`.
- UI code uses `console.error()` and `console.warn()` for browser-side diagnostics. Examples: `frontend/transcription-ui/src/context/AuthProvider.tsx`, `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`.

## Comments

**When to Comment:**
- Use comments to document operational constraints, safety boundaries, and non-obvious failure modes. Examples: shutdown budget comments in `backend/pipeline/ingestion/settings.py`, PgBouncer transaction-mode notes in `backend/pipeline/storage/connection.py`, stale-closure explanation in `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`.
- Keep comments close to the code that enforces the behavior. Examples: `_DEFAULT_CAPS` registry comments in `backend/pipeline/ingestion/settings.py`, `_COLLECTORS` registry comments in `backend/pipeline/ingestion/router.py`.
- Avoid comments that restate simple code. The project style guides are in `.github/instructions/PYTHON_STYLE.instructions.md` and `.github/instructions/JS_TS_STYLE.instructions.md`.

**JSDoc/TSDoc:**
- Python public APIs and non-trivial functions use triple-quoted docstrings with Google-style sections when useful. Examples: `create_pool()` in `backend/pipeline/storage/connection.py`, `NormalizerSettings` in `backend/pipeline/ingestion/settings.py`.
- Python tests use short docstrings on test classes and methods to state the behavior under test. Examples: `backend/pipeline/ingestion/tests/test_router.py`, `backend/pipeline/storage/tests/test_feed_store.py`.
- TypeScript uses JSDoc sparingly for public API behavior that is not obvious from decorators or names. Example: `deactivateFeed()` in `frontend/api/src/feeds/feedsController.ts`.

## Function Design

**Size:** Keep leaf helpers small and move orchestration into named service or runtime classes. Examples: conversion helpers in `frontend/api/src/feeds/feedsController.ts`, routing helpers in `backend/pipeline/ingestion/router.py`, service methods in `backend/services/feeds/service.py`.

**Parameters:** Prefer typed, explicit parameters for boundary functions; use settings/dataclass objects for grouped configuration. Examples: `create_pool()` in `backend/pipeline/storage/connection.py`, `NormalizerSettings` in `backend/pipeline/ingestion/settings.py`, `AlloyDBSettings` in `backend/pipeline/storage/settings.py`.

**Return Values:** Return domain models or simple status values from service/storage APIs, and convert to HTTP responses only at the API boundary. Examples: `FeedService.get_feed()` returns `Feed | None` in `backend/services/feeds/service.py`; `FeedStore.update_feed_progress()` returns a boolean as exercised by `backend/pipeline/storage/tests/test_feed_store.py`.

## Module Design

**Exports:** 
- Python modules expose functions/classes directly and keep private registries/helpers prefixed with `_`. Examples: `_COLLECTORS` in `backend/pipeline/ingestion/router.py`, `_DEFAULT_CAPS` in `backend/pipeline/ingestion/settings.py`.
- TypeScript uses named exports. Examples: `export class FeedsController` in `frontend/api/src/feeds/feedsController.ts`, `export async function listTranscripts` in `frontend/transcription-ui/src/service/listTranscripts.ts`, `export function TranscriptView` in `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`.
- TypeScript avoids default exports in many service/API files, but the React UI contains compatibility default exports for components such as `TranscriptView` in `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`; match the nearest file's import style when editing UI components.

**Barrel Files:** 
- Shared TypeScript types are exported through `frontend/common/src/index.ts` and consumed through `@transcription/common`.
- Python packages mostly import concrete modules directly rather than relying on broad barrel files. Examples: `backend/pipeline/ingestion/router.py`, `backend/services/feeds/main.py`.
- Generated protobuf bindings live under `backend/pipeline/schema_types/`; regenerate through `mise run generate:protos` after editing files in `protos/`.

---

*Convention analysis: 2026-05-24*

# Coding Conventions

**Analysis Date:** 2026-06-26

## Naming Patterns

**Files:**
- Use `snake_case.py` for Python production modules, matching paths such as `backend/pipeline/ingestion/collector_runtime.py`, `backend/pipeline/storage/feed_store.py`, `backend/pipeline/common/log_helper.py`, and `model/src/gemini_sft/config.py`.
- Use `test_*.py` for Python tests under package-local test directories such as `backend/services/feeds/tests/test_service.py`, `backend/pipeline/storage/tests/test_feed_store.py`, `backend/pipeline/ingestion/tests/test_router.py`, and `model/tests/common/tests/test_manifest.py`.
- Keep some function-style pipeline tests next to the module package when that is the existing local pattern, as in `backend/pipeline/notification/test_send_notification.py`, `backend/pipeline/notification/test_request_handler.py`, and `backend/pipeline/notification/test_notification_deduplication.py`.
- Use `PascalCase.tsx` for React component files, as in `frontend/transcription-ui/src/components/transcripts/TranscriptRow.tsx`, `frontend/transcription-ui/src/components/common/RequireAdmin.tsx`, and `frontend/transcription-ui/src/components/audio/AudioControl.tsx`.
- Use `use*.ts` for React hooks, as in `frontend/transcription-ui/src/hooks/useAudioSegments.ts`, `frontend/transcription-ui/src/hooks/useAudioPlayback.ts`, and `frontend/transcription-ui/src/hooks/useUserInfo.ts`.
- Use `camelCase.ts` for frontend service and utility files, as in `frontend/transcription-ui/src/service/listFeeds.ts`, `frontend/transcription-ui/src/utils/timeUtils.ts`, and `frontend/api/src/feeds/actorHeaders.ts`.
- Use `.test.ts` and `.test.tsx` for TypeScript tests beside the unit under test, as in `frontend/api/src/feeds/feedsController.test.ts`, `frontend/transcription-ui/src/service/listFeeds.test.ts`, and `frontend/transcription-ui/src/components/transcripts/TranscriptRow.test.tsx`.

**Functions:**
- Use `snake_case` for Python functions and methods, including async functions such as `backend/services/feeds/service.py` `create_feed`, `update_feed`, `list_feeds`, and private helpers such as `backend/pipeline/storage/feed_store.py` `_row_to_feed`.
- Prefix private Python helpers with `_`, as in `backend/pipeline/ingestion/router.py` `_COLLECTORS`, `backend/pipeline/storage/feed_store.py` `_require_actor_id`, and `model/src/gemini_sft/config.py` `_load_run_config`.
- Use `camelCase` for TypeScript functions and methods, as in `frontend/api/src/feeds/feedsController.ts` `convertFeedBackend`, `appendTagFilters`, and `listFeeds`.
- Use `PascalCase` for React components and TSOA controllers, as in `frontend/transcription-ui/src/components/transcripts/TranscriptRow.tsx` `TranscriptRow` and `frontend/api/src/feeds/feedsController.ts` `FeedsController`.
- Use `use*` names for React hooks that call React hooks, as in `frontend/transcription-ui/src/hooks/useAudioSegments.ts` `useAudioSegments`.

**Variables:**
- Use `UPPER_SNAKE_CASE` for Python module constants, as in `backend/pipeline/ingestion/collector_runtime.py` `_PIPELINE_GCS_UPLOAD_FAILED`, `backend/pipeline/storage/feed_store.py` `_CREATE_FEED_UNIQUE_CONSTRAINTS`, and `model/src/gemini_sft/config.py` `ADAPTER_SIZES`.
- Use `snake_case` for Python locals and function parameters, as in `backend/pipeline/storage/feed_store.py` `source_type`, `status_reason_raw`, and `claim_types`.
- Use `camelCase` for TypeScript locals and object fields exposed to frontend code, as in `frontend/api/src/feeds/feedsController.ts` `lastHeartbeatParsed`, `sourceFeedId`, and `statusReasonDetail`.
- Use backend wire names only at API boundaries, as in `frontend/api/src/feeds/feedsController.ts` `FeedBackend.source_type`, `source_feed_id`, and `last_heartbeat`, then convert to frontend camelCase.
- Use module-level mutable caches sparingly and name them plainly, as in `frontend/api/src/config.ts` `adminCache` and `cachedGroupId`.

**Types:**
- Use `PascalCase` for Python classes, dataclasses, enums, and `TypedDict` contracts, as in `backend/pipeline/storage/feed_store.py` `SourceType`, `FeedStatus`, `LeasedFeed`, and `PaginatedFeeds`.
- Use `enum.StrEnum` for string-backed Python enum values stored externally, as in `backend/pipeline/storage/feed_store.py` `SourceType`, `FeedStatus`, and `FeedStatusReason`.
- Use frozen dataclasses for immutable model package contracts, as in `model/src/gemini_sft/config.py` `RunPaths` and `RunConfig`, and `model/src/common/manifest.py` `CanonicalRow`.
- Use Pydantic `BaseModel` classes for FastAPI request/response models, as in `backend/services/feeds/models.py` `Tag`, `FeedUpdate`, `Feed`, and `ListFeedsResponse`.
- Use TypeScript `interface` for local API backend shapes and `class` only where decorators require runtime metadata, as in `frontend/api/src/feeds/feedsController.ts` `FeedBackend`, `FeedCreateBackend`, and `ListFeedsQueryParams`.

## Code Style

**Formatting:**
- Format Python with Ruff using `pyproject.toml` `[tool.ruff]`, target `py313`, `line-length = 80`, and `extend-exclude` for generated protobuf paths such as `backend/pipeline/schema_types` and `**/*_pb2.py`.
- Format Python through mise tasks in `.mise.toml`: `mise run format:ruff` runs `uv run ruff format`, and `mise run format` also formats Terraform and frontend code.
- Format notebooks through `scripts/notebook_formatter.py` via `.mise.toml` tasks `format:notebooks` and `lint:notebooks`; keep `model/colabs/**/*.ipynb` and `model/colabs/**/*.py` exempt from normal Ruff lint in `pyproject.toml`.
- Format TypeScript/React with Prettier from `.prettierrc`: semicolons enabled, single quotes, trailing commas `es5`, `printWidth` 80, `tabWidth` 2, and sorted imports through `@trivago/prettier-plugin-sort-imports`.
- Run frontend format checks through `frontend/api/package.json` `format:check`, `frontend/transcription-ui/package.json` `format:check`, or the aggregate `.mise.toml` task `lint:frontend:prettier`.

**Linting:**
- Use Ruff as the Python linter with `select = ["ALL"]` in `pyproject.toml`; add per-file ignores instead of weakening code locally without a reason.
- Keep Ruff ignore lists sorted; `.mise.toml` task `lint:ruff:sorted` parses `pyproject.toml` and fails if `tool.ruff.lint.ignore` or per-file ignore codes are unsorted.
- Keep Python cyclomatic complexity under the Ruff mccabe limit in `pyproject.toml` `[tool.ruff.lint.mccabe] max-complexity = 10`.
- Run Python type checks with `ty` through `.mise.toml` `lint:ty`; `pyproject.toml` also configures Pyright for Python 3.13 and excludes `model/` and `backend/services/local-whisper-api/` from `ty`.
- Use ESLint flat config for `frontend/api` in `frontend/api/eslint.config.js`; it combines `@eslint/js`, `typescript-eslint`, Node globals, and `eslint-config-prettier`.
- Use ESLint flat config for `frontend/transcription-ui` in `frontend/transcription-ui/eslint.config.js`; it combines `typescript-eslint`, React Hooks, React Refresh, TanStack Query rules, CSS linting, browser globals, and `eslint-config-prettier`.
- Keep frontend type checks strict: `frontend/transcription-ui/tsconfig.app.json` enables `strict`, `noUnusedLocals`, `noUnusedParameters`, `noFallthroughCasesInSwitch`, and `noUncheckedSideEffectImports`; `frontend/common/tsconfig.json` and `frontend/api/tsconfig.json` also use strict TypeScript.
- Pre-commit hooks in `.pre-commit-config.yaml` run protobuf generation, schema validation, Ruff check/format, `ty`, notebook linting, frontend ESLint/Prettier/type checks, route generation, and OpenAPI verification.

## Import Organization

**Order:**
1. Python imports use standard library, third-party packages, then first-party `backend` or `model` modules. Examples: `backend/pipeline/ingestion/collector_runtime.py`, `backend/pipeline/storage/feed_store.py`, and `model/src/gemini_sft/config.py`.
2. Python type-only imports belong behind `if TYPE_CHECKING`, as in `backend/pipeline/ingestion/router.py`, `backend/services/feeds/service.py`, and `backend/pipeline/storage/feed_store.py`.
3. Python modules commonly start with `from __future__ import annotations` when modern annotations or type-only imports are used, as in `backend/pipeline/ingestion/router.py`, `backend/services/feeds/service.py`, and `model/src/gemini_sft/config.py`.
4. TypeScript API imports follow `.prettierrc` ordering for `frontend/api/**/*`: `node:` imports, `@google-cloud/`, `express`, third-party modules, `./`, then `../`. `frontend/api/src/feeds/feedsController.ts` shows type imports, shared package imports, `tsoa`, same-directory imports, and parent imports in that order.
5. TypeScript UI imports follow `.prettierrc` ordering for `frontend/transcription-ui/**/*`: `react`, third-party modules, `@` packages, relative imports excluding CSS, then CSS. `frontend/transcription-ui/src/components/transcripts/TranscriptRow.tsx` and `frontend/transcription-ui/vite.config.ts` follow this pattern.

**Path Aliases:**
- Do not use `@/` aliases; no `@/` imports are present under `frontend/transcription-ui`, `frontend/api`, or `frontend/common`.
- Use the linked shared package `@transcription/common` for frontend shared types and converters, as in `frontend/api/src/feeds/feedsController.ts`, `frontend/transcription-ui/src/components/transcripts/TranscriptRow.tsx`, and `frontend/common/src/index.ts`.
- Use explicit `.js` extensions for relative ESM imports in the Node API package, as in `frontend/api/src/feeds/feedsController.ts` and `frontend/api/src/authentication.ts`.
- Use repository-root Python imports for backend code, as in `backend.services.feeds.main` importing `backend.pipeline.storage.feed_store`; `.mise.toml` sets `PYTHONPATH = "."`.

## Error Handling

**Patterns:**
- Build Python error messages in a `msg` local before raising when formatting is nontrivial, as in `backend/pipeline/ingestion/router.py`, `backend/pipeline/storage/feed_store.py`, and `model/src/gemini_sft/config.py`.
- Preserve parse/validation causes with `raise ... from e` or `raise ... from exc`, as in `backend/services/feeds/main.py`, `backend/pipeline/storage/feed_store.py`, and `model/src/gemini_sft/config.py`.
- Use domain-specific Python exceptions from `backend/pipeline/common/exceptions.py`, including `FeedAlreadyExistsError`, `FeedNameAlreadyExistsError`, `FeedStateConflictError`, and `NonRetryableError`.
- Return `None` or `False` from service-layer methods for invalid IDs before touching storage, as in `backend/services/feeds/service.py` `update_feed`, `get_feed`, `deactivate_feed`, `delete_feed`, and `reset_feed`.
- Convert service/storage exceptions into HTTP responses at FastAPI boundaries, as in `backend/services/feeds/main.py` mapping `ValueError` to 400, duplicate feed errors to 409, and missing feeds to 404.
- In TypeScript, catch `unknown`, normalize errors, and throw `HttpError` at controller boundaries, as in `frontend/api/src/feeds/feedsController.ts` and helper handling in `frontend/api/src/utils.ts`.
- In frontend services and hooks, fail through thrown `Error` for request failures and suppress noncritical polling failures with `console.error` plus empty results, as in `frontend/transcription-ui/src/service/listFeeds.test.ts` and `frontend/transcription-ui/src/hooks/useAudioSegments.ts`.
- Validate required environment variables at module load in `frontend/api/src/config.ts`; hard-required service URLs throw, while optional deployment metadata logs with `console.error`.

## Logging

**Framework:** Python `logging`; TypeScript uses `console`

**Patterns:**
- Define `logger = logging.getLogger(__name__)` in Python modules, as in `backend/pipeline/common/log_helper.py`, `backend/pipeline/ingestion/collector_runtime.py`, `backend/pipeline/storage/feed_store.py`, and `integration_tests/e2e/test_transcription_pipeline.py`.
- Initialize Python logging through `backend/pipeline/common/log_helper.py` `setup_logging`; it installs system, thread, and asyncio exception handlers and configures Google Cloud Logging only in GCP environments.
- Use structured Python log fields through `extra={"json_fields": {...}}` for pipeline events, as in `backend/pipeline/common/log_helper.py` `record_pipeline_stage`, `backend/services/feeds/service.py` `deactivate_feed`, and `backend/pipeline/ingestion/tests/test_chunk_ingested.py`.
- Keep stable `event_type` strings in structured logs when tests or log-based metrics depend on them, as in `backend/pipeline/common/log_helper.py` and `backend/pipeline/ingestion/slo_contract.py`.
- Use `caplog` or `assertLogs` in tests for log contracts, as in `backend/pipeline/common/tests/test_actor_identity.py` and `backend/pipeline/ingestion/tests/test_chunk_ingested.py`.
- Use TypeScript `console.warn` for expected-but-unusual UI states and `console.error` for external API/admin lookup failures, as in `frontend/transcription-ui/src/hooks/useAudioSegments.ts`, `frontend/api/src/authentication.ts`, and `frontend/api/src/config.ts`.

## Comments

**When to Comment:**
- Comment invariants that constrain future edits, as in `backend/pipeline/ingestion/collector_runtime.py` documenting shutdown wait points and heartbeat separation.
- Comment cross-file registration requirements, as in `backend/pipeline/storage/feed_store.py` `SourceType` and `backend/pipeline/ingestion/router.py` `_COLLECTORS`.
- Comment test intent when a test pins a contract, as in `backend/pipeline/ingestion/tests/test_slo_contract_lint.py`, `backend/pipeline/ingestion/tests/test_chunk_ingested.py`, and `model/tests/common/tests/test_manifest.py`.
- Avoid comments for simple assignments; local exceptions exist for API contract examples and UI event behavior in `frontend/api/src/feeds/feedsController.ts` and `frontend/transcription-ui/src/components/transcripts/TranscriptRow.tsx`.

**JSDoc/TSDoc:**
- Use Python docstrings on public classes/functions and complex helpers, following Google-style intent configured in `pyproject.toml` `[tool.ruff.lint.pydocstyle] convention = "google"`.
- Keep TypeScript comments focused on API docs where they feed TSOA/OpenAPI or explain query semantics, as in `frontend/api/src/feeds/feedsController.ts` `ListFeedsQueryParams`.
- Do not require docstrings on every Python public symbol; `pyproject.toml` ignores Ruff docstring rules `D100` through `D107` and several formatting-specific `D` rules.

## Function Design

**Size:** Keep simple conversion helpers and validators small; isolate complex orchestration in purpose-built classes such as `backend/pipeline/ingestion/collector_runtime.py` `CollectorRuntime`, `backend/pipeline/storage/feed_store.py` `FeedStore`, and `frontend/transcription-ui/src/hooks/useAudioSegments.ts` `useAudioSegments`.

**Parameters:** Use keyword-only arguments for security or audit-sensitive values, as in `backend/services/feeds/service.py` methods requiring `actor_id` and tests in `backend/services/feeds/tests/test_service.py`.

**Return Values:** Return typed Pydantic models at FastAPI service boundaries (`backend/services/feeds/service.py`), typed dictionaries for storage rows (`backend/pipeline/storage/feed_store.py`), and typed frontend DTOs from conversion helpers (`frontend/api/src/feeds/feedsController.ts`).

**Async:** Use async/await throughout I/O paths, including FastAPI handlers in `backend/services/feeds/main.py`, asyncpg storage in `backend/pipeline/storage/feed_store.py`, collector runtime loops in `backend/pipeline/ingestion/collector_runtime.py`, TSOA controller methods in `frontend/api/src/feeds/feedsController.ts`, and React Query calls in `frontend/transcription-ui/src/hooks/useAudioSegments.ts`.

**Validation:** Validate data at boundaries: Pydantic models in `backend/services/feeds/models.py`, enum conversion in `backend/pipeline/storage/feed_store.py`, config parsing in `model/src/gemini_sft/config.py`, and query parameter conversion in `frontend/api/src/feeds/feedsController.ts`.

## Module Design

**Exports:** Python modules are imported directly by path; `__init__.py` files such as `backend/pipeline/storage/__init__.py`, `backend/services/feeds/__init__.py`, and `backend/pipeline/ingestion/__init__.py` are not broad public barrels.

**Barrel Files:** The frontend shared package intentionally exports through `frontend/common/src/index.ts`; add shared frontend types or converters there when they need to be consumed by both `frontend/api` and `frontend/transcription-ui`.

**Service Boundaries:** Keep FastAPI route wiring in `backend/services/*/main.py`, business methods in `backend/services/*/service.py`, and Pydantic contracts in `backend/services/*/models.py`, following `backend/services/feeds/main.py`, `backend/services/feeds/service.py`, and `backend/services/feeds/models.py`.

**Storage Boundaries:** Keep SQL strings and builders in `backend/pipeline/storage/*_queries.py`, row conversion and storage behavior in `backend/pipeline/storage/*_store.py`, and connection lifecycle in `backend/pipeline/storage/connection.py`.

**Frontend Boundaries:** Keep proxy controllers in `frontend/api/src/*/*Controller.ts`, UI API calls in `frontend/transcription-ui/src/service/*.ts`, stateful data access in `frontend/transcription-ui/src/hooks/*.ts`, and reusable shared contracts in `frontend/common/src/types/*.ts`.

---

*Convention analysis: 2026-06-26*

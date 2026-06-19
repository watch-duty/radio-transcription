# Coding Conventions

**Analysis Date:** 2026-06-19

## Naming Patterns

**Files:**
- Use Python `snake_case.py` modules and `test_*.py` tests in backend and model code, matching `backend/pipeline/ingestion/source_runtime_specs.py`, `backend/pipeline/ingestion/tests/test_source_runtime_specs.py`, and `model/src/gemini_sft/config.py`.
- Use package-local TypeScript naming already present in each frontend package: `camelCase.ts` and `camelCase.test.ts` for services/controllers such as `frontend/transcription-ui/src/service/listFeeds.ts`, `frontend/transcription-ui/src/service/listFeeds.test.ts`, `frontend/api/src/feeds/feedsController.ts`, and `frontend/api/src/feeds/feedsController.test.ts`.
- Use `PascalCase.tsx` and `PascalCase.test.tsx` for React components such as `frontend/transcription-ui/src/components/feeds/FeedTable.tsx` and `frontend/transcription-ui/src/components/feeds/FeedTable.test.tsx`.
- Use lowercase domain files for shared frontend types and utilities, as in `frontend/common/src/types/feeds.ts`, `frontend/common/src/types/audio.ts`, and `frontend/common/src/utils/statusUtils.ts`.
- Repo instruction files exist at `.github/instructions/PYTHON_STYLE.instructions.md` and `.github/instructions/JS_TS_STYLE.instructions.md`; current frontend source uses `.test.ts(x)` and camel/Pascal file names, so add new files by matching the surrounding package convention.

**Functions:**
- Use Python `snake_case` for functions and methods, with `_private_helper` for module-internal helpers, as in `backend/services/feeds/service.py`, `backend/pipeline/ingestion/router.py`, and `model/src/gemini_sft/config.py`.
- Use `lowerCamelCase` for TypeScript functions and methods, as in `frontend/transcription-ui/src/service/listFeeds.ts` (`listFeedsPage`, `listFeeds`) and `frontend/api/src/utils.ts` (`handleBackendError`, `getServiceClient`).
- Use `UpperCamelCase` for React components and controller classes, as in `frontend/transcription-ui/src/components/feeds/FeedTable.tsx` (`FeedTable`) and `frontend/api/src/feeds/feedsController.ts` (`FeedsController`).
- Test names are descriptive behavior sentences in both Python and TypeScript, as in `backend/services/feeds/tests/test_api.py` (`test_create_feed_already_exists`) and `frontend/transcription-ui/src/service/listFeeds.test.ts` (`should loop and fetch all pages when response is paginated ListFeedsResponse object`).

**Variables:**
- Use uppercase module constants for Python and TypeScript constants, as in `backend/pipeline/storage/tests/test_feed_store.py` (`_FEED_ID`, `_FEED_STATUS_REASON_VALUES`) and `frontend/transcription-ui/src/context/AuthProvider.tsx` (`REFRESH_TOKEN_INTERVAL`, `MAX_REFRESH_ATTEMPTS`).
- Use leading underscores for Python module-private helpers and fixtures, as in `backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py` (`_make_call`, `_mock_transport`) and `model/tests/gemini_sft/test_workflow.py` (`_manifest`, `_seed_source_manifests`).
- Use `lowerCamelCase` for TypeScript local variables and props, as in `frontend/transcription-ui/src/components/feeds/FeedTable.tsx` (`sortConfig`, `gridTemplateColumns`, `onFiltersChange`).

**Types:**
- Use Python `PascalCase` for dataclasses, `TypedDict`, Pydantic models, and exceptions, as in `backend/pipeline/ingestion/models.py` (`CapturedChunk`, `FeedFailure`, `CaptureResources`) and `backend/pipeline/storage/feed_store.py` (`LeasedFeed`, `PaginatedFeeds`, `FeedStatusReason`).
- Use `enum.StrEnum` for Python domain enums whose values are serialized strings, as in `backend/pipeline/storage/feed_store.py` (`SourceType`, `FeedStatus`, `FeedStatusReason`) and `backend/pipeline/ingestion/models.py` (`AudioMimeType`).
- Use TypeScript `interface` for object shapes and `type` for unions, as in `frontend/common/src/types/feeds.ts` (`Feed`, `ListFeedsResponse`, `BackendFeedStatus`).
- Use TypeScript enum members in `CONSTANT_CASE`, as in `frontend/common/src/types/feeds.ts` (`SourceType.BCFY_FEEDS`, `SourceType.OPENMHZ`).

## Code Style

**Formatting:**
- Python is formatted with Ruff using `line-length = 80` and `target-version = "py313"` in `pyproject.toml`.
- TypeScript, TSX, JavaScript, and CSS are formatted with Prettier using semicolons, single quotes, `printWidth: 80`, `tabWidth: 2`, and sorted imports from `.prettierrc`.
- Notebooks under `model/colabs/` are formatted through `scripts/notebook_formatter.py` and Ruff tasks in `.mise.toml`.
- Terraform is formatted by `terraform fmt -recursive` through `.mise.toml`.
- Use the aggregate commands from `.mise.toml`: `mise run format`, `mise run lint`, and the pre-commit hooks in `.pre-commit-config.yaml`.

**Linting:**
- Python linting is Ruff `select = ["ALL"]` with an explicit ignore list in `pyproject.toml`; keep ignore lists sorted because `.mise.toml` defines `lint:ruff:sorted`.
- Python type checking uses `ty check` through `.mise.toml` and `.pre-commit-config.yaml`; `pyproject.toml` excludes `model/` and `backend/services/local-whisper-api/` from `ty`.
- Frontend API linting uses `frontend/api/eslint.config.js` with `@eslint/js`, `typescript-eslint`, Node globals, and Prettier compatibility.
- Frontend UI linting uses `frontend/transcription-ui/eslint.config.js` with `typescript-eslint`, React Hooks, React Refresh, TanStack Query, CSS linting, browser globals, and Prettier compatibility.
- TypeScript strictness is enforced by `frontend/transcription-ui/tsconfig.app.json` and `frontend/transcription-ui/tsconfig.node.json` (`strict`, `noUnusedLocals`, `noUnusedParameters`, `erasableSyntaxOnly`, `noUncheckedSideEffectImports`) and by `frontend/api/tsconfig.json` extending `@tsconfig/node22`.

## Import Organization

**Order:**
1. Python files use future imports, standard library, third-party libraries, then local packages, following `.github/instructions/PYTHON_STYLE.instructions.md` and examples such as `backend/pipeline/storage/feed_store.py`.
2. Python typing-only imports go inside `if TYPE_CHECKING:` blocks, as in `backend/services/feeds/main.py`, `backend/pipeline/ingestion/router.py`, and `backend/pipeline/storage/feed_store.py`.
3. Frontend API imports are sorted by `.prettierrc`: `node:` modules, `@google-cloud/`, `express`, third-party modules, `./`, then `../`, matching `frontend/api/src/feeds/feedsController.ts`.
4. Frontend UI imports are sorted by `.prettierrc`: React, third-party modules, `@...` packages, relative non-CSS imports, then CSS imports, matching `frontend/transcription-ui/src/components/feeds/FeedTable.tsx`.
5. Use `import type` for TypeScript type-only imports, as in `frontend/api/src/feeds/feedsController.ts`, `frontend/transcription-ui/src/components/feeds/FeedTable.tsx`, and `frontend/transcription-ui/src/service/listFeeds.ts`.

**Path Aliases:**
- Python imports use repository-root absolute package paths such as `backend.pipeline.storage.feed_store` and `backend.pipeline.common.auth`, enabled by `PYTHONPATH = "."` in `.mise.toml`.
- Frontend shared code is consumed as the linked package `@transcription/common`, declared in `frontend/api/package.json` and `frontend/transcription-ui/package.json`, and re-exported from `frontend/common/src/index.ts`.
- No TypeScript `@/` alias is configured in `frontend/transcription-ui/tsconfig.app.json`, `frontend/transcription-ui/vite.config.ts`, or `frontend/api/tsconfig.json`; use relative imports within each frontend package.

## Error Handling

**Patterns:**
- Raise typed or built-in Python exceptions at domain boundaries with explicit messages and exception chaining, as in `model/src/gemini_sft/config.py` (`RunConfigError`) and `backend/pipeline/storage/feed_store.py` (`FeedAlreadyExistsError`, `FeedNameAlreadyExistsError`).
- Convert storage/service exceptions to FastAPI `HTTPException` at API boundaries, as in `backend/services/feeds/main.py`.
- Treat invalid UUIDs and missing records as `None`/`False` at service boundaries rather than leaking parser errors, as in `backend/services/feeds/service.py`.
- Classify ingestion source failures with `FeedFailure` and bounded `FeedStatusReason` values, as documented and implemented in `backend/pipeline/ingestion/models.py`.
- Frontend API controllers catch `unknown`, normalize downstream failures with `handleBackendError`, and throw `HttpError`, as in `frontend/api/src/feeds/feedsController.ts` and `frontend/api/src/utils.ts`.
- Frontend UI service functions let `apiFetch` failures reject and test those paths with Vitest, as in `frontend/transcription-ui/src/service/listFeeds.ts` and `frontend/transcription-ui/src/service/listFeeds.test.ts`.

## Logging

**Framework:** Python `logging`; TypeScript uses `console` in frontend/API proxy code.

**Patterns:**
- Use module loggers in Python (`logger = logging.getLogger(__name__)`), as in `backend/services/feeds/main.py`, `backend/pipeline/storage/feed_store.py`, and `model/src/gemini_sft/prepare.py`.
- Use centralized setup from `backend/pipeline/common/log_helper.py`; it installs process/thread/asyncio exception handlers and Cloud Logging in GCP environments.
- Use structured task logging with `get_task_logger` and `TaskJsonFormatter` for Dataflow-style tasks in `backend/pipeline/common/log_helper.py`.
- Put contextual JSON fields in Python log `extra` where service events need structured payloads, as in `backend/services/feeds/service.py`.
- Use `logger.exception` when retaining stack traces at isolation points, as in `backend/pipeline/normalization/processor.py` and `backend/pipeline/evaluation/processor.py`.
- TypeScript API proxy errors are serialized to JSON in `frontend/api/src/utils.ts`; UI session failures use `console.error` in `frontend/transcription-ui/src/context/AuthProvider.tsx`.

## Comments

**When to Comment:**
- Comment domain contracts, multi-step operational invariants, and non-obvious constraints near the code they govern, as in `backend/pipeline/ingestion/models.py` and `backend/pipeline/storage/feed_store.py`.
- Keep small inline comments for test intent or environment workarounds, as in `frontend/transcription-ui/src/test/setup.ts` and `backend/pipeline/segmentation/tests/test_orchestration.py`.
- Use TODO comments with enough context and an issue link when available, as in `frontend/transcription-ui/src/service/listFeeds.ts`.

**JSDoc/TSDoc:**
- Use docstrings for Python public APIs, non-trivial behavior, exceptions, and domain models, following `.github/instructions/PYTHON_STYLE.instructions.md` and examples in `backend/pipeline/ingestion/models.py`.
- TypeScript API contracts rely primarily on interfaces and tsoa decorators in `frontend/api/src/feeds/feedsController.ts`; use JSDoc where generated OpenAPI metadata or developer-facing behavior needs explanation.
- React components in `frontend/transcription-ui/src/components/**` usually avoid heavy JSDoc; prefer clear prop interfaces such as `FeedTableProps` in `frontend/transcription-ui/src/components/feeds/FeedTable.tsx`.

## Function Design

**Size:** Keep small conversion/validation helpers private and module-local, as in `frontend/api/src/feeds/feedsController.ts` (`convertFeedBackend`, `convertFeedCreate`) and `model/src/gemini_sft/config.py` (`_required_str`, `_required_gcs_uri`). Larger orchestration functions belong behind explicit contracts and focused tests, as in `backend/pipeline/ingestion/models.py` and `backend/pipeline/segmentation/orchestration.py`.

**Parameters:** Prefer keyword-only Python parameters for multi-option APIs and dataclasses where order matters, as in `backend/pipeline/ingestion/models.py` (`@dataclasses.dataclass(frozen=True, kw_only=True)`) and `backend/services/feeds/service.py` (`list_feeds(..., *, limit, next_token, ...)`). TypeScript props should be declared as interfaces and destructured at component boundaries, as in `frontend/transcription-ui/src/components/feeds/FeedTable.tsx`.

**Return Values:** Return typed domain objects at boundaries: Pydantic models in `backend/services/feeds/main.py`, `TypedDict`/dataclass values in `backend/pipeline/storage/feed_store.py` and `backend/pipeline/ingestion/models.py`, and typed promises in `frontend/transcription-ui/src/service/listFeeds.ts`. Use `None`/`False` for not-found or invalid service IDs where callers map to HTTP status, as in `backend/services/feeds/service.py`.

## Module Design

**Exports:** Use named TypeScript exports throughout frontend packages, as in `frontend/common/src/index.ts`, `frontend/transcription-ui/src/service/listFeeds.ts`, and `frontend/api/src/utils.ts`. Python modules expose domain classes/functions directly and keep helper functions private with `_`, as in `backend/pipeline/ingestion/router.py` and `model/src/gemini_sft/config.py`.

**Barrel Files:** `frontend/common/src/index.ts` is the shared frontend barrel for domain types and utilities consumed by `@transcription/common`. Backend Python does not use broad barrel modules beyond package `__init__.py` markers such as `backend/pipeline/common/__init__.py` and `backend/services/feeds/__init__.py`.

---

*Convention analysis: 2026-06-19*

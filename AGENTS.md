# Agent Instructions

Read and follow [.agents/instructions.md](.agents/instructions.md) before
making code changes or reviewing code in this repository.

## Do Not Run Broad Local Tests By Default

This repository has resource-heavy Docker/testcontainers and E2E lanes. Broad
local test commands have previously exhausted developer machines.

- Default to targeted low-resource checks locally.
- For docs-only changes, use `git diff --check` instead of Python tests unless
  the user asks for tests.
- Do not run local E2E/API/component/full integration tests unless the user
  explicitly asks and confirms the machine is prepared.
- Avoid unscoped `uv run pytest`, `uv run pytest integration_tests/`,
  `mise run test:e2e`, `mise run test:component`, and
  `docker compose ... integration-tests` unless explicitly approved.
- Prefer GitHub Actions for full E2E/resource-stack validation.

<!-- GSD:project-start source:PROJECT.md -->
## Project

**Feed Audit Notification Delivery**

This project adds a best-effort notification path for radio transcription feed
audit events. The transcription engine remains the owner of feed audit history,
while Watch Duty backend receives near-real-time event notifications through a
webhook for reporter/channel alerting and operational response.

**Core Value:** Feed audit notifications must make feed lifecycle and ingestion problems visible
to Watch Duty quickly without affecting ingestion or feed lifecycle writes.

### Constraints

- **Critical path**: Notification logging, routing, and webhook delivery must not
  add synchronous network calls, extra database reads, or failure coupling to
  ingestion and feed lifecycle writes.
- **Payload**: The WD webhook receives the flat feed audit event payload with
  `event_type` and `schema_version`; avoid nested wrapper formats and avoid
  duplicate encode/decode work.
- **Reliability**: Use short local subscriber retry plus Pub/Sub redelivery and
  DLQ. Do not implement a custom delivery table in v1.
- **Security**: Pub/Sub push to the relay uses Cloud Run IAM/OIDC, and the relay
  authenticates to WD with the configured radio-transcription API key.
- **Maintainability**: Reuse shared helpers across async and sync feed storage
  paths, and do not duplicate feed audit payload construction logic.
<!-- GSD:project-end -->

<!-- GSD:stack-start source:codebase/STACK.md -->
## Technology Stack

## Languages
- Python 3.13 - Backend pipeline services, CloudEvent handlers, storage layer, ingestion workers, and local ASR service. Version bounds are declared in `pyproject.toml`, `backend/pipeline/common/pyproject.toml`, `backend/pipeline/normalization/pyproject.toml`, `backend/pipeline/segmentation/pyproject.toml`, `backend/pipeline/transcription/pyproject.toml`, `backend/pipeline/evaluation/pyproject.toml`, `backend/pipeline/notification/pyproject.toml`, `backend/services/audio_segments/pyproject.toml`, `backend/services/feeds/pyproject.toml`, `backend/services/rules/pyproject.toml`, and `backend/services/local-whisper-api/pyproject.toml`.
- TypeScript 6.0 - Frontend API proxy, shared frontend types, and React UI. Manifests live in `frontend/api/package.json`, `frontend/common/package.json`, and `frontend/transcription-ui/package.json`.
- Python >=3.11 - Model and ASR research package under `model/pyproject.toml`, with notebook and Gemini SFT workflows in `model/src/common/` and `model/src/gemini_sft/`.
- Terraform >=1.3, project tool pin 1.14.5 - Google Cloud infrastructure modules in `terraform/modules/alloydb/`, `terraform/modules/cloud_function/`, `terraform/modules/container_mig/`, `terraform/modules/gcs_bucket/`, `terraform/modules/memorystore_for_redis/`, and `terraform/modules/asr_evaluation/`. Provider constraints are in `terraform/modules/alloydb/versions.tf`, `terraform/modules/container_mig/versions.tf`, and `terraform/modules/gcs_bucket/versions.tf`.
- Protocol Buffers - Pipeline event contracts in `protos/alert_notification.proto`, `protos/continuous_audio.proto`, `protos/evaluated_transcribed_audio.proto`, `protos/normalized_audio.proto`, `protos/segmented_audio.proto`, `protos/streaming_state.proto`, and `protos/transcribed_audio.proto`.
- SQL - AlloyDB ingestion schema migrations in `terraform/modules/alloydb/sql/ingestion/`.
- Dockerfile and Docker Compose - Service runtime images and local integration environment in `backend/**/Dockerfile`, `frontend/api/Dockerfile`, `model/notebook_docker/Dockerfile`, `model/nemo_docker/Dockerfile`, `docker-compose.yml`, `docker-compose.whisper.yml`, and `asr-eval-docker-compose.yml`.
- YAML/TOML/HCL - GitHub Actions in `.github/workflows/`, mise tasks in `.mise.toml`, Python package metadata in `pyproject.toml` and service pyprojects, and Terraform modules in `terraform/modules/`.
## Runtime
- Python 3.13.2 is pinned for development in `.tool-versions`; backend Docker images use `python:3.13-slim` in `backend/pipeline/ingestion/Dockerfile`, `backend/pipeline/normalization/Dockerfile`, `backend/pipeline/transcription/Dockerfile`, `backend/pipeline/evaluation/Dockerfile`, `backend/pipeline/notification/Dockerfile`, `backend/services/audio_segments/Dockerfile`, `backend/services/feeds/Dockerfile`, `backend/services/rules/Dockerfile`, and `backend/services/local-whisper-api/Dockerfile`.
- Node.js 22.14.0 is pinned for development in `.tool-versions`; the frontend API runtime image uses `node:22-slim` in `frontend/api/Dockerfile`.
- Apache Beam Python 3.13 SDK 2.74.0 is the segmentation base image in `backend/pipeline/segmentation/Dockerfile`, matching `apache-beam[gcp]>=2.74.0` in `backend/pipeline/segmentation/pyproject.toml`.
- PyTorch 2.5.1 CUDA 12.4 runtime supports notebook ASR experiments in `model/notebook_docker/Dockerfile`.
- NVIDIA NeMo 26.02.00 with NeMo v2.7.2 supports heavy NeMo/Canary ASR work in `model/nemo_docker/Dockerfile`.
- Local development composes Pub/Sub emulator, fake GCS, Postgres, Redis, backend services, frontend API, mock audio services, and integration tests in `docker-compose.yml`.
- `uv` 0.9.28 is pinned for development in `.tool-versions`; root lockfile `uv.lock` covers the backend workspace declared in `pyproject.toml`.
- Backend Dockerfiles copy `uv` from `ghcr.io/astral-sh/uv:0.11.13` in `backend/pipeline/ingestion/Dockerfile`, `backend/pipeline/normalization/Dockerfile`, `backend/pipeline/segmentation/Dockerfile`, `backend/pipeline/transcription/Dockerfile`, `backend/pipeline/evaluation/Dockerfile`, `backend/pipeline/notification/Dockerfile`, `backend/services/audio_segments/Dockerfile`, `backend/services/feeds/Dockerfile`, and `backend/services/rules/Dockerfile`.
- `backend/services/local-whisper-api/Dockerfile` uses `ghcr.io/astral-sh/uv:0.7.12` for the local Whisper service.
- Yarn is used for frontend packages through `frontend/api/yarn.lock`, `frontend/common/yarn.lock`, and `frontend/transcription-ui/yarn.lock`; CI enables Corepack in `.github/workflows/ci.yml`.
- Lockfiles: present for backend root `uv.lock`, model `model/uv.lock`, and each frontend package lockfile under `frontend/`.
## Frameworks
- FastAPI >=0.110.0 - Internal Python APIs and local ASR service in `backend/services/audio_segments/main.py`, `backend/services/feeds/main.py`, `backend/services/rules/main.py`, `backend/pipeline/transcription/main.py`, and `backend/services/local-whisper-api/main.py`.
- Uvicorn >=0.27.0 - ASGI runtime for FastAPI services in `backend/services/audio_segments/Dockerfile`, `backend/services/feeds/Dockerfile`, `backend/services/rules/Dockerfile`, `backend/pipeline/transcription/Dockerfile`, and `backend/services/local-whisper-api/Dockerfile`.
- Functions Framework - Python CloudEvent functions in `backend/pipeline/normalization/main.py`, `backend/pipeline/evaluation/main.py`, `backend/pipeline/notification/send_notification.py`, and `backend/pipeline/ingestion/collectors/echo/main.py`; Node API proxy runtime in `frontend/api/package.json` and `frontend/api/Dockerfile`.
- Apache Beam / Google Dataflow - Segmentation pipeline in `backend/pipeline/segmentation/main.py`, `backend/pipeline/segmentation/orchestration.py`, and `backend/pipeline/segmentation/Dockerfile`.
- Express 5.2.1 and tsoa 7 alpha - TypeScript BFF/API proxy in `frontend/api/src/index.ts`, controllers under `frontend/api/src/`, `frontend/api/tsoa.json`, and generated OpenAPI `frontend/api/openapi.yaml`.
- React 19.2.0 and Vite 8.1.0 - Browser UI in `frontend/transcription-ui/src/main.tsx`, `frontend/transcription-ui/src/App.tsx`, and `frontend/transcription-ui/vite.config.ts`.
- Material UI 9, Toolpad Core, React Router 7, React Query 5, WaveSurfer - UI framework and data/audio playback stack in `frontend/transcription-ui/package.json`.
- Terraform Google provider >=6.0 - Cloud infrastructure modules in `terraform/modules/`.
- pytest >=9, pytest-asyncio, pytest-xdist, pytest-cov - Python tests under `backend/`, `integration_tests/`, and `model/tests/`; configured in `pyproject.toml` and `.mise.toml`.
- Vitest 4 and Testing Library - TypeScript API/UI tests under `frontend/api/src/**/*.test.ts`, `frontend/transcription-ui/src/**/*.test.ts`, and `frontend/transcription-ui/src/**/*.test.tsx`.
- Testcontainers - Component and integration tests for AlloyDB Omni and fake GCS under `integration_tests/` and collector integration tests under `backend/pipeline/ingestion/collectors/tests/`.
- Docker Compose E2E - End-to-end test stack is defined in `docker-compose.yml` and executed by `.github/workflows/integration-tests.yml`.
- mise - Task runner and tool bootstrap in `.mise.toml`.
- Ruff 0.14.14 and ty 0.0.42 - Python linting, formatting, and type checks in `pyproject.toml` and `.mise.toml`.
- ESLint 10 and Prettier 3.8 - TypeScript linting/formatting in `frontend/api/eslint.config.js`, `frontend/transcription-ui/eslint.config.js`, `.prettierrc`, and `.prettierignore`.
- TypeScript compiler - Build and typecheck commands in `frontend/api/package.json`, `frontend/common/package.json`, and `frontend/transcription-ui/package.json`.
- grpcio-tools and betterproto - Protobuf generation commands in `.mise.toml` and guidance in `backend/pipeline/README.md`.
- Docker Buildx Bake - CI image baking and GitHub Container Registry publishing in `.github/workflows/bake-main.yml`.
- pre-commit - Hook configuration in `.pre-commit-config.yaml`.
## Key Dependencies
- `google-cloud-pubsub` - Ordered pipeline event publishing in `backend/pipeline/common/clients/pubsub_client.py`, `backend/pipeline/normalization/main.py`, `backend/pipeline/transcription/main.py`, and `backend/pipeline/evaluation/main.py`.
- `google-cloud-storage` and `gcloud-aio-storage` - Sync and async GCS access in `backend/pipeline/common/clients/gcs_client.py`, `backend/pipeline/normalization/main.py`, `backend/pipeline/ingestion/collectors/echo/main.py`, `backend/services/local-whisper-api/main.py`, `model/src/common/gcs_utils.py`, and `model/src/gemini_sft/`.
- `google-cloud-speech` - Google Chirp v3 transcriber in `backend/pipeline/transcription/transcribers/chirp.py`.
- `google-genai` - Gemini transcription and Vertex Gemini SFT/batch workflows in `backend/pipeline/transcription/transcribers/gemini.py` and `model/src/common/gemini/vertex.py`.
- `google-cloud-secret-manager` - Broadcastify Calls JWT retrieval in `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py`.
- `google-auth`, `google-auth-library`, `jose` - Google OIDC verification, service-to-service ID tokens, OAuth code exchange, and JWT decoding in `backend/pipeline/common/auth.py`, `backend/pipeline/common/auth_client.py`, `frontend/api/src/auth/authController.ts`, and `frontend/api/src/authentication.ts`.
- `asyncpg` and `psycopg[binary]` - AlloyDB/Postgres access in `backend/pipeline/storage/connection.py`, `backend/pipeline/storage/sync_connection.py`, and service packages under `backend/services/`.
- `redis` - Deduplication/cache layer in `backend/pipeline/common/storage/redis_service.py`, `backend/pipeline/notification/send_notification.py`, and `backend/services/rules/pyproject.toml`.
- `protobuf` and `cloudevents` - Pub/Sub message contracts and CloudEvent handlers in `protos/`, `backend/pipeline/schema_types/`, and `backend/pipeline/*/main.py`.
- `opentelemetry-*`, `google-cloud-logging`, `google-cloud-monitoring` - Cloud Trace, Cloud Logging, and custom monitoring in `backend/pipeline/common/tracing_utils.py`, `backend/pipeline/common/log_helper.py`, and `backend/pipeline/common/clients/monitoring_client.py`.
- `apache-beam[gcp]`, `onnxruntime`, `pedalboard`, `numba`, `av`, `soundfile`, `numpy` - Audio segmentation, VAD, and DSP in `backend/pipeline/segmentation/pyproject.toml` and `backend/pipeline/segmentation/audio/`.
- `aiohttp`, `httpx`, `requests`, `urllib3`, `curl-cffi` - HTTP clients for collectors, internal service calls, OpenMHz websocket/media access, and notifications in `backend/pipeline/ingestion/collectors/`, `backend/pipeline/common/clients/`, and `backend/pipeline/notification/request_handler.py`.
- `faster-whisper` and `python-multipart` - Local ASR API in `backend/services/local-whisper-api/pyproject.toml` and `backend/services/local-whisper-api/main.py`.
- `torchaudio`, `datasets`, `huggingface_hub`, `evaluate`, `jiwer`, `nemo_text_processing`, `google-genai` - Model evaluation and Gemini SFT extras in `model/pyproject.toml`, `model/notebook_docker/requirements.txt`, and `model/nemo_docker/requirements.txt`.
- `@react-oauth/google`, `@mui/material`, `@tanstack/react-query`, `react-virtuoso`, `@wavesurfer/react`, `wavesurfer.js` - UI auth, data fetching, virtualization, and waveform playback in `frontend/transcription-ui/package.json`.
- `axios`, `cookie-parser`, `cors`, `express`, `tsoa`, `js-yaml` - Frontend API proxy, OpenAPI generation, auth, and CORS in `frontend/api/package.json`.
## Configuration
- Development tools are pinned in `.tool-versions` and orchestrated by `.mise.toml`.
- Python workspace membership and dependency groups are declared in `pyproject.toml`.
- Frontend package dependencies and scripts are declared in `frontend/api/package.json`, `frontend/common/package.json`, and `frontend/transcription-ui/package.json`.
- TypeScript configuration lives in `frontend/api/tsconfig.json`, `frontend/common/tsconfig.json`, `frontend/transcription-ui/tsconfig.json`, `frontend/transcription-ui/tsconfig.app.json`, and `frontend/transcription-ui/tsconfig.node.json`.
- Vite mode-specific proxying and mock auth behavior live in `frontend/transcription-ui/vite.config.ts` and `frontend/transcription-ui/mockAuthPlugin.ts`.
- `.env` loading is configured by `.mise.toml`; env-like files are present at `frontend/api/.env.example`, `frontend/transcription-ui/.env.example`, `frontend/transcription-ui/.env.local-dev.example`, and `local_dev/LOCAL.env`. Contents were not read.
- Project-local agent skill directories are not detected: `.codex/skills/` is absent and `.agents/skills/` is absent; `.agents/` exists.
- Backend service images are defined by Dockerfiles under `backend/pipeline/` and `backend/services/`.
- Frontend API image is defined in `frontend/api/Dockerfile`.
- ASR notebook and NeMo images are defined in `model/notebook_docker/Dockerfile` and `model/nemo_docker/Dockerfile`.
- Local and CI compose stacks are defined in `docker-compose.yml`, `docker-compose.override.yml`, `docker-compose.whisper.yml`, and `asr-eval-docker-compose.yml`.
- CI, integration tests, image baking, and deployment signaling live in `.github/workflows/ci.yml`, `.github/workflows/integration-tests.yml`, `.github/workflows/bake-main.yml`, and `.github/workflows/trigger-deploy.yml`.
- Google Cloud infrastructure modules live under `terraform/modules/`.
- Frontend UI Firebase Hosting metadata lives in `frontend/transcription-ui/firebase.json`.
## Platform Requirements
- Install the versions in `.tool-versions`: `uv` 0.9.28, Python 3.13.2, Node.js 22.14.0, Terraform 1.14.5, and `jq`.
- Use `mise run generate:protos` from `.mise.toml` after changing files under `protos/`.
- Use Docker Compose for full local runs through `.mise.toml` tasks `dev`, `dev:start`, `dev:whisper`, and `test:e2e`.
- Use Google Cloud ADC for model and ASR workflows that access GCS or Vertex; guidance is in `ASR_CONTRIBUTING.md`.
- Use `frontend/api/README.md` for API Gateway/OpenAPI placeholders and Cloud Identity admin-group setup.
- Google Cloud is the primary platform: Cloud Functions Gen 2 via `terraform/modules/cloud_function/main.tf`, Cloud Run Job for schema migration via `terraform/modules/alloydb/main.tf`, Google Dataflow Flex Template via `backend/pipeline/segmentation/Dockerfile`, GCE regional managed instance groups via `terraform/modules/container_mig/main.tf`, GCS via `terraform/modules/gcs_bucket/main.tf`, AlloyDB via `terraform/modules/alloydb/main.tf`, and Memorystore Redis via `terraform/modules/memorystore_for_redis/main.tf`.
- Container images are built and pushed to GitHub Container Registry by `.github/workflows/bake-main.yml` and referenced in `docker-compose.yml`.
- Private deployment is triggered from the public repo by `.github/workflows/trigger-deploy.yml`.
- Runtime authentication uses Google OAuth/OIDC, service account ID tokens, Google Cloud API Gateway/Endpoints userinfo headers, and Cloud Identity group membership checks in `frontend/api/src/authentication.ts` and `frontend/api/src/config.ts`.
<!-- GSD:stack-end -->

<!-- GSD:conventions-start source:CONVENTIONS.md -->
## Conventions

## Naming Patterns
- Use `snake_case.py` for Python production modules, matching paths such as `backend/pipeline/ingestion/collector_runtime.py`, `backend/pipeline/storage/feed_store.py`, `backend/pipeline/common/log_helper.py`, and `model/src/gemini_sft/config.py`.
- Use `test_*.py` for Python tests under package-local test directories such as `backend/services/feeds/tests/test_service.py`, `backend/pipeline/storage/tests/test_feed_store.py`, `backend/pipeline/ingestion/tests/test_router.py`, and `model/tests/common/tests/test_manifest.py`.
- Keep some function-style pipeline tests next to the module package when that is the existing local pattern, as in `backend/pipeline/notification/test_send_notification.py`, `backend/pipeline/notification/test_request_handler.py`, and `backend/pipeline/notification/test_notification_deduplication.py`.
- Use `PascalCase.tsx` for React component files, as in `frontend/transcription-ui/src/components/transcripts/TranscriptRow.tsx`, `frontend/transcription-ui/src/components/common/RequireAdmin.tsx`, and `frontend/transcription-ui/src/components/audio/AudioControl.tsx`.
- Use `use*.ts` for React hooks, as in `frontend/transcription-ui/src/hooks/useAudioSegments.ts`, `frontend/transcription-ui/src/hooks/useAudioPlayback.ts`, and `frontend/transcription-ui/src/hooks/useUserInfo.ts`.
- Use `camelCase.ts` for frontend service and utility files, as in `frontend/transcription-ui/src/service/listFeeds.ts`, `frontend/transcription-ui/src/utils/timeUtils.ts`, and `frontend/api/src/feeds/actorHeaders.ts`.
- Use `.test.ts` and `.test.tsx` for TypeScript tests beside the unit under test, as in `frontend/api/src/feeds/feedsController.test.ts`, `frontend/transcription-ui/src/service/listFeeds.test.ts`, and `frontend/transcription-ui/src/components/transcripts/TranscriptRow.test.tsx`.
- Use `snake_case` for Python functions and methods, including async functions such as `backend/services/feeds/service.py` `create_feed`, `update_feed`, `list_feeds`, and private helpers such as `backend/pipeline/storage/feed_store.py` `_row_to_feed`.
- Prefix private Python helpers with `_`, as in `backend/pipeline/ingestion/router.py` `_COLLECTORS`, `backend/pipeline/storage/feed_store.py` `_require_actor_id`, and `model/src/gemini_sft/config.py` `_load_run_config`.
- Use `camelCase` for TypeScript functions and methods, as in `frontend/api/src/feeds/feedsController.ts` `convertFeedBackend`, `appendTagFilters`, and `listFeeds`.
- Use `PascalCase` for React components and TSOA controllers, as in `frontend/transcription-ui/src/components/transcripts/TranscriptRow.tsx` `TranscriptRow` and `frontend/api/src/feeds/feedsController.ts` `FeedsController`.
- Use `use*` names for React hooks that call React hooks, as in `frontend/transcription-ui/src/hooks/useAudioSegments.ts` `useAudioSegments`.
- Use `UPPER_SNAKE_CASE` for Python module constants, as in `backend/pipeline/ingestion/collector_runtime.py` `_PIPELINE_GCS_UPLOAD_FAILED`, `backend/pipeline/storage/feed_store.py` `_CREATE_FEED_UNIQUE_CONSTRAINTS`, and `model/src/gemini_sft/config.py` `ADAPTER_SIZES`.
- Use `snake_case` for Python locals and function parameters, as in `backend/pipeline/storage/feed_store.py` `source_type`, `status_reason_raw`, and `claim_types`.
- Use `camelCase` for TypeScript locals and object fields exposed to frontend code, as in `frontend/api/src/feeds/feedsController.ts` `lastHeartbeatParsed`, `sourceFeedId`, and `statusReasonDetail`.
- Use backend wire names only at API boundaries, as in `frontend/api/src/feeds/feedsController.ts` `FeedBackend.source_type`, `source_feed_id`, and `last_heartbeat`, then convert to frontend camelCase.
- Use module-level mutable caches sparingly and name them plainly, as in `frontend/api/src/config.ts` `adminCache` and `cachedGroupId`.
- Use `PascalCase` for Python classes, dataclasses, enums, and `TypedDict` contracts, as in `backend/pipeline/storage/feed_store.py` `SourceType`, `FeedStatus`, `LeasedFeed`, and `PaginatedFeeds`.
- Use `enum.StrEnum` for string-backed Python enum values stored externally, as in `backend/pipeline/storage/feed_store.py` `SourceType`, `FeedStatus`, and `FeedStatusReason`.
- Use frozen dataclasses for immutable model package contracts, as in `model/src/gemini_sft/config.py` `RunPaths` and `RunConfig`, and `model/src/common/manifest.py` `CanonicalRow`.
- Use Pydantic `BaseModel` classes for FastAPI request/response models, as in `backend/services/feeds/models.py` `Tag`, `FeedUpdate`, `Feed`, and `ListFeedsResponse`.
- Use TypeScript `interface` for local API backend shapes and `class` only where decorators require runtime metadata, as in `frontend/api/src/feeds/feedsController.ts` `FeedBackend`, `FeedCreateBackend`, and `ListFeedsQueryParams`.
## Code Style
- Format Python with Ruff using `pyproject.toml` `[tool.ruff]`, target `py313`, `line-length = 80`, and `extend-exclude` for generated protobuf paths such as `backend/pipeline/schema_types` and `**/*_pb2.py`.
- Format Python through mise tasks in `.mise.toml`: `mise run format:ruff` runs `uv run ruff format`, and `mise run format` also formats Terraform and frontend code.
- Format notebooks through `scripts/notebook_formatter.py` via `.mise.toml` tasks `format:notebooks` and `lint:notebooks`; keep `model/colabs/**/*.ipynb` and `model/colabs/**/*.py` exempt from normal Ruff lint in `pyproject.toml`.
- Format TypeScript/React with Prettier from `.prettierrc`: semicolons enabled, single quotes, trailing commas `es5`, `printWidth` 80, `tabWidth` 2, and sorted imports through `@trivago/prettier-plugin-sort-imports`.
- Run frontend format checks through `frontend/api/package.json` `format:check`, `frontend/transcription-ui/package.json` `format:check`, or the aggregate `.mise.toml` task `lint:frontend:prettier`.
- Use Ruff as the Python linter with `select = ["ALL"]` in `pyproject.toml`; add per-file ignores instead of weakening code locally without a reason.
- Keep Ruff ignore lists sorted; `.mise.toml` task `lint:ruff:sorted` parses `pyproject.toml` and fails if `tool.ruff.lint.ignore` or per-file ignore codes are unsorted.
- Keep Python cyclomatic complexity under the Ruff mccabe limit in `pyproject.toml` `[tool.ruff.lint.mccabe] max-complexity = 10`.
- Run Python type checks with `ty` through `.mise.toml` `lint:ty`; `pyproject.toml` also configures Pyright for Python 3.13 and excludes `model/` and `backend/services/local-whisper-api/` from `ty`.
- Use ESLint flat config for `frontend/api` in `frontend/api/eslint.config.js`; it combines `@eslint/js`, `typescript-eslint`, Node globals, and `eslint-config-prettier`.
- Use ESLint flat config for `frontend/transcription-ui` in `frontend/transcription-ui/eslint.config.js`; it combines `typescript-eslint`, React Hooks, React Refresh, TanStack Query rules, CSS linting, browser globals, and `eslint-config-prettier`.
- Keep frontend type checks strict: `frontend/transcription-ui/tsconfig.app.json` enables `strict`, `noUnusedLocals`, `noUnusedParameters`, `noFallthroughCasesInSwitch`, and `noUncheckedSideEffectImports`; `frontend/common/tsconfig.json` and `frontend/api/tsconfig.json` also use strict TypeScript.
- Pre-commit hooks in `.pre-commit-config.yaml` run protobuf generation, schema validation, Ruff check/format, `ty`, notebook linting, frontend ESLint/Prettier/type checks, route generation, and OpenAPI verification.
## Import Organization
- Do not use `@/` aliases; no `@/` imports are present under `frontend/transcription-ui`, `frontend/api`, or `frontend/common`.
- Use the linked shared package `@transcription/common` for frontend shared types and converters, as in `frontend/api/src/feeds/feedsController.ts`, `frontend/transcription-ui/src/components/transcripts/TranscriptRow.tsx`, and `frontend/common/src/index.ts`.
- Use explicit `.js` extensions for relative ESM imports in the Node API package, as in `frontend/api/src/feeds/feedsController.ts` and `frontend/api/src/authentication.ts`.
- Use repository-root Python imports for backend code, as in `backend.services.feeds.main` importing `backend.pipeline.storage.feed_store`; `.mise.toml` sets `PYTHONPATH = "."`.
## Error Handling
- Build Python error messages in a `msg` local before raising when formatting is nontrivial, as in `backend/pipeline/ingestion/router.py`, `backend/pipeline/storage/feed_store.py`, and `model/src/gemini_sft/config.py`.
- Preserve parse/validation causes with `raise ... from e` or `raise ... from exc`, as in `backend/services/feeds/main.py`, `backend/pipeline/storage/feed_store.py`, and `model/src/gemini_sft/config.py`.
- Use domain-specific Python exceptions from `backend/pipeline/common/exceptions.py`, including `FeedAlreadyExistsError`, `FeedNameAlreadyExistsError`, `FeedStateConflictError`, and `NonRetryableError`.
- Return `None` or `False` from service-layer methods for invalid IDs before touching storage, as in `backend/services/feeds/service.py` `update_feed`, `get_feed`, `deactivate_feed`, `delete_feed`, and `reset_feed`.
- Convert service/storage exceptions into HTTP responses at FastAPI boundaries, as in `backend/services/feeds/main.py` mapping `ValueError` to 400, duplicate feed errors to 409, and missing feeds to 404.
- In TypeScript, catch `unknown`, normalize errors, and throw `HttpError` at controller boundaries, as in `frontend/api/src/feeds/feedsController.ts` and helper handling in `frontend/api/src/utils.ts`.
- In frontend services and hooks, fail through thrown `Error` for request failures and suppress noncritical polling failures with `console.error` plus empty results, as in `frontend/transcription-ui/src/service/listFeeds.test.ts` and `frontend/transcription-ui/src/hooks/useAudioSegments.ts`.
- Validate required environment variables at module load in `frontend/api/src/config.ts`; hard-required service URLs throw, while optional deployment metadata logs with `console.error`.
## Logging
- Define `logger = logging.getLogger(__name__)` in Python modules, as in `backend/pipeline/common/log_helper.py`, `backend/pipeline/ingestion/collector_runtime.py`, `backend/pipeline/storage/feed_store.py`, and `integration_tests/e2e/test_transcription_pipeline.py`.
- Initialize Python logging through `backend/pipeline/common/log_helper.py` `setup_logging`; it installs system, thread, and asyncio exception handlers and configures Google Cloud Logging only in GCP environments.
- Use structured Python log fields through `extra={"json_fields": {...}}` for pipeline events, as in `backend/pipeline/common/log_helper.py` `record_pipeline_stage`, `backend/services/feeds/service.py` `deactivate_feed`, and `backend/pipeline/ingestion/tests/test_chunk_ingested.py`.
- Keep stable `event_type` strings in structured logs when tests or log-based metrics depend on them, as in `backend/pipeline/common/log_helper.py` and `backend/pipeline/ingestion/slo_contract.py`.
- Use `caplog` or `assertLogs` in tests for log contracts, as in `backend/pipeline/common/tests/test_actor_identity.py` and `backend/pipeline/ingestion/tests/test_chunk_ingested.py`.
- Use TypeScript `console.warn` for expected-but-unusual UI states and `console.error` for external API/admin lookup failures, as in `frontend/transcription-ui/src/hooks/useAudioSegments.ts`, `frontend/api/src/authentication.ts`, and `frontend/api/src/config.ts`.
## Comments
- Comment invariants that constrain future edits, as in `backend/pipeline/ingestion/collector_runtime.py` documenting shutdown wait points and heartbeat separation.
- Comment cross-file registration requirements, as in `backend/pipeline/storage/feed_store.py` `SourceType` and `backend/pipeline/ingestion/router.py` `_COLLECTORS`.
- Comment test intent when a test pins a contract, as in `backend/pipeline/ingestion/tests/test_slo_contract_lint.py`, `backend/pipeline/ingestion/tests/test_chunk_ingested.py`, and `model/tests/common/tests/test_manifest.py`.
- Avoid comments for simple assignments; local exceptions exist for API contract examples and UI event behavior in `frontend/api/src/feeds/feedsController.ts` and `frontend/transcription-ui/src/components/transcripts/TranscriptRow.tsx`.
- Use Python docstrings on public classes/functions and complex helpers, following Google-style intent configured in `pyproject.toml` `[tool.ruff.lint.pydocstyle] convention = "google"`.
- Keep TypeScript comments focused on API docs where they feed TSOA/OpenAPI or explain query semantics, as in `frontend/api/src/feeds/feedsController.ts` `ListFeedsQueryParams`.
- Do not require docstrings on every Python public symbol; `pyproject.toml` ignores Ruff docstring rules `D100` through `D107` and several formatting-specific `D` rules.
## Function Design
## Module Design
<!-- GSD:conventions-end -->

<!-- GSD:architecture-start source:ARCHITECTURE.md -->
## Architecture

## System Overview
```text
```
## Component Responsibilities
| Component | Responsibility | File |
|-----------|----------------|------|
| VM ingestion entry point | Starts the collector runtime, validates source registries, and blocks until graceful shutdown. | `backend/pipeline/ingestion/main.py` |
| Ingestion router | Maps `SourceType` values to collector functions and Pub/Sub topic families. | `backend/pipeline/ingestion/router.py` |
| Collector runtime | Leases feeds, runs async collector tasks, uploads captured audio to GCS, bookmarks progress, publishes Pub/Sub, renews heartbeats, and records failures. | `backend/pipeline/ingestion/collector_runtime.py` |
| Source runtime registry | Defines claimable source types, per-worker caps, URL bases, and continuous versus segmented topic routing. | `backend/pipeline/ingestion/source_runtime_specs.py` |
| Echo ingestion | Handles GCS object finalize events for Echo MP3 recordings through a synchronous Cloud Function path. | `backend/pipeline/ingestion/collectors/echo/main.py` |
| Segmentation pipeline | Runs the streaming Apache Beam/Dataflow DAG that orders, stitches, VAD-classifies, uploads raw segments, and emits `SegmentedAudio`. | `backend/pipeline/segmentation/orchestration.py` |
| Normalization function | Converts `SegmentedAudio` claim-checks into canonical/playback audio, persists segment metadata, and emits `NormalizedAudio`. | `backend/pipeline/normalization/processor.py` |
| Transcription service | Consumes `NormalizedAudio`, invokes a selected transcriber, writes transcript annotations, and emits `TranscribedAudio`. | `backend/pipeline/transcription/processor.py` |
| Transcriber factory | Selects Chirp, Gemini, local Whisper, or mock transcriber implementations from runtime config. | `backend/pipeline/transcription/transcribers/factory.py` |
| Evaluation function | Evaluates transcripts against rules, writes evaluation annotations, and publishes evaluated alert payloads. | `backend/pipeline/evaluation/processor.py` |
| Notification function | Deduplicates evaluated alerts, fetches feed tags, builds notification protobufs, and posts outbound notifications. | `backend/pipeline/notification/send_notification.py` |
| Feeds service | Exposes feed lifecycle CRUD/reset/deactivate routes and resolves trusted actor headers for audited mutations. | `backend/services/feeds/main.py` |
| Audio segments service | Exposes segment listing, segment creation, and annotation creation over FastAPI. | `backend/services/audio_segments/main.py` |
| Rules service | Exposes rule CRUD routes and delegates to AlloyDB-backed rule storage. | `backend/services/rules/main.py` |
| Storage layer | Owns AlloyDB access, SQL contracts, feed lifecycle/audit SQL, pagination, and service stores. | `backend/pipeline/storage` |
| Protobuf contracts | Define Pub/Sub message boundaries between pipeline stages. | `protos` |
| Frontend BFF | Provides TSOA routes, Google auth integration, service-to-service ID-token clients, and backend response conversion. | `frontend/api/src` |
| React UI | Provides operator views for feeds, transcripts, rules, docs, auth, playback, and polling. | `frontend/transcription-ui/src` |
| Shared frontend types | Publishes UI/BFF domain types and status conversion helpers. | `frontend/common/src/index.ts` |
| Model package | Provides shared ASR/model helpers and the `gemini-sft` CLI workflow. | `model/src` |
| Infrastructure modules | Define reusable GCP resources and AlloyDB SQL migrations. | `terraform/modules` |
## Pattern Overview
- Audio bytes move through GCS claim-check URIs; Pub/Sub carries protobuf metadata from `protos/*.proto`.
- Backend services use a controller/service/store split: FastAPI route in `backend/services/*/main.py`, domain service in `backend/services/*/service.py`, SQL store in `backend/pipeline/storage/*_store.py`.
- Ingestion separates source-specific collection from runtime-owned side effects. Collectors emit `CapturedChunk`, `SourceObservation`, or `FeedFailure`; `CollectorRuntime` owns GCS, Pub/Sub, bookmarks, heartbeats, and failure budgeting.
- The frontend separates browser views in `frontend/transcription-ui/src`, BFF controllers in `frontend/api/src`, and shared TypeScript contracts in `frontend/common/src`.
- Research/model workflows are packaged separately under `model/src` and use GCS-authoritative run state instead of production service state.
## Layers
- Purpose: Define message and type boundaries used by production pipeline, API, UI, and model workflows.
- Location: `protos`, `frontend/common/src`, `backend/services/*/models.py`, `backend/pipeline/common/rules/models.py`, `model/src/common`
- Contains: Protobuf schemas, Pydantic models, TypeScript interfaces, manifest helpers, scoring helpers.
- Depends on: Protobuf tooling, Pydantic, TypeScript packages, Python standard libraries.
- Used by: `backend/pipeline/*`, `backend/services/*`, `frontend/api/src`, `frontend/transcription-ui/src`, `model/src/gemini_sft`.
- Purpose: Claim feeds, collect source audio, upload staged audio, publish source-specific claim-check messages, and maintain feed lifecycle state.
- Location: `backend/pipeline/ingestion`
- Contains: VM runtime, source collector registry, source runtime specs, collector implementations, failure classifiers, health/memory watchdogs, retry policy.
- Depends on: `backend/pipeline/storage`, `backend/pipeline/common`, GCS, Pub/Sub, AlloyDB.
- Used by: VM capturer deployment and Echo Cloud Function deployment.
- Purpose: Transform continuous audio chunks into ordered speech/non-speech segments with VAD and stateful stitching.
- Location: `backend/pipeline/segmentation`
- Contains: Apache Beam pipeline assembly, stateful transforms, pure-Python stitching engine, VAD/audio utilities, Beam coders.
- Depends on: Pub/Sub, GCS, Beam/Dataflow, ONNX VAD models under `backend/pipeline/segmentation/audio/models`.
- Used by: Continuous ingestion topics before normalization.
- Purpose: Convert staged/raw segment audio into canonical FLAC, playback M4A, optional mono transcription FLAC, persisted audio segment rows, and downstream normalized claim-checks.
- Location: `backend/pipeline/normalization`
- Contains: Functions Framework entry point, warm client container, `NormalizationEventProcessor`, audio transcode helpers.
- Depends on: `backend/pipeline/common/storage/gcs_uploader.py`, `backend/pipeline/schema_types`, `backend/pipeline/common/clients/audio_segments_client.py`, GCS, Pub/Sub.
- Used by: Cloud Function or Cloud Run triggered from segmented Pub/Sub.
- Purpose: Invoke ASR, persist transcript annotation data, and publish `TranscribedAudio`.
- Location: `backend/pipeline/transcription`
- Contains: FastAPI Pub/Sub push endpoint, warm service container, processor, transcriber interface and implementations.
- Depends on: Google Chirp/Gemini/local Whisper/mock transcribers, `backend/pipeline/common/clients/audio_segments_client.py`, Pub/Sub.
- Used by: Cloud Run ASGI service triggered by Pub/Sub push.
- Purpose: Evaluate transcript text against configured rules, persist evaluation annotations, publish alert candidates, deduplicate, and send outbound notifications.
- Location: `backend/pipeline/evaluation`, `backend/pipeline/notification`
- Contains: Evaluation processor/service, remote/static rule evaluators, notification conversion, Redis-backed dedupe, outbound request handler.
- Depends on: Rules API, Audio Segments API, Feeds API, Pub/Sub, Redis, outbound notification endpoint.
- Used by: Cloud Function/Cloud Run functions triggered by transcribed/evaluated Pub/Sub topics.
- Purpose: Provide internal HTTP APIs for feed lifecycle, audio segments/annotations, and rules.
- Location: `backend/services`
- Contains: FastAPI apps, Pydantic models, thin service classes, tests.
- Depends on: `backend/pipeline/storage`, `backend/pipeline/common/auth.py`, AlloyDB.
- Used by: Pipeline functions through service clients and by the frontend BFF.
- Purpose: Centralize AlloyDB access, feed lifecycle invariants, audit event SQL, keyset pagination, and sync/async store variants.
- Location: `backend/pipeline/storage`
- Contains: `FeedStore`, `SyncFeedStore`, `AudioSegmentStore`, `RulesStore`, `TranscriptStore`, SQL query modules, connection helpers, pagination.
- Depends on: `terraform/modules/alloydb/sql/ingestion` schema, `asyncpg`, `psycopg`, Pydantic service models.
- Used by: FastAPI services, ingestion runtime, Echo ingestion, integration tests.
- Purpose: Authenticate users, expose UI-facing REST routes, translate casing/shape between common UI types and backend service APIs, and call backend services with Google ID-token clients.
- Location: `frontend/api/src`
- Contains: Express app, TSOA controllers, generated route registration, auth handler, config, downstream HTTP utilities.
- Depends on: `frontend/common/src`, Google auth libraries, downstream backend service URLs.
- Used by: `frontend/transcription-ui/src` browser services.
- Purpose: Render operator views, manage auth context, poll transcript/audio segment data, and call BFF endpoints.
- Location: `frontend/transcription-ui/src`
- Contains: Routes, MUI shell, auth provider, React Query hooks, service wrappers, components, playback utilities.
- Depends on: `frontend/common/src`, BFF API base URL, Google OAuth client ID.
- Used by: Browser users.
- Purpose: Package reusable ASR/model utilities and the Gemini supervised fine-tuning workflow.
- Location: `model/src`
- Contains: `common` helpers, Gemini Vertex helpers, manifest/scoring utilities, `gemini_sft` CLI.
- Depends on: GCS, Vertex Gemini APIs, optional audio/scoring/HF extras from `model/pyproject.toml`.
- Used by: Researchers/operators running `gemini-sft prepare`, `gemini-sft tune`, and `gemini-sft eval`.
- Purpose: Define deployable GCP resources and database schema.
- Location: `terraform/modules`
- Contains: AlloyDB, Cloud Function, container MIG, GCS bucket, Memorystore Redis, ASR evaluation modules, SQL migrations.
- Depends on: Terraform module consumers outside this subtree and SQL migrations under `terraform/modules/alloydb/sql/ingestion`.
- Used by: Deployment automation and local/integration setup.
## Data Flow
### Primary Audio Processing Path
### Echo Ingestion Path
### UI And Admin Request Path
### Gemini SFT Operator Path
- AlloyDB is the system of record for feeds, feed lifecycle state, feed audit events, audio segments, annotations, rules, and legacy transcript storage through `backend/pipeline/storage`.
- GCS stores staged raw chunks, canonical/playback/transcription audio, and model/SFT artifacts; pipeline messages carry GCS URIs instead of large audio payloads.
- Pub/Sub carries protobuf claim-check messages and uses feed IDs as ordering keys where order matters.
- Apache Beam/Dataflow owns state and timers for continuous audio ordering/stitching in `backend/pipeline/segmentation/transforms/stateful.py`.
- Redis backs notification deduplication through `backend/pipeline/common/storage/redis_service.py` and `backend/pipeline/notification/notification_deduplication.py`.
- React Query owns browser-side request caches in hooks under `frontend/transcription-ui/src/hooks`.
- Serverless entry points cache clients/processors in module-level or lifespan containers such as `NormalizationServiceContainer`, `TranscriptionServiceContainer`, `EvaluationServiceContainer`, and `NotificationServiceContainer`.
## Key Abstractions
- Purpose: Data-only source metadata for source type, topic kind, claimability, caps, and URL base.
- Examples: `backend/pipeline/ingestion/source_runtime_specs.py`, `backend/pipeline/storage/feed_store.py`
- Pattern: Registry plus startup invariant; update `SourceType`, SQL seed data, `SourceRuntimeSpec`, router, and tests together.
- Purpose: Isolate source-specific collection from runtime-owned side effects and feed lifecycle state.
- Examples: `backend/pipeline/ingestion/models.py`, `backend/pipeline/ingestion/collectors/README.md`, `backend/pipeline/ingestion/router.py`
- Pattern: Async generator yields `CapturedChunk` or `SourceObservation`; typed failures use `FeedFailure`.
- Purpose: Centralize lifecycle transitions, lease fencing, failure budgeting, recovery, hard delete, reset, and audit event inserts.
- Examples: `backend/pipeline/storage/feed_store.py`, `backend/pipeline/storage/feed_queries.py`, `backend/pipeline/storage/feed_audit_sql.py`, `backend/pipeline/storage/sync_feed_store.py`
- Pattern: Store methods call SQL constants/fragments; services pass explicit `actor_id`; audit JSON allowlists live in SQL helper fragments.
- Purpose: Define durable Pub/Sub boundaries for audio chunks and stage outputs.
- Examples: `protos/continuous_audio.proto`, `protos/segmented_audio.proto`, `protos/normalized_audio.proto`, `protos/transcribed_audio.proto`, `protos/evaluated_transcribed_audio.proto`, `protos/alert_notification.proto`
- Pattern: Protobuf metadata plus GCS URIs, with generated Python bindings under `backend/pipeline/schema_types`.
- Purpose: Keep cloud/function entry point code small and make stage logic testable.
- Examples: `backend/pipeline/normalization/processor.py`, `backend/pipeline/transcription/processor.py`, `backend/pipeline/evaluation/processor.py`
- Pattern: Entry point parses/wires clients; processor owns parsing, business logic, persistence, and publishing.
- Purpose: Select ASR backend at runtime while keeping transcription processor independent of implementation details.
- Examples: `backend/pipeline/transcription/transcribers/base.py`, `backend/pipeline/transcription/transcribers/factory.py`, `backend/pipeline/transcription/enums.py`
- Pattern: Factory returns an implementation from `TRANSCRIBER_TYPE` and `TRANSCRIBER_CONFIG`.
- Purpose: Expose internal HTTP APIs over store-backed domain services.
- Examples: `backend/services/feeds/main.py`, `backend/services/audio_segments/main.py`, `backend/services/rules/main.py`
- Pattern: Lifespan creates AlloyDB pool and service; route handlers translate validation/store errors into HTTP responses.
- Purpose: Present UI-friendly API contracts and handle auth/admin checks before calling backend services.
- Examples: `frontend/api/src/feeds/feedsController.ts`, `frontend/api/src/audio/audioController.ts`, `frontend/api/src/rules/rulesController.ts`, `frontend/api/src/auth/authController.ts`
- Pattern: Controller decorators generate routes; controller methods convert common types to backend wire format and use `getServiceClient`.
- Purpose: Keep UI components declarative while service files own HTTP calls.
- Examples: `frontend/transcription-ui/src/hooks/useAudioSegments.ts`, `frontend/transcription-ui/src/service/listAudioSegments.ts`, `frontend/transcription-ui/src/utils/apiUtils.ts`
- Pattern: Hooks own cache keys and pagination/polling; `service/*.ts` owns endpoint construction and bearer tokens.
- Purpose: External TOML config drives repeatable model prepare/tune/eval workflows with GCS as source of truth.
- Examples: `model/src/gemini_sft/config.py`, `model/src/gemini_sft/prepare.py`, `model/src/gemini_sft/tune.py`, `model/src/gemini_sft/evaluate.py`
- Pattern: CLI subcommands share `RunConfig`, persist `config.json`, and mirror artifacts locally under `results/`.
## Entry Points
- Location: `backend/pipeline/ingestion/main.py`
- Triggers: Container process start.
- Responsibilities: Initialize logging/tracing/settings, validate source routing and caps, start `CollectorRuntime`.
- Location: `backend/pipeline/ingestion/collectors/echo/main.py`
- Triggers: Eventarc GCS object finalize events.
- Responsibilities: Resolve Echo feed, download MP3, stage audio, publish segmented claim-check, update sync feed state.
- Location: `backend/pipeline/segmentation/main.py`
- Triggers: CLI/container process start with Beam options.
- Responsibilities: Build and run the streaming Beam DAG from `backend/pipeline/segmentation/orchestration.py`.
- Location: `backend/pipeline/normalization/main.py`
- Triggers: Pub/Sub CloudEvent containing `SegmentedAudio`.
- Responsibilities: Get/warm `NormalizationEventProcessor` and process the event.
- Location: `backend/pipeline/transcription/main.py`
- Triggers: Pub/Sub push HTTP POST to `/`.
- Responsibilities: Warm transcriber/publisher/API clients and process `NormalizedAudio`.
- Location: `backend/pipeline/evaluation/main.py`
- Triggers: Pub/Sub CloudEvent containing `TranscribedAudio`.
- Responsibilities: Create evaluator and process transcript evaluation.
- Location: `backend/pipeline/notification/send_notification.py`
- Triggers: Pub/Sub CloudEvent containing `EvaluatedTranscribedAudio`.
- Responsibilities: Deduplicate, fetch tags, convert to `AlertNotification`, and send outbound request.
- Location: `backend/services/audio_segments/main.py`, `backend/services/feeds/main.py`, `backend/services/rules/main.py`
- Triggers: Internal HTTP requests from pipeline clients or BFF.
- Responsibilities: Authenticate OIDC, validate requests, call service/store classes, return JSON.
- Location: `frontend/api/src/index.ts`
- Triggers: Node/Cloud Run process serving Express.
- Responsibilities: Configure middleware, register TSOA routes, centralize error handling.
- Location: `frontend/transcription-ui/src/main.tsx`
- Triggers: Browser load of Vite bundle.
- Responsibilities: Create OAuth, React Query, auth, and routing providers.
- Location: `model/src/gemini_sft/cli.py`
- Triggers: `gemini-sft` console script.
- Responsibilities: Dispatch `prepare`, `tune`, and `eval` workflows.
## Architectural Constraints
- **Threading:** `CollectorRuntime` uses `uvloop` for async feed tasks and separate OS threads for heartbeat and memory watchdog behavior in `backend/pipeline/ingestion/collector_runtime.py`. Serverless processors use warm cached clients in module-level or FastAPI lifespan containers. Beam/Dataflow uses serialized state/timer APIs in `backend/pipeline/segmentation/transforms/stateful.py`.
- **Global state:** Warm containers are module-level in `backend/pipeline/normalization/main.py`, `backend/pipeline/evaluation/main.py`, and `backend/pipeline/notification/send_notification.py`; Echo caches `gcs_client`, `pubsub_client`, and `feed_store` in `backend/pipeline/ingestion/collectors/echo/main.py`; React Query uses a process-level `QueryClient` in `frontend/transcription-ui/src/main.tsx`.
- **Circular imports:** Not detected in the architecture scan. Keep storage imports below services where possible: stores may import service models for validation, but FastAPI services should not build SQL or audit rows directly.
- **Generated protobufs:** Generated Python protobuf bindings live under `backend/pipeline/schema_types` and are excluded by lint config in `pyproject.toml`. Regenerate from `protos/*.proto` after schema changes using the command documented in `backend/pipeline/README.md`.
- **Source type changes:** Adding a claimable source type requires coordinated updates to `backend/pipeline/storage/feed_store.py`, `terraform/modules/alloydb/sql/ingestion/002_source_types.sql`, `terraform/modules/alloydb/sql/ingestion/006_seed_source_types.sql`, `backend/pipeline/ingestion/source_runtime_specs.py`, `backend/pipeline/ingestion/router.py`, and tests.
- **Audit writes:** Audited feed lifecycle mutations require explicit actor IDs through `backend/services/feeds/main.py`, `backend/services/feeds/service.py`, and `backend/pipeline/storage/feed_store.py`. Audit event SQL belongs in `backend/pipeline/storage/feed_audit_sql.py` and query modules.
- **Hot feed table:** The feed leasing table has hot-path protections and SQL/index constraints under `terraform/modules/alloydb/sql/ci/hot_protection_check.sql`; avoid adding indexes on frequently updated feed columns without checking that contract.
- **Secrets:** Environment configuration is referenced by config files and code, but secret values must stay outside docs and source maps. Do not read `.env`, `.env.*`, `*.env`, credential, key, or secret files.
## Anti-Patterns
### Collector Writes Lifecycle State Directly
### Source Type Registry Drift
### Services Build Audit Rows
### UI Calls Backend Services Directly
### Editing Generated Routes Or Protobuf Outputs By Hand
## Error Handling
- Ingestion source failures use `FeedFailure` and `failure_policy` under `backend/pipeline/ingestion`; runtime pipeline failures use `_PipelineFailure` in `backend/pipeline/ingestion/collector_runtime.py`.
- Normalization treats malformed inputs and permanent processing failures as no-retry or DLQ conditions in `backend/pipeline/normalization/processor.py`.
- Transcription re-raises transient ASR/API failures and writes transcript annotations with error details for permanent failures in `backend/pipeline/transcription/processor.py`.
- Evaluation validates required fields before evaluating and writes evaluation annotations through `backend/pipeline/evaluation/processor.py`.
- FastAPI services translate `ValueError`, missing rows, conflicts, and store exceptions into `HTTPException` in `backend/services/*/main.py`.
- The BFF wraps downstream service errors with `HttpError` through `frontend/api/src/utils.ts` and centralizes Express error responses in `frontend/api/src/index.ts`.
## Cross-Cutting Concerns
<!-- GSD:architecture-end -->

<!-- GSD:skills-start source:skills/ -->
## Project Skills

No project skills found. Add skills to any of: `.claude/skills/`, `.agents/skills/`, `.cursor/skills/`, `.github/skills/`, or `.codex/skills/` with a `SKILL.md` index file.
<!-- GSD:skills-end -->

<!-- GSD:workflow-start source:GSD defaults -->
## GSD Workflow Enforcement

Before using Edit, Write, or other file-changing tools, start work through a GSD command so planning artifacts and execution context stay in sync.

Use these entry points:
- `/gsd-quick` for small fixes, doc updates, and ad-hoc tasks
- `/gsd-debug` for investigation and bug fixing
- `/gsd-execute-phase` for planned phase work

Do not make direct repo edits outside a GSD workflow unless the user explicitly asks to bypass it.
<!-- GSD:workflow-end -->

<!-- GSD:profile-start -->
## Developer Profile

> Profile not yet configured. Run `/gsd-profile-user` to generate your developer profile.
> This section is managed by `generate-claude-profile` -- do not edit manually.
<!-- GSD:profile-end -->

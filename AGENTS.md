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

**Feed Audit Events V1**

Feed Audit Events V1 adds durable, queryable history for meaningful feed
mutations in the radio transcription backend. It is for Watch Duty engineers
and future admin tooling that need to answer what happened to a feed, when it
happened, what changed, and whether the cause was a human action or system
runtime behavior.

This project is not full event sourcing. The current `feeds` row remains the
authoritative current-state model; the new work adds an append-only audit
history and a cleaner current diagnostic detail field.

**Core Value:** Operators can reconstruct meaningful feed lifecycle and configuration changes
from durable backend data instead of relying on short-lived logs.

### Constraints

- **Brownfield architecture**: Preserve the existing current-state `feeds`
  model, storage-layer SQL patterns, and FastAPI service boundaries — the
  ingestion runtime already depends on current-state lease queries and fenced
  writes.
- **Database consistency**: Feed mutations and audit inserts must commit or
  roll back together — audit history is only useful if it cannot drift from
  successful state changes.
- **Compatibility**: Existing consumers of `quarantine_reason` must keep
  working during the v1 rollout — add `status_reason_detail` without removing
  the old field immediately.
- **Signal quality**: Do not audit routine heartbeat or lease churn by default
  — the audit table must stay understandable and affordable.
- **Security**: Do not persist secrets, tokens, raw credential-bearing
  exception strings, or unbounded provider responses in diagnostic detail —
  persisted reason text must be bounded and scrubbed where needed.
- **Delivery boundary**: WD backend delivery is a later phase — v1 schema should
  support it without introducing dispatcher state or webhook attempts yet.
<!-- GSD:project-end -->

<!-- GSD:stack-start source:codebase/STACK.md -->
## Technology Stack

## Languages
- Python 3.13 - backend pipeline, FastAPI services, CloudEvent handlers, ingestion workers, and Terraform helper scripts under `backend/`, `local_dev/`, `scripts/`, and root `pyproject.toml`.
- TypeScript 6.0 - frontend API proxy, shared frontend types, and React UI under `frontend/api/`, `frontend/common/`, and `frontend/transcription-ui/`.
- Terraform 1.14.5 - reusable GCP infrastructure modules under `terraform/modules/`.
- Protocol Buffers - pipeline event contracts under `protos/`, generated into `backend/pipeline/schema_types/`.
- Shell/Dockerfile/YAML - container builds, Docker Compose local stack, GitHub Actions, and mise tasks in `backend/**/Dockerfile`, `docker-compose.yml`, `.github/workflows/`, and `.mise.toml`.
- Python 3.11+ - model subtree package runtime declared in `model/pyproject.toml`; backend still requires Python 3.13 via root `pyproject.toml`.
## Runtime
- Python `>=3.13,<3.14` for root/backend workspace packages in `pyproject.toml` and `backend/**/pyproject.toml`.
- Python `>=3.11` for the model package in `model/pyproject.toml`.
- Node.js 22.14.0 from `.tool-versions`; `frontend/api/tsconfig.json` extends `@tsconfig/node22`.
- Terraform 1.14.5 and uv 0.9.28 from `.tool-versions`.
- Python: `uv` workspace with root `uv.lock` and `model/uv.lock`; workspace members are declared in `[tool.uv.workspace]` in `pyproject.toml`.
- TypeScript: Yarn classic lockfiles (`# yarn lockfile v1`) in `frontend/common/yarn.lock`, `frontend/api/yarn.lock`, and `frontend/transcription-ui/yarn.lock`.
- Lockfile: present for Python root, Python model, and all three frontend packages.
## Frameworks
- FastAPI `>=0.110.0` + Uvicorn `>=0.27.0` - backend HTTP APIs in `backend/services/audio_segments/`, `backend/services/feeds/`, `backend/services/rules/`, `backend/services/transcripts/`, and `backend/services/local-whisper-api/`.
- Functions Framework `>=3.10.1` - CloudEvent entry points in `backend/pipeline/transcription/main.py`, `backend/pipeline/evaluation/main.py`, `backend/pipeline/notification/send_notification.py`, `backend/pipeline/normalization/main.py`, and `backend/pipeline/ingestion/collectors/echo/main.py`.
- Apache Beam `apache-beam[gcp]>=2.74.0` - segmentation/Dataflow pipeline in `backend/pipeline/segmentation/`.
- Express 5 + tsoa 7 alpha - frontend API proxy and generated OpenAPI routes in `frontend/api/src/index.ts`, `frontend/api/tsoa.json`, and `frontend/api/openapi.yaml`.
- React 19 + Vite 8 + MUI 9 - browser UI in `frontend/transcription-ui/`.
- Docker Compose - full local pipeline stack in `docker-compose.yml`, local Whisper overlay in `docker-compose.whisper.yml`, and ASR evaluation stack in `asr-eval-docker-compose.yml`.
- pytest 9 + pytest-asyncio + pytest-cov + pytest-xdist - backend/model tests configured in `pyproject.toml`, `.mise.toml`, and `model/pyproject.toml`.
- Vitest 4 + Testing Library + jsdom - frontend API/UI tests declared in `frontend/api/package.json` and `frontend/transcription-ui/package.json`.
- Testcontainers + Docker - storage/component tests and CI pre-pulls in `pyproject.toml` and `.github/workflows/ci.yml`.
- mise - task runner in `.mise.toml`; use tasks such as `mise run generate:protos`, `mise run lint`, `mise run test:unit`, and `mise run dev`.
- Ruff 0.14 + ty 0.0.42 + Pyright settings - Python formatting/lint/type-check config in `pyproject.toml`.
- ESLint 10 + Prettier 3 + TypeScript 6 - frontend checks in `frontend/api/eslint.config.js`, `frontend/transcription-ui/eslint.config.js`, and package scripts.
- grpcio-tools - protobuf generation from `protos/*.proto` to `backend/pipeline/schema_types/`.
- Docker Buildx/GHCR - CI image builds in `.github/workflows/ci.yml` and `.github/workflows/bake-main.yml`.
## Key Dependencies
- `google-cloud-pubsub`, `google-cloud-storage`, `google-cloud-speech`, `google-cloud-secret-manager`, `google-cloud-logging`, `google-cloud-monitoring`, and `google-auth` - GCP messaging, object storage, Speech-to-Text, secrets, logging, monitoring, and auth across `backend/pipeline/common/pyproject.toml` and pipeline package manifests.
- `asyncpg>=0.29.0` and `psycopg[binary]>=3.2.0` - AlloyDB/Postgres access in `backend/pipeline/storage/connection.py` and `backend/pipeline/storage/sync_connection.py`.
- `redis>=7.3.0` - notification deduplication/cache integration in `backend/pipeline/common/storage/redis_service.py`.
- `pydantic>=2.10.6` / `pydantic-settings>=2.0.0` - API and pipeline models under `backend/services/**/models.py` and config utilities.
- `opentelemetry-api`, `opentelemetry-sdk`, `opentelemetry-exporter-gcp-trace`, and `opentelemetry-exporter-gcp-monitoring` - telemetry setup in `backend/pipeline/common/tracing_utils.py`.
- `google-genai>=2.3,<3` - optional Vertex AI Gemini tuning and batch inference in `model/src/common/gemini/vertex.py`.
- `apache-beam[gcp]>=2.74.0` - Dataflow-compatible segmentation runtime in `backend/pipeline/segmentation/pyproject.toml` and `backend/pipeline/segmentation/Dockerfile`.
- `faster-whisper>=1.0.0` - local ASR API in `backend/services/local-whisper-api/`.
- `onnxruntime`, `pedalboard`, `numba`, `numpy`, `soundfile`, `av`, and FFmpeg/ffprobe - audio segmentation and normalization in `backend/pipeline/segmentation/pyproject.toml`, `backend/pipeline/normalization/pyproject.toml`, and service Dockerfiles.
- `aiohttp`, `curl-cffi`, `requests`, `urllib3`, and `tenacity` - provider ingestion, internal service clients, retries, and HTTP calls under `backend/pipeline/ingestion/` and `backend/pipeline/common/clients/`.
- `@react-oauth/google`, `google-auth-library`, and `jose` - Google OAuth/JWT flows in `frontend/transcription-ui/src/main.tsx`, `frontend/api/src/auth/authController.ts`, and `frontend/api/src/authentication.ts`.
- `@tanstack/react-query`, `@mui/material`, `@toolpad/core`, `wavesurfer.js`, `howler`, `react-virtuoso`, and `swagger-ui-react` - UI state/data fetching, components, audio playback, virtualization, and docs in `frontend/transcription-ui/package.json`.
## Configuration
- mise loads `.env` through `[env] _.file = ".env"` in `.mise.toml`; do not read or commit local env contents.
- Env placeholder files are present at `frontend/api/.env.example`, `frontend/transcription-ui/.env.example`, `frontend/transcription-ui/.env.local-dev.example`, and `local_dev/LOCAL.env`; contents are treated as secret-bearing and not quoted.
- Backend service URLs and auth config are validated centrally in `frontend/api/src/config.ts`: `ALLOWED_ORIGIN`, `TRANSCRIPTS_API_URL`, `RULES_API_URL`, `FEEDS_STORE_API_URL`, `AUDIO_SEGMENTS_API_URL`, `PROJECT_ID`, `API_PUBLIC_URL`, `GOOGLE_AUTH_CLIENT_ID`, `GOOGLE_AUTH_CLIENT_SECRET`, `AUTH_BACKEND`, and `WORKSPACE_ADMIN_GROUP_EMAIL`.
- AlloyDB config is loaded from `ALLOYDB_*` env vars in `backend/pipeline/storage/settings.py`.
- Ingestion worker config is loaded from GCS, Pub/Sub, lease, retry, health, and watchdog env vars in `backend/pipeline/ingestion/settings.py`.
- Root Python config: `pyproject.toml`, `uv.lock`, `.mise.toml`, `.pre-commit-config.yaml`.
- Model config: `model/pyproject.toml`, `model/uv.lock`, `model/notebook_docker/Dockerfile`, `model/nemo_docker/Dockerfile`.
- Frontend config: `frontend/api/package.json`, `frontend/api/tsconfig.json`, `frontend/api/tsoa.json`, `frontend/transcription-ui/package.json`, `frontend/transcription-ui/vite.config.ts`, and frontend ESLint configs.
- Infrastructure config: `terraform/modules/**`, `.github/workflows/ci.yml`, `.github/workflows/integration-tests.yml`, `.github/workflows/bake-main.yml`, and `.github/workflows/trigger-deploy.yml`.
## Platform Requirements
- Install tools from `.tool-versions`: uv 0.9.28, Python 3.13.2, Node 22.14.0, Terraform 1.14.5, and jq.
- Use `mise` tasks in `.mise.toml` for local workflows; broad E2E/component/API Docker tests are resource-heavy per `AGENTS.md` and `.agents/instructions.md`.
- Local stack uses Docker Compose services for Pub/Sub emulator, fake GCS, Postgres 15, Redis 7, backend pipeline services, frontend API, mock servers, and optional local Whisper in `docker-compose.yml` and `docker-compose.whisper.yml`.
- FFmpeg/ffprobe are required for audio processing and are copied into service images from `mwader/static-ffmpeg:6.1.1`.
- GCP is the primary platform: Cloud Functions Gen2 for CloudEvent handlers via `terraform/modules/cloud_function/`, GCE regional Managed Instance Groups on Container-Optimized OS via `terraform/modules/container_mig/`, Dataflow-compatible Beam image in `backend/pipeline/segmentation/Dockerfile`, AlloyDB via `terraform/modules/alloydb/`, GCS via `terraform/modules/gcs_bucket/`, and Memorystore Redis via `terraform/modules/memorystore_for_redis/`.
- GitHub Actions runs CI, image builds, integration tests, and private deployment dispatches in `.github/workflows/`.
- Container images are built for GHCR/GCP deployment from `backend/**/Dockerfile`, `frontend/api/Dockerfile`, and model Dockerfiles.
<!-- GSD:stack-end -->

<!-- GSD:conventions-start source:CONVENTIONS.md -->
## Conventions

## Naming Patterns
- Use Python `snake_case.py` modules and `test_*.py` tests in backend and model code, matching `backend/pipeline/ingestion/source_runtime_specs.py`, `backend/pipeline/ingestion/tests/test_source_runtime_specs.py`, and `model/src/gemini_sft/config.py`.
- Use package-local TypeScript naming already present in each frontend package: `camelCase.ts` and `camelCase.test.ts` for services/controllers such as `frontend/transcription-ui/src/service/listFeeds.ts`, `frontend/transcription-ui/src/service/listFeeds.test.ts`, `frontend/api/src/feeds/feedsController.ts`, and `frontend/api/src/feeds/feedsController.test.ts`.
- Use `PascalCase.tsx` and `PascalCase.test.tsx` for React components such as `frontend/transcription-ui/src/components/feeds/FeedTable.tsx` and `frontend/transcription-ui/src/components/feeds/FeedTable.test.tsx`.
- Use lowercase domain files for shared frontend types and utilities, as in `frontend/common/src/types/feeds.ts`, `frontend/common/src/types/audio.ts`, and `frontend/common/src/utils/statusUtils.ts`.
- Repo instruction files exist at `.github/instructions/PYTHON_STYLE.instructions.md` and `.github/instructions/JS_TS_STYLE.instructions.md`; current frontend source uses `.test.ts(x)` and camel/Pascal file names, so add new files by matching the surrounding package convention.
- Use Python `snake_case` for functions and methods, with `_private_helper` for module-internal helpers, as in `backend/services/feeds/service.py`, `backend/pipeline/ingestion/router.py`, and `model/src/gemini_sft/config.py`.
- Use `lowerCamelCase` for TypeScript functions and methods, as in `frontend/transcription-ui/src/service/listFeeds.ts` (`listFeedsPage`, `listFeeds`) and `frontend/api/src/utils.ts` (`handleBackendError`, `getServiceClient`).
- Use `UpperCamelCase` for React components and controller classes, as in `frontend/transcription-ui/src/components/feeds/FeedTable.tsx` (`FeedTable`) and `frontend/api/src/feeds/feedsController.ts` (`FeedsController`).
- Test names are descriptive behavior sentences in both Python and TypeScript, as in `backend/services/feeds/tests/test_api.py` (`test_create_feed_already_exists`) and `frontend/transcription-ui/src/service/listFeeds.test.ts` (`should loop and fetch all pages when response is paginated ListFeedsResponse object`).
- Use uppercase module constants for Python and TypeScript constants, as in `backend/pipeline/storage/tests/test_feed_store.py` (`_FEED_ID`, `_FEED_STATUS_REASON_VALUES`) and `frontend/transcription-ui/src/context/AuthProvider.tsx` (`REFRESH_TOKEN_INTERVAL`, `MAX_REFRESH_ATTEMPTS`).
- Use leading underscores for Python module-private helpers and fixtures, as in `backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py` (`_make_call`, `_mock_transport`) and `model/tests/gemini_sft/test_workflow.py` (`_manifest`, `_seed_source_manifests`).
- Use `lowerCamelCase` for TypeScript local variables and props, as in `frontend/transcription-ui/src/components/feeds/FeedTable.tsx` (`sortConfig`, `gridTemplateColumns`, `onFiltersChange`).
- Use Python `PascalCase` for dataclasses, `TypedDict`, Pydantic models, and exceptions, as in `backend/pipeline/ingestion/models.py` (`CapturedChunk`, `FeedFailure`, `CaptureResources`) and `backend/pipeline/storage/feed_store.py` (`LeasedFeed`, `PaginatedFeeds`, `FeedStatusReason`).
- Use `enum.StrEnum` for Python domain enums whose values are serialized strings, as in `backend/pipeline/storage/feed_store.py` (`SourceType`, `FeedStatus`, `FeedStatusReason`) and `backend/pipeline/ingestion/models.py` (`AudioMimeType`).
- Use TypeScript `interface` for object shapes and `type` for unions, as in `frontend/common/src/types/feeds.ts` (`Feed`, `ListFeedsResponse`, `BackendFeedStatus`).
- Use TypeScript enum members in `CONSTANT_CASE`, as in `frontend/common/src/types/feeds.ts` (`SourceType.BCFY_FEEDS`, `SourceType.OPENMHZ`).
## Code Style
- Python is formatted with Ruff using `line-length = 80` and `target-version = "py313"` in `pyproject.toml`.
- TypeScript, TSX, JavaScript, and CSS are formatted with Prettier using semicolons, single quotes, `printWidth: 80`, `tabWidth: 2`, and sorted imports from `.prettierrc`.
- Notebooks under `model/colabs/` are formatted through `scripts/notebook_formatter.py` and Ruff tasks in `.mise.toml`.
- Terraform is formatted by `terraform fmt -recursive` through `.mise.toml`.
- Use the aggregate commands from `.mise.toml`: `mise run format`, `mise run lint`, and the pre-commit hooks in `.pre-commit-config.yaml`.
- Python linting is Ruff `select = ["ALL"]` with an explicit ignore list in `pyproject.toml`; keep ignore lists sorted because `.mise.toml` defines `lint:ruff:sorted`.
- Python type checking uses `ty check` through `.mise.toml` and `.pre-commit-config.yaml`; `pyproject.toml` excludes `model/` and `backend/services/local-whisper-api/` from `ty`.
- Frontend API linting uses `frontend/api/eslint.config.js` with `@eslint/js`, `typescript-eslint`, Node globals, and Prettier compatibility.
- Frontend UI linting uses `frontend/transcription-ui/eslint.config.js` with `typescript-eslint`, React Hooks, React Refresh, TanStack Query, CSS linting, browser globals, and Prettier compatibility.
- TypeScript strictness is enforced by `frontend/transcription-ui/tsconfig.app.json` and `frontend/transcription-ui/tsconfig.node.json` (`strict`, `noUnusedLocals`, `noUnusedParameters`, `erasableSyntaxOnly`, `noUncheckedSideEffectImports`) and by `frontend/api/tsconfig.json` extending `@tsconfig/node22`.
## Import Organization
- Python imports use repository-root absolute package paths such as `backend.pipeline.storage.feed_store` and `backend.pipeline.common.auth`, enabled by `PYTHONPATH = "."` in `.mise.toml`.
- Frontend shared code is consumed as the linked package `@transcription/common`, declared in `frontend/api/package.json` and `frontend/transcription-ui/package.json`, and re-exported from `frontend/common/src/index.ts`.
- No TypeScript `@/` alias is configured in `frontend/transcription-ui/tsconfig.app.json`, `frontend/transcription-ui/vite.config.ts`, or `frontend/api/tsconfig.json`; use relative imports within each frontend package.
## Error Handling
- Raise typed or built-in Python exceptions at domain boundaries with explicit messages and exception chaining, as in `model/src/gemini_sft/config.py` (`RunConfigError`) and `backend/pipeline/storage/feed_store.py` (`FeedAlreadyExistsError`, `FeedNameAlreadyExistsError`).
- Convert storage/service exceptions to FastAPI `HTTPException` at API boundaries, as in `backend/services/feeds/main.py`.
- Treat invalid UUIDs and missing records as `None`/`False` at service boundaries rather than leaking parser errors, as in `backend/services/feeds/service.py`.
- Classify ingestion source failures with `FeedFailure` and bounded `FeedStatusReason` values, as documented and implemented in `backend/pipeline/ingestion/models.py`.
- Frontend API controllers catch `unknown`, normalize downstream failures with `handleBackendError`, and throw `HttpError`, as in `frontend/api/src/feeds/feedsController.ts` and `frontend/api/src/utils.ts`.
- Frontend UI service functions let `apiFetch` failures reject and test those paths with Vitest, as in `frontend/transcription-ui/src/service/listFeeds.ts` and `frontend/transcription-ui/src/service/listFeeds.test.ts`.
## Logging
- Use module loggers in Python (`logger = logging.getLogger(__name__)`), as in `backend/services/feeds/main.py`, `backend/pipeline/storage/feed_store.py`, and `model/src/gemini_sft/prepare.py`.
- Use centralized setup from `backend/pipeline/common/log_helper.py`; it installs process/thread/asyncio exception handlers and Cloud Logging in GCP environments.
- Use structured task logging with `get_task_logger` and `TaskJsonFormatter` for Dataflow-style tasks in `backend/pipeline/common/log_helper.py`.
- Put contextual JSON fields in Python log `extra` where service events need structured payloads, as in `backend/services/feeds/service.py`.
- Use `logger.exception` when retaining stack traces at isolation points, as in `backend/pipeline/normalization/processor.py` and `backend/pipeline/evaluation/processor.py`.
- TypeScript API proxy errors are serialized to JSON in `frontend/api/src/utils.ts`; UI session failures use `console.error` in `frontend/transcription-ui/src/context/AuthProvider.tsx`.
## Comments
- Comment domain contracts, multi-step operational invariants, and non-obvious constraints near the code they govern, as in `backend/pipeline/ingestion/models.py` and `backend/pipeline/storage/feed_store.py`.
- Keep small inline comments for test intent or environment workarounds, as in `frontend/transcription-ui/src/test/setup.ts` and `backend/pipeline/segmentation/tests/test_orchestration.py`.
- Use TODO comments with enough context and an issue link when available, as in `frontend/transcription-ui/src/service/listFeeds.ts`.
- Use docstrings for Python public APIs, non-trivial behavior, exceptions, and domain models, following `.github/instructions/PYTHON_STYLE.instructions.md` and examples in `backend/pipeline/ingestion/models.py`.
- TypeScript API contracts rely primarily on interfaces and tsoa decorators in `frontend/api/src/feeds/feedsController.ts`; use JSDoc where generated OpenAPI metadata or developer-facing behavior needs explanation.
- React components in `frontend/transcription-ui/src/components/**` usually avoid heavy JSDoc; prefer clear prop interfaces such as `FeedTableProps` in `frontend/transcription-ui/src/components/feeds/FeedTable.tsx`.
## Function Design
## Module Design
<!-- GSD:conventions-end -->

<!-- GSD:architecture-start source:ARCHITECTURE.md -->
## Architecture

## System Overview
```text
|                    Radio Transcription Monorepo                                  |
| Audio source capture | Operator/data APIs    | Operator UI and model tooling     |
| `backend/pipeline/`  | `backend/services/`   | `frontend/`, `model/src/`         |
|              Typed contracts, claim-check messages, and stores                   |
|              `protos/`, `backend/pipeline/schema_types/`,                        |
|              `backend/pipeline/storage/`                                         |
| GCS audio objects    | Pub/Sub topics        | AlloyDB and Redis state           |
| `gcs_uri` fields     | protobuf payloads     | `terraform/modules/alloydb/`,     |
|                      |                       | `backend/pipeline/storage/`       |
```
## Component Responsibilities
| Component | Responsibility | File |
|-----------|----------------|------|
| Ingestion runtime | Lease feeds, run source collectors, upload chunks, publish initial Pub/Sub messages, record observations and failures | `backend/pipeline/ingestion/collector_runtime.py` |
| Source routing | Map `source_type` values to collector implementations and reject unsupported capture modes | `backend/pipeline/ingestion/router.py` |
| Runtime source specs | Define source capture mode, claimability, runtime caps, and default lease durations | `backend/pipeline/ingestion/source_runtime_specs.py` |
| Collector contracts | Define `CapturedChunk`, `SourceObservation`, `FeedFailure`, and runtime-owned side-effect boundaries | `backend/pipeline/ingestion/models.py` |
| Echo ingestion function | Receive external echo notifications and publish segmented audio for the `echo` source type | `backend/pipeline/ingestion/collectors/echo/main.py` |
| GCP helpers | Upload audio objects and publish typed `ContinuousAudio` or `SegmentedAudio` protobuf messages | `backend/pipeline/common/gcp_helper.py` |
| Segmentation pipeline | Consume continuous audio, stitch ordered chunks, run VAD, upload raw segments, publish `SegmentedAudio` | `backend/pipeline/segmentation/orchestration.py` |
| Normalization stage | Transcode raw audio, write canonical/playback/transcription objects, persist audio segment metadata, publish `NormalizedAudio` | `backend/pipeline/normalization/processor.py` |
| Transcription stage | Select a transcriber, write transcript annotations, publish `TranscribedAudio` | `backend/pipeline/transcription/processor.py` |
| Evaluation stage | Evaluate transcripts against rules, write transcript/evaluation records, publish notification candidates | `backend/pipeline/evaluation/processor.py` |
| Notification stage | Deduplicate notifications, enrich feed metadata, call the outbound notification endpoint | `backend/pipeline/notification/send_notification.py` |
| FastAPI services | Expose feed, audio segment, transcript, and rule CRUD APIs with OIDC auth | `backend/services/feeds/main.py`, `backend/services/audio_segments/main.py`, `backend/services/transcripts/main.py`, `backend/services/rules/main.py` |
| Storage layer | Encapsulate AlloyDB access and operational SQL for feeds, transcripts, rules, and audio segments | `backend/pipeline/storage/feed_store.py`, `backend/pipeline/storage/transcript_store.py`, `backend/pipeline/storage/rules_store.py`, `backend/pipeline/storage/audio_segment_store.py` |
| BFF API | Expose TSOA/Express endpoints, handle user auth, proxy service calls with ID tokens | `frontend/api/src/index.ts`, `frontend/api/src/utils.ts`, `frontend/api/src/authentication.ts` |
| Shared frontend types | Share API/domain contracts between the BFF and React UI | `frontend/common/src/index.ts`, `frontend/common/src/types/` |
| React UI | Render feed search, transcripts, rules, feed configuration, docs, auth, and audio playback workflows | `frontend/transcription-ui/src/App.tsx`, `frontend/transcription-ui/src/main.tsx` |
| Gemini SFT CLI | Prepare data, launch Vertex tuning, and evaluate ASR fine-tuning runs | `model/src/gemini_sft/cli.py` |
| Manifest helpers | Validate and merge canonical ASR JSONL manifests | `model/src/common/manifest.py`, `model/data/manifests/README.md` |
| Infrastructure | Define AlloyDB, Cloud Functions, buckets, Redis, MIG collectors, and supporting cloud resources | `terraform/modules/` |
## Pattern Overview
- Audio payloads move by object reference. Pipeline messages carry GCS URIs and metadata defined in `protos/*.proto`; large audio bytes are stored through helpers in `backend/pipeline/common/gcp_helper.py`.
- Source ingestion is split between capture logic and runtime side effects. Collectors return typed events from `backend/pipeline/ingestion/models.py`; `backend/pipeline/ingestion/collector_runtime.py` owns leases, uploads, publishing, bookmarks, and failure policy.
- Continuous and pre-segmented sources converge on the `SegmentedAudio` contract. `bcfy_feeds` emits `ContinuousAudio` through `backend/pipeline/common/gcp_helper.py`; `backend/pipeline/segmentation/orchestration.py` converts it to `SegmentedAudio`. Other sources publish `SegmentedAudio` directly.
- Backend services use a thin HTTP layer, domain service classes, and store classes. `backend/services/feeds/main.py` delegates to `backend/services/feeds/service.py`, which delegates to `backend/pipeline/storage/feed_store.py`.
- The frontend uses a BFF boundary. React services in `frontend/transcription-ui/src/service/` call TSOA controllers in `frontend/api/src/`, and the BFF calls backend services through `frontend/api/src/utils.ts`.
- Model and research workflows share manifest contracts with the product pipeline through `model/src/common/manifest.py` and `model/data/manifests/README.md`.
## Layers
- Purpose: Convert external audio source events into `CapturedChunk`, `SourceObservation`, or `FeedFailure` values.
- Location: `backend/pipeline/ingestion/collectors/`
- Contains: Source-specific clients, parsers, polling loops, event handlers, and collector tests.
- Depends on: Contracts in `backend/pipeline/ingestion/models.py` and runtime specs in `backend/pipeline/ingestion/source_runtime_specs.py`.
- Used by: `backend/pipeline/ingestion/collector_runtime.py` and direct Cloud Function entrypoints such as `backend/pipeline/ingestion/collectors/echo/main.py`.
- Purpose: Lease feeds, run collectors, upload audio, publish the first pipeline message, heartbeat active work, and apply failure/quarantine policy.
- Location: `backend/pipeline/ingestion/`
- Contains: Runtime orchestration, source routing, feed scheduling, source caps, failure classification, and ingestion settings.
- Depends on: `backend/pipeline/storage/feed_store.py`, `backend/pipeline/common/gcp_helper.py`, `backend/pipeline/common/settings.py`, and collector contracts in `backend/pipeline/ingestion/models.py`.
- Used by: VM collector entrypoint `backend/pipeline/ingestion/main.py`.
- Purpose: Define the typed data exchanged between pipeline stages.
- Location: `protos/`, generated Python package `backend/pipeline/schema_types/`
- Contains: `ContinuousAudio`, `SegmentedAudio`, `NormalizedAudio`, `TranscribedAudio`, `EvaluatedTranscribedAudio`, `AlertNotification`, and Beam streaming state messages.
- Depends on: Protobuf generation configured by `.mise.toml` and `backend/pipeline/README.md`.
- Used by: All pipeline stages under `backend/pipeline/`.
- Purpose: Convert ordered continuous chunks into speech segments with stateful stitching and VAD.
- Location: `backend/pipeline/segmentation/`
- Contains: Apache Beam orchestration in `backend/pipeline/segmentation/orchestration.py`, transforms in `backend/pipeline/segmentation/transforms/`, audio/VAD helpers in `backend/pipeline/segmentation/audio/`, and state models in `backend/pipeline/segmentation/state/`.
- Depends on: Pub/Sub `ContinuousAudio`, generated `backend/pipeline/schema_types/streaming_state.py`, and GCS helpers in `backend/pipeline/common/gcp_helper.py`.
- Used by: Dataflow entrypoint `backend/pipeline/segmentation/main.py`.
- Purpose: Process claim-check CloudEvents for normalization, transcription, evaluation, and notification.
- Location: `backend/pipeline/{normalization,transcription,evaluation,notification}/`
- Contains: `main.py` entrypoints, `processor.py` orchestration, settings, helper clients, and stage tests.
- Depends on: Generated protobufs in `backend/pipeline/schema_types/`, backend service APIs, GCS, Pub/Sub, and storage helpers.
- Used by: Cloud Functions defined by Terraform modules in `terraform/modules/`.
- Purpose: Expose authenticated CRUD/query endpoints over operational data.
- Location: `backend/services/`
- Contains: FastAPI `main.py`, pydantic request/response `models.py`, domain `service.py`, and unit tests for each service.
- Depends on: OIDC auth in `backend/pipeline/common/auth.py`, tracing in `backend/pipeline/common/tracing.py`, AlloyDB stores in `backend/pipeline/storage/`, and settings in `backend/pipeline/common/settings.py`.
- Used by: Pipeline stages and the BFF in `frontend/api/src/`.
- Purpose: Centralize SQL, asyncpg pools, retries, and state transitions against AlloyDB.
- Location: `backend/pipeline/storage/`
- Contains: Store classes, SQL query modules, connection helpers, and storage tests.
- Depends on: AlloyDB schema migrations in `terraform/modules/alloydb/sql/ingestion/`.
- Used by: `backend/services/*`, ingestion runtime, and pipeline processors.
- Purpose: Provide a browser-facing API, validate user identity/admin access, generate OpenAPI, and proxy backend services with Google ID tokens.
- Location: `frontend/api/src/`
- Contains: TSOA controllers, authentication middleware, service client utilities, config validation, and generated route/OpenAPI configuration.
- Depends on: Shared types in `frontend/common/src/`, `tsoa` config in `frontend/api/tsoa.json`, and backend service URLs from `frontend/api/src/config.ts`.
- Used by: React services in `frontend/transcription-ui/src/service/`.
- Purpose: Render operator workflows for feeds, transcripts, rules, docs, auth, and audio playback.
- Location: `frontend/transcription-ui/src/`
- Contains: `App.tsx` routes, feature components under `src/components/`, service clients under `src/service/`, auth context under `src/context/`, and hooks under `src/hooks/`.
- Depends on: Shared types in `frontend/common/src/`, BFF endpoints, Google OAuth, TanStack Query, and MUI.
- Used by: Browser users and local Vite development.
- Purpose: Provide ASR dataset preparation, Gemini supervised fine-tuning, evaluation, and shared manifest utilities.
- Location: `model/src/`
- Contains: Common manifest and scoring helpers in `model/src/common/`, Gemini helpers in `model/src/common/gemini/`, and the `gemini-sft` CLI package in `model/src/gemini_sft/`.
- Depends on: Manifest rules in `model/data/manifests/README.md` and package metadata in `model/pyproject.toml`.
- Used by: Researchers, notebooks in `model/colabs/`, and model scripts in `model/scripts/`.
## Data Flow
### Primary Audio Processing Path
### Direct Segmented Source Path
### Operator UI and API Path
### Gemini SFT Workflow
- Feed lifecycle, leases, failure episodes, source observations, rules, transcripts, audio segments, and annotations persist in AlloyDB through `backend/pipeline/storage/` and migrations in `terraform/modules/alloydb/sql/ingestion/`.
- In-flight audio bytes and normalized artifacts persist in GCS through URIs carried in protobuf messages from `protos/`.
- Pipeline ordering and retries rely on Pub/Sub, CloudEvent retry semantics, and Beam state in `backend/pipeline/segmentation/state/`.
- Notification idempotency uses Redis through `backend/pipeline/notification/notification_deduplication.py`.
- UI state is request/cache oriented through TanStack Query providers in `frontend/transcription-ui/src/main.tsx`; durable UI data lives behind backend service APIs.
## Key Abstractions
- Purpose: Represent an audio source plus operational state, routing metadata, and lease ownership.
- Examples: `backend/pipeline/storage/feed_store.py`, `backend/services/feeds/models.py`, `frontend/common/src/types/feeds.ts`.
- Pattern: Enum-backed source/status models with store-owned state transitions.
- Purpose: Keep source collectors pure from infrastructure side effects.
- Examples: `backend/pipeline/ingestion/models.py`, `backend/pipeline/ingestion/collector_runtime.py`.
- Pattern: Collectors yield `CapturedChunk`, `SourceObservation`, or `FeedFailure`; the runtime performs persistence and publishing.
- Purpose: Centralize source capture mode, lease eligibility, concurrency caps, and lease duration.
- Examples: `backend/pipeline/ingestion/source_runtime_specs.py`, `backend/pipeline/ingestion/router.py`.
- Pattern: One registry entry per source type; runtime validates registry consistency at startup.
- Purpose: Provide explicit stage contracts for Pub/Sub and generated Python types.
- Examples: `protos/continuous_audio.proto`, `protos/segmented_audio.proto`, `protos/normalized_audio.proto`, `protos/transcribed_audio.proto`, `protos/evaluated_transcribed_audio.proto`, `protos/alert_notification.proto`.
- Pattern: Edit proto definitions, generate code through `mise run generate:protos`, and consume generated classes from `backend/pipeline/schema_types/`.
- Purpose: Encapsulate SQL and transactional behavior for a domain aggregate.
- Examples: `backend/pipeline/storage/feed_store.py`, `backend/pipeline/storage/audio_segment_store.py`, `backend/pipeline/storage/transcript_store.py`, `backend/pipeline/storage/rules_store.py`.
- Pattern: FastAPI services and processors call typed store methods instead of embedding SQL in request handlers.
- Purpose: Lazily construct reusable clients/settings for Cloud Function invocations.
- Examples: `backend/pipeline/normalization/main.py`, `backend/pipeline/transcription/main.py`, `backend/pipeline/evaluation/main.py`, `backend/pipeline/notification/send_notification.py`.
- Pattern: `main.py` handles CloudEvent parsing and dependency container setup; `processor.py` owns stage behavior.
- Purpose: Hide speech-to-text provider differences from the transcription processor.
- Examples: `backend/pipeline/transcription/transcribers/factory.py`, `backend/pipeline/transcription/transcribers/chirp.py`, `backend/pipeline/transcription/transcribers/local_api.py`, `backend/pipeline/transcription/transcribers/mock.py`.
- Pattern: Select implementation via transcription settings and call a provider-neutral interface.
- Purpose: Evaluate transcript text against static or remote rules.
- Examples: `backend/pipeline/evaluation/rules_evaluation/evaluator.py`, `backend/pipeline/evaluation/service.py`.
- Pattern: `EvaluationService` builds `EvaluatedTranscribedAudio`; evaluator implementations provide rules.
- Purpose: Define browser-facing routes, OpenAPI metadata, auth requirements, and request/response types.
- Examples: `frontend/api/src/feeds/feedsController.ts`, `frontend/api/src/rules/rulesController.ts`, `frontend/api/tsoa.json`.
- Pattern: Add controller methods and regenerate TSOA routes instead of editing generated output.
- Purpose: Group UI components, service calls, hooks, and shared types around operator workflows.
- Examples: `frontend/transcription-ui/src/components/feeds/`, `frontend/transcription-ui/src/components/transcripts/`, `frontend/transcription-ui/src/service/feeds.ts`, `frontend/common/src/types/feeds.ts`.
- Pattern: Shared types live in `frontend/common/src/types/`; UI-specific behavior lives under `frontend/transcription-ui/src/`.
## Entry Points
- Location: `backend/pipeline/ingestion/main.py`
- Triggers: Collector VM or local process startup.
- Responsibilities: Load ingestion settings, validate source registry/topic consistency, run `CollectorRuntime`.
- Location: `backend/pipeline/ingestion/collectors/echo/main.py`
- Triggers: External echo audio notification event.
- Responsibilities: Validate notification payload, fetch/upload audio, publish `SegmentedAudio`.
- Location: `backend/pipeline/segmentation/main.py`
- Triggers: Dataflow/Beam job invocation.
- Responsibilities: Build and run the streaming segmentation graph from `backend/pipeline/segmentation/orchestration.py`.
- Location: `backend/pipeline/normalization/main.py`
- Triggers: Pub/Sub CloudEvent containing `SegmentedAudio`.
- Responsibilities: Normalize audio objects, persist segment metadata, publish `NormalizedAudio`.
- Location: `backend/pipeline/transcription/main.py`
- Triggers: Pub/Sub CloudEvent containing `NormalizedAudio`.
- Responsibilities: Transcribe audio and publish `TranscribedAudio`.
- Location: `backend/pipeline/evaluation/main.py`
- Triggers: Pub/Sub CloudEvent containing `TranscribedAudio`.
- Responsibilities: Evaluate transcript text, persist transcript/evaluation data, publish alert candidates.
- Location: `backend/pipeline/notification/send_notification.py`
- Triggers: Pub/Sub CloudEvent containing `AlertNotification`.
- Responsibilities: Deduplicate and deliver outbound notifications.
- Location: `backend/services/feeds/main.py`, `backend/services/audio_segments/main.py`, `backend/services/transcripts/main.py`, `backend/services/rules/main.py`
- Triggers: HTTP requests from BFF or pipeline stages.
- Responsibilities: Authenticated CRUD/query operations over AlloyDB-backed domain data.
- Location: `frontend/api/src/index.ts`
- Triggers: Node process startup.
- Responsibilities: Register TSOA routes, auth middleware, CORS, error handling, and docs endpoints.
- Location: `frontend/transcription-ui/src/main.tsx`
- Triggers: Browser page load through Vite/build assets.
- Responsibilities: Mount providers, configure routing, and render operator workflows.
- Location: `model/src/gemini_sft/cli.py`
- Triggers: `gemini-sft` console command from `model/pyproject.toml`.
- Responsibilities: Dispatch `prepare`, `tune`, and `eval` ASR workflow commands.
## Architectural Constraints
- **Threading:** Ingestion uses a long-running runtime plus heartbeat behavior in `backend/pipeline/ingestion/collector_runtime.py`; Beam segmentation uses stateful streaming transforms in `backend/pipeline/segmentation/orchestration.py`; FastAPI services and stores use async IO through `backend/pipeline/storage/connection.py`.
- **Global state:** Cloud Function modules cache dependency containers or clients in stage entrypoints such as `backend/pipeline/normalization/main.py`, `backend/pipeline/transcription/main.py`, `backend/pipeline/evaluation/main.py`, and `backend/pipeline/notification/send_notification.py`.
- **Circular imports:** No intentional circular dependency chain is represented in the inspected layers. Preserve one-way dependencies from entrypoints to processors/services to stores, and from UI to BFF to backend services.
- **Generated code:** Do not edit generated protobuf files under `backend/pipeline/schema_types/` or TSOA route output configured by `frontend/api/tsoa.json`. Edit `protos/*.proto` or `frontend/api/src/*Controller.ts` and regenerate through the configured tasks.
- **Source registry consistency:** A source type is represented across storage enum/seed SQL, runtime specs, routing, collector code, and shared frontend feed types. Keep `backend/pipeline/storage/feed_store.py`, `terraform/modules/alloydb/sql/ingestion/`, `backend/pipeline/ingestion/source_runtime_specs.py`, `backend/pipeline/ingestion/router.py`, and `frontend/common/src/types/feeds.ts` aligned.
- **Auth boundary:** Browser requests terminate at `frontend/api/src/`; backend service auth uses OIDC verification in `backend/pipeline/common/auth.py`. React components do not call `backend/services/*` directly.
- **Local development secrets:** Environment files such as `local_dev/LOCAL.env` are configuration inputs only and are not source of architectural truth.
## Anti-Patterns
### Collector-Owned Infrastructure Side Effects
### Generated Contract Edits
### Partial Source-Type Registration
### Diagnostic Text as Control Flow
### UI Direct-to-Service Calls
## Error Handling
- Ingestion classifies source failures through `backend/pipeline/ingestion/failure_policy.py` and records failure episodes through `backend/pipeline/storage/feed_store.py`.
- Beam segmentation publishes malformed or failed records to DLQ output in `backend/pipeline/segmentation/orchestration.py:157`.
- Function processors publish DLQ messages or raise retryable errors from stage code such as `backend/pipeline/normalization/processor.py`, `backend/pipeline/transcription/processor.py`, and `backend/pipeline/evaluation/processor.py`.
- FastAPI services raise HTTP exceptions from `backend/services/*/main.py` and keep persistence errors inside service/store boundaries.
- BFF error handling is centralized in `frontend/api/src/index.ts:30`, converting thrown errors into API responses.
- Notification retryability is explicit in `backend/pipeline/notification/send_notification.py`; dedupe keys are cleared for retryable outbound failures.
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

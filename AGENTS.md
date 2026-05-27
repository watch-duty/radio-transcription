<!-- GSD:project-start source:PROJECT.md -->
## Project

**SFT Dataset Versioning**

This project adds a deterministic, leak-safe dataset split and artifact generator for supervised fine tuning of emergency radio ASR models. It turns existing labeled audio manifests into versioned GCS artifacts that can be consumed by NeMo, Whisper, and Gemini fine-tuning workflows on Vertex AI or adjacent training runners.

The project is brownfield: the repository already has ingestion, transcription, evaluation, and early SFT manifest-building code. This work scopes the missing dataset-versioning layer: source-group-aware train/SFT Eval Split creation, model-specific input manifests, provenance, validation reports, and GCS organization.

**Core Value:** Every SFT run must train and compare models on the same auditable dataset version without source leakage between train and SFT Eval Split.

### Constraints

- **Leakage**: No Source Group may appear in both train and SFT Eval Split - same radio feed/device/location can leak speaker, scanner, agency, channel, acoustics, and phrase distribution.
- **Ambiguity**: Ambiguous source identity must fail rather than guess - especially Echo rows where `echo_name` is duplicated across area codes.
- **Compatibility**: Generated model inputs must match current NeMo, Whisper, and Gemini/Vertex AI requirements as verified from current docs during implementation.
- **Storage**: Generated dataset artifacts and derived clips live in GCS under `gs://wd-transcription-data/sft/{dataset_version_id}/`.
- **Reproducibility**: Splits must be deterministic by seed, input manifest set, and split configuration.
- **Minimal transformation**: Reuse existing clips when valid; derive audio only when needed; avoid padding and avoid resampling unless a target model/input format requires it.
- **Git hygiene**: Git stores code, tests, templates, and planning docs; not generated manifests, credentials, or audio payloads.
<!-- GSD:project-end -->

<!-- GSD:stack-start source:codebase/STACK.md -->
## Technology Stack

## Languages
- Python 3.13.2 - backend pipeline workers, CloudEvent functions, FastAPI services, integration tests, and model tooling under `backend/`, `integration_tests/`, `local_dev/`, and `model/`; runtime is declared in `.tool-versions`, `.mise.toml`, `pyproject.toml`, and service Dockerfiles such as `backend/pipeline/ingestion/Dockerfile`.
- TypeScript 6.0.2 - frontend proxy API, React UI, and shared browser/API types under `frontend/api`, `frontend/transcription-ui`, and `frontend/common`; package versions are declared in `frontend/api/package.json`, `frontend/transcription-ui/package.json`, and `frontend/common/package.json`.
- Terraform 1.14.5 - reusable GCP infrastructure modules under `terraform/modules/*`, with provider constraints in `terraform/modules/alloydb/versions.tf`, `terraform/modules/gcs_bucket/versions.tf`, and `terraform/modules/container_mig/versions.tf`.
- Protocol Buffers - pipeline message schemas in `protos/*.proto`, generated into `backend/pipeline/schema_types` by the `generate:protos` task in `.mise.toml` and the protobuf generation notes in `backend/pipeline/README.md`.
- SQL - AlloyDB schema migrations in `terraform/modules/alloydb/sql/ingestion/*.sql`, including tables for `feeds`, `rules`, `transcripts`, `audio_segments`, and `annotations`.
- Shell/YAML/JSON - Docker, Compose, GitHub Actions, pre-commit, Firebase Hosting, OpenAPI, and tool configuration in `docker-compose.yml`, `asr-eval-docker-compose.yml`, `.github/workflows/*.yml`, `.pre-commit-config.yaml`, `frontend/transcription-ui/firebase.json`, and `frontend/api/openapi.yaml`.
- Jupyter notebooks - ASR evaluation notebooks in `model/colabs/*.ipynb`, supported by Docker images in `model/notebook_docker/Dockerfile` and `model/nemo_docker/Dockerfile`.
## Runtime
- CPython 3.13.2 is the main backend runtime, pinned by `.tool-versions`; root project constraints require `>=3.13, <3.14` in `pyproject.toml`.
- `python:3.13-slim` is used for most service/function images, including `backend/pipeline/ingestion/Dockerfile`, `backend/pipeline/transcription/Dockerfile`, `backend/pipeline/evaluation/Dockerfile`, `backend/pipeline/notification/Dockerfile`, `backend/services/transcripts/Dockerfile`, `backend/services/feeds/Dockerfile`, and `backend/services/rules/Dockerfile`.
- Apache Beam Python SDK 2.73.0 runs the normalization/Dataflow Flex Template image in `backend/pipeline/normalization/Dockerfile`, with the matching Python dependency declared in `backend/pipeline/normalization/pyproject.toml`.
- Node.js 22.14.0 is pinned by `.tool-versions`; Node 22 slim images build and run the frontend proxy API in `frontend/api/Dockerfile`.
- ASR experimentation uses GPU-capable containers: `pytorch/pytorch:2.5.1-cuda12.4-cudnn9-runtime` in `model/notebook_docker/Dockerfile` and `nvcr.io/nvidia/nemo:26.02.00` in `model/nemo_docker/Dockerfile`.
- Local end-to-end development uses Docker Compose services and emulators in `docker-compose.yml`, plus ASR evaluation containers in `asr-eval-docker-compose.yml`.
- `uv` 0.9.28 manages Python dependencies from `pyproject.toml` with lockfile `uv.lock` present; workspace members are `backend/pipeline/normalization` and `backend/pipeline/transcription` in `pyproject.toml`.
- `yarn` manages TypeScript packages with lockfiles at `frontend/api/yarn.lock`, `frontend/transcription-ui/yarn.lock`, and `frontend/common/yarn.lock`.
- `mise` orchestrates tool installation and repo tasks from `.mise.toml`; pinned tool versions live in `.tool-versions`.
- Lockfile status: `uv.lock` present, `frontend/api/yarn.lock` present, `frontend/transcription-ui/yarn.lock` present, `frontend/common/yarn.lock` present.
## Frameworks
- Google Functions Framework 3.10.1 - Python CloudEvent/HTTP entrypoints in `backend/pipeline/transcription/main.py`, `backend/pipeline/evaluation/main.py`, `backend/pipeline/notification/send_notification.py`, `backend/pipeline/ingestion/collectors/echo/main.py`, `backend/pipeline/ingestion/oldest_feed_publisher/main.py`, and `backend/pipeline/ingestion/broadcastify_credential_rotation/main.py`.
- FastAPI 0.136.1 with Uvicorn 0.46.0 - management APIs in `backend/services/transcripts/main.py`, `backend/services/feeds/main.py`, and `backend/services/rules/main.py`.
- Apache Beam 2.73.0 - streaming normalization pipeline in `backend/pipeline/normalization/main.py`, `backend/pipeline/normalization/options.py`, and `backend/pipeline/normalization/orchestration.py`.
- Express 5.2.1 with TSOA 7.0.0-alpha.0 - frontend proxy/API Gateway backing service in `frontend/api/src/index.ts`, `frontend/api/tsoa.json`, and controllers under `frontend/api/src/**/*Controller.ts`.
- React 19.2.0 with Vite 8.0.8 - transcription UI in `frontend/transcription-ui/src/main.tsx`, `frontend/transcription-ui/src/App.tsx`, and `frontend/transcription-ui/vite.config.ts`.
- Material UI 9.0.0, Toolpad Core 0.16.0, TanStack React Query 5.99.0, React Router 7.14.1, Wavesurfer 7.12.6 - UI framework, state/query, routing, and audio visualization in `frontend/transcription-ui/package.json`.
- pytest 9.0.3, pytest-asyncio 1.3.0, pytest-xdist, pytest-cov, and testcontainers 4.14.2 - backend and integration test stack declared in `pyproject.toml` and used under `backend/**/tests` and `integration_tests`.
- Python `unittest` is still used by the `test:unit` task in `.mise.toml` for `backend/pipeline`.
- Vitest 3.x/4.x with React Testing Library and jsdom - frontend/API tests declared in `frontend/api/package.json`, `frontend/transcription-ui/package.json`, and `frontend/transcription-ui/vitest.config.ts`.
- Docker Compose E2E tests run the full pipeline via `mise run test:e2e` in `.mise.toml` and GitHub Actions workflow `.github/workflows/integration-tests.yml`.
- Docker and Docker Compose - local pipeline, service images, ASR notebooks, and CI smoke builds in `docker-compose.yml`, `asr-eval-docker-compose.yml`, and `backend/**/Dockerfile`.
- Terraform with Google provider `>= 6.0` - GCP modules in `terraform/modules/alloydb`, `terraform/modules/gcs_bucket`, `terraform/modules/cloud_function`, `terraform/modules/container_mig`, `terraform/modules/memorystore_for_redis`, and `terraform/modules/asr_evaluation`.
- Ruff 0.15.12 and ty 0.0.33 - Python formatting, linting, and type checking configured in `pyproject.toml`, `.mise.toml`, and `.pre-commit-config.yaml`.
- ESLint 10.x and Prettier 3.8.1 - TypeScript/CSS lint and formatting configured in `frontend/api/eslint.config.js`, `frontend/transcription-ui/eslint.config.js`, and `.prettierrc`.
- grpcio-tools/betterproto - protobuf generation configured in `.mise.toml`, `backend/pipeline/transcription/Dockerfile`, `backend/pipeline/normalization/Dockerfile`, and `backend/services/transcripts/Dockerfile`.
## Key Dependencies
- `google-cloud-pubsub` 2.37.0 - ordered Pub/Sub publishing in `backend/pipeline/common/clients/pubsub_client.py`, `backend/pipeline/common/gcp_helper.py`, and `backend/pipeline/transcription/main.py`.
- `google-cloud-storage` 2.19.0 and `gcloud-aio-storage` 9.6.4 - synchronous/asynchronous GCS reads and writes in `backend/pipeline/common/storage/gcs_uploader.py`, `backend/pipeline/common/clients/gcs_client.py`, `backend/pipeline/common/gcp_helper.py`, and `model/colabs/common/gcs_utils.py`.
- `google-cloud-speech` 2.38.0 - Google Chirp V3 transcription in `backend/pipeline/transcription/transcribers/chirp.py`.
- `google-cloud-secret-manager` 2.27.0 - Broadcastify JWT retrieval and rotation in `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py` and `backend/pipeline/ingestion/broadcastify_credential_rotation/main.py`.
- `asyncpg` 0.31.0 and `psycopg` 3.3.3 - async and sync AlloyDB access in `backend/pipeline/storage/connection.py`, `backend/pipeline/storage/sync_connection.py`, and store modules under `backend/pipeline/storage`.
- `redis` 7.4.0 - notification deduplication backed by Memorystore/Redis in `backend/pipeline/common/storage/redis_service.py` and `backend/pipeline/notification/notification_deduplication.py`.
- `pydantic` 2.13.3 and `pydantic-settings` - API models, typed configs, and shared rule/transcript/feed schemas in `backend/services/*/models.py`, `backend/pipeline/common/rules/models.py`, and `backend/pipeline/transcription/transcribers/chirp.py`.
- `opentelemetry-api` 1.41.1, `opentelemetry-sdk` 1.41.1, and `opentelemetry-exporter-gcp-trace` 1.12.0 - Cloud Trace propagation/export in `backend/pipeline/common/tracing_utils.py`.
- `functions-framework` 3.10.1 and `cloudevents` 1.12.0 - CloudEvents entrypoints in `backend/pipeline/transcription/main.py`, `backend/pipeline/evaluation/main.py`, and `backend/pipeline/notification/send_notification.py`.
- `apache-beam[gcp]` 2.73.0, `onnxruntime`, `pedalboard`, `numba`, `numpy`, and `soundfile` - streaming normalization, VAD, DSP, and audio export in `backend/pipeline/normalization/pyproject.toml`.
- `curl-cffi`, `aiohttp`, `requests`, and `urllib3` - external feed polling, authenticated stream capture, and notification delivery in `backend/pipeline/ingestion/collectors/*`, `backend/pipeline/evaluation/rules_evaluation/evaluator.py`, and `backend/pipeline/notification/request_handler.py`.
- `google-genai`, `huggingface_hub`, `datasets`, `evaluate`, `jiwer`, `nemo_text_processing`, `torchaudio`, and `soundfile` - ASR model evaluation and Gemini SFT tooling in `model/pyproject.toml`, `model/notebook_docker/requirements.txt`, `model/colabs/common/vertex.py`, and `model/colabs/common/inference_hf.py`.
- `@google-cloud/functions-framework`, `google-auth-library`, `express`, `tsoa`, `jsonwebtoken`, `axios`, and `cookie-parser` - frontend proxy API runtime and auth in `frontend/api/package.json`, `frontend/api/src/index.ts`, `frontend/api/src/auth/authController.ts`, and `frontend/api/src/authentication.ts`.
- `@react-oauth/google`, `@tanstack/react-query`, `@mui/material`, `@toolpad/core`, `wavesurfer.js`, `howler`, and `react-virtuoso` - UI auth, data fetching, layout, and audio playback in `frontend/transcription-ui/package.json`.
- Google Cloud AlloyDB - PostgreSQL-compatible database provisioned by `terraform/modules/alloydb/main.tf`; schema lives in `terraform/modules/alloydb/sql/ingestion/*.sql`; clients live in `backend/pipeline/storage/connection.py` and `backend/pipeline/storage/settings.py`.
- Google Cloud Storage - bucket module in `terraform/modules/gcs_bucket/main.tf`, schema staging bucket in `terraform/modules/alloydb/main.tf`, local emulator service in `docker-compose.yml`, and GCS clients in `backend/pipeline/common/clients/gcs_client.py`.
- Google Cloud Pub/Sub - local emulator initialized by `local_dev/pubsub_init.py` and `docker-compose.yml`; production publishing clients in `backend/pipeline/common/clients/pubsub_client.py`.
- Google Cloud Dataflow - Flex Template runtime based on `backend/pipeline/normalization/Dockerfile` and Beam options in `backend/pipeline/normalization/options.py`.
- Google Cloud Functions Gen 2 / Cloud Run - reusable function module in `terraform/modules/cloud_function/main.tf` and service images under `backend/**/Dockerfile`.
- Google Compute Engine Managed Instance Groups - ingestion worker deployment module in `terraform/modules/container_mig/main.tf` using Container-Optimized OS and Artifact Registry images.
- Google Memorystore for Redis - Terraform module in `terraform/modules/memorystore_for_redis/main.tf` and Redis client in `backend/pipeline/common/storage/redis_service.py`.
- Firebase Hosting - static UI hosting configuration in `frontend/transcription-ui/firebase.json`.
- API Gateway - OpenAPI/TSOA configuration in `frontend/api/tsoa.json` and API Gateway Admin API reads in `frontend/api/src/docs/docsController.ts`.
- GitHub Actions - CI, integration tests, deployment trigger, and Linear title automation in `.github/workflows/ci.yml`, `.github/workflows/integration-tests.yml`, `.github/workflows/trigger-deploy.yml`, and `.github/workflows/prepend-linear-issue-to-pr-title.yml`.
## Configuration
- `.mise.toml` loads `.env` via `[env] _.file = ".env"` and sets `PYTHONPATH = "."`; a root `.env` file is not present in the scanned tree.
- `local_dev/LOCAL.env` is present and used by Docker Compose E2E tasks in `.mise.toml`; contents were not read.
- `frontend/api/.env.example` and `frontend/transcription-ui/.env.example` are present; contents were not read.
- Python backend env vars are loaded directly from `os.environ` in `backend/pipeline/storage/settings.py`, `backend/pipeline/ingestion/settings.py`, `backend/pipeline/transcription/main.py`, `backend/pipeline/evaluation/main.py`, `backend/pipeline/notification/send_notification.py`, `backend/pipeline/notification/request_handler.py`, and collector modules under `backend/pipeline/ingestion/collectors`.
- Frontend API env vars are centralized in `frontend/api/src/config.ts`; UI build-time env vars are accessed in `frontend/transcription-ui/src/main.tsx`, `frontend/transcription-ui/src/service/*.ts`, and `frontend/transcription-ui/src/components/common/AlertIcon.tsx`.
- Python build and dependency config: `pyproject.toml`, `uv.lock`, `backend/pipeline/normalization/pyproject.toml`, `backend/pipeline/transcription/pyproject.toml`, `model/pyproject.toml`, and `model/scripts/sft/requirements.txt`.
- TypeScript build and dependency config: `frontend/api/package.json`, `frontend/api/tsconfig.json`, `frontend/api/tsoa.json`, `frontend/transcription-ui/package.json`, `frontend/transcription-ui/tsconfig*.json`, `frontend/transcription-ui/vite.config.ts`, and `frontend/common/package.json`.
- Container build config: `backend/pipeline/*/Dockerfile`, `backend/services/*/Dockerfile`, `backend/pipeline/ingestion/collectors/echo/Dockerfile`, `backend/pipeline/ingestion/oldest_feed_publisher/Dockerfile`, `backend/pipeline/ingestion/broadcastify_credential_rotation/Dockerfile`, `frontend/api/Dockerfile`, `model/notebook_docker/Dockerfile`, and `model/nemo_docker/Dockerfile`.
- Infrastructure build config: Terraform modules in `terraform/modules/*`, GitHub workflows in `.github/workflows/*.yml`, and pre-commit hooks in `.pre-commit-config.yaml`.
## Platform Requirements
- Install `mise`, then run `mise install` using `.tool-versions` and `.mise.toml`; setup instructions live in `CONTRIBUTING.md`.
- Docker is required for local end-to-end pipeline development in `docker-compose.yml`, model containers in `asr-eval-docker-compose.yml`, and CI image smoke tests in `.github/workflows/ci.yml`.
- Google Cloud CLI and ADC are required for frontend proxy development against GCP services, as described in `CONTRIBUTING.md`; proxy API auth uses `google-auth-library` in `frontend/api/src/transcripts/transcriptsController.ts`, `frontend/api/src/feeds/feedsController.ts`, and `frontend/api/src/rules/rulesController.ts`.
- ASR GPU workflows require GCE GPU instances and Docker/NVIDIA runtime; guidance and Terraform entrypoint are in `ASR_CONTRIBUTING.md` and `terraform/modules/asr_evaluation/main.tf`.
- Event pipeline runs on GCP using Pub/Sub, GCS, Cloud Functions/Cloud Run, Dataflow, AlloyDB, Memorystore for Redis, Cloud Logging, Cloud Trace, and Cloud Monitoring, as evidenced by `backend/pipeline/*`, `backend/services/*`, and `terraform/modules/*`.
- Continuous ingestion worker fleet runs as a GCE regional Managed Instance Group using Container-Optimized OS and container images from Artifact Registry, configured by `terraform/modules/container_mig/main.tf`.
- UI static assets are built by Vite and hosted via Firebase Hosting configuration in `frontend/transcription-ui/firebase.json`; API traffic is mediated by the Express/TSOA proxy in `frontend/api` and API Gateway configuration in `frontend/api/tsoa.json`.
- Deployment orchestration is split: this repo contains reusable modules and CI in `.github/workflows/*.yml`; `.github/workflows/trigger-deploy.yml` dispatches a private deployment workflow when `terraform/modules/` or `protos/` changes.
<!-- GSD:stack-end -->

<!-- GSD:conventions-start source:CONVENTIONS.md -->
## Conventions

## Naming Patterns
- Use `snake_case.py` for Python implementation and test modules: `backend/pipeline/storage/feed_store.py`, `backend/pipeline/ingestion/normalizer_runtime.py`, `backend/pipeline/storage/tests/test_feed_store.py`.
- Use package-local `tests/` directories for most Python unit tests: `backend/pipeline/ingestion/tests/`, `backend/pipeline/normalization/tests/`, `backend/services/feeds/tests/`.
- Keep integration tests under the top-level `integration_tests/` tree with purpose-specific subdirectories: `integration_tests/storage/test_feed_store_integration.py`, `integration_tests/api/test_transcripts_api.py`, `integration_tests/e2e/test_transcription_pipeline.py`.
- Use `PascalCase.tsx` for React components and matching `.test.tsx` files: `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`, `frontend/transcription-ui/src/components/transcripts/TranscriptView.test.tsx`.
- Use `camelCase.ts` for TypeScript service/util modules and matching `.test.ts`: `frontend/transcription-ui/src/service/listTranscripts.ts`, `frontend/transcription-ui/src/service/listTranscripts.test.ts`.
- Use `*Controller.ts` for TSOA controllers and co-located `*Controller.test.ts` tests: `frontend/api/src/transcripts/transcriptsController.ts`, `frontend/api/src/transcripts/transcriptsController.test.ts`.
- Keep shared TypeScript contracts in plural domain files under `frontend/common/src/types/`: `frontend/common/src/types/transcripts.ts`, `frontend/common/src/types/feeds.ts`.
- Keep model/SFT CLI code under `model/scripts/sft/` with small adapter modules under `model/scripts/sft/adapters/`: `model/scripts/sft/pipeline.py`, `model/scripts/sft/preflight.py`, `model/scripts/sft/adapters/gcs_manifest.py`.
- Use `snake_case` for Python functions and private helpers; prefix module-private helpers with `_`: `backend/pipeline/ingestion/router.py` uses `supported_source_types()`, `resolve_topic_path()`, and `route_capturer()`, while `model/scripts/sft/pipeline.py` uses `_load_registry()`, `_make_adapter()`, and `_build_split_jsonl()`.
- Use `async def` for I/O and pipeline coordination in Python when the called dependencies are async: `backend/pipeline/storage/feed_store.py`, `backend/pipeline/storage/connection.py`, `backend/pipeline/ingestion/retry.py`.
- Use `camelCase` for TypeScript functions and component props: `frontend/api/src/transcripts/transcriptsController.ts` uses `convertTranscriptResponse()`, `frontend/transcription-ui/src/service/listTranscripts.ts` exports `listTranscripts()`.
- Name React components in `PascalCase` and export the component function directly: `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`, `frontend/transcription-ui/src/components/feeds/FeedTable.tsx`.
- For tests, name Python methods/functions by behavior with `test_...`: `backend/pipeline/ingestion/tests/test_retry.py`, `integration_tests/storage/test_feed_store_integration.py`. TypeScript uses `describe()` groups and `it('should ...')` behavior strings in `frontend/api/src/transcripts/transcriptsController.test.ts`.
- Use `snake_case` for Python locals, arguments, and fields: `feed_id`, `worker_id`, `last_bookmark_time` in `backend/pipeline/storage/feed_store.py`.
- Use `_UPPER_CASE` or `UPPER_CASE` module constants for stable configuration and test fixtures: `_FEED_ID` in `backend/pipeline/storage/tests/test_feed_store.py`, `DEFAULT_REFRESH_INTERVAL` in `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`, `PREFLIGHT_TOKEN_CAP` in `model/scripts/sft/preflight.py`.
- Prefix internal Python instance attributes with `_`: `FeedStore._pool` in `backend/pipeline/storage/feed_store.py`, `NormalizerRuntime._shutdown` in `backend/pipeline/ingestion/normalizer_runtime.py`.
- Use `camelCase` for TypeScript locals and serialized API-facing model names in TS: `queryParams`, `isAlert`, `startTimestamp` in `frontend/api/src/transcripts/transcriptsController.ts` and `frontend/common/src/types/transcripts.ts`.
- Preserve backend wire formats at API boundaries, then convert them into frontend `camelCase`: `TranscriptResponse` uses `feed_id`, `transmission_id`, and `start_timestamp`, while `Transcript` uses `feedId`, `transmissionId`, and `startTimestamp` in `frontend/api/src/transcripts/transcriptsController.ts`.
- Use Python `enum.StrEnum` for string-valued domain enums: `SourceType` and `FeedStatus` in `backend/pipeline/storage/feed_store.py`, `AudioMimeType` in `backend/pipeline/ingestion/models.py`.
- Use `TypedDict` for dict-shaped records returned by storage and evaluators: `LeasedFeed`, `HeartbeatResult`, and `Feed` in `backend/pipeline/storage/feed_store.py`; `EvaluationResult` in `backend/pipeline/evaluation/rules_evaluation/evaluator.py`.
- Use frozen dataclasses for immutable domain payloads and configuration: `CapturedChunk` and `CaptureResources` in `backend/pipeline/ingestion/models.py`, `AudioChunkData` and `NormalizeAudioConfig` in `backend/pipeline/normalization/common/datatypes.py`, `CanonicalRow` in `model/colabs/common/manifest.py`.
- Use Pydantic models for FastAPI request/response bodies: `backend/services/feeds/models.py`, `backend/services/transcripts/models.py`, `backend/services/audio_segments/models.py`.
- Use TypeScript `interface` for object contracts and `type` for unions/compositions: `frontend/common/src/types/transcripts.ts`, `frontend/common/src/types/feeds.ts`, `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`.
- Use `TYPE_CHECKING` imports to avoid runtime dependency costs and circular imports in Python: `backend/pipeline/ingestion/router.py`, `backend/services/feeds/service.py`, `integration_tests/conftest.py`.
## Code Style
- Python formatting is Ruff-managed with `line-length = 80` and `target-version = "py313"` in `pyproject.toml`.
- Run Python formatting with `uv run ruff format` or `mise run format:ruff` from `.mise.toml`.
- TypeScript formatting is Prettier-managed by `.prettierrc`: semicolons enabled, single quotes, trailing commas where valid in ES5, `printWidth` 80, and `tabWidth` 2.
- Frontend imports are sorted by `@trivago/prettier-plugin-sort-imports` using per-tree orders in `.prettierrc`.
- Notebook formatting is handled by `mise run format:notebooks` and validated by `mise run lint:notebooks` in `.mise.toml`; notebooks under `model/colabs/**/*.ipynb` are excluded from Ruff source linting in `pyproject.toml`.
- Terraform formatting is part of `mise run format` via `terraform fmt -recursive` in `.mise.toml`.
- Python linting uses Ruff `select = ["ALL"]` with a curated ignore list in `pyproject.toml`; future Python code should satisfy Ruff unless the surrounding subtree has a specific per-file ignore.
- Ruff import sorting treats `backend` and `local_dev` as first-party in `pyproject.toml`.
- `ty check` is the Python type-checker in `.mise.toml` and `.pre-commit-config.yaml`; `tool.ty.src.exclude = ["model/"]` keeps model code outside the root Ty check.
- Model colab Python code is intentionally exempt from root Ruff rules via `pyproject.toml`, while `model/pyproject.toml` defines a separate `common` package and pytest config for `model/colabs/common/tests`.
- `model/scripts/sft/**.py` has a relaxed Ruff profile in `pyproject.toml`; keep CLI scripts readable and tested, but do not assume the stricter backend annotation and branch-count rules apply there.
- `frontend/api/eslint.config.js` uses ESLint flat config with `@eslint/js`, `typescript-eslint`, Node globals, and `eslint-config-prettier`.
- `frontend/transcription-ui/eslint.config.js` adds React hooks, React Refresh, TanStack Query, browser globals, CSS linting, and Prettier compatibility.
- Pre-commit runs proto generation, Ruff check/format, Ty, notebook linting, API/UI ESLint, Prettier, TypeScript checks, TSOA route generation, and OpenAPI spec verification via `.pre-commit-config.yaml`.
## Import Organization
- Python uses repository-root imports such as `backend.pipeline.storage.feed_store` and `integration_tests.feed_utils`; `.mise.toml` sets `PYTHONPATH = "."`.
- Model common code is imported as `common.*` when running under `model/`; `model/pyproject.toml` maps package `common` to `model/colabs/common`.
- Frontend shared package is imported as `@transcription/common` from both `frontend/api/src/` and `frontend/transcription-ui/src/`.
- The frontend UI ESLint/Prettier config reserves the `^@` import group in `.prettierrc`, but source currently uses package imports like `@tanstack/react-query`, `@mui/material`, and `@transcription/common` rather than a local `@/` alias.
## Error Handling
- Raise typed domain exceptions when callers need semantic handling. Use `FeedAlreadyExistsError`, `FeedNameAlreadyExistsError`, and `AlreadyExistsError` from `backend/pipeline/common/exceptions.py`.
- Convert storage/domain exceptions to HTTP errors at FastAPI boundaries. `backend/services/feeds/main.py` maps `ValueError` to `400`, duplicate feed exceptions to `409`, missing resources to `404`, and successful deletes to `204`.
- Convert backend proxy failures to `HttpError` at TypeScript controller boundaries. `frontend/api/src/utils.ts` normalizes `GaxiosError` and unknown errors; controllers like `frontend/api/src/transcripts/transcriptsController.ts` catch `unknown`, call `handleBackendError()`, and throw `HttpError`.
- Validate external or persisted enum strings before constructing domain records. `backend/pipeline/storage/feed_store.py` converts row strings to `SourceType` and `FeedStatus`, then raises `ValueError` with context if an unknown value appears.
- Prefer fail-loud behavior for malformed model/evaluation inputs. `model/colabs/common/manifest.py` raises `ValueError` for missing prediction and ground-truth keys in `merge_predictions_to_manifest()`, while `load_manifest()` soft-fails unreadable or malformed manifest files to `[]` with logs.
- Async runtime retry loops should preserve cancellation and lease-loss semantics. `backend/pipeline/ingestion/retry.py` raises `LeaseExpiredError` when heartbeat state is lost and raises `asyncio.CancelledError` when shutdown is set.
- CLI workflows should return integer status codes and log clean user-facing messages for expected failures. `model/scripts/sft/pipeline.py` returns `1` for missing prompt override files and unknown dataset config rather than printing tracebacks.
- Do not suppress `CancelledError` in collectors; `backend/pipeline/ingestion/models.py` documents that capture functions must use `try/finally` and never suppress cancellation.
## Logging
- Use `logger = logging.getLogger(__name__)` for normal Python modules: `backend/pipeline/storage/feed_store.py`, `backend/pipeline/evaluation/processor.py`, `model/colabs/common/manifest.py`, `model/scripts/sft/preflight.py`.
- Initialize backend logging once with `setup_logging()` from `backend/pipeline/common/logging.py`; it uses Cloud Logging and tracing in GCP and `logging.basicConfig(..., force=True)` locally.
- Use structured JSON Dataflow logs with contextual `LoggerAdapter`s from `backend/pipeline/normalization/common/logging.py`. Use `get_task_logger(__name__, {"system": "...", "component": "..."})` as in `backend/pipeline/normalization/audio/audio_processor.py`.
- Use `%s`-style logging in stricter backend code to satisfy logging lint rules, as in `backend/pipeline/storage/connection.py`.
- Some relaxed model/SFT files use f-string logging because `model/scripts/**.py` and `model/colabs/**/*.py` have per-file Ruff ignores in `pyproject.toml`; do not copy that style into strict backend modules.
- In TypeScript proxy code, log backend failures as JSON through `console.error(JSON.stringify(...))` in `frontend/api/src/utils.ts`; the Express fallback logs raw errors in `frontend/api/src/index.ts`.
- In the React UI, keep console logging limited to exceptional client-side failures such as auth/session refresh errors in `frontend/transcription-ui/src/context/AuthProvider.tsx`.
## Comments
- Comment domain invariants and operational contracts where future changes can silently break production behavior. Examples: the capture/runtime contract in `backend/pipeline/ingestion/models.py`, the `SourceType` three-place change warning in `backend/pipeline/storage/feed_store.py`, and the SFT hard-gate contract in `model/scripts/sft/preflight.py`.
- Use comments to document non-obvious platform constraints. Examples: PgBouncer transaction-mode limitations in `backend/pipeline/storage/connection.py`, JSDOM media stubs in `frontend/transcription-ui/src/test/setup.ts`, and single-dataset JSONL reuse in `model/scripts/sft/pipeline.py`.
- Keep comments near the code they constrain; do not duplicate broad architecture prose in implementation files unless it prevents a known class of regression.
- Tests may include regression comments when they encode a previously fragile invariant, as in `model/colabs/common/tests/test_manifest.py` and `backend/pipeline/ingestion/tests/test_runtime.py`.
- Use TSOA decorators and class/interface comments for API contract generation, especially in `frontend/api/src/*/*Controller.ts`.
- Use short JSDoc comments for request/query classes when decorators need metadata, such as `ListTranscriptsQueryParams.limit` in `frontend/api/src/transcripts/transcriptsController.ts`.
- React component files mostly avoid exported TSDoc and rely on prop interfaces, clear component names, and tests: `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`.
- Python public functions/classes often include Google-style docstrings, and Ruff uses `pydocstyle` with `convention = "google"` in `pyproject.toml`; docstring-required rules are ignored, but new complex public APIs should still document args, returns, raises, and invariants.
## Function Design
## Module Design
<!-- GSD:conventions-end -->

<!-- GSD:architecture-start source:ARCHITECTURE.md -->
## Architecture

## System Overview
```text
|                   External Audio Sources                    |
| Broadcastify | OpenMHz / Fire | Echo GCS Notify             |
| `backend/...` | `backend/...` | `backend/.../echo`          |
| Ingestion and Claim-Check Publication                       |
| `backend/pipeline/ingestion`                                |
| `backend/pipeline/common/gcp_helper.py`                     |
| writes staged audio to GCS and publishes `AudioChunk`        |
| Streaming Normalization                                     |
| `backend/pipeline/normalization`                            |
| reads `AudioChunk`, stitches, normalizes, publishes          |
| `NormalizedAudio`                                           |
| Transcription                                                |
| `backend/pipeline/transcription`                            |
| reads `NormalizedAudio`, calls transcriber, publishes        |
| `TranscribedAudio`                                          |
| Evaluation, Storage, and Notification                       |
| `backend/pipeline/evaluation`                               |
| `backend/services/transcripts`                              |
| `backend/pipeline/notification`                             |
| AlloyDB / GCS / Redis       |   | Frontend API and React UI |
| `backend/pipeline/storage`  |   | `frontend/api`            |
| `terraform/modules/alloydb` |   | `frontend/transcription-ui` |
| Offline Model and SFT Tooling                               |
| `model/scripts/sft`, `model/colabs/common`                  |
| builds/evaluates datasets against GCS and Vertex AI          |
```
## Component Responsibilities
| Component | Responsibility | File |
|-----------|----------------|------|
| Ingestion CLI | Bootstraps settings, validates source topic routing, and runs the ingestion runtime. | `backend/pipeline/ingestion/main.py` |
| Ingestion runtime | Owns feed leasing, heartbeat renewal, collector task lifecycle, GCS upload, Pub/Sub publication, bookmarks, quarantine reporting, health state, and shutdown. | `backend/pipeline/ingestion/normalizer_runtime.py` |
| Source router | Maps `SourceType` values to collector functions and segmented or continuous Pub/Sub topics. | `backend/pipeline/ingestion/router.py` |
| Collector contract | Defines the `CapturedChunk`, `CaptureResources`, and `CollectorFn` interface used by all source collectors. | `backend/pipeline/ingestion/models.py` |
| Broadcastify feed collector | Captures continuous Icecast audio and segments it through ffmpeg. | `backend/pipeline/ingestion/collectors/icecast/icecast_collector.py` |
| OpenMHz collector | Consumes OpenMHz websocket events, downloads referenced M4A audio, and yields staged chunks. | `backend/pipeline/ingestion/collectors/openmhz/collector.py` |
| Echo ingestion function | Handles Echo bucket notifications as a separate Cloud Function and publishes `AudioChunk` messages. | `backend/pipeline/ingestion/collectors/echo/main.py` |
| Oldest feed publisher | Emits oldest-start-time metrics for alerting and scheduler visibility. | `backend/pipeline/ingestion/oldest_feed_publisher/main.py` |
| Normalization DAG | Defines the Beam streaming pipeline from Pub/Sub input through parse, stitch, normalize, serialize, and DLQ output. | `backend/pipeline/normalization/orchestration.py` |
| Normalization transforms | Implement Pub/Sub parsing, protobuf serialization, ordered stitching, VAD flushing, and derivative audio export. | `backend/pipeline/normalization/transforms/stateless.py`, `backend/pipeline/normalization/transforms/stateful.py` |
| Audio processor | Downloads audio, detects speech, converts/export normalized FLAC/M4A outputs. | `backend/pipeline/normalization/audio/audio_processor.py` |
| Transcription function | CloudEvent entry point with cached transcriber, publisher, and processor instances. | `backend/pipeline/transcription/main.py` |
| Transcription processor | Parses `NormalizedAudio`, invokes a transcriber, builds `TranscribedAudio`, and publishes ordered Pub/Sub output. | `backend/pipeline/transcription/processor.py` |
| Transcriber abstraction | Defines and constructs concrete transcription backends such as Google Chirp v3 and mock transcribers. | `backend/pipeline/transcription/transcribers/base.py`, `backend/pipeline/transcription/transcribers/factory.py` |
| Evaluation function | Parses transcription events, evaluates rules, stores transcripts, and publishes alerts. | `backend/pipeline/evaluation/main.py`, `backend/pipeline/evaluation/processor.py` |
| Rules evaluator | Applies static or remote rulesets against transcript text. | `backend/pipeline/evaluation/rules_evaluation/evaluator.py` |
| Notification function | Converts alert events to notification payloads, deduplicates with Redis, and sends outbound notifications. | `backend/pipeline/notification/send_notification.py` |
| Storage layer | Centralizes AlloyDB connection pooling and query-backed stores for feeds, transcripts, rules, and audio segments. | `backend/pipeline/storage/connection.py`, `backend/pipeline/storage/feed_store.py`, `backend/pipeline/storage/transcript_store.py`, `backend/pipeline/storage/rules_store.py`, `backend/pipeline/storage/audio_segment_store.py` |
| Feed service | FastAPI CRUD API over feed records and ingestion control operations. | `backend/services/feeds/main.py`, `backend/services/feeds/service.py` |
| Rules service | FastAPI CRUD API over alert/evaluation rules. | `backend/services/rules/main.py`, `backend/services/rules/service.py` |
| Transcripts service | FastAPI API for transcript creation, listing, lookup, and deletion. | `backend/services/transcripts/main.py`, `backend/services/transcripts/service.py` |
| Frontend API facade | Express/TSOA gateway that handles browser auth, generated OpenAPI routes, backend ID-token clients, and response conversion. | `frontend/api/src/index.ts`, `frontend/api/src/authentication.ts` |
| React UI | Browser application for transcript review, feed/rules management, API docs, and authenticated routes. | `frontend/transcription-ui/src/main.tsx`, `frontend/transcription-ui/src/App.tsx` |
| Shared frontend types | TypeScript contract package shared by the UI and API facade. | `frontend/common/src/index.ts`, `frontend/common/src/types/feeds.ts`, `frontend/common/src/types/transcripts.ts`, `frontend/common/src/types/rules.ts` |
| Protobuf contracts | Pub/Sub message schemas for raw chunks, normalized audio, transcribed audio, evaluated audio, alerts, and Beam state. | `protos/raw_audio_chunk.proto`, `protos/normalized_audio.proto`, `protos/transcribed_audio.proto`, `protos/evaluated_transcribed_audio.proto`, `protos/alert_notification.proto`, `protos/streaming_state.proto` |
| SFT/model tooling | Builds ASR training/evaluation JSONL, validates datasets, shares scoring and Vertex helpers. | `model/scripts/sft/pipeline.py`, `model/scripts/sft/preflight.py`, `model/colabs/common` |
| Infrastructure | Defines deployment building blocks for AlloyDB, GCS, Redis, Cloud Functions, container MIGs, and ASR evaluation. | `terraform/modules` |
## Pattern Overview
- Use GCS for audio payloads and Pub/Sub protobuf messages for claim metadata. Message schemas live in `protos/*.proto`; generated Python bindings are consumed from `backend/pipeline/schema_types`.
- Keep ingestion source-specific logic behind `CollectorFn` implementations in `backend/pipeline/ingestion/collectors`, while `NormalizerRuntime` owns leasing, upload, publication, bookmarks, health, and failures in `backend/pipeline/ingestion/normalizer_runtime.py`.
- Use AlloyDB as the durable control plane for feeds, transcripts, rules, and audio segments through stores in `backend/pipeline/storage`.
- Put browser-facing auth/session behavior in `frontend/api`; React code in `frontend/transcription-ui` calls the facade instead of direct backend service URLs.
- Keep offline dataset, scoring, and Vertex AI operations in `model/scripts/sft` and `model/colabs/common` rather than in serving code.
## Layers
- Purpose: Browser workflows for transcript review, feed administration, rules, docs, and login.
- Location: `frontend/transcription-ui`
- Contains: React routes, MUI components, TanStack Query hooks, browser services, auth context.
- Depends on: `frontend/api`, `frontend/common`, Google OAuth browser client.
- Used by: End users and local development workflows.
- Purpose: Convert browser requests into authenticated calls to backend services and expose generated TSOA routes/docs.
- Location: `frontend/api`
- Contains: Express app, TSOA controllers, auth/session endpoints, OpenAPI generation, backend error mapping.
- Depends on: `frontend/common`, Google auth libraries, backend FastAPI services.
- Used by: `frontend/transcription-ui`.
- Purpose: Provide internal CRUD APIs for feeds, rules, and transcripts.
- Location: `backend/services`
- Contains: FastAPI apps, service classes, Pydantic request/response models.
- Depends on: `backend/pipeline/storage`, `backend/pipeline/common/auth.py`, generated protobuf classes.
- Used by: `frontend/api`, evaluation functions, local and integration tests.
- Purpose: Lease active feeds, capture source audio, stage audio in GCS, and publish raw audio claim messages.
- Location: `backend/pipeline/ingestion`
- Contains: Runtime, settings, routing, health server, collectors, Echo Cloud Function, oldest-feed publisher.
- Depends on: `backend/pipeline/storage`, `backend/pipeline/common`, `backend/pipeline/schema_types`, GCS, Pub/Sub, AlloyDB.
- Used by: Deployment entry points and integration tests.
- Purpose: Parse raw chunk events, enforce ordering, stitch audio, detect speech, normalize outputs, and publish normalized claim messages.
- Location: `backend/pipeline/normalization`
- Contains: Beam pipeline assembly, stateful/stateless DoFns, audio processor, options.
- Depends on: Apache Beam, GCS, Pub/Sub, protobuf contracts, VAD/audio processing libraries.
- Used by: Dataflow or local Beam execution.
- Purpose: Convert normalized audio into transcript protobuf events through configurable transcriber backends.
- Location: `backend/pipeline/transcription`
- Contains: Cloud Function entry point, processor, publisher, transcriber interface/factory, Chirp and mock implementations.
- Depends on: Google Speech APIs, Pub/Sub, GCS, protobuf contracts.
- Used by: Pub/Sub-triggered Cloud Functions and tests.
- Purpose: Evaluate transcripts against rules, persist transcript results, publish alert decisions, deduplicate notifications, and call outbound notification endpoints.
- Location: `backend/pipeline/evaluation`, `backend/pipeline/notification`
- Contains: Cloud Function entry points, evaluation service, rule evaluators, notification request handler.
- Depends on: Rules API, Transcripts API, Pub/Sub, Redis, outbound notification endpoint.
- Used by: Pub/Sub-triggered Cloud Functions.
- Purpose: Own durable database access and typed message contracts shared across services.
- Location: `backend/pipeline/storage`, `protos`, `backend/pipeline/schema_types`
- Contains: AsyncPG pools, stores, SQL query helpers, generated protobuf modules.
- Depends on: AlloyDB/PostgreSQL, generated protobuf code.
- Used by: Backend pipeline code, FastAPI services, tests.
- Purpose: Build training/evaluation datasets, score ASR outputs, submit Vertex AI jobs, and share prompt/manifest contracts.
- Location: `model/scripts/sft`, `model/colabs/common`
- Contains: CLI pipeline, dataset adapters, preflight validation, scoring, GCS helpers, Vertex helpers.
- Depends on: GCS, Vertex AI, optional model/scoring dependencies.
- Used by: Offline workflows and model development.
- Purpose: Define deployment modules, local emulators/mock services, and end-to-end/integration tests.
- Location: `terraform`, `local_dev`, `integration_tests`
- Contains: Terraform modules, Docker/local support files, pytest suites.
- Depends on: GCP, local environment configuration, service containers.
- Used by: Deployment, CI, local development, and regression testing.
## Data Flow
### Primary Audio-to-Alert Path
### Browser Management Path
### SFT Dataset Path
- Ingestion runtime state is held on `NormalizerRuntime` instance fields and support threads in `backend/pipeline/ingestion/normalizer_runtime.py`.
- Feed ownership and progress are durable in AlloyDB through `backend/pipeline/storage/feed_store.py`, with fenced updates and heartbeat renewal.
- Beam ordering and flush state are held in state specs and timers in `backend/pipeline/normalization/transforms/stateful.py`.
- FastAPI services create per-process connection pools in lifespan handlers such as `backend/services/feeds/main.py:29`.
- Cloud Function modules cache warm clients/processors in module globals such as `backend/pipeline/transcription/main.py:112`, `backend/pipeline/evaluation/main.py:21`, `backend/pipeline/notification/send_notification.py:75`, and `backend/pipeline/ingestion/collectors/echo/main.py:60`.
- Frontend server state is managed through TanStack Query in `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx` and auth context in `frontend/transcription-ui/src/context/AuthProvider.tsx`.
## Key Abstractions
- Purpose: Keep source capture implementations interchangeable while preventing collectors from owning runtime responsibilities.
- Examples: `CapturedChunk`, `CaptureResources`, and `CollectorFn` in `backend/pipeline/ingestion/models.py`.
- Pattern: Async generator interface that yields local staged audio plus metadata to `NormalizerRuntime`.
- Purpose: Coordinate feed leasing, capture task orchestration, upload/publication, progress bookmarks, heartbeat, health, and shutdown.
- Examples: `backend/pipeline/ingestion/normalizer_runtime.py`.
- Pattern: Composition root around collector functions, store clients, GCS/Pub/Sub clients, and watchdog threads.
- Purpose: Encapsulate SQL and row/protobuf/domain mapping for each durable resource.
- Examples: `backend/pipeline/storage/feed_store.py`, `backend/pipeline/storage/transcript_store.py`, `backend/pipeline/storage/rules_store.py`, `backend/pipeline/storage/audio_segment_store.py`.
- Pattern: AsyncPG-backed repositories used by FastAPI services and pipeline runtimes.
- Purpose: Carry typed metadata across Pub/Sub while audio bytes remain in GCS.
- Examples: `protos/raw_audio_chunk.proto`, `protos/normalized_audio.proto`, `protos/transcribed_audio.proto`, `protos/evaluated_transcribed_audio.proto`, `protos/alert_notification.proto`.
- Pattern: Claim-check contracts with generated Python bindings in `backend/pipeline/schema_types`.
- Purpose: Allow transcription backends to be selected by configuration while preserving a common processor path.
- Examples: `backend/pipeline/transcription/transcribers/base.py`, `backend/pipeline/transcription/transcribers/factory.py`, `backend/pipeline/transcription/transcribers/chirp.py`, `backend/pipeline/transcription/transcribers/mock.py`.
- Pattern: ABC plus factory selected by `TRANSCRIBER_TYPE`.
- Purpose: Separate transcript event processing from ruleset loading and text matching.
- Examples: `backend/pipeline/evaluation/service.py`, `backend/pipeline/evaluation/rules_evaluation/evaluator.py`.
- Pattern: Service object delegates to static or remote evaluator implementations.
- Purpose: Keep browser UI and API facade request/response contracts aligned.
- Examples: `frontend/common/src/types/feeds.ts`, `frontend/common/src/types/transcripts.ts`, `frontend/common/src/types/rules.ts`.
- Pattern: Shared package imported as `@transcription/common`.
- Purpose: Convert source-specific dataset manifests into a canonical ASR training/evaluation row format.
- Examples: `model/colabs/common/manifest.py`, `model/scripts/sft/adapters/gcs_manifest.py`.
- Pattern: Adapter interface plus registry-driven SFT pipeline.
## Entry Points
- Location: `backend/pipeline/ingestion/main.py`
- Triggers: Container process or local CLI execution.
- Responsibilities: Validate settings/topic routing and run `NormalizerRuntime`.
- Location: `backend/pipeline/ingestion/collectors/echo/main.py`
- Triggers: CloudEvent bucket notification.
- Responsibilities: Resolve feed metadata, upload/copy staged Echo audio, publish `AudioChunk`, record failures.
- Location: `backend/pipeline/ingestion/oldest_feed_publisher/main.py`
- Triggers: HTTP Cloud Function.
- Responsibilities: Query oldest active feed start time and publish monitoring metric data.
- Location: `backend/pipeline/normalization/main.py`
- Triggers: Beam/Dataflow process.
- Responsibilities: Parse pipeline options and run `get_pipeline`.
- Location: `backend/pipeline/transcription/main.py`
- Triggers: Pub/Sub CloudEvent containing `NormalizedAudio`.
- Responsibilities: Transcribe normalized audio and publish `TranscribedAudio`.
- Location: `backend/pipeline/evaluation/main.py`
- Triggers: Pub/Sub CloudEvent containing `TranscribedAudio`.
- Responsibilities: Evaluate transcript rules, write transcript service records, publish alert events.
- Location: `backend/pipeline/notification/send_notification.py`
- Triggers: Pub/Sub CloudEvent containing alert notification data.
- Responsibilities: Deduplicate and send outbound notification requests.
- Location: `backend/services/feeds/main.py`
- Triggers: HTTP requests through FastAPI/Uvicorn.
- Responsibilities: Feed CRUD, deactivate, and reset operations.
- Location: `backend/services/rules/main.py`
- Triggers: HTTP requests through FastAPI/Uvicorn.
- Responsibilities: Rules CRUD operations.
- Location: `backend/services/transcripts/main.py`
- Triggers: HTTP requests through FastAPI/Uvicorn.
- Responsibilities: Transcript creation, lookup, listing, and deletion.
- Location: `frontend/api/src/index.ts`
- Triggers: Express/Functions Framework HTTP execution.
- Responsibilities: Register TSOA routes, handle browser auth, proxy backend calls, expose OpenAPI docs.
- Location: `frontend/transcription-ui/src/main.tsx`
- Triggers: Browser load through Vite/build output.
- Responsibilities: Mount providers, route views, manage browser workflows.
- Location: `model/scripts/sft/pipeline.py`
- Triggers: Python CLI subcommands.
- Responsibilities: Build SFT JSONL datasets, preflight data, and provide tuning/evaluation command surface.
- Location: `backend/scripts/bulk_import_feeds.py`
- Triggers: Operator script.
- Responsibilities: Import feed definitions into feed storage.
## Architectural Constraints
- **Threading:** Ingestion is an asyncio/uvloop runtime with additional OS threads for heartbeat and RSS watchdog behavior in `backend/pipeline/ingestion/normalizer_runtime.py`. Collector implementations must not block the event loop and should yield control through async operations as specified by `backend/pipeline/ingestion/models.py`.
- **Beam state:** Normalization uses Beam keyed state and timers in `backend/pipeline/normalization/transforms/stateful.py`; ordering and flushing changes must preserve state schema compatibility in `protos/streaming_state.proto`.
- **Global state:** Warm module-level caches exist in Cloud Function modules and docs helpers: `backend/pipeline/transcription/main.py`, `backend/pipeline/evaluation/main.py`, `backend/pipeline/notification/send_notification.py`, `backend/pipeline/ingestion/collectors/echo/main.py`, and `frontend/api/src/docs/docsController.ts`.
- **Generated contracts:** Edit `protos/*.proto` and regenerate Python bindings rather than hand-editing `backend/pipeline/schema_types`. Edit TSOA controllers and regenerate routes/spec rather than hand-editing generated TSOA output.
- **Source type registry:** Adding or changing a claimable source type spans `backend/pipeline/storage/feed_store.py`, SQL seed/migration files in `terraform/modules/alloydb/sql/ingestion`, `_DEFAULT_CAPS` in `backend/pipeline/ingestion/settings.py`, `_COLLECTORS` in `backend/pipeline/ingestion/router.py`, and shared UI/API types in `frontend/common/src/types/feeds.ts` when browser-facing.
- **Circular imports:** Not detected during architecture inspection. Keep shared concerns in `backend/pipeline/common`, `backend/pipeline/storage`, `frontend/common`, or `model/colabs/common` instead of importing across feature entry points.
## Anti-Patterns
### Updating a Source Type in One Place
### Letting Collectors Own Runtime Side Effects
### Bypassing the Frontend API Facade
### Editing Generated Files by Hand
## Error Handling
- Ingestion validates configuration at startup in `backend/pipeline/ingestion/main.py` and `backend/pipeline/ingestion/settings.py`.
- Ingestion records per-feed failures and quarantines repeated failures through `FeedStore.report_feed_failure` in `backend/pipeline/storage/feed_store.py:309`.
- Ingestion treats feed-fence violations as process-fatal in `backend/pipeline/ingestion/normalizer_runtime.py:893`.
- Beam parsing/serialization/normalization errors are tagged to DLQ outputs in `backend/pipeline/normalization/transforms/stateless.py` and `backend/pipeline/normalization/transforms/stateful.py`.
- Transcription and evaluation processors parse Pub/Sub payloads explicitly and raise processing errors from `backend/pipeline/transcription/processor.py` and `backend/pipeline/evaluation/processor.py`.
- The frontend API converts backend HTTP errors through `frontend/api/src/utils.ts` and centralizes Express error handling in `frontend/api/src/index.ts`.
- The React UI converts failed fetches to `ApiError` in `frontend/transcription-ui/src/utils/apiUtils.ts` and displays route-level/application-level feedback in `frontend/transcription-ui/src/App.tsx`.
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

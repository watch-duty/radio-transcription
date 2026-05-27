# Technology Stack

**Analysis Date:** 2026-05-27

## Languages

**Primary:**
- Python 3.13.2 - backend pipeline workers, CloudEvent functions, FastAPI services, integration tests, and model tooling under `backend/`, `integration_tests/`, `local_dev/`, and `model/`; runtime is declared in `.tool-versions`, `.mise.toml`, `pyproject.toml`, and service Dockerfiles such as `backend/pipeline/ingestion/Dockerfile`.
- TypeScript 6.0.2 - frontend proxy API, React UI, and shared browser/API types under `frontend/api`, `frontend/transcription-ui`, and `frontend/common`; package versions are declared in `frontend/api/package.json`, `frontend/transcription-ui/package.json`, and `frontend/common/package.json`.

**Secondary:**
- Terraform 1.14.5 - reusable GCP infrastructure modules under `terraform/modules/*`, with provider constraints in `terraform/modules/alloydb/versions.tf`, `terraform/modules/gcs_bucket/versions.tf`, and `terraform/modules/container_mig/versions.tf`.
- Protocol Buffers - pipeline message schemas in `protos/*.proto`, generated into `backend/pipeline/schema_types` by the `generate:protos` task in `.mise.toml` and the protobuf generation notes in `backend/pipeline/README.md`.
- SQL - AlloyDB schema migrations in `terraform/modules/alloydb/sql/ingestion/*.sql`, including tables for `feeds`, `rules`, `transcripts`, `audio_segments`, and `annotations`.
- Shell/YAML/JSON - Docker, Compose, GitHub Actions, pre-commit, Firebase Hosting, OpenAPI, and tool configuration in `docker-compose.yml`, `asr-eval-docker-compose.yml`, `.github/workflows/*.yml`, `.pre-commit-config.yaml`, `frontend/transcription-ui/firebase.json`, and `frontend/api/openapi.yaml`.
- Jupyter notebooks - ASR evaluation notebooks in `model/colabs/*.ipynb`, supported by Docker images in `model/notebook_docker/Dockerfile` and `model/nemo_docker/Dockerfile`.

## Runtime

**Environment:**
- CPython 3.13.2 is the main backend runtime, pinned by `.tool-versions`; root project constraints require `>=3.13, <3.14` in `pyproject.toml`.
- `python:3.13-slim` is used for most service/function images, including `backend/pipeline/ingestion/Dockerfile`, `backend/pipeline/transcription/Dockerfile`, `backend/pipeline/evaluation/Dockerfile`, `backend/pipeline/notification/Dockerfile`, `backend/services/transcripts/Dockerfile`, `backend/services/feeds/Dockerfile`, and `backend/services/rules/Dockerfile`.
- Apache Beam Python SDK 2.73.0 runs the normalization/Dataflow Flex Template image in `backend/pipeline/normalization/Dockerfile`, with the matching Python dependency declared in `backend/pipeline/normalization/pyproject.toml`.
- Node.js 22.14.0 is pinned by `.tool-versions`; Node 22 slim images build and run the frontend proxy API in `frontend/api/Dockerfile`.
- ASR experimentation uses GPU-capable containers: `pytorch/pytorch:2.5.1-cuda12.4-cudnn9-runtime` in `model/notebook_docker/Dockerfile` and `nvcr.io/nvidia/nemo:26.02.00` in `model/nemo_docker/Dockerfile`.
- Local end-to-end development uses Docker Compose services and emulators in `docker-compose.yml`, plus ASR evaluation containers in `asr-eval-docker-compose.yml`.

**Package Manager:**
- `uv` 0.9.28 manages Python dependencies from `pyproject.toml` with lockfile `uv.lock` present; workspace members are `backend/pipeline/normalization` and `backend/pipeline/transcription` in `pyproject.toml`.
- `yarn` manages TypeScript packages with lockfiles at `frontend/api/yarn.lock`, `frontend/transcription-ui/yarn.lock`, and `frontend/common/yarn.lock`.
- `mise` orchestrates tool installation and repo tasks from `.mise.toml`; pinned tool versions live in `.tool-versions`.
- Lockfile status: `uv.lock` present, `frontend/api/yarn.lock` present, `frontend/transcription-ui/yarn.lock` present, `frontend/common/yarn.lock` present.

## Frameworks

**Core:**
- Google Functions Framework 3.10.1 - Python CloudEvent/HTTP entrypoints in `backend/pipeline/transcription/main.py`, `backend/pipeline/evaluation/main.py`, `backend/pipeline/notification/send_notification.py`, `backend/pipeline/ingestion/collectors/echo/main.py`, `backend/pipeline/ingestion/oldest_feed_publisher/main.py`, and `backend/pipeline/ingestion/broadcastify_credential_rotation/main.py`.
- FastAPI 0.136.1 with Uvicorn 0.46.0 - management APIs in `backend/services/transcripts/main.py`, `backend/services/feeds/main.py`, and `backend/services/rules/main.py`.
- Apache Beam 2.73.0 - streaming normalization pipeline in `backend/pipeline/normalization/main.py`, `backend/pipeline/normalization/options.py`, and `backend/pipeline/normalization/orchestration.py`.
- Express 5.2.1 with TSOA 7.0.0-alpha.0 - frontend proxy/API Gateway backing service in `frontend/api/src/index.ts`, `frontend/api/tsoa.json`, and controllers under `frontend/api/src/**/*Controller.ts`.
- React 19.2.0 with Vite 8.0.8 - transcription UI in `frontend/transcription-ui/src/main.tsx`, `frontend/transcription-ui/src/App.tsx`, and `frontend/transcription-ui/vite.config.ts`.
- Material UI 9.0.0, Toolpad Core 0.16.0, TanStack React Query 5.99.0, React Router 7.14.1, Wavesurfer 7.12.6 - UI framework, state/query, routing, and audio visualization in `frontend/transcription-ui/package.json`.

**Testing:**
- pytest 9.0.3, pytest-asyncio 1.3.0, pytest-xdist, pytest-cov, and testcontainers 4.14.2 - backend and integration test stack declared in `pyproject.toml` and used under `backend/**/tests` and `integration_tests`.
- Python `unittest` is still used by the `test:unit` task in `.mise.toml` for `backend/pipeline`.
- Vitest 3.x/4.x with React Testing Library and jsdom - frontend/API tests declared in `frontend/api/package.json`, `frontend/transcription-ui/package.json`, and `frontend/transcription-ui/vitest.config.ts`.
- Docker Compose E2E tests run the full pipeline via `mise run test:e2e` in `.mise.toml` and GitHub Actions workflow `.github/workflows/integration-tests.yml`.

**Build/Dev:**
- Docker and Docker Compose - local pipeline, service images, ASR notebooks, and CI smoke builds in `docker-compose.yml`, `asr-eval-docker-compose.yml`, and `backend/**/Dockerfile`.
- Terraform with Google provider `>= 6.0` - GCP modules in `terraform/modules/alloydb`, `terraform/modules/gcs_bucket`, `terraform/modules/cloud_function`, `terraform/modules/container_mig`, `terraform/modules/memorystore_for_redis`, and `terraform/modules/asr_evaluation`.
- Ruff 0.15.12 and ty 0.0.33 - Python formatting, linting, and type checking configured in `pyproject.toml`, `.mise.toml`, and `.pre-commit-config.yaml`.
- ESLint 10.x and Prettier 3.8.1 - TypeScript/CSS lint and formatting configured in `frontend/api/eslint.config.js`, `frontend/transcription-ui/eslint.config.js`, and `.prettierrc`.
- grpcio-tools/betterproto - protobuf generation configured in `.mise.toml`, `backend/pipeline/transcription/Dockerfile`, `backend/pipeline/normalization/Dockerfile`, and `backend/services/transcripts/Dockerfile`.

## Key Dependencies

**Critical:**
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

**Infrastructure:**
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

**Environment:**
- `.mise.toml` loads `.env` via `[env] _.file = ".env"` and sets `PYTHONPATH = "."`; a root `.env` file is not present in the scanned tree.
- `local_dev/LOCAL.env` is present and used by Docker Compose E2E tasks in `.mise.toml`; contents were not read.
- `frontend/api/.env.example` and `frontend/transcription-ui/.env.example` are present; contents were not read.
- Python backend env vars are loaded directly from `os.environ` in `backend/pipeline/storage/settings.py`, `backend/pipeline/ingestion/settings.py`, `backend/pipeline/transcription/main.py`, `backend/pipeline/evaluation/main.py`, `backend/pipeline/notification/send_notification.py`, `backend/pipeline/notification/request_handler.py`, and collector modules under `backend/pipeline/ingestion/collectors`.
- Frontend API env vars are centralized in `frontend/api/src/config.ts`; UI build-time env vars are accessed in `frontend/transcription-ui/src/main.tsx`, `frontend/transcription-ui/src/service/*.ts`, and `frontend/transcription-ui/src/components/common/AlertIcon.tsx`.

**Build:**
- Python build and dependency config: `pyproject.toml`, `uv.lock`, `backend/pipeline/normalization/pyproject.toml`, `backend/pipeline/transcription/pyproject.toml`, `model/pyproject.toml`, and `model/scripts/sft/requirements.txt`.
- TypeScript build and dependency config: `frontend/api/package.json`, `frontend/api/tsconfig.json`, `frontend/api/tsoa.json`, `frontend/transcription-ui/package.json`, `frontend/transcription-ui/tsconfig*.json`, `frontend/transcription-ui/vite.config.ts`, and `frontend/common/package.json`.
- Container build config: `backend/pipeline/*/Dockerfile`, `backend/services/*/Dockerfile`, `backend/pipeline/ingestion/collectors/echo/Dockerfile`, `backend/pipeline/ingestion/oldest_feed_publisher/Dockerfile`, `backend/pipeline/ingestion/broadcastify_credential_rotation/Dockerfile`, `frontend/api/Dockerfile`, `model/notebook_docker/Dockerfile`, and `model/nemo_docker/Dockerfile`.
- Infrastructure build config: Terraform modules in `terraform/modules/*`, GitHub workflows in `.github/workflows/*.yml`, and pre-commit hooks in `.pre-commit-config.yaml`.

## Platform Requirements

**Development:**
- Install `mise`, then run `mise install` using `.tool-versions` and `.mise.toml`; setup instructions live in `CONTRIBUTING.md`.
- Docker is required for local end-to-end pipeline development in `docker-compose.yml`, model containers in `asr-eval-docker-compose.yml`, and CI image smoke tests in `.github/workflows/ci.yml`.
- Google Cloud CLI and ADC are required for frontend proxy development against GCP services, as described in `CONTRIBUTING.md`; proxy API auth uses `google-auth-library` in `frontend/api/src/transcripts/transcriptsController.ts`, `frontend/api/src/feeds/feedsController.ts`, and `frontend/api/src/rules/rulesController.ts`.
- ASR GPU workflows require GCE GPU instances and Docker/NVIDIA runtime; guidance and Terraform entrypoint are in `ASR_CONTRIBUTING.md` and `terraform/modules/asr_evaluation/main.tf`.

**Production:**
- Event pipeline runs on GCP using Pub/Sub, GCS, Cloud Functions/Cloud Run, Dataflow, AlloyDB, Memorystore for Redis, Cloud Logging, Cloud Trace, and Cloud Monitoring, as evidenced by `backend/pipeline/*`, `backend/services/*`, and `terraform/modules/*`.
- Continuous ingestion worker fleet runs as a GCE regional Managed Instance Group using Container-Optimized OS and container images from Artifact Registry, configured by `terraform/modules/container_mig/main.tf`.
- UI static assets are built by Vite and hosted via Firebase Hosting configuration in `frontend/transcription-ui/firebase.json`; API traffic is mediated by the Express/TSOA proxy in `frontend/api` and API Gateway configuration in `frontend/api/tsoa.json`.
- Deployment orchestration is split: this repo contains reusable modules and CI in `.github/workflows/*.yml`; `.github/workflows/trigger-deploy.yml` dispatches a private deployment workflow when `terraform/modules/` or `protos/` changes.

---

*Stack analysis: 2026-05-27*

# Technology Stack

**Analysis Date:** 2026-06-26

## Languages

**Primary:**
- Python 3.13 - Backend pipeline services, CloudEvent handlers, storage layer, ingestion workers, and local ASR service. Version bounds are declared in `pyproject.toml`, `backend/pipeline/common/pyproject.toml`, `backend/pipeline/normalization/pyproject.toml`, `backend/pipeline/segmentation/pyproject.toml`, `backend/pipeline/transcription/pyproject.toml`, `backend/pipeline/evaluation/pyproject.toml`, `backend/pipeline/notification/pyproject.toml`, `backend/services/audio_segments/pyproject.toml`, `backend/services/feeds/pyproject.toml`, `backend/services/rules/pyproject.toml`, and `backend/services/local-whisper-api/pyproject.toml`.
- TypeScript 6.0 - Frontend API proxy, shared frontend types, and React UI. Manifests live in `frontend/api/package.json`, `frontend/common/package.json`, and `frontend/transcription-ui/package.json`.
- Python >=3.11 - Model and ASR research package under `model/pyproject.toml`, with notebook and Gemini SFT workflows in `model/src/common/` and `model/src/gemini_sft/`.

**Secondary:**
- Terraform >=1.3, project tool pin 1.14.5 - Google Cloud infrastructure modules in `terraform/modules/alloydb/`, `terraform/modules/cloud_function/`, `terraform/modules/container_mig/`, `terraform/modules/gcs_bucket/`, `terraform/modules/memorystore_for_redis/`, and `terraform/modules/asr_evaluation/`. Provider constraints are in `terraform/modules/alloydb/versions.tf`, `terraform/modules/container_mig/versions.tf`, and `terraform/modules/gcs_bucket/versions.tf`.
- Protocol Buffers - Pipeline event contracts in `protos/alert_notification.proto`, `protos/continuous_audio.proto`, `protos/evaluated_transcribed_audio.proto`, `protos/normalized_audio.proto`, `protos/segmented_audio.proto`, `protos/streaming_state.proto`, and `protos/transcribed_audio.proto`.
- SQL - AlloyDB ingestion schema migrations in `terraform/modules/alloydb/sql/ingestion/`.
- Dockerfile and Docker Compose - Service runtime images and local integration environment in `backend/**/Dockerfile`, `frontend/api/Dockerfile`, `model/notebook_docker/Dockerfile`, `model/nemo_docker/Dockerfile`, `docker-compose.yml`, `docker-compose.whisper.yml`, and `asr-eval-docker-compose.yml`.
- YAML/TOML/HCL - GitHub Actions in `.github/workflows/`, mise tasks in `.mise.toml`, Python package metadata in `pyproject.toml` and service pyprojects, and Terraform modules in `terraform/modules/`.

## Runtime

**Environment:**
- Python 3.13.2 is pinned for development in `.tool-versions`; backend Docker images use `python:3.13-slim` in `backend/pipeline/ingestion/Dockerfile`, `backend/pipeline/normalization/Dockerfile`, `backend/pipeline/transcription/Dockerfile`, `backend/pipeline/evaluation/Dockerfile`, `backend/pipeline/notification/Dockerfile`, `backend/services/audio_segments/Dockerfile`, `backend/services/feeds/Dockerfile`, `backend/services/rules/Dockerfile`, and `backend/services/local-whisper-api/Dockerfile`.
- Node.js 22.14.0 is pinned for development in `.tool-versions`; the frontend API runtime image uses `node:22-slim` in `frontend/api/Dockerfile`.
- Apache Beam Python 3.13 SDK 2.74.0 is the segmentation base image in `backend/pipeline/segmentation/Dockerfile`, matching `apache-beam[gcp]>=2.74.0` in `backend/pipeline/segmentation/pyproject.toml`.
- PyTorch 2.5.1 CUDA 12.4 runtime supports notebook ASR experiments in `model/notebook_docker/Dockerfile`.
- NVIDIA NeMo 26.02.00 with NeMo v2.7.2 supports heavy NeMo/Canary ASR work in `model/nemo_docker/Dockerfile`.
- Local development composes Pub/Sub emulator, fake GCS, Postgres, Redis, backend services, frontend API, mock audio services, and integration tests in `docker-compose.yml`.

**Package Manager:**
- `uv` 0.9.28 is pinned for development in `.tool-versions`; root lockfile `uv.lock` covers the backend workspace declared in `pyproject.toml`.
- Backend Dockerfiles copy `uv` from `ghcr.io/astral-sh/uv:0.11.13` in `backend/pipeline/ingestion/Dockerfile`, `backend/pipeline/normalization/Dockerfile`, `backend/pipeline/segmentation/Dockerfile`, `backend/pipeline/transcription/Dockerfile`, `backend/pipeline/evaluation/Dockerfile`, `backend/pipeline/notification/Dockerfile`, `backend/services/audio_segments/Dockerfile`, `backend/services/feeds/Dockerfile`, and `backend/services/rules/Dockerfile`.
- `backend/services/local-whisper-api/Dockerfile` uses `ghcr.io/astral-sh/uv:0.7.12` for the local Whisper service.
- Yarn is used for frontend packages through `frontend/api/yarn.lock`, `frontend/common/yarn.lock`, and `frontend/transcription-ui/yarn.lock`; CI enables Corepack in `.github/workflows/ci.yml`.
- Lockfiles: present for backend root `uv.lock`, model `model/uv.lock`, and each frontend package lockfile under `frontend/`.

## Frameworks

**Core:**
- FastAPI >=0.110.0 - Internal Python APIs and local ASR service in `backend/services/audio_segments/main.py`, `backend/services/feeds/main.py`, `backend/services/rules/main.py`, `backend/pipeline/transcription/main.py`, and `backend/services/local-whisper-api/main.py`.
- Uvicorn >=0.27.0 - ASGI runtime for FastAPI services in `backend/services/audio_segments/Dockerfile`, `backend/services/feeds/Dockerfile`, `backend/services/rules/Dockerfile`, `backend/pipeline/transcription/Dockerfile`, and `backend/services/local-whisper-api/Dockerfile`.
- Functions Framework - Python CloudEvent functions in `backend/pipeline/normalization/main.py`, `backend/pipeline/evaluation/main.py`, `backend/pipeline/notification/send_notification.py`, and `backend/pipeline/ingestion/collectors/echo/main.py`; Node API proxy runtime in `frontend/api/package.json` and `frontend/api/Dockerfile`.
- Apache Beam / Google Dataflow - Segmentation pipeline in `backend/pipeline/segmentation/main.py`, `backend/pipeline/segmentation/orchestration.py`, and `backend/pipeline/segmentation/Dockerfile`.
- Express 5.2.1 and tsoa 7 alpha - TypeScript BFF/API proxy in `frontend/api/src/index.ts`, controllers under `frontend/api/src/`, `frontend/api/tsoa.json`, and generated OpenAPI `frontend/api/openapi.yaml`.
- React 19.2.0 and Vite 8.1.0 - Browser UI in `frontend/transcription-ui/src/main.tsx`, `frontend/transcription-ui/src/App.tsx`, and `frontend/transcription-ui/vite.config.ts`.
- Material UI 9, Toolpad Core, React Router 7, React Query 5, WaveSurfer - UI framework and data/audio playback stack in `frontend/transcription-ui/package.json`.
- Terraform Google provider >=6.0 - Cloud infrastructure modules in `terraform/modules/`.

**Testing:**
- pytest >=9, pytest-asyncio, pytest-xdist, pytest-cov - Python tests under `backend/`, `integration_tests/`, and `model/tests/`; configured in `pyproject.toml` and `.mise.toml`.
- Vitest 4 and Testing Library - TypeScript API/UI tests under `frontend/api/src/**/*.test.ts`, `frontend/transcription-ui/src/**/*.test.ts`, and `frontend/transcription-ui/src/**/*.test.tsx`.
- Testcontainers - Component and integration tests for AlloyDB Omni and fake GCS under `integration_tests/` and collector integration tests under `backend/pipeline/ingestion/collectors/tests/`.
- Docker Compose E2E - End-to-end test stack is defined in `docker-compose.yml` and executed by `.github/workflows/integration-tests.yml`.

**Build/Dev:**
- mise - Task runner and tool bootstrap in `.mise.toml`.
- Ruff 0.14.14 and ty 0.0.42 - Python linting, formatting, and type checks in `pyproject.toml` and `.mise.toml`.
- ESLint 10 and Prettier 3.8 - TypeScript linting/formatting in `frontend/api/eslint.config.js`, `frontend/transcription-ui/eslint.config.js`, `.prettierrc`, and `.prettierignore`.
- TypeScript compiler - Build and typecheck commands in `frontend/api/package.json`, `frontend/common/package.json`, and `frontend/transcription-ui/package.json`.
- grpcio-tools and betterproto - Protobuf generation commands in `.mise.toml` and guidance in `backend/pipeline/README.md`.
- Docker Buildx Bake - CI image baking and GitHub Container Registry publishing in `.github/workflows/bake-main.yml`.
- pre-commit - Hook configuration in `.pre-commit-config.yaml`.

## Key Dependencies

**Critical:**
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

**Infrastructure:**
- `apache-beam[gcp]`, `onnxruntime`, `pedalboard`, `numba`, `av`, `soundfile`, `numpy` - Audio segmentation, VAD, and DSP in `backend/pipeline/segmentation/pyproject.toml` and `backend/pipeline/segmentation/audio/`.
- `aiohttp`, `httpx`, `requests`, `urllib3`, `curl-cffi` - HTTP clients for collectors, internal service calls, OpenMHz websocket/media access, and notifications in `backend/pipeline/ingestion/collectors/`, `backend/pipeline/common/clients/`, and `backend/pipeline/notification/request_handler.py`.
- `faster-whisper` and `python-multipart` - Local ASR API in `backend/services/local-whisper-api/pyproject.toml` and `backend/services/local-whisper-api/main.py`.
- `torchaudio`, `datasets`, `huggingface_hub`, `evaluate`, `jiwer`, `nemo_text_processing`, `google-genai` - Model evaluation and Gemini SFT extras in `model/pyproject.toml`, `model/notebook_docker/requirements.txt`, and `model/nemo_docker/requirements.txt`.
- `@react-oauth/google`, `@mui/material`, `@tanstack/react-query`, `react-virtuoso`, `@wavesurfer/react`, `wavesurfer.js` - UI auth, data fetching, virtualization, and waveform playback in `frontend/transcription-ui/package.json`.
- `axios`, `cookie-parser`, `cors`, `express`, `tsoa`, `js-yaml` - Frontend API proxy, OpenAPI generation, auth, and CORS in `frontend/api/package.json`.

## Configuration

**Environment:**
- Development tools are pinned in `.tool-versions` and orchestrated by `.mise.toml`.
- Python workspace membership and dependency groups are declared in `pyproject.toml`.
- Frontend package dependencies and scripts are declared in `frontend/api/package.json`, `frontend/common/package.json`, and `frontend/transcription-ui/package.json`.
- TypeScript configuration lives in `frontend/api/tsconfig.json`, `frontend/common/tsconfig.json`, `frontend/transcription-ui/tsconfig.json`, `frontend/transcription-ui/tsconfig.app.json`, and `frontend/transcription-ui/tsconfig.node.json`.
- Vite mode-specific proxying and mock auth behavior live in `frontend/transcription-ui/vite.config.ts` and `frontend/transcription-ui/mockAuthPlugin.ts`.
- `.env` loading is configured by `.mise.toml`; env-like files are present at `frontend/api/.env.example`, `frontend/transcription-ui/.env.example`, `frontend/transcription-ui/.env.local-dev.example`, and `local_dev/LOCAL.env`. Contents were not read.
- Project-local agent skill directories are not detected: `.codex/skills/` is absent and `.agents/skills/` is absent; `.agents/` exists.

**Build:**
- Backend service images are defined by Dockerfiles under `backend/pipeline/` and `backend/services/`.
- Frontend API image is defined in `frontend/api/Dockerfile`.
- ASR notebook and NeMo images are defined in `model/notebook_docker/Dockerfile` and `model/nemo_docker/Dockerfile`.
- Local and CI compose stacks are defined in `docker-compose.yml`, `docker-compose.override.yml`, `docker-compose.whisper.yml`, and `asr-eval-docker-compose.yml`.
- CI, integration tests, image baking, and deployment signaling live in `.github/workflows/ci.yml`, `.github/workflows/integration-tests.yml`, `.github/workflows/bake-main.yml`, and `.github/workflows/trigger-deploy.yml`.
- Google Cloud infrastructure modules live under `terraform/modules/`.
- Frontend UI Firebase Hosting metadata lives in `frontend/transcription-ui/firebase.json`.

## Platform Requirements

**Development:**
- Install the versions in `.tool-versions`: `uv` 0.9.28, Python 3.13.2, Node.js 22.14.0, Terraform 1.14.5, and `jq`.
- Use `mise run generate:protos` from `.mise.toml` after changing files under `protos/`.
- Use Docker Compose for full local runs through `.mise.toml` tasks `dev`, `dev:start`, `dev:whisper`, and `test:e2e`.
- Use Google Cloud ADC for model and ASR workflows that access GCS or Vertex; guidance is in `ASR_CONTRIBUTING.md`.
- Use `frontend/api/README.md` for API Gateway/OpenAPI placeholders and Cloud Identity admin-group setup.

**Production:**
- Google Cloud is the primary platform: Cloud Functions Gen 2 via `terraform/modules/cloud_function/main.tf`, Cloud Run Job for schema migration via `terraform/modules/alloydb/main.tf`, Google Dataflow Flex Template via `backend/pipeline/segmentation/Dockerfile`, GCE regional managed instance groups via `terraform/modules/container_mig/main.tf`, GCS via `terraform/modules/gcs_bucket/main.tf`, AlloyDB via `terraform/modules/alloydb/main.tf`, and Memorystore Redis via `terraform/modules/memorystore_for_redis/main.tf`.
- Container images are built and pushed to GitHub Container Registry by `.github/workflows/bake-main.yml` and referenced in `docker-compose.yml`.
- Private deployment is triggered from the public repo by `.github/workflows/trigger-deploy.yml`.
- Runtime authentication uses Google OAuth/OIDC, service account ID tokens, Google Cloud API Gateway/Endpoints userinfo headers, and Cloud Identity group membership checks in `frontend/api/src/authentication.ts` and `frontend/api/src/config.ts`.

---

*Stack analysis: 2026-06-26*

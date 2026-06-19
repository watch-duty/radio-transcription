# Technology Stack

**Analysis Date:** 2026-06-19

## Languages

**Primary:**
- Python 3.13 - backend pipeline, FastAPI services, CloudEvent handlers, ingestion workers, and Terraform helper scripts under `backend/`, `local_dev/`, `scripts/`, and root `pyproject.toml`.
- TypeScript 6.0 - frontend API proxy, shared frontend types, and React UI under `frontend/api/`, `frontend/common/`, and `frontend/transcription-ui/`.

**Secondary:**
- Terraform 1.14.5 - reusable GCP infrastructure modules under `terraform/modules/`.
- Protocol Buffers - pipeline event contracts under `protos/`, generated into `backend/pipeline/schema_types/`.
- Shell/Dockerfile/YAML - container builds, Docker Compose local stack, GitHub Actions, and mise tasks in `backend/**/Dockerfile`, `docker-compose.yml`, `.github/workflows/`, and `.mise.toml`.
- Python 3.11+ - model subtree package runtime declared in `model/pyproject.toml`; backend still requires Python 3.13 via root `pyproject.toml`.

## Runtime

**Environment:**
- Python `>=3.13,<3.14` for root/backend workspace packages in `pyproject.toml` and `backend/**/pyproject.toml`.
- Python `>=3.11` for the model package in `model/pyproject.toml`.
- Node.js 22.14.0 from `.tool-versions`; `frontend/api/tsconfig.json` extends `@tsconfig/node22`.
- Terraform 1.14.5 and uv 0.9.28 from `.tool-versions`.

**Package Manager:**
- Python: `uv` workspace with root `uv.lock` and `model/uv.lock`; workspace members are declared in `[tool.uv.workspace]` in `pyproject.toml`.
- TypeScript: Yarn classic lockfiles (`# yarn lockfile v1`) in `frontend/common/yarn.lock`, `frontend/api/yarn.lock`, and `frontend/transcription-ui/yarn.lock`.
- Lockfile: present for Python root, Python model, and all three frontend packages.

## Frameworks

**Core:**
- FastAPI `>=0.110.0` + Uvicorn `>=0.27.0` - backend HTTP APIs in `backend/services/audio_segments/`, `backend/services/feeds/`, `backend/services/rules/`, `backend/services/transcripts/`, and `backend/services/local-whisper-api/`.
- Functions Framework `>=3.10.1` - CloudEvent entry points in `backend/pipeline/transcription/main.py`, `backend/pipeline/evaluation/main.py`, `backend/pipeline/notification/send_notification.py`, `backend/pipeline/normalization/main.py`, and `backend/pipeline/ingestion/collectors/echo/main.py`.
- Apache Beam `apache-beam[gcp]>=2.74.0` - segmentation/Dataflow pipeline in `backend/pipeline/segmentation/`.
- Express 5 + tsoa 7 alpha - frontend API proxy and generated OpenAPI routes in `frontend/api/src/index.ts`, `frontend/api/tsoa.json`, and `frontend/api/openapi.yaml`.
- React 19 + Vite 8 + MUI 9 - browser UI in `frontend/transcription-ui/`.
- Docker Compose - full local pipeline stack in `docker-compose.yml`, local Whisper overlay in `docker-compose.whisper.yml`, and ASR evaluation stack in `asr-eval-docker-compose.yml`.

**Testing:**
- pytest 9 + pytest-asyncio + pytest-cov + pytest-xdist - backend/model tests configured in `pyproject.toml`, `.mise.toml`, and `model/pyproject.toml`.
- Vitest 4 + Testing Library + jsdom - frontend API/UI tests declared in `frontend/api/package.json` and `frontend/transcription-ui/package.json`.
- Testcontainers + Docker - storage/component tests and CI pre-pulls in `pyproject.toml` and `.github/workflows/ci.yml`.

**Build/Dev:**
- mise - task runner in `.mise.toml`; use tasks such as `mise run generate:protos`, `mise run lint`, `mise run test:unit`, and `mise run dev`.
- Ruff 0.14 + ty 0.0.42 + Pyright settings - Python formatting/lint/type-check config in `pyproject.toml`.
- ESLint 10 + Prettier 3 + TypeScript 6 - frontend checks in `frontend/api/eslint.config.js`, `frontend/transcription-ui/eslint.config.js`, and package scripts.
- grpcio-tools - protobuf generation from `protos/*.proto` to `backend/pipeline/schema_types/`.
- Docker Buildx/GHCR - CI image builds in `.github/workflows/ci.yml` and `.github/workflows/bake-main.yml`.

## Key Dependencies

**Critical:**
- `google-cloud-pubsub`, `google-cloud-storage`, `google-cloud-speech`, `google-cloud-secret-manager`, `google-cloud-logging`, `google-cloud-monitoring`, and `google-auth` - GCP messaging, object storage, Speech-to-Text, secrets, logging, monitoring, and auth across `backend/pipeline/common/pyproject.toml` and pipeline package manifests.
- `asyncpg>=0.29.0` and `psycopg[binary]>=3.2.0` - AlloyDB/Postgres access in `backend/pipeline/storage/connection.py` and `backend/pipeline/storage/sync_connection.py`.
- `redis>=7.3.0` - notification deduplication/cache integration in `backend/pipeline/common/storage/redis_service.py`.
- `pydantic>=2.10.6` / `pydantic-settings>=2.0.0` - API and pipeline models under `backend/services/**/models.py` and config utilities.
- `opentelemetry-api`, `opentelemetry-sdk`, `opentelemetry-exporter-gcp-trace`, and `opentelemetry-exporter-gcp-monitoring` - telemetry setup in `backend/pipeline/common/tracing_utils.py`.
- `google-genai>=2.3,<3` - optional Vertex AI Gemini tuning and batch inference in `model/src/common/gemini/vertex.py`.

**Infrastructure:**
- `apache-beam[gcp]>=2.74.0` - Dataflow-compatible segmentation runtime in `backend/pipeline/segmentation/pyproject.toml` and `backend/pipeline/segmentation/Dockerfile`.
- `faster-whisper>=1.0.0` - local ASR API in `backend/services/local-whisper-api/`.
- `onnxruntime`, `pedalboard`, `numba`, `numpy`, `soundfile`, `av`, and FFmpeg/ffprobe - audio segmentation and normalization in `backend/pipeline/segmentation/pyproject.toml`, `backend/pipeline/normalization/pyproject.toml`, and service Dockerfiles.
- `aiohttp`, `curl-cffi`, `requests`, `urllib3`, and `tenacity` - provider ingestion, internal service clients, retries, and HTTP calls under `backend/pipeline/ingestion/` and `backend/pipeline/common/clients/`.
- `@react-oauth/google`, `google-auth-library`, and `jose` - Google OAuth/JWT flows in `frontend/transcription-ui/src/main.tsx`, `frontend/api/src/auth/authController.ts`, and `frontend/api/src/authentication.ts`.
- `@tanstack/react-query`, `@mui/material`, `@toolpad/core`, `wavesurfer.js`, `howler`, `react-virtuoso`, and `swagger-ui-react` - UI state/data fetching, components, audio playback, virtualization, and docs in `frontend/transcription-ui/package.json`.

## Configuration

**Environment:**
- mise loads `.env` through `[env] _.file = ".env"` in `.mise.toml`; do not read or commit local env contents.
- Env placeholder files are present at `frontend/api/.env.example`, `frontend/transcription-ui/.env.example`, `frontend/transcription-ui/.env.local-dev.example`, and `local_dev/LOCAL.env`; contents are treated as secret-bearing and not quoted.
- Backend service URLs and auth config are validated centrally in `frontend/api/src/config.ts`: `ALLOWED_ORIGIN`, `TRANSCRIPTS_API_URL`, `RULES_API_URL`, `FEEDS_STORE_API_URL`, `AUDIO_SEGMENTS_API_URL`, `PROJECT_ID`, `API_PUBLIC_URL`, `GOOGLE_AUTH_CLIENT_ID`, `GOOGLE_AUTH_CLIENT_SECRET`, `AUTH_BACKEND`, and `WORKSPACE_ADMIN_GROUP_EMAIL`.
- AlloyDB config is loaded from `ALLOYDB_*` env vars in `backend/pipeline/storage/settings.py`.
- Ingestion worker config is loaded from GCS, Pub/Sub, lease, retry, health, and watchdog env vars in `backend/pipeline/ingestion/settings.py`.

**Build:**
- Root Python config: `pyproject.toml`, `uv.lock`, `.mise.toml`, `.pre-commit-config.yaml`.
- Model config: `model/pyproject.toml`, `model/uv.lock`, `model/notebook_docker/Dockerfile`, `model/nemo_docker/Dockerfile`.
- Frontend config: `frontend/api/package.json`, `frontend/api/tsconfig.json`, `frontend/api/tsoa.json`, `frontend/transcription-ui/package.json`, `frontend/transcription-ui/vite.config.ts`, and frontend ESLint configs.
- Infrastructure config: `terraform/modules/**`, `.github/workflows/ci.yml`, `.github/workflows/integration-tests.yml`, `.github/workflows/bake-main.yml`, and `.github/workflows/trigger-deploy.yml`.

## Platform Requirements

**Development:**
- Install tools from `.tool-versions`: uv 0.9.28, Python 3.13.2, Node 22.14.0, Terraform 1.14.5, and jq.
- Use `mise` tasks in `.mise.toml` for local workflows; broad E2E/component/API Docker tests are resource-heavy per `AGENTS.md` and `.agents/instructions.md`.
- Local stack uses Docker Compose services for Pub/Sub emulator, fake GCS, Postgres 15, Redis 7, backend pipeline services, frontend API, mock servers, and optional local Whisper in `docker-compose.yml` and `docker-compose.whisper.yml`.
- FFmpeg/ffprobe are required for audio processing and are copied into service images from `mwader/static-ffmpeg:6.1.1`.

**Production:**
- GCP is the primary platform: Cloud Functions Gen2 for CloudEvent handlers via `terraform/modules/cloud_function/`, GCE regional Managed Instance Groups on Container-Optimized OS via `terraform/modules/container_mig/`, Dataflow-compatible Beam image in `backend/pipeline/segmentation/Dockerfile`, AlloyDB via `terraform/modules/alloydb/`, GCS via `terraform/modules/gcs_bucket/`, and Memorystore Redis via `terraform/modules/memorystore_for_redis/`.
- GitHub Actions runs CI, image builds, integration tests, and private deployment dispatches in `.github/workflows/`.
- Container images are built for GHCR/GCP deployment from `backend/**/Dockerfile`, `frontend/api/Dockerfile`, and model Dockerfiles.

---

*Stack analysis: 2026-06-19*

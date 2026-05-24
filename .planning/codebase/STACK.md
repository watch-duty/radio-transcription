# Technology Stack

**Analysis Date:** 2026-05-24

## Languages

**Primary:**
- Python `>=3.13, <3.14` - backend services, CloudEvent handlers, ingestion runtime, storage layer, and pipeline code in `pyproject.toml`, `backend/pipeline/`, and `backend/services/`.
- TypeScript - frontend API proxy and React UI in `frontend/api/package.json`, `frontend/api/src/`, `frontend/transcription-ui/package.json`, and `frontend/transcription-ui/src/`.

**Secondary:**
- Python `>=3.10` - model/evaluation shared package in `model/pyproject.toml` and `model/colabs/common/`.
- Terraform `>=1.3` modules with Google provider `>=6.0` - infrastructure modules in `terraform/modules/*/versions.tf` and `terraform/modules/*/*.tf`.
- Protocol Buffers - pipeline contracts in `protos/*.proto`; Python bindings are generated into `backend/pipeline/schema_types/` by `.mise.toml`.
- SQL - AlloyDB/PostgreSQL schema and guards in `terraform/modules/alloydb/sql/ingestion/*.sql` and `terraform/modules/alloydb/sql/ci/hot_protection_check.sql`.
- Shell/YAML - Docker, GitHub Actions, cloud-init, and task orchestration in `Dockerfile` files, `.github/workflows/*.yml`, `.mise.toml`, and `terraform/modules/container_mig/cloud_config.yaml.tftpl`.
- Jupyter notebooks - ASR experiments and model evaluations in `model/colabs/*.ipynb`.

## Runtime

**Environment:**
- Python `3.13.2` via `.tool-versions`; backend Docker images use `python:3.13-slim` in `backend/pipeline/*/Dockerfile` and `backend/services/*/Dockerfile`.
- Node.js `22.14.0` via `.tool-versions`; frontend API Docker image uses `node:22-slim` in `frontend/api/Dockerfile`.
- Terraform `1.14.5` via `.tool-versions`; modules declare `required_version = ">= 1.3"` in `terraform/modules/*/versions.tf`.
- Apache Beam Python SDK image `apache/beam_python3.13_sdk:2.73.0` for normalization in `backend/pipeline/normalization/Dockerfile`.
- ASR evaluation images: `pytorch/pytorch:2.5.1-cuda12.4-cudnn9-runtime` in `model/notebook_docker/Dockerfile` and `nvcr.io/nvidia/nemo:26.02.00` in `model/nemo_docker/Dockerfile`.

**Package Manager:**
- Python: `uv` `0.9.28` via `.tool-versions`; lockfile `uv.lock` is present; Dockerfiles copy `uv` from `ghcr.io/astral-sh/uv` in `backend/pipeline/*/Dockerfile`.
- TypeScript: `yarn` workspaces are independent package directories with lockfiles at `frontend/common/yarn.lock`, `frontend/api/yarn.lock`, and `frontend/transcription-ui/yarn.lock`.
- Node package-manager activation is via Corepack in `.github/workflows/ci.yml`.
- Repo-local project skills: not detected under `.codex/skills/` or `.agents/skills/`; `.agents/` exists but has no `skills/*/SKILL.md` files.

## Frameworks

**Core:**
- Functions Framework `>=3.10.1` - Python Pub/Sub/Eventarc handlers in `backend/pipeline/transcription/main.py`, `backend/pipeline/evaluation/main.py`, `backend/pipeline/notification/send_notification.py`, and `backend/pipeline/ingestion/collectors/echo/main.py`.
- `@google-cloud/functions-framework` `^5.0.2` - Node HTTP proxy API runtime in `frontend/api/package.json` and `frontend/api/Dockerfile`.
- FastAPI `>=0.110.0` with Uvicorn `>=0.27.0` - rules, transcripts, and feeds services in `backend/services/rules/main.py`, `backend/services/transcripts/main.py`, and `backend/services/feeds/main.py`.
- Apache Beam `apache-beam[gcp]>=2.73.0` - streaming normalization pipeline in `backend/pipeline/normalization/pyproject.toml` and `backend/pipeline/normalization/orchestration.py`.
- Express `^5.2.1` and tsoa `^7.0.0-alpha.0` - frontend API proxy and OpenAPI generation in `frontend/api/package.json`, `frontend/api/src/index.ts`, and `frontend/api/tsoa.json`.
- React `^19.2.0`, React Router `^7.14.1`, Vite `^8.0.8`, Material UI `^9.0.0`, TanStack Query `^5.99.0` - browser UI in `frontend/transcription-ui/package.json`.

**Testing:**
- Python: `pytest>=9.0.2`, `pytest-asyncio>=1.3.0`, `pytest-xdist`, `pytest-cov`, and `unittest` commands in `pyproject.toml`, `.mise.toml`, and `.github/workflows/ci.yml`.
- TypeScript API: Vitest `^3.0.0` in `frontend/api/package.json`.
- TypeScript UI: Vitest `^4.1.4`, Testing Library, jsdom, and setup file `frontend/transcription-ui/src/test/setup.ts` via `frontend/transcription-ui/vitest.config.js`.
- Component and E2E infrastructure exists under `integration_tests/`, but integration and E2E execution is intentionally excluded from this mapping request.

**Build/Dev:**
- Mise task runner - tool install, lint, format, Docker Compose dev, proto generation, and test entrypoints in `.mise.toml`.
- Ruff `>=0.14.14`, ty `>=0.0.21`, Prettier `^3.8.1`, ESLint `^10.x`, TypeScript `^6.0.2` - code quality configured in `pyproject.toml`, `.prettierrc`, `frontend/api/eslint.config.js`, and `frontend/transcription-ui/eslint.config.js`.
- Docker Compose - local full-stack pipeline and ASR evaluation services in `docker-compose.yml`, `docker-compose.override.yml`, and `asr-eval-docker-compose.yml`.
- Terraform modules - reusable infrastructure definitions in `terraform/modules/alloydb/`, `terraform/modules/cloud_function/`, `terraform/modules/container_mig/`, `terraform/modules/gcs_bucket/`, `terraform/modules/memorystore_for_redis/`, and `terraform/modules/asr_evaluation/`.

## Key Dependencies

**Critical:**
- `google-cloud-pubsub>=2.35.0` - Pub/Sub publishers, emulator setup, and tests in `pyproject.toml`, `backend/pipeline/common/clients/pubsub_client.py`, and `local_dev/pubsub_init.py`.
- `google-cloud-storage>=2.18.2` plus `gcloud-aio-storage>=9.6.4` - synchronous and async GCS access in `pyproject.toml`, `backend/pipeline/common/clients/gcs_client.py`, `backend/pipeline/common/gcp_helper.py`, and `backend/pipeline/common/storage/gcs_uploader.py`.
- `google-cloud-speech>=2.37.0` - Google Speech-to-Text V2 Chirp transcriber in `backend/pipeline/transcription/pyproject.toml` and `backend/pipeline/transcription/transcribers/chirp.py`.
- `asyncpg>=0.29.0` and `psycopg[binary]>=3.2.0` - async and sync AlloyDB/PostgreSQL access in `pyproject.toml`, `backend/pipeline/storage/connection.py`, and `backend/pipeline/storage/sync_connection.py`.
- `redis>=7.3.0` - Redis/Memorystore notification deduplication in `pyproject.toml` and `backend/pipeline/common/storage/redis_service.py`.
- `opentelemetry-api>=1.41.1`, `opentelemetry-sdk>=1.41.1`, and `opentelemetry-exporter-gcp-trace>=1.12.0` - tracing in `pyproject.toml` and `backend/pipeline/common/tracing_utils.py`.
- `curl-cffi>=0.9.1`, `aiohttp>=3.13.3`, `uvloop>=0.21.0`, and `tenacity>=9.1.4` - high-concurrency ingestion, external polling, retries, and event loop behavior in `pyproject.toml` and `backend/pipeline/ingestion/`.
- `onnxruntime>=1.20.0`, `pedalboard>=0.9.18`, `numba>=0.61.0`, `numpy>=2.2.6`, and `soundfile>=0.13.1` - audio processing and VAD in `backend/pipeline/normalization/pyproject.toml` and `backend/pipeline/normalization/audio/`.

**Infrastructure:**
- `google-cloud-logging>=3.14.0`, `google-cloud-monitoring>=2.29.1`, and `google-cloud-secret-manager>=2.26.0` - Cloud Logging, custom metrics, and Secret Manager access in `pyproject.toml`, `backend/pipeline/common/logging.py`, `backend/pipeline/common/clients/monitoring_client.py`, and `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py`.
- `@react-oauth/google`, `google-auth-library`, `jsonwebtoken`, and `pyjwt` - Google OAuth and JWT handling in `frontend/transcription-ui/package.json`, `frontend/api/package.json`, `frontend/api/src/auth/authController.ts`, `frontend/api/src/authentication.ts`, and `backend/pipeline/common/auth.py`.
- `@wavesurfer/react`, `wavesurfer.js`, and `howler` - browser audio playback in `frontend/transcription-ui/package.json` and `frontend/transcription-ui/src/components/audio/`.
- `google-genai`, `transformers`, `huggingface_hub`, `evaluate`, `jiwer`, `nemo_text_processing`, `peft`, and `tensorboardX` - model research/evaluation dependencies in `model/notebook_docker/requirements.txt`, `model/nemo_docker/requirements.txt`, and `model/pyproject.toml`.
- `boto3` - Echo S3 archive scanning for model data sources in `pyproject.toml` and `model/data_sources/echo/s3_file_scanner.py`.

## Configuration

**Environment:**
- Tool versions are pinned in `.tool-versions`; task orchestration and `PYTHONPATH` are configured in `.mise.toml`.
- Backend runtime env vars are loaded directly from `os.environ` in `backend/pipeline/storage/settings.py`, `backend/pipeline/ingestion/settings.py`, `backend/pipeline/transcription/main.py`, `backend/pipeline/evaluation/main.py`, `backend/pipeline/notification/send_notification.py`, `backend/pipeline/notification/request_handler.py`, and `backend/pipeline/common/storage/redis_service.py`.
- Frontend API env vars are centralized and validated in `frontend/api/src/config.ts`.
- UI build/runtime env vars use Vite `import.meta.env` in `frontend/transcription-ui/src/main.tsx`, `frontend/transcription-ui/src/service/*.ts`, and `frontend/transcription-ui/src/components/common/AlertIcon.tsx`.
- `.mise.toml` loads `.env`; `.env`-style files must be treated as secret-bearing local configuration. Env example files exist at `frontend/api/.env.example` and `frontend/transcription-ui/.env.example`; `local_dev/LOCAL.env` exists for local Docker Compose.
- Key backend env var names include `ALLOYDB_HOST`, `ALLOYDB_PORT`, `ALLOYDB_USER`, `ALLOYDB_DB`, `ALLOYDB_PASSWORD`, `AUDIO_STAGING_BUCKET`, `CONTINUOUS_PUBSUB_TOPIC_PATH`, `SEGMENTED_PUBSUB_TOPIC_PATH`, `GOOGLE_CLOUD_PROJECT`, `TRANSCRIBER_TYPE`, `TRANSCRIBER_CONFIG`, `OUTPUT_TOPIC`, `RULES_EVALUATION_RESULTS_TOPIC`, `TRANSCRIPTS_API_URL`, `RULES_API_URL`, `APP_URL`, `FEEDS_API_URL`, `NOTIFICATION_ENDPOINT`, `NOTIFICATION_ENDPOINT_API_KEY`, `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`, and `REDIS_CERTIFICATE_PATH`.
- Key frontend env var names include `ALLOWED_ORIGIN`, `FEEDS_STORE_API_URL`, `API_PUBLIC_URL`, `GOOGLE_AUTH_CLIENT_ID`, `GOOGLE_AUTH_CLIENT_SECRET`, `VITE_GOOGLE_AUTH_CLIENT_ID`, `VITE_API_BASE_URL`, and `VITE_ALERT_ICON_SYMBOL_NAME`.

**Build:**
- Python workspace membership and source overrides are configured in `pyproject.toml` for `backend/pipeline/normalization` and `backend/pipeline/transcription`.
- Python package lockfile is `uv.lock`; TypeScript lockfiles are `frontend/common/yarn.lock`, `frontend/api/yarn.lock`, and `frontend/transcription-ui/yarn.lock`.
- Protobuf generation command is `generate:protos` in `.mise.toml`; generated Python output is expected under `backend/pipeline/schema_types/`.
- Docker images are defined by `backend/pipeline/ingestion/Dockerfile`, `backend/pipeline/normalization/Dockerfile`, `backend/pipeline/transcription/Dockerfile`, `backend/pipeline/evaluation/Dockerfile`, `backend/pipeline/notification/Dockerfile`, `backend/services/*/Dockerfile`, `frontend/api/Dockerfile`, `model/notebook_docker/Dockerfile`, and `model/nemo_docker/Dockerfile`.
- OpenAPI and API Gateway metadata are generated from tsoa configuration in `frontend/api/tsoa.json` and post-processed by `frontend/api/scripts/post-process-spec.js`.
- Firebase Hosting static UI configuration is in `frontend/transcription-ui/firebase.json`.

## Platform Requirements

**Development:**
- Install Mise, Docker, Python/uv, Node/Yarn, and Terraform as described in `CONTRIBUTING.md`.
- Use `mise install` from `.tool-versions` and `.mise.toml`.
- Python unit and model tasks use `uv run` commands from `.mise.toml`; frontend tasks use `yarn --cwd frontend/api ...` and `yarn --cwd frontend/transcription-ui ...` from `.mise.toml` and package scripts.
- Local full-stack development uses Docker Compose services in `docker-compose.yml`; local emulators include Pub/Sub, fake GCS, Postgres, Redis, mock audio, and mock notification services.
- ASR evaluation development uses `asr-eval-docker-compose.yml`, `model/notebook_docker/Dockerfile`, and `model/nemo_docker/Dockerfile`; GPU VM support is defined in `terraform/modules/asr_evaluation/`.

**Production:**
- Runtime target is Google Cloud: Pub/Sub, Cloud Functions/Cloud Run functions, Dataflow/Flex Templates, GCE Managed Instance Groups, AlloyDB, GCS, Memorystore for Redis, Cloud Logging, Cloud Trace, Cloud Monitoring, Secret Manager, API Gateway, and Firebase Hosting.
- Infrastructure modules are public reusable modules under `terraform/modules/`; deployment orchestration is triggered from `.github/workflows/trigger-deploy.yml` into a private deployment repository.
- CI runs on GitHub Actions from `.github/workflows/ci.yml` and `.github/workflows/integration-tests.yml`.

---

*Stack analysis: 2026-05-24*

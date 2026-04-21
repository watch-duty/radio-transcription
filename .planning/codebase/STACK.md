# Technology Stack

**Analysis Date:** 2026-04-21

A polyglot monorepo for GCP-native radio-audio transcription. Python 3.13 powers the streaming ingestion, transcription, evaluation, notification, and REST/gRPC-adjacent HTTP services; TypeScript/React 19 powers the operator UI and a thin API gateway. All source-language versions are pinned via `.tool-versions` and `.mise.toml`, making `mise install` the bootstrap entry point.

## Languages

**Primary:**
- Python 3.13.2 — all backend services in `backend/pipeline/` and `backend/services/`, plus the transcription pipeline at `backend/pipeline/transcription/`. Pinned `requires-python = ">=3.13, <3.14"` in `pyproject.toml`. Ruff `target-version = "py313"`.
- TypeScript 6.0.x — frontend UI in `frontend/transcription-ui/` and API gateway in `frontend/api/`. Shared types in `frontend/common/`.

**Secondary:**
- HCL / Terraform 1.14.5 — infrastructure-as-code under `terraform/modules/` (AlloyDB, Cloud Function, Container MIG, GCS Bucket, Memorystore Redis, ASR evaluation).
- Protocol Buffers — four message schemas in `protos/` (`raw_audio_chunk.proto`, `transcribed_audio.proto`, `evaluated_transcribed_audio.proto`, `alert_notification.proto`). Generated Python bindings live at `backend/pipeline/schema_types/`.
- SQL (PostgreSQL dialect) — DDL migrations under `terraform/modules/alloydb/sql/ingestion/`, applied via a Cloud Run Job mounted on GCS FUSE.
- YAML — cloud-init template at `terraform/modules/container_mig/cloud_config.yaml.tftpl`, GitHub Actions workflows in `.github/workflows/`, OpenAPI spec at `frontend/api/openapi.yaml`.
- Bash — `backend/pipeline/transcription/entrypoint.sh` (Dataflow launcher), Cloud Run Job schema-migration script inlined in Terraform.

## Runtime

**Environment:**
- Python 3.13.2 (CPython). The transcription Dataflow pipeline runs under `apache/beam_python3.13_sdk:2.71.0`. Other Python services run on `python:3.13-slim`. Python services set `PYTHONDONTWRITEBYTECODE=1` and `PYTHONUNBUFFERED=1`.
- Node.js 22.14.0 — frontend builds and the `frontend/api` Cloud Function. Docker image `node:22-slim`.
- Container-Optimized OS (COS, `cos-stable` family) — GCE Managed Instance Group VMs for the ingestion fleet. Containers orchestrated via cloud-init.
- GCE Compute Engine (Container MIG) for long-lived ingestion collectors; Cloud Run for request-scoped services; Dataflow (Apache Beam) for the transcription streaming job; Cloud Functions Gen 2 for the Broadcastify credential rotator and (via `frontend/api/Dockerfile`) the HTTP API gateway.

**Package Manager:**
- `uv` 0.9.28 — single source of Python truth. The root `pyproject.toml` declares a uv workspace with member `backend/pipeline/transcription`. `tool.uv.sources.transcription-pipeline = { workspace = true }` wires the sub-project as a first-party dependency. Dockerfiles pin `ghcr.io/astral-sh/uv:0.7.12`.
- Lockfile: `uv.lock` (present at repo root, 355 KB, regenerated via `uv sync`).
- `yarn` (via `corepack enable`) — per-package lockfiles at `frontend/api/yarn.lock`, `frontend/common/yarn.lock`, `frontend/transcription-ui/yarn.lock`. The repo-root `yarn.lock` is a stub (86 bytes). Install is `yarn install --frozen-lockfile --non-interactive` per package.

## Frameworks

**Core:**
- FastAPI `>=0.110.0` — `backend/pipeline/rules/main.py`, `backend/services/feeds/main.py`, `backend/services/transcripts/main.py`. All three apps mount `verify_oidc_token` as a global `Depends` and share the AlloyDB pool lifespan pattern from `backend/pipeline/storage/connection.py`.
- Uvicorn `>=0.27.0` — ASGI server for the FastAPI services. Invoked in Dockerfiles as `uvicorn backend.<path>.main:app --host 0.0.0.0 --port $PORT` (8080 in Cloud Run, 8086/8087/8089 locally).
- functions-framework `>=3.10.1` — Cloud Event / HTTP entrypoint for `backend/pipeline/evaluation/main.py`, `backend/pipeline/notification/send_notification.py`, `backend/pipeline/ingestion/broadcastify_credential_rotation/main.py`, `backend/pipeline/ingestion/collectors/echo/main.py`. Use `@functions_framework.cloud_event` for Pub/Sub/Eventarc-triggered handlers.
- Apache Beam `[gcp] >= 2.71.0` — streaming transcription in `backend/pipeline/transcription/orchestration.py`, deployed as a Dataflow Flex Template. Version must match the `apache/beam_python3.13_sdk` base image tag (see comment in `backend/pipeline/transcription/Dockerfile`).
- aiohttp `>=3.13.3` + `uvloop >=0.21.0` — async IO for the ingestion `NormalizerRuntime` at `backend/pipeline/ingestion/normalizer_runtime.py`. aiohttp `TCPConnector` limit sized to `max_feeds_per_worker` for GCS upload concurrency.
- React 19.2 + react-router 7.14 — UI in `frontend/transcription-ui/`. Material UI 9, `@mui/x-date-pickers` 9, `@emotion/react` 11.
- Express 5.2 + tsoa 7 (alpha) — `frontend/api/` HTTP gateway. tsoa generates `routes.ts` and `openapi.yaml` from decorated controllers.
- Pydantic `>=2.10.6` / `pydantic-settings >=2.0.0` — FastAPI request/response models and env-backed settings (`backend/services/feeds/models.py`, `backend/services/transcripts/models.py`, etc.).

**Testing:**
- pytest `>=9.0.2` + `pytest-asyncio >=1.3.0` (with `asyncio_mode = "auto"`) + `pytest-cov` — unit and integration tests. Fixtures use `asyncio_default_fixture_loop_scope = "function"`.
- `testcontainers[postgres]` + `docker` — component tests spin up real Postgres (via `postgres:15-alpine`) for `integration_tests/storage/`.
- `fakeredis` — unit test double for Redis in notification tests.
- `httpx` — HTTP mocking / async client in integration tests.
- `boto3` — present as a test dep (assumed for Wasabi S3 mocks in OpenMHZ download tests).
- Vitest 4 + `@testing-library/react` 16 + `jsdom` 29 — frontend UI tests.
- Vitest 3 — `frontend/api` tests.
- `soundfile` — audio fixture generation for transcription pipeline tests.
- `pyopenssl` — TLS helpers used in tests.

**Build/Dev:**
- `mise` (jdx) — unified tool-version manager. CI uses `jdx/mise-action@v3`. `.mise.toml` defines task graph (`lint`, `format`, `test:unit`, `test:e2e`, `generate:protos`).
- Vite 8 + `@vitejs/plugin-react-swc` 4 — frontend bundler. `vite.config.ts` proxies `/api`, `/openapi.yaml`, `/gcs` for local dev.
- ruff `>=0.14.14` — Python lint + format. Config in `[tool.ruff]` of root `pyproject.toml`, `line-length = 80`, `select = ["ALL"]` with a curated ignore list sorted-check-enforced in CI.
- ty `>=0.0.21` — Astral's type-checker (primary). pyright also configured at `pythonVersion = "3.13"` as secondary.
- ESLint 10 + `typescript-eslint` 8 + `@eslint/js` 10 — TypeScript lint. Prettier 3.8 with `@trivago/prettier-plugin-sort-imports`.
- pre-commit 4.5 — `.pre-commit-config.yaml` runs ruff, terraform fmt, and frontend lint/format on commit.
- grpcio-tools — proto-bindings generator. Invoked via `mise run generate:protos` and inside every Python Dockerfile.
- Docker + Docker Compose — local dev orchestration in `docker-compose.yml` (Postgres, Redis, Pub/Sub emulator, all services, mock server, integration-tests container).
- Firebase Hosting — `frontend/transcription-ui/firebase.json` rewrites all routes to `/index.html`; deploys the Vite `dist/` build.

## Key Dependencies

**Critical:**
- `apache-beam[gcp] >= 2.71.0` — transcription streaming pipeline. Version is locked to the Dataflow Flex Template base image; updating one requires updating the other.
- `google-cloud-speech >= 2.37.0` — Chirp (Speech-to-Text v2) transcription. Chirp-specific prompts at `backend/pipeline/transcription/chirp_prompt.txt` and `chirp_phrase_hints.txt`.
- `google-cloud-pubsub >= 2.35.0` — `PubSubClient` in `backend/pipeline/common/clients/pubsub_client.py`. Uses `PublisherOptions(enable_message_ordering=True)`.
- `google-cloud-storage >= 2.18.2` (sync) and `gcloud-aio-storage >= 9.6.4` (async) — dual client strategy. Sync for Cloud-Run-triggered Echo handler; async for the ingestion `NormalizerRuntime` with shared `aiohttp.TCPConnector`.
- `google-cloud-logging >= 3.14.0` — structured Cloud Logging client. Bootstrapped in `setup_logging()` at `backend/pipeline/common/logging.py` only when `is_gcp_env()` is true; falls back to stdlib `basicConfig` locally.
- `google-cloud-monitoring >= 2.29.1` — async MetricServiceAsyncClient in `backend/pipeline/common/clients/monitoring_client.py` for feed-level telemetry.
- `google-cloud-secret-manager >= 2.26.0` — Broadcastify JWT fetch (`backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py::_get_jwt_token`) and credential rotation.
- `asyncpg >= 0.29.0` — async AlloyDB connection pool in `backend/pipeline/storage/connection.py`. `statement_cache_size=0` required for PgBouncer transaction-mode pooling on AlloyDB port 6432.
- `psycopg[binary] >= 3.2.0` — sync Postgres client for the Echo Cloud Run sync path (`backend/pipeline/storage/sync_connection.py`, `sync_feed_store.py`).
- `redis >= 7.3.0` — notification dedup cache at `backend/pipeline/common/storage/redis_service.py`. SSL enabled in GCP env, plain TCP locally.
- `pyjwt >= 2.12.0` — Broadcastify JWT sign/validate in the credential rotator (`backend/pipeline/ingestion/broadcastify_credential_rotation/main.py`).
- `curl-cffi >= 0.9.1` — used by the OpenMHZ collector (`backend/pipeline/ingestion/collectors/openmhz/collector.py`) and its WebSocket transport (`_ws_transport.py`). Chosen over plain `aiohttp` to impersonate a browser TLS fingerprint against OpenMHZ's bot detection.
- `ten-vad >= 1.0.6.8` — voice activity detection inside the transcription pipeline (`backend/pipeline/transcription/vads.py`). Requires `libomp5 libc++1 libc++abi1` system packages (installed in Beam Dockerfile and CI).
- `pydub >= 0.25.1` + `audioop-lts >= 0.2.1` — audio manipulation. `audioop-lts` shims Python 3.13's removal of `audioop`. Requires `ffmpeg` at runtime.
- `tenacity >= 9.1.4` — exponential backoff for AlloyDB pool creation (`create_pool_with_retry`) and bookmark writes.
- `cachetools >= 5.5.0` — TTL caches used around feed metadata and JWT refresh.

**Infrastructure:**
- `functions-framework >= 3.10.1` — Cloud Event entry point for every event-driven service.
- `cloudevents >= 1.12.0` — CloudEvent dataclass used across Pub/Sub and Eventarc consumers.
- `urllib3 >= 2.6.3` — retry-enabled `PoolManager` in the notification `RequestHandler`.
- `requests >= 2.32.5` — sync HTTP (Broadcastify auth, TranscriptsClient).
- `grpcio-tools >= 1.65.5` — protoc + grpc_tools for bindings generation (build-time only, per `dependency-groups.build`).
- `numpy >= 2.2.6` + `scipy >= 1.17.1` — DSP for the transcription pipeline (`dsp.py`, `audio_processor.py`).
- `crcmod >= 1.7` — GCS upload CRC32C validation.

**Frontend critical:**
- `@tanstack/react-query ^5.99` — server-state caching in the UI.
- `@react-oauth/google ^0.13.4` — Google OAuth token acquisition (`main.tsx` wraps app in `<GoogleOAuthProvider>` keyed by `VITE_GOOGLE_AUTH_CLIENT_ID`).
- `@mui/material ^9`, `@mui/icons-material ^9`, `@mui/x-date-pickers ^9` — UI component library.
- `wavesurfer.js ^7.12` + `@wavesurfer/react ^1.0.12` + `howler ^2.2.4` — audio waveform rendering and playback.
- `swagger-ui-react ^5.32` — embedded API docs viewer.
- `tsoa 7.0.0-alpha` — controller decorators → routes + OpenAPI generation in `frontend/api`.
- `google-auth-library ^10.6`, `jsonwebtoken ^9.0.3` — Google ID token decoding in `frontend/api/src/authentication.ts`.
- `express ^5.2`, `cors ^2.8.6`, `axios ^1.14`, `js-yaml ^4.1` — HTTP gateway deps.

## Configuration

**Environment:**
- Environment variables are the single source of configuration. `mise` loads `.env` via `_.file = ".env"` in `.mise.toml`. Local docker-compose services load `./local_dev/LOCAL.env`.
- Required at startup (ingestion): `AUDIO_STAGING_BUCKET`, `CONTINUOUS_PUBSUB_TOPIC_PATH`, `ALLOYDB_HOST`, `ALLOYDB_USER`, `ALLOYDB_DB`, `ALLOYDB_PASSWORD`, `BROADCASTIFY_USERNAME`, `BROADCASTIFY_PASSWORD`, `BROADCASTIFY_JWT_SECRET_ID`, `GOOGLE_CLOUD_PROJECT`. See `backend/pipeline/ingestion/settings.py::NormalizerSettings` and `backend/pipeline/storage/settings.py::AlloyDBSettings`.
- Required at startup (evaluation): `RULES_EVALUATION_RESULTS_TOPIC`, `TRANSCRIPTS_API_URL`, `RULES_API_URL` (optional — falls back to `StaticTextEvaluator`).
- Required at startup (notification): `APP_URL`, `NOTIFICATION_ENDPOINT`, `NOTIFICATION_ENDPOINT_API_KEY`, `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD` (GCP only), `REDIS_CERTIFICATE_PATH` (GCP only).
- Required at startup (echo): `AUDIO_STAGING_BUCKET`, `RAW_AUDIO_TOPIC`, AlloyDB env (same as ingestion).
- Environment detection: set `IS_GCP=true` in production Terraform to flip Cloud Logging, Cloud Monitoring, and Redis TLS on (`backend/pipeline/common/env.py::is_gcp_env`). Never override in local-dev.
- Frontend env: `VITE_GOOGLE_AUTH_CLIENT_ID` (UI), `ALLOWED_ORIGIN`, `TRANSCRIPTS_API_URL`, `RULES_API_URL`, `FEEDS_STORE_API_URL`, `PROJECT_ID`, `API_PUBLIC_URL` (API gateway — see `frontend/api/src/config.ts`).

**Build:**
- Root Python: `pyproject.toml` (build-system `setuptools>=70`, package name `radio-transcription`).
- Workspace member: `backend/pipeline/transcription/pyproject.toml` (build-system inherits; packages intentionally empty — Beam consumes the file tree directly).
- Frontend builds: per-package `tsconfig.json`. UI uses project references (`tsconfig.app.json`, `tsconfig.node.json`).
- Ruff config: inline `[tool.ruff]` in root `pyproject.toml`. `extend-exclude = ["model/colabs", "backend/pipeline/schema_types", "**/*_pb2.py", "**/*_pb2.pyi"]`.
- pytest config: `[tool.pytest.ini_options]` — silences `httplib2` and `pydub` deprecation warnings.

## Platform Requirements

**Development:**
- `mise` installed on host (installs uv, python 3.13.2, node 22.14.0, terraform 1.14.5 per `.tool-versions`).
- `docker` + `docker compose` for the local pipeline.
- `ffmpeg` system package (pydub dependency). CI installs via apt.
- `libomp5 libc++1 libc++abi1` — TenVAD system dependencies. Required in CI and all Beam-adjacent runtimes.
- `corepack enable` — activates yarn per `package.json` packageManager fields.

**Production:**
- GCP project (Cloud Run, Cloud Run Jobs, Cloud Functions Gen 2, Dataflow, GCE MIG, AlloyDB, Memorystore Redis, GCS, Pub/Sub, Eventarc, Secret Manager, Artifact Registry, Cloud Scheduler).
- VPC with private service access for AlloyDB; Direct VPC egress required for Cloud Run workloads that touch AlloyDB.
- Firebase Hosting for the UI (`frontend/transcription-ui/firebase.json`).
- Dataflow Flex Template storage bucket (for the transcription image and template JSON).

---

*Stack analysis: 2026-04-21*

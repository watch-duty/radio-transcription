# External Integrations

**Analysis Date:** 2026-06-19

## APIs & External Services

**Google Cloud Platform:**
- Cloud Pub/Sub - event bus for audio chunks, normalized audio, transcriptions, evaluation results, and notifications.
  - SDK/Client: `google-cloud-pubsub` in `backend/pipeline/common/pyproject.toml`; wrappers in `backend/pipeline/common/clients/pubsub_client.py`.
  - Auth: Application Default Credentials / service accounts; local emulator via `PUBSUB_EMULATOR_HOST` in `docker-compose.yml` and `local_dev/pubsub_init.py`.
- Cloud Storage / GCS - audio staging, canonical audio, Echo ingestion events, model data, and schema migration staging.
  - SDK/Client: `google-cloud-storage`, `gcloud-aio-storage`; usage in `backend/pipeline/common/storage/gcs_uploader.py`, `backend/pipeline/segmentation/storage.py`, `backend/pipeline/ingestion/collectors/echo/main.py`, and `model/src/common/gcs_utils.py`.
  - Auth: ADC/service accounts; local emulator via `STORAGE_EMULATOR_HOST` and `fake-gcs-server` in `docker-compose.yml`.
- Speech-to-Text v2 / Chirp 3 - synchronous transcription in `backend/pipeline/transcription/transcribers/chirp.py`.
  - SDK/Client: `google-cloud-speech>=2.37.0`.
  - Auth: ADC/service account; project/region/model configured by `PROJECT_ID` and `TRANSCRIBER_CONFIG`.
- Secret Manager - Broadcastify Calls JWT retrieval in `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py` and AlloyDB schema migration password access in `terraform/modules/alloydb/main.tf`.
  - SDK/Client: `google-cloud-secret-manager`.
  - Auth: `GOOGLE_CLOUD_PROJECT`, `BROADCASTIFY_JWT_SECRET_ID`, and service-account IAM.
- Cloud Identity API - admin group membership checks in `frontend/api/src/config.ts`.
  - SDK/Client: `google-auth-library` obtains OAuth token; `axios` calls `cloudidentity.googleapis.com`.
  - Auth: service account with Cloud Identity access; env `WORKSPACE_ADMIN_GROUP_EMAIL`.
- Cloud Logging, Cloud Trace, and Cloud Monitoring - structured logs, trace export, and custom metrics in `backend/pipeline/common/log_helper.py` and `backend/pipeline/common/tracing_utils.py`.
  - SDK/Client: `google-cloud-logging`, `opentelemetry-exporter-gcp-trace`, `opentelemetry-exporter-gcp-monitoring`, `google-cloud-monitoring`.
  - Auth: ADC/service accounts; requires `GOOGLE_CLOUD_PROJECT` in GCP.
- API Gateway / Cloud Endpoints metadata - frontend API auth reads `x-apigateway-api-userinfo` and `x-endpoint-api-userinfo` in `frontend/api/src/authentication.ts`; OpenAPI backend extension is generated from `frontend/api/tsoa.json`.
  - SDK/Client: tsoa/OpenAPI config plus Google gateway headers.
  - Auth: `google_id_token` security scheme in `frontend/api/tsoa.json`.

**Audio Source Providers:**
- Broadcastify Feeds and Broadcastify Calls - feed metadata, live call audio, archives, and Icecast streams.
  - SDK/Client: `aiohttp`, `requests`; runtime collectors in `backend/pipeline/ingestion/source_runtime_specs.py`, `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py`, and `backend/pipeline/ingestion/collectors/icecast/icecast_collector.py`; model scripts in `model/data_sources/broadcastify/bcfy_api.py`.
  - Auth: `BROADCASTIFY_USERNAME`, `BROADCASTIFY_PASSWORD`, `BROADCASTIFY_JWT_SECRET_ID`, `BROADCASTIFY_APP_ID`, `BROADCASTIFY_API_KEY_ID`, `BROADCASTIFY_API_TOKEN`, and Secret Manager JWT storage.
- OpenMHz - WebSocket/event stream and media downloads for radio call recordings.
  - SDK/Client: `curl-cffi` and custom WebSocket transport in `backend/pipeline/ingestion/collectors/openmhz/`.
  - Auth: Not detected; endpoint defaults are in `backend/pipeline/ingestion/source_runtime_specs.py`.
- Fire Notifications / TextMeFires - HTTP polling API and MP3 downloads.
  - SDK/Client: `aiohttp` runtime client in `backend/pipeline/ingestion/collectors/fire_notifications/client.py`; model script in `model/data_sources/fire_notifications/fn_api.py`.
  - Auth: Basic auth via `FIRE_NOTIFICATIONS_USER`, `FIRE_NOTIFICATIONS_PASSWORD`, `FIRE_NOTIFICATIONS_S3_BASE`, `FIRE_NOTIFICATIONS_URL_BASE`, and model-only `FN_AUTH_PASSWORD`.
- Watch Duty Echo recordings - GCS Eventarc ingestion at runtime and AWS S3 scanning for model data.
  - SDK/Client: Eventarc/GCS handler in `backend/pipeline/ingestion/collectors/echo/main.py`; `boto3` S3 scanner in `model/data_sources/echo/s3_file_scanner.py`.
  - Auth: GCP service account for runtime GCS; AWS credential chain for `boto3` model scanning.

**Model & Research Services:**
- Vertex AI Gemini - Gemini SFT tuning and batch inference in `model/src/common/gemini/vertex.py` and `model/src/gemini_sft/`.
  - SDK/Client: `google-genai>=2.3,<3`.
  - Auth: ADC/service account; requires explicit project/location arguments and GCS `gs://` input/output URIs.
- Hugging Face Hub / datasets / evaluate - optional model evaluation flows in `model/pyproject.toml` and notebooks under `model/colabs/`.
  - SDK/Client: `huggingface_hub`, `datasets`, `evaluate`, `torchaudio`.
  - Auth: `HF_TOKEN` read in `model/src/common/auth_utils.py`.
- NVIDIA NeMo - ASR evaluation container in `model/nemo_docker/Dockerfile` and requirements in `model/nemo_docker/requirements.txt`.
  - SDK/Client: NVIDIA NeMo Docker image `nvcr.io/nvidia/nemo:26.02.00`.
  - Auth: Container registry/GPU environment as required by NVIDIA tooling.

**Internal HTTP Services:**
- Backend FastAPI services - feeds, rules, transcripts, and audio-segments APIs.
  - SDK/Client: `requests` clients in `backend/pipeline/common/clients/`; services in `backend/services/**/main.py`.
  - Auth: Google OIDC tokens from `backend/pipeline/common/auth_client.py`; local mode bypass in `backend/pipeline/common/auth.py`.
- Frontend API proxy - Express/tsoa proxy for UI access and generated OpenAPI.
  - SDK/Client: `frontend/api/src/index.ts`, controllers under `frontend/api/src/**`.
  - Auth: Google OAuth login, refresh-token cookie, JWT/userinfo parsing, and optional admin group lookup.

## Data Storage

**Databases:**
- AlloyDB for PostgreSQL - feed, transcript, audio segment, rule, and ingestion state storage.
  - Connection: `ALLOYDB_HOST`, `ALLOYDB_PORT`, `ALLOYDB_USER`, `ALLOYDB_DB`, `ALLOYDB_PASSWORD`, pool/timeout env vars in `backend/pipeline/storage/settings.py`.
  - Client: `asyncpg` pools in `backend/pipeline/storage/connection.py`; `psycopg` sync connections in `backend/pipeline/storage/sync_connection.py`; Terraform in `terraform/modules/alloydb/`.
- Local Postgres - Docker Compose development database in `docker-compose.yml`.
  - Connection: local Compose env and service network; contents of `local_dev/LOCAL.env` are not read.
  - Client: same async/sync Postgres storage layer as production.

**File Storage:**
- GCS buckets for staging audio, canonical audio, Echo recordings, model artifacts, and schema SQL staging.
  - Files: `backend/pipeline/ingestion/settings.py`, `backend/pipeline/normalization/main.py`, `backend/pipeline/ingestion/collectors/echo/main.py`, `model/src/common/gcs_utils.py`, `terraform/modules/gcs_bucket/main.tf`.
- AWS S3 Echo recordings for model data scanning.
  - Files: `model/data_sources/echo/s3_file_scanner.py`, `model/data/README.md`.

**Caching:**
- Memorystore for Redis / Redis - notification deduplication and rules/service cache support.
  - Connection: `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`, `REDIS_CERTIFICATE_PATH` in `backend/pipeline/common/storage/redis_service.py`.
  - Client: `redis` package; infrastructure in `terraform/modules/memorystore_for_redis/main.tf`; local Redis in `docker-compose.yml`.

## Authentication & Identity

**Auth Provider:**
- Google OAuth for user login.
  - Implementation: UI wraps `GoogleOAuthProvider` in `frontend/transcription-ui/src/main.tsx`; API exchanges auth codes and refreshes tokens in `frontend/api/src/auth/authController.ts`.
  - Env: `VITE_GOOGLE_AUTH_CLIENT_ID`, `GOOGLE_AUTH_CLIENT_ID`, `GOOGLE_AUTH_CLIENT_SECRET`, `ALLOWED_ORIGIN`, `AUTH_BACKEND`.
- Google API Gateway / ID token auth for API requests.
  - Implementation: `frontend/api/src/authentication.ts` accepts gateway userinfo headers or bearer JWTs; `frontend/api/tsoa.json` defines `google_id_token`.
- Google service-to-service OIDC.
  - Implementation: `backend/pipeline/common/auth_client.py` fetches ID tokens for service audiences; `backend/pipeline/common/auth.py` verifies tokens for FastAPI services.
- Provider credentials.
  - Implementation: Broadcastify Basic auth/JWT and Secret Manager in ingestion collectors; Fire Notifications Basic auth in `backend/pipeline/ingestion/collectors/fire_notifications/`; AWS credentials via `boto3` default chain for model S3 scanner.

## Monitoring & Observability

**Error Tracking:**
- Dedicated third-party error tracking not detected.

**Logs:**
- Google Cloud Logging is configured through `backend/pipeline/common/log_helper.py` and package dependency `google-cloud-logging`.
- OpenTelemetry traces and metrics are exported to Cloud Trace and Cloud Monitoring from `backend/pipeline/common/tracing_utils.py`.
- GCE ASR evaluation VM installs Google Cloud Ops Agent for GPU metrics in `terraform/modules/asr_evaluation/main.tf`.
- CI and local stack logs are handled by GitHub Actions workflows and Docker Compose in `.github/workflows/` and `docker-compose.yml`.

## CI/CD & Deployment

**Hosting:**
- GCP Cloud Functions Gen2 for Functions Framework handlers via `terraform/modules/cloud_function/main.tf`.
- GCE regional Managed Instance Groups on Container-Optimized OS for long-running container workers via `terraform/modules/container_mig/main.tf`.
- Apache Beam/Dataflow-compatible segmentation container via `backend/pipeline/segmentation/Dockerfile`.
- Static UI deployment target is not defined in this repo; the UI build is produced by `frontend/transcription-ui/package.json`.

**CI Pipeline:**
- GitHub Actions CI in `.github/workflows/ci.yml` runs Python lint/type checks, model tests, frontend lint/build/test, Terraform checks, backend tests, Docker smoke tests, and AlloyDB SQL guards.
- GitHub Actions integration stack in `.github/workflows/integration-tests.yml` uses Docker Compose and `local_dev/LOCAL.env`.
- GHCR staging image bake is in `.github/workflows/bake-main.yml`.
- Private deployment dispatch is in `.github/workflows/trigger-deploy.yml`, using GitHub secrets for the target repo/PAT names only.

## Environment Configuration

**Required env vars:**
- Frontend API proxy: `ALLOWED_ORIGIN`, `TRANSCRIPTS_API_URL`, `RULES_API_URL`, `FEEDS_STORE_API_URL`, `AUDIO_SEGMENTS_API_URL`, `PROJECT_ID`, `API_PUBLIC_URL`, `GOOGLE_AUTH_CLIENT_ID`, `GOOGLE_AUTH_CLIENT_SECRET`, `AUTH_BACKEND`, `WORKSPACE_ADMIN_GROUP_EMAIL` (`frontend/api/src/config.ts`).
- UI: `VITE_GOOGLE_AUTH_CLIENT_ID`, `VITE_API_BASE_URL`, `VITE_PROXY_API_TARGET`, `VITE_PROXY_API_ORIGIN`, `VITE_AUTH_BACKEND` (`frontend/transcription-ui/vite.config.ts`, `frontend/transcription-ui/src/main.tsx`).
- AlloyDB/Postgres: `ALLOYDB_HOST`, `ALLOYDB_PORT`, `ALLOYDB_USER`, `ALLOYDB_DB`, `ALLOYDB_PASSWORD`, `ALLOYDB_POOL_MIN_SIZE`, `ALLOYDB_POOL_MAX_SIZE`, `ALLOYDB_COMMAND_TIMEOUT_SEC`, `ALLOYDB_CONNECT_TIMEOUT_SEC` (`backend/pipeline/storage/settings.py`).
- Ingestion: `AUDIO_STAGING_BUCKET`, `CONTINUOUS_PUBSUB_TOPIC_PATH`, `SEGMENTED_PUBSUB_TOPIC_PATH`, `GOOGLE_CLOUD_PROJECT`, `WORKER_ID`, source caps, retry, health, and watchdog env vars (`backend/pipeline/ingestion/settings.py`).
- Pipeline functions: `OUTPUT_TOPIC`, `AUDIO_CANONICAL_BUCKET`, `AUDIO_SEGMENTS_API_URL`, `TRANSCRIPTS_API_URL`, `RULES_API_URL`, `RULES_EVALUATION_RESULTS_TOPIC`, `RULES_CACHE_TTL_SECONDS`, `TRANSCRIBER_TYPE`, `TRANSCRIBER_CONFIG`, `LOCAL_ASR_API_URL` (`backend/pipeline/*/main.py`, `backend/pipeline/transcription/transcribers/local_api.py`).
- Notifications: `APP_URL`, `FEEDS_API_URL`, `NOTIFICATION_ENDPOINT`, `NOTIFICATION_ENDPOINT_API_KEY`, Redis env vars (`backend/pipeline/notification/send_notification.py`, `backend/pipeline/notification/request_handler.py`, `backend/pipeline/common/storage/redis_service.py`).
- Source providers: `BCFY_FEEDS_URL_BASE`, `BCFY_CALLS_URL_BASE`, `BROADCASTIFY_USERNAME`, `BROADCASTIFY_PASSWORD`, `BROADCASTIFY_JWT_SECRET_ID`, `MOCK_JWT_TOKEN`, `OPENMHZ_TRANSPORT`, `FIRE_NOTIFICATIONS_URL_BASE`, `FIRE_NOTIFICATIONS_S3_BASE`, `FIRE_NOTIFICATIONS_USER`, `FIRE_NOTIFICATIONS_PASSWORD` (`backend/pipeline/ingestion/`).
- Model/research: `HF_TOKEN`, `FN_AUTH_PASSWORD`, `BROADCASTIFY_APP_ID`, `BROADCASTIFY_API_KEY_ID`, `BROADCASTIFY_API_TOKEN` (`model/src/common/auth_utils.py`, `model/data_sources/`).

**Secrets location:**
- Local secret-bearing env files are present but not read: `frontend/api/.env.example`, `frontend/transcription-ui/.env.example`, `frontend/transcription-ui/.env.local-dev.example`, and `local_dev/LOCAL.env`.
- Production Broadcastify JWT is read from GCP Secret Manager by `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py`.
- AlloyDB schema migration reads the database password from GCP Secret Manager in `terraform/modules/alloydb/main.tf`.
- GitHub deployment secrets are referenced by name in `.github/workflows/trigger-deploy.yml`.

## Webhooks & Callbacks

**Incoming:**
- Pub/Sub push CloudEvents handled by Functions Framework entry points: `backend/pipeline/normalization/main.py`, `backend/pipeline/transcription/main.py`, `backend/pipeline/evaluation/main.py`, `backend/pipeline/notification/send_notification.py`, and Terraform Pub/Sub trigger support in `terraform/modules/cloud_function/main.tf`.
- Eventarc/GCS `OBJECT_FINALIZE` events for Echo recordings handled by `backend/pipeline/ingestion/collectors/echo/main.py`.
- HTTP API requests to FastAPI services in `backend/services/**/main.py` and Express/tsoa proxy routes in `frontend/api/src/**`.
- Local dev emulated callbacks from Pub/Sub and GCS setup scripts in `local_dev/pubsub_init.py` and `local_dev/gcs_init.py`.

**Outgoing:**
- Pub/Sub publishes between pipeline stages from `backend/pipeline/common/clients/pubsub_client.py`, `backend/pipeline/transcription/processor.py`, `backend/pipeline/normalization/processor.py`, and ingestion helpers.
- Notification POSTs to `NOTIFICATION_ENDPOINT` with `X-Api-Key` in `backend/pipeline/notification/request_handler.py`.
- Internal service-to-service HTTP calls through clients in `backend/pipeline/common/clients/` and frontend proxy controllers in `frontend/api/src/**`.
- External provider HTTP/WebSocket calls to Broadcastify, OpenMHz, Fire Notifications, Echo S3, Google Speech-to-Text, Vertex AI Gemini, Cloud Identity, and GCS are made from the files listed in the sections above.

---

*Integration audit: 2026-06-19*

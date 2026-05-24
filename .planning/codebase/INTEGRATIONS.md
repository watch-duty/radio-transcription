# External Integrations

**Analysis Date:** 2026-05-24

## APIs & External Services

**Google Cloud Platform:**
- Pub/Sub - pipeline event bus for raw audio chunks, normalized audio claim checks, transcribed audio, evaluation results, DLQs, and local emulator topics.
  - SDK/Client: `google-cloud-pubsub>=2.35.0`, Apache Beam Pub/Sub IO, and `google.cloud.pubsub_v1` in `pyproject.toml`, `backend/pipeline/common/clients/pubsub_client.py`, `backend/pipeline/common/gcp_helper.py`, `backend/pipeline/normalization/orchestration.py`, and `local_dev/pubsub_init.py`.
  - Auth: Application Default Credentials, service accounts, `GOOGLE_CLOUD_PROJECT`, `PROJECT_ID`, `PUBSUB_EMULATOR_HOST` for local emulator use.
- Cloud Storage / GCS - staging, canonical/playback audio, Echo source events, schema SQL staging, model manifests, ASR results, and browser audio playback.
  - SDK/Client: `google-cloud-storage`, `gcloud-aio-storage`, Beam GCS support in `pyproject.toml`, `backend/pipeline/common/clients/gcs_client.py`, `backend/pipeline/common/gcp_helper.py`, `backend/pipeline/common/storage/gcs_uploader.py`, `backend/pipeline/ingestion/collectors/echo/main.py`, and `model/colabs/common/gcs_utils.py`.
  - Auth: Application Default Credentials, service accounts, `AUDIO_STAGING_BUCKET`, `RAW_AUDIO_TOPIC`, `DEV_RECORDINGS_BUCKET`, `canonical_audio_bucket` Beam option, `GOOGLE_APPLICATION_CREDENTIALS` in `asr-eval-docker-compose.yml`.
- Speech-to-Text V2 / Chirp - production transcription engine for normalized GCS audio references.
  - SDK/Client: `google-cloud-speech>=2.37.0` and `SpeechClient` in `backend/pipeline/transcription/pyproject.toml` and `backend/pipeline/transcription/transcribers/chirp.py`.
  - Auth: Application Default Credentials, `PROJECT_ID`, `TRANSCRIBER_TYPE`, `TRANSCRIBER_CONFIG`.
- Cloud Functions / Cloud Run Functions Framework - Pub/Sub and Eventarc CloudEvent handlers.
  - SDK/Client: `functions-framework>=3.10.1`, `@google-cloud/functions-framework^5.0.2`, Terraform `google_cloudfunctions2_function` in `pyproject.toml`, `frontend/api/package.json`, `backend/pipeline/*/Dockerfile`, `frontend/api/Dockerfile`, and `terraform/modules/cloud_function/main.tf`.
  - Auth: service accounts and deployment-supplied environment variables.
- Dataflow / Apache Beam - streaming normalization pipeline reading from Pub/Sub and writing claim-check messages and DLQ messages.
  - SDK/Client: `apache-beam[gcp]>=2.73.0` in `backend/pipeline/normalization/pyproject.toml`, `backend/pipeline/normalization/Dockerfile`, `backend/pipeline/normalization/main.py`, and `backend/pipeline/normalization/orchestration.py`.
  - Auth: Dataflow worker service account / Application Default Credentials.
- Cloud Logging, Cloud Trace, and Cloud Monitoring - logging, OpenTelemetry trace export, and custom metrics.
  - SDK/Client: `google-cloud-logging`, `opentelemetry-exporter-gcp-trace`, `google-cloud-monitoring` in `pyproject.toml`, `backend/pipeline/common/logging.py`, `backend/pipeline/common/tracing_utils.py`, `backend/pipeline/common/clients/monitoring_client.py`, and `backend/pipeline/ingestion/quarantine_telemetry.py`.
  - Auth: Application Default Credentials, `GOOGLE_CLOUD_PROJECT`.
- Secret Manager - secret retrieval for Broadcastify Calls JWT and AlloyDB schema migration password injection.
  - SDK/Client: `google-cloud-secret-manager` in `pyproject.toml`, `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py`, and `terraform/modules/alloydb/main.tf`.
  - Auth: service account IAM grants in `terraform/modules/alloydb/main.tf`, `GOOGLE_CLOUD_PROJECT`, `BROADCASTIFY_JWT_SECRET_ID`.
- API Gateway / OpenAPI - generated frontend API spec with Google backend extensions and docs discovery.
  - SDK/Client: tsoa config in `frontend/api/tsoa.json`; Google API Gateway REST call in `frontend/api/src/docs/docsController.ts`.
  - Auth: `PROJECT_ID`, `API_PUBLIC_URL`, API Gateway verified `google_id_token`.

**Radio/audio source services:**
- Broadcastify live feeds - Icecast stream capture via ffmpeg using Basic Auth.
  - SDK/Client: ffmpeg subprocess in `backend/pipeline/ingestion/collectors/icecast/icecast_collector.py`.
  - Auth: `BROADCASTIFY_USERNAME`, `BROADCASTIFY_PASSWORD`; optional `BCFY_FEEDS_URL_BASE` in `backend/pipeline/ingestion/router.py`.
- Broadcastify Calls API - polling for call metadata and downloading audio URLs.
  - SDK/Client: `aiohttp` collector in `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py`; model utility uses `requests` in `model/data_sources/broadcastify/bcfy_api.py`.
  - Auth: Secret Manager JWT via `BROADCASTIFY_JWT_SECRET_ID` and `GOOGLE_CLOUD_PROJECT`; model utilities use `BROADCASTIFY_APP_ID`, `BROADCASTIFY_API_KEY_ID`, and `BROADCASTIFY_API_TOKEN`.
- OpenMHz - Socket.IO/WebSocket events and call audio download.
  - SDK/Client: `curl-cffi` AsyncSession and WebSocket transport in `backend/pipeline/ingestion/collectors/openmhz/collector.py` and `backend/pipeline/ingestion/collectors/openmhz/_ws_transport.py`.
  - Auth: no explicit credential in source; optional `OPENMHZ_TRANSPORT`.
- Fire Notifications - HTTP polling for file lists and audio download.
  - SDK/Client: `curl-cffi` collector in `backend/pipeline/ingestion/collectors/fire_notifications/collector.py`; model utility uses `requests` in `model/data_sources/fire_notifications/fn_api.py`.
  - Auth: production collector requires `FIRE_NOTIFICATIONS_URL_BASE` and `FIRE_NOTIFICATIONS_S3_BASE`; model utility uses `FN_AUTH_PASSWORD`.
- Echo recordings - GCS/Eventarc production path and AWS S3 model-data scanning path.
  - SDK/Client: GCS Eventarc handler in `backend/pipeline/ingestion/collectors/echo/main.py`; AWS S3 scanner uses `boto3` in `model/data_sources/echo/s3_file_scanner.py`.
  - Auth: Application Default Credentials for GCS; AWS credential chain for `boto3`; `AUDIO_STAGING_BUCKET`, `RAW_AUDIO_TOPIC`, and optional `DEV_RECORDINGS_BUCKET`.

**Application service APIs:**
- Transcripts, Rules, and Feeds FastAPI services - internal management APIs backed by AlloyDB.
  - SDK/Client: FastAPI services in `backend/services/transcripts/main.py`, `backend/services/rules/main.py`, and `backend/services/feeds/main.py`; HTTP clients in `backend/pipeline/common/clients/transcripts_client.py`, `backend/pipeline/evaluation/rules_evaluation/evaluator.py`, and `backend/pipeline/notification/send_notification.py`.
  - Auth: Google OIDC bearer tokens from `backend/pipeline/common/auth.py`; URLs from `TRANSCRIPTS_API_URL`, `RULES_API_URL`, and `FEEDS_API_URL`.
- Frontend proxy API - Node/Express/tsoa API facade for the browser UI.
  - SDK/Client: `express`, `tsoa`, `google-auth-library`, and `axios` in `frontend/api/package.json`, `frontend/api/src/index.ts`, and `frontend/api/src/*Controller.ts`.
  - Auth: `GOOGLE_AUTH_CLIENT_ID`, `GOOGLE_AUTH_CLIENT_SECRET`, `ALLOWED_ORIGIN`, refresh-token cookie in `frontend/api/src/auth/authController.ts`.

**Notifications:**
- External notification endpoint - POSTs alert payloads to a configured downstream endpoint with an API key header.
  - SDK/Client: `urllib3.PoolManager` with retry in `backend/pipeline/notification/request_handler.py`.
  - Auth: `NOTIFICATION_ENDPOINT_API_KEY`; URL from `NOTIFICATION_ENDPOINT`.

**Model and research services:**
- Hugging Face Hub and Datasets - model downloads, auth, and public ASR dataset evaluation.
  - SDK/Client: `huggingface_hub`, `transformers`, `datasets`, and `evaluate` in `model/pyproject.toml`, `model/notebook_docker/requirements.txt`, `model/colabs/common/auth_utils.py`, `model/colabs/common/inference_hf.py`, and `model/colabs/common/public_dataset_evaluation.py`.
  - Auth: `HF_TOKEN` from environment, Google Colab Secrets, or interactive prompt.
- Google GenAI / Gemini and Google Speech experiments - notebook-based ASR experiments.
  - SDK/Client: `google-genai`, `google-cloud-speech`, and notebook references in `model/notebook_docker/requirements.txt`, `model/nemo_docker/requirements.txt`, `model/colabs/gemini_transcribe_audio.ipynb`, `model/colabs/gemini_create_inference_manifest.ipynb`, and `model/colabs/chirp_transcribe_audio.ipynb`.
  - Auth: notebook/runtime Google credentials or provider-specific notebook setup.
- NVIDIA NeMo / PyTorch GPU evaluation - ASR model evaluation containers.
  - SDK/Client: `nvcr.io/nvidia/nemo:26.02.00` in `model/nemo_docker/Dockerfile`, PyTorch CUDA image in `model/notebook_docker/Dockerfile`, and Terraform GPU VM in `terraform/modules/asr_evaluation/main.tf`.
  - Auth: container registry access as configured by Docker/GCP environment; GCS access via ADC for data.

## Data Storage

**Databases:**
- AlloyDB for PostgreSQL - durable storage for feeds, feed properties, rules, transcripts, audio segment annotations, lease/bookmark state, and schema migrations.
  - Connection: `ALLOYDB_HOST`, `ALLOYDB_PORT`, `ALLOYDB_USER`, `ALLOYDB_DB`, `ALLOYDB_PASSWORD`, pool settings from `backend/pipeline/storage/settings.py`.
  - Client: `asyncpg` in `backend/pipeline/storage/connection.py`; `psycopg` in `backend/pipeline/storage/sync_connection.py`; Terraform in `terraform/modules/alloydb/main.tf`; SQL in `terraform/modules/alloydb/sql/ingestion/*.sql`.
- Local Postgres - local/dev and CI backing database.
  - Connection: Docker Compose service in `docker-compose.yml`; CI service in `.github/workflows/ci.yml`.
  - Client: same `asyncpg` / `psycopg` storage layer in `backend/pipeline/storage/`.

**File Storage:**
- Google Cloud Storage - raw/staged/canonical/playback audio, SQL migration staging, model manifests, and inference result manifests.
  - Paths/clients: `backend/pipeline/common/gcp_helper.py`, `backend/pipeline/common/storage/gcs_uploader.py`, `backend/pipeline/ingestion/collectors/echo/main.py`, `model/colabs/common/gcs_utils.py`, `terraform/modules/gcs_bucket/main.tf`, and `terraform/modules/alloydb/main.tf`.
- AWS S3 - Echo archive scanning for model data-source preparation.
  - Paths/clients: `boto3` scanner in `model/data_sources/echo/s3_file_scanner.py`.
- Local fake GCS server - local Docker Compose emulator service in `docker-compose.yml`.

**Caching:**
- Redis / Memorystore - notification deduplication cache.
  - Connection: `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`, `REDIS_CERTIFICATE_PATH` in `backend/pipeline/common/storage/redis_service.py`.
  - Client: `redis>=7.3.0` in `pyproject.toml`; Terraform Memorystore module in `terraform/modules/memorystore_for_redis/main.tf`; local Redis service in `docker-compose.yml`.
- In-process TTL cache - rules fetch cache for remote evaluator.
  - Client: `cachetools` in `pyproject.toml` and `backend/pipeline/evaluation/rules_evaluation/evaluator.py`.

## Authentication & Identity

**Auth Provider:**
- Google OAuth for browser users.
  - Implementation: React UI uses `@react-oauth/google` in `frontend/transcription-ui/src/main.tsx` and `frontend/transcription-ui/src/components/Login.tsx`; Node proxy exchanges codes and refresh tokens via `google-auth-library` in `frontend/api/src/auth/authController.ts`.
  - Env: `VITE_GOOGLE_AUTH_CLIENT_ID`, `GOOGLE_AUTH_CLIENT_ID`, `GOOGLE_AUTH_CLIENT_SECRET`, `ALLOWED_ORIGIN`.
- Google OIDC / service-to-service auth.
  - Implementation: FastAPI services depend on `verify_oidc_token` in `backend/pipeline/common/auth.py`; backend clients fetch metadata-server ID tokens with `get_id_token` in `backend/pipeline/common/auth.py`.
  - Env: `IS_GCP`, service account credentials from Google Cloud runtime.
- API Gateway verified JWTs.
  - Implementation: tsoa security definition in `frontend/api/tsoa.json`; proxy API decodes already-verified JWTs in `frontend/api/src/authentication.ts`.
- External provider credentials.
  - Implementation: Broadcastify Basic Auth in `backend/pipeline/ingestion/collectors/icecast/icecast_collector.py`, Broadcastify JWT from Secret Manager in `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py`, notification API key in `backend/pipeline/notification/request_handler.py`, Hugging Face token in `model/colabs/common/auth_utils.py`, and Fire Notifications password in `model/data_sources/fire_notifications/fn_api.py`.

## Monitoring & Observability

**Error Tracking:**
- None detected as a separate hosted error-tracking product such as Sentry; errors are logged through Python/Node logging and Google Cloud Logging clients in `backend/pipeline/common/logging.py`, `frontend/api/src/index.ts`, and service modules.

**Logs:**
- Cloud Logging is initialized from `backend/pipeline/common/logging.py` and used across `backend/pipeline/`.
- Structured SLO/event logs are emitted by ingestion code in `backend/pipeline/ingestion/slo_contract.py`, `backend/pipeline/ingestion/normalizer_runtime.py`, and collectors under `backend/pipeline/ingestion/collectors/`.
- OpenTelemetry trace propagation and export are in `backend/pipeline/common/tracing_utils.py`; traceparent attributes are propagated through Pub/Sub in `backend/pipeline/common/gcp_helper.py`, `backend/pipeline/transcription/processor.py`, `backend/pipeline/evaluation/processor.py`, and `backend/pipeline/notification/send_notification.py`.
- Custom Cloud Monitoring metrics are emitted through `backend/pipeline/common/clients/monitoring_client.py` and `backend/pipeline/ingestion/quarantine_telemetry.py`.

## CI/CD & Deployment

**Hosting:**
- Browser UI is built with Vite and configured for Firebase Hosting in `frontend/transcription-ui/firebase.json`.
- Frontend proxy API is packaged as a Node Functions Framework container in `frontend/api/Dockerfile`.
- Python event handlers and services are packaged by Dockerfiles under `backend/pipeline/` and `backend/services/`.
- Stream capturer workers run as Docker containers on GCE Managed Instance Groups using `terraform/modules/container_mig/main.tf` and `terraform/modules/container_mig/cloud_config.yaml.tftpl`.
- Normalization runs as an Apache Beam/Dataflow Flex Template image from `backend/pipeline/normalization/Dockerfile`.
- Terraform modules define GCP infrastructure under `terraform/modules/`; deployment is handled by a private deployment repository triggered from `.github/workflows/trigger-deploy.yml`.

**CI Pipeline:**
- GitHub Actions CI in `.github/workflows/ci.yml` runs Python lint/type checks, frontend lint/build/type/spec checks, Terraform fmt/validate, unit tests, Docker smoke builds, and AlloyDB SQL guards.
- Integration workflow in `.github/workflows/integration-tests.yml` runs component and E2E jobs through Docker Compose, but this mapping did not execute them.
- Private deployment trigger in `.github/workflows/trigger-deploy.yml` calls GitHub workflows in a private repository using GitHub Secrets names `PRIVATE_REPO_PAT` and `PRIVATE_REPO_TARGET`.

## Environment Configuration

**Required env vars:**
- AlloyDB/runtime storage: `ALLOYDB_HOST`, `ALLOYDB_PORT`, `ALLOYDB_USER`, `ALLOYDB_DB`, `ALLOYDB_PASSWORD`, `ALLOYDB_POOL_MIN_SIZE`, `ALLOYDB_POOL_MAX_SIZE`, `ALLOYDB_COMMAND_TIMEOUT_SEC`, `ALLOYDB_CONNECT_TIMEOUT_SEC` from `backend/pipeline/storage/settings.py`.
- Ingestion worker: `AUDIO_STAGING_BUCKET`, `CONTINUOUS_PUBSUB_TOPIC_PATH`, `SEGMENTED_PUBSUB_TOPIC_PATH`, `GOOGLE_CLOUD_PROJECT`, `WORKER_ID`, `MAX_FEEDS_PER_WORKER`, `LEASE_POLL_INTERVAL_SEC`, `HEARTBEAT_INTERVAL_SEC`, `FEED_FAILURE_THRESHOLD`, retry knobs, health-check knobs, and RSS watchdog knobs from `backend/pipeline/ingestion/settings.py`.
- Source collectors: `BROADCASTIFY_USERNAME`, `BROADCASTIFY_PASSWORD`, `BROADCASTIFY_JWT_SECRET_ID`, `BCFY_FEEDS_URL_BASE`, `BCFY_CALLS_URL_BASE`, `OPENMHZ_TRANSPORT`, `FIRE_NOTIFICATIONS_URL_BASE`, `FIRE_NOTIFICATIONS_S3_BASE`, `MOCK_JWT_TOKEN`, `ICECAST_SOURCE_FEED_ID`, and `ICECAST_LOCAL_OUTPUT_DIR` from `backend/pipeline/ingestion/collectors/` and `backend/pipeline/ingestion/router.py`.
- Transcription/evaluation/notification: `PROJECT_ID`, `OUTPUT_TOPIC`, `TRANSCRIBER_TYPE`, `TRANSCRIBER_CONFIG`, `RULES_EVALUATION_RESULTS_TOPIC`, `TRANSCRIPTS_API_URL`, `RULES_API_URL`, `APP_URL`, `FEEDS_API_URL`, `NOTIFICATION_ENDPOINT`, `NOTIFICATION_ENDPOINT_API_KEY` from `backend/pipeline/transcription/main.py`, `backend/pipeline/evaluation/main.py`, `backend/pipeline/notification/send_notification.py`, and `backend/pipeline/notification/request_handler.py`.
- Redis: `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`, `REDIS_CERTIFICATE_PATH` from `backend/pipeline/common/storage/redis_service.py`.
- Frontend API/UI: `ALLOWED_ORIGIN`, `FEEDS_STORE_API_URL`, `API_PUBLIC_URL`, `GOOGLE_AUTH_CLIENT_ID`, `GOOGLE_AUTH_CLIENT_SECRET`, `VITE_GOOGLE_AUTH_CLIENT_ID`, `VITE_API_BASE_URL`, and `VITE_ALERT_ICON_SYMBOL_NAME` from `frontend/api/src/config.ts`, `frontend/transcription-ui/src/main.tsx`, and `frontend/transcription-ui/src/service/*.ts`.
- Model/evaluation utilities: `HF_TOKEN`, `BROADCASTIFY_APP_ID`, `BROADCASTIFY_API_KEY_ID`, `BROADCASTIFY_API_TOKEN`, `FN_AUTH_PASSWORD`, and GCP project/bucket notebook variables from `model/colabs/common/auth_utils.py`, `model/data_sources/broadcastify/bcfy_api.py`, and `model/data_sources/fire_notifications/fn_api.py`.

**Secrets location:**
- GCP Secret Manager stores Broadcastify JWT material for runtime access through `BROADCASTIFY_JWT_SECRET_ID` in `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py`.
- GCP Secret Manager stores AlloyDB schema migration password references through `password_secret_id` in `terraform/modules/alloydb/main.tf`.
- GitHub Actions deployment secrets are referenced by name in `.github/workflows/trigger-deploy.yml`.
- Local developer env files are loaded by `.mise.toml` and package scripts; `.env`-style files and `local_dev/LOCAL.env` must be treated as secret-bearing configuration. Env example files are present at `frontend/api/.env.example` and `frontend/transcription-ui/.env.example`.

## Webhooks & Callbacks

**Incoming:**
- Pub/Sub CloudEvents: `transcribe_claim_check` in `backend/pipeline/transcription/main.py`, `evaluate_transcribed_audio_segment` in `backend/pipeline/evaluation/main.py`, and `send_notification` in `backend/pipeline/notification/send_notification.py`.
- GCS/Eventarc CloudEvents: `handle_notification` for Echo object-finalize events in `backend/pipeline/ingestion/collectors/echo/main.py`.
- HTTP APIs: FastAPI endpoints in `backend/services/transcripts/main.py`, `backend/services/rules/main.py`, and `backend/services/feeds/main.py`; Express/tsoa proxy endpoints in `frontend/api/src/*Controller.ts`.
- Health checks: ingestion worker `/healthz` server in `backend/pipeline/ingestion/health_server.py`; MIG health aggregation in `terraform/modules/container_mig/cloud_config.yaml.tftpl`.
- Terraform Cloud Function module supports Pub/Sub-triggered or HTTP functions via `terraform/modules/cloud_function/main.tf`.

**Outgoing:**
- Pub/Sub publishes: raw audio chunks in `backend/pipeline/common/gcp_helper.py`, normalized claims and DLQ messages in `backend/pipeline/normalization/orchestration.py`, transcribed audio in `backend/pipeline/transcription/processor.py`, and evaluation alerts in `backend/pipeline/evaluation/processor.py`.
- HTTP service calls: rules fetches in `backend/pipeline/evaluation/rules_evaluation/evaluator.py`, transcript writes in `backend/pipeline/common/clients/transcripts_client.py`, feed tag fetches in `backend/pipeline/notification/send_notification.py`, and proxy forwarding in `frontend/api/src/*Controller.ts`.
- External notification POSTs: `backend/pipeline/notification/request_handler.py`.
- GCS uploads/downloads: `backend/pipeline/common/gcp_helper.py`, `backend/pipeline/common/storage/gcs_uploader.py`, `backend/pipeline/ingestion/collectors/echo/main.py`, and `model/colabs/common/gcs_utils.py`.
- External source polling/downloads: Broadcastify, OpenMHz, Fire Notifications, and Echo S3 clients under `backend/pipeline/ingestion/collectors/` and `model/data_sources/`.
- Google Speech recognizer calls: `backend/pipeline/transcription/transcribers/chirp.py`.

---

*Integration audit: 2026-05-24*

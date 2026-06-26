# External Integrations

**Analysis Date:** 2026-06-26

## APIs & External Services

**Google Cloud Messaging:**
- Google Cloud Pub/Sub - Pipeline event bus for continuous audio chunks, segmented audio, normalized audio, transcribed audio, evaluated audio, and notifications.
  - SDK/Client: `google-cloud-pubsub` in `backend/pipeline/common/clients/pubsub_client.py`, `backend/pipeline/normalization/main.py`, `backend/pipeline/transcription/main.py`, and `backend/pipeline/evaluation/main.py`.
  - Auth: Application Default Credentials or runtime service account; topic env vars include `CONTINUOUS_PUBSUB_TOPIC_PATH`, `SEGMENTED_PUBSUB_TOPIC_PATH`, `OUTPUT_TOPIC`, and `RULES_EVALUATION_RESULTS_TOPIC`.
  - Contracts: `protos/continuous_audio.proto`, `protos/segmented_audio.proto`, `protos/normalized_audio.proto`, `protos/transcribed_audio.proto`, `protos/evaluated_transcribed_audio.proto`, and `protos/alert_notification.proto`.
  - Infrastructure: Pub/Sub-triggered Cloud Functions are supported by `terraform/modules/cloud_function/main.tf`.

**Google Cloud Storage:**
- GCS audio/object storage - Staging audio, canonical audio, playback audio, Echo object events, local Whisper URI downloads, and model/SFT run artifacts.
  - SDK/Client: `google-cloud-storage` and `gcloud-aio-storage` in `backend/pipeline/common/clients/gcs_client.py`, `backend/pipeline/common/storage/gcs_uploader.py`, `backend/pipeline/normalization/main.py`, `backend/pipeline/ingestion/collectors/echo/main.py`, `backend/services/local-whisper-api/main.py`, `model/src/common/gcs_utils.py`, and `model/src/gemini_sft/`.
  - Auth: Application Default Credentials or runtime service account.
  - Config: `AUDIO_STAGING_BUCKET`, `AUDIO_CANONICAL_BUCKET`, `DEV_RECORDINGS_BUCKET`, and `STORAGE_EMULATOR_HOST`.
  - Infrastructure: `terraform/modules/gcs_bucket/main.tf`; local emulator in `docker-compose.yml`.

**Google Cloud Speech and Vertex AI:**
- Google Cloud Speech-to-Text V2 Chirp v3 - Primary configurable ASR transcriber.
  - SDK/Client: `google-cloud-speech` in `backend/pipeline/transcription/transcribers/chirp.py`.
  - Auth: Application Default Credentials or runtime service account.
  - Config: `PROJECT_ID`, `TRANSCRIBER_TYPE`, and `TRANSCRIBER_CONFIG`.
- Vertex AI Gemini / Google GenAI - Gemini transcriber, Gemini SFT tuning, and batch inference.
  - SDK/Client: `google-genai` in `backend/pipeline/transcription/transcribers/gemini.py` and `model/src/common/gemini/vertex.py`.
  - Auth: Application Default Credentials or runtime service account.
  - Config: `TRANSCRIBER_TYPE=gemini`, `TRANSCRIBER_CONFIG`, and operator TOML for SFT as shaped by `model/scripts/sft/run_config.example.toml` and parsed by `model/src/gemini_sft/config.py`.

**Google Identity and Admin Authorization:**
- Google OAuth 2.0 - Browser login flow and refresh-token cookie handling.
  - SDK/Client: `@react-oauth/google` in `frontend/transcription-ui/src/main.tsx`; `google-auth-library` `OAuth2Client` in `frontend/api/src/auth/authController.ts`.
  - Auth: `GOOGLE_AUTH_CLIENT_ID`, `GOOGLE_AUTH_CLIENT_SECRET`, and `VITE_GOOGLE_AUTH_CLIENT_ID`.
- Google OIDC / service-to-service identity - Internal FastAPI auth and BFF-to-service calls.
  - SDK/Client: `google-auth` in `backend/pipeline/common/auth.py`, `backend/pipeline/common/auth_client.py`, `backend/pipeline/common/clients/audio_segments_client.py`, and `backend/pipeline/common/clients/feeds_client.py`; `jose` in `frontend/api/src/authentication.ts`.
  - Auth: runtime service account metadata server, bearer ID tokens, API Gateway `x-apigateway-api-userinfo`, and Cloud Endpoints `x-endpoint-api-userinfo`.
- Cloud Identity API - Google Workspace admin group membership check for UI/API authorization.
  - SDK/Client: `axios` plus `google-auth-library` in `frontend/api/src/config.ts`.
  - Auth: backend service account access token.
  - Config: `WORKSPACE_ADMIN_GROUP_EMAIL` and `PROJECT_ID`.
  - Setup notes: `frontend/api/README.md`.

**Google Secret Manager:**
- Broadcastify Calls JWT storage - Reads the shared Broadcastify JWT from Secret Manager.
  - SDK/Client: `google-cloud-secret-manager` in `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py`.
  - Auth: runtime service account with Secret Manager access.
  - Config: `GOOGLE_CLOUD_PROJECT` and `BROADCASTIFY_JWT_SECRET_ID`; tests may use `MOCK_JWT_TOKEN`.

**Google Observability:**
- Cloud Logging - Structured backend logs.
  - SDK/Client: `google-cloud-logging` configured through `backend/pipeline/common/log_helper.py` and imported by backend package manifests.
  - Auth: Application Default Credentials or runtime service account.
- Cloud Trace - OpenTelemetry spans exported to Google Cloud Trace.
  - SDK/Client: `opentelemetry-exporter-gcp-trace` in `backend/pipeline/common/tracing_utils.py`.
  - Auth: `GOOGLE_CLOUD_PROJECT`; local console export can use `OTEL_TRACES_EXPORTER`.
- Cloud Monitoring - Custom metrics for ingestion and quarantine telemetry.
  - SDK/Client: `google-cloud-monitoring` in `backend/pipeline/common/clients/monitoring_client.py`.
  - Auth: `GOOGLE_CLOUD_PROJECT`.

**Upstream Audio Sources:**
- Broadcastify Icecast partner streams - Continuous stream capture through ffmpeg and Basic Auth.
  - SDK/Client: `aiohttp` probes and `ffmpeg` subprocesses in `backend/pipeline/ingestion/collectors/icecast/icecast_collector.py`.
  - Auth: `BROADCASTIFY_USERNAME` and `BROADCASTIFY_PASSWORD`.
  - Config: `BCFY_FEEDS_URL_BASE` with default source metadata in `backend/pipeline/ingestion/source_runtime_specs.py`.
- Broadcastify Calls API - Polls live call metadata and downloads media files.
  - SDK/Client: `aiohttp` in `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py`.
  - Auth: bearer JWT fetched from Secret Manager using `BROADCASTIFY_JWT_SECRET_ID`.
  - Config: `BCFY_CALLS_URL_BASE` with default source metadata in `backend/pipeline/ingestion/source_runtime_specs.py`.
- Broadcastify model data API - Offline model data source scripts fetch feed and archive metadata.
  - SDK/Client: `requests` in `model/data_sources/broadcastify/bcfy_api.py`.
  - Auth: HMAC/JWT-style token from `BROADCASTIFY_APP_ID`, `BROADCASTIFY_API_KEY_ID`, and `BROADCASTIFY_API_TOKEN`.
- OpenMHz - WebSocket event stream and hosted media downloads.
  - SDK/Client: `curl-cffi` `AsyncSession` and websocket transport in `backend/pipeline/ingestion/collectors/openmhz/collector.py` and `backend/pipeline/ingestion/collectors/openmhz/_ws_transport.py`.
  - Auth: Not detected in application code.
  - Config: `OPENMHZ_TRANSPORT`; default API/media metadata in `backend/pipeline/ingestion/source_runtime_specs.py`.
- Fire Notifications - Polling API plus S3-style MP3 downloads.
  - SDK/Client: `aiohttp` in `backend/pipeline/ingestion/collectors/fire_notifications/client.py` and `backend/pipeline/ingestion/collectors/fire_notifications/collector.py`.
  - Auth: Basic Auth via `FIRE_NOTIFICATIONS_USER` and `FIRE_NOTIFICATIONS_PASSWORD`.
  - Config: `FIRE_NOTIFICATIONS_URL_BASE` and `FIRE_NOTIFICATIONS_S3_BASE`.
- Fire Notifications model data API - Offline data source scripts list Fire Notifications archive files.
  - SDK/Client: `requests` in `model/data_sources/fire_notifications/fn_api.py`.
  - Auth: `FN_AUTH_PASSWORD`.

**Notification Destinations:**
- External alert notification endpoint - Sends evaluated alert payloads as JSON with an API key header.
  - SDK/Client: `urllib3.PoolManager` in `backend/pipeline/notification/request_handler.py`.
  - Auth: `NOTIFICATION_ENDPOINT_API_KEY`.
  - Config: `NOTIFICATION_ENDPOINT` and `APP_URL`.

**Model and ML Ecosystem:**
- Hugging Face - Optional datasets/model access for ASR evaluation and local model workflows.
  - SDK/Client: `huggingface_hub`, `datasets`, `transformers`, and `faster-whisper` in `model/pyproject.toml`, `model/notebook_docker/requirements.txt`, and `backend/services/local-whisper-api/pyproject.toml`.
  - Auth: `HF_TOKEN` in `model/src/common/auth_utils.py`.
- NVIDIA NeMo - Heavy ASR evaluation runtime.
  - SDK/Client: NeMo image and sparse checkout in `model/nemo_docker/Dockerfile`; extra packages in `model/nemo_docker/requirements.txt`.
  - Auth: Not detected.

## Data Storage

**Databases:**
- Google AlloyDB for PostgreSQL - Feeds, rules, transcripts, audio segments, annotations, feed lifecycle, and audit events.
  - Connection: `ALLOYDB_HOST`, `ALLOYDB_PORT`, `ALLOYDB_USER`, `ALLOYDB_DB`, `ALLOYDB_PASSWORD`, `ALLOYDB_POOL_MIN_SIZE`, `ALLOYDB_POOL_MAX_SIZE`, `ALLOYDB_COMMAND_TIMEOUT_SEC`, and `ALLOYDB_CONNECT_TIMEOUT_SEC`.
  - Client: `asyncpg` pools in `backend/pipeline/storage/connection.py`; synchronous connection helper in `backend/pipeline/storage/sync_connection.py`.
  - Schema: SQL migrations in `terraform/modules/alloydb/sql/ingestion/`, applied by `terraform/modules/alloydb/main.tf`.
  - Local: `postgres:15-alpine` in `docker-compose.yml` and override settings in `docker-compose.override.yml`.

**File Storage:**
- Google Cloud Storage - Pipeline staging/canonical/playback audio, Echo recording input events, local Whisper downloads, Gemini SFT durable run state, and model artifacts.
  - Infrastructure: `terraform/modules/gcs_bucket/main.tf`.
  - Local: `fsouza/fake-gcs-server` in `docker-compose.yml`.
- Local filesystem - Mock audio served from `local_dev/mock_audio/` as documented in `documentation/local-dev-mock-audio.md`.

**Caching:**
- Redis / Memorystore for Redis - Notification deduplication, cache provider, and rules service dependency.
  - Connection: `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`, and `REDIS_CERTIFICATE_PATH`.
  - Client: `backend/pipeline/common/storage/redis_service.py`.
  - Infrastructure: `terraform/modules/memorystore_for_redis/main.tf`; local `redis:7-alpine` in `docker-compose.yml`.
- In-process admin membership cache - `frontend/api/src/config.ts` caches Cloud Identity group membership results.

## Authentication & Identity

**Auth Provider:**
- Google OAuth and OIDC.
  - Implementation: UI wraps React in `GoogleOAuthProvider` in `frontend/transcription-ui/src/main.tsx`; BFF exchanges Google auth codes and sets an HTTP-only `refresh_token` cookie in `frontend/api/src/auth/authController.ts`; TSOA auth decodes gateway userinfo headers or bearer JWTs in `frontend/api/src/authentication.ts`.
  - Internal services: FastAPI services require `verify_oidc_token` from `backend/pipeline/common/auth.py`; internal Python clients fetch service account ID tokens through `backend/pipeline/common/auth_client.py`.
  - Admin authorization: `frontend/api/src/config.ts` checks Cloud Identity transitive membership when `WORKSPACE_ADMIN_GROUP_EMAIL` is set.
  - Trusted actor propagation: Feeds API admin mutations require `X-WD-Actor-Id` from the BFF in `backend/services/feeds/main.py`.

## Monitoring & Observability

**Error Tracking:**
- Dedicated third-party error tracking is not detected.
- Backend errors are logged through Python logging and Google Cloud Logging setup in `backend/pipeline/common/log_helper.py`.
- Frontend/API errors are logged with `console.error` in `frontend/api/src/index.ts`, `frontend/api/src/config.ts`, and controller utilities.

**Logs:**
- Python backend uses `setup_logging()` in pipeline entry points such as `backend/pipeline/normalization/main.py`, `backend/pipeline/transcription/main.py`, `backend/pipeline/evaluation/main.py`, `backend/pipeline/notification/send_notification.py`, and `backend/pipeline/ingestion/main.py`.
- Cloud Trace propagation uses W3C `traceparent` and baggage helpers in `backend/pipeline/common/tracing_utils.py`.
- Custom Cloud Monitoring time series are written by `backend/pipeline/common/clients/monitoring_client.py`.
- CI coverage summaries are posted by `.github/actions/post-backend-coverage/action.yml` and `.github/actions/post-frontend-coverage/action.yml`.

## CI/CD & Deployment

**Hosting:**
- Backend Python services target Google Cloud Functions Gen 2, Cloud Run/ASGI services, Dataflow, and GCE Managed Instance Groups depending on component.
  - Cloud Function module: `terraform/modules/cloud_function/main.tf`.
  - Dataflow segmentation image: `backend/pipeline/segmentation/Dockerfile`.
  - GCE MIG container module: `terraform/modules/container_mig/main.tf` and `terraform/modules/container_mig/cloud_config.yaml.tftpl`.
  - AlloyDB schema migration Cloud Run Job: `terraform/modules/alloydb/main.tf`.
- Frontend API proxy runs as a Node Functions Framework HTTP target from `frontend/api/Dockerfile` and `frontend/api/src/index.ts`.
- Frontend UI is a Vite static build with Firebase Hosting metadata in `frontend/transcription-ui/firebase.json`.
- Model GPU evaluation can run on a GCE instance from `terraform/modules/asr_evaluation/main.tf`.

**CI Pipeline:**
- GitHub Actions CI is defined in `.github/workflows/ci.yml`.
- Component and Docker Compose E2E tests are defined in `.github/workflows/integration-tests.yml`.
- Staging base images are built and pushed to GHCR by `.github/workflows/bake-main.yml`.
- Public-to-private deployment signaling uses `.github/workflows/trigger-deploy.yml`, GitHub CLI, and repository secrets.

## Environment Configuration

**Required env vars:**
- Backend storage: `ALLOYDB_HOST`, `ALLOYDB_PORT`, `ALLOYDB_USER`, `ALLOYDB_DB`, `ALLOYDB_PASSWORD`.
- Ingestion: `AUDIO_STAGING_BUCKET`, `CONTINUOUS_PUBSUB_TOPIC_PATH`, `SEGMENTED_PUBSUB_TOPIC_PATH`, `WORKER_ID`, `MAX_FEEDS_PER_WORKER`, `GOOGLE_CLOUD_PROJECT`, `FEED_AUDIT_ACTOR_ID`.
- Normalization: `PROJECT_ID`, `AUDIO_CANONICAL_BUCKET`, `OUTPUT_TOPIC`, `AUDIO_SEGMENTS_API_URL`.
- Transcription: `PROJECT_ID`, `OUTPUT_TOPIC`, `AUDIO_SEGMENTS_API_URL`, `TRANSCRIBER_TYPE`, `TRANSCRIBER_CONFIG`, `LOCAL_ASR_API_URL`.
- Evaluation: `AUDIO_SEGMENTS_API_URL`, `RULES_API_URL`, `RULES_EVALUATION_RESULTS_TOPIC`, `RULES_CACHE_TTL_SECONDS`.
- Notification: `NOTIFICATION_ENDPOINT`, `NOTIFICATION_ENDPOINT_API_KEY`, `APP_URL`, `FEEDS_API_URL`, `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`, `REDIS_CERTIFICATE_PATH`.
- Frontend API: `ALLOWED_ORIGIN`, `RULES_API_URL`, `FEEDS_STORE_API_URL`, `AUDIO_SEGMENTS_API_URL`, `PROJECT_ID`, `API_PUBLIC_URL`, `GOOGLE_AUTH_CLIENT_ID`, `GOOGLE_AUTH_CLIENT_SECRET`, `AUTH_BACKEND`, `WORKSPACE_ADMIN_GROUP_EMAIL`.
- Frontend UI: `VITE_API_BASE_URL`, `VITE_GOOGLE_AUTH_CLIENT_ID`, `VITE_PROXY_API_TARGET`, `VITE_PROXY_API_ORIGIN`, `VITE_AUTH_BACKEND`, `VITE_ALERT_ICON_SYMBOL_NAME`.
- Upstream sources: `BROADCASTIFY_USERNAME`, `BROADCASTIFY_PASSWORD`, `BROADCASTIFY_JWT_SECRET_ID`, `BCFY_FEEDS_URL_BASE`, `BCFY_CALLS_URL_BASE`, `OPENMHZ_TRANSPORT`, `FIRE_NOTIFICATIONS_URL_BASE`, `FIRE_NOTIFICATIONS_S3_BASE`, `FIRE_NOTIFICATIONS_USER`, `FIRE_NOTIFICATIONS_PASSWORD`.
- Model/data workflows: `BROADCASTIFY_APP_ID`, `BROADCASTIFY_API_KEY_ID`, `BROADCASTIFY_API_TOKEN`, `FN_AUTH_PASSWORD`, `HF_TOKEN`.

**Secrets location:**
- Local env files are present at `frontend/api/.env.example`, `frontend/transcription-ui/.env.example`, `frontend/transcription-ui/.env.local-dev.example`, and `local_dev/LOCAL.env`; contents were not read.
- `.mise.toml` is configured to load `.env`; contents were not read and the file is not present in the scanned root.
- Broadcastify Calls JWT is stored in Google Secret Manager and addressed by `BROADCASTIFY_JWT_SECRET_ID` in `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py`.
- AlloyDB schema migration reads the database password from Secret Manager through `password_secret_id` in `terraform/modules/alloydb/main.tf`.
- GitHub deployment secrets are referenced by `.github/workflows/trigger-deploy.yml` as `PRIVATE_REPO_PAT` and `PRIVATE_REPO_TARGET`.

## Webhooks & Callbacks

**Incoming:**
- Pub/Sub CloudEvents trigger normalization, evaluation, and notification functions in `backend/pipeline/normalization/main.py`, `backend/pipeline/evaluation/main.py`, and `backend/pipeline/notification/send_notification.py`.
- Pub/Sub push HTTP requests hit the transcription ASGI app at `POST /` in `backend/pipeline/transcription/main.py`.
- Eventarc GCS `OBJECT_FINALIZE` events trigger Echo ingestion in `backend/pipeline/ingestion/collectors/echo/main.py`.
- Browser/API calls enter the TypeScript BFF routes generated by TSOA from `frontend/api/src/*Controller.ts`.
- Google OAuth login posts authorization codes to `frontend/api/src/auth/authController.ts`.
- Ingestion worker health checks call `GET /healthz` in `backend/pipeline/ingestion/health_server.py`; GCE MIG health checks are configured by `terraform/modules/container_mig/main.tf`.

**Outgoing:**
- Notification service posts alert payloads to `NOTIFICATION_ENDPOINT` from `backend/pipeline/notification/request_handler.py`.
- Backend services call Audio Segments, Feeds, and Rules APIs through internal HTTP clients in `backend/pipeline/common/clients/audio_segments_client.py`, `backend/pipeline/common/clients/feeds_client.py`, and `backend/pipeline/evaluation/rules_evaluation/evaluator.py`.
- Frontend UI service modules call the BFF/API using `VITE_API_BASE_URL` from `frontend/transcription-ui/src/service/`.
- Ingestion collectors call upstream Broadcastify, OpenMHz, and Fire Notifications endpoints from `backend/pipeline/ingestion/collectors/`.
- Model data source scripts call Broadcastify and Fire Notifications APIs from `model/data_sources/broadcastify/bcfy_api.py` and `model/data_sources/fire_notifications/fn_api.py`.
- GitHub Actions trigger private deployment workflows through `gh workflow run` in `.github/workflows/trigger-deploy.yml`.

---

*Integration audit: 2026-06-26*

# External Integrations

**Analysis Date:** 2026-04-21

All services are Google Cloud Platform-native. Outbound integrations fall into four groups: (1) audio sources (Broadcastify, OpenMHZ, Echo), (2) GCP managed services (AlloyDB, Pub/Sub, GCS, Cloud Speech, Logging/Monitoring, Secret Manager), (3) the downstream Watchduty notification webhook, and (4) Google identity for UI OAuth and service-to-service OIDC.

## APIs & External Services

**Audio source providers:**
- Broadcastify Calls API — authenticated call-metadata polling + audio download. Endpoint family `https://api.bcfy.io/common/v1/*` (auth URL `https://api.bcfy.io/common/v1/auth` is the only one hardcoded in `backend/pipeline/ingestion/broadcastify_credential_rotation/main.py`). The Calls collector reads JWTs that the credential-rotator Cloud Function refreshes into Secret Manager on a Cloud Scheduler cadence.
  - SDK/Client: `aiohttp` (async collector) + `requests` with `urllib3.util.retry.Retry` (credential rotator).
  - Auth: Basic Auth for credential rotation (env `BROADCASTIFY_USERNAME`, `BROADCASTIFY_PASSWORD`, plus `BROADCASTIFY_API_KEY`, `BROADCASTIFY_API_APP_ID`, `BROADCASTIFY_API_KEY_ID`). JWT Bearer for the Calls API — JWT sourced from Secret Manager secret id `BROADCASTIFY_JWT_SECRET_ID`. Signed locally using `pyjwt`.
  - Retry policy: `_MAX_5XX_RETRIES = 3` for 5xx; non-retryable on 401/403 (raises `AuthError`), 404, 429.
  - Collector: `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py`.
- Broadcastify Icecast — live streaming-audio feeds over Icecast/HTTP. The collector shells out to `ffmpeg` to segment the stream.
  - SDK/Client: `asyncio.subprocess` wrapping `ffmpeg`. No Python HTTP client in the hot path.
  - Auth: HTTP Basic Auth header built from env `BROADCASTIFY_USERNAME`/`BROADCASTIFY_PASSWORD` in `_build_auth_header()`.
  - Collector: `backend/pipeline/ingestion/collectors/icecast/icecast_collector.py`. Local-dev variant `local_icecast_collector.py`.
- OpenMHZ — public trunked-radio call events via Socket.IO/WebSocket, with MP3/M4A audio objects hosted on Wasabi S3.
  - SDK/Client: `curl-cffi` (async) with Chromium TLS fingerprint impersonation. WebSocket transport implements Engine.IO v4 + Socket.IO v4 parsing manually (`_parse_eio_open`, `_parse_sio_event`).
  - Auth: none required at the API layer; Wasabi S3 URLs served in event payloads.
  - Retry: `MAX_RECONNECT_FAILURES = 10` reconnect attempts, exponential backoff capped at 30 s; `_DOWNLOAD_MAX_RETRIES = 3` for audio object GETs.
  - Collector: `backend/pipeline/ingestion/collectors/openmhz/collector.py` and `_ws_transport.py`.
- Echo (Watchduty-owned recording service) — writes MP3s into a GCS bucket; Eventarc OBJECT_FINALIZE fires the Cloud Run service in `backend/pipeline/ingestion/collectors/echo/main.py`. Path structure `{channel}-{location}/{YYYYMMDD}/{channel}_{YYYYMMDD}_{HHMMSS}.mp3`. The handler deduplicates via `if_generation_match=0` and emits a deterministic `session_id = uuid5(NAMESPACE_URL, staging_uri)`.

**GCP managed services:**
- Cloud Pub/Sub — sole inter-service bus. Topics resolved per-source in `backend/pipeline/ingestion/router.py::resolve_topic_path`.
  - SDK/Client: `google-cloud-pubsub >= 2.35.0` (`PublisherClient` with `enable_message_ordering=True`).
  - Wrapper: `backend/pipeline/common/clients/pubsub_client.py::PubSubClient`.
  - Topics (prod names derived from env): `CONTINUOUS_PUBSUB_TOPIC_PATH`, `SEGMENTED_PUBSUB_TOPIC_PATH`, `RAW_AUDIO_TOPIC`, `RULES_EVALUATION_RESULTS_TOPIC`, and evaluation/notification intermediates. Local analogs in `local_dev/LOCAL.env`: `staging-audio-topic`, `canonical-audio-topic`, `transcription-text-topic`, `rules-evaluation-results-topic`.
  - Emulator: `gcr.io/google.com/cloudsdktool/cloud-sdk:emulators` in docker-compose, `PUBSUB_EMULATOR_HOST=pubsub-emulator:8085`.
- Cloud Storage (GCS) — audio staging, canonical audio, playback assets, Dataflow template bundle, AlloyDB schema migration SQL.
  - Sync SDK: `google-cloud-storage >= 2.18.2` (Cloud-Run handlers).
  - Async SDK: `gcloud-aio-storage >= 9.6.4` (`NormalizerRuntime` hot path). Shared via `backend/pipeline/common/clients/gcs_client.py::GcsClient` which owns an `aiohttp.TCPConnector` sized to `max_feeds_per_worker`.
  - Eventarc trigger: GCS `OBJECT_FINALIZE` → Echo Cloud Run service.
- AlloyDB for PostgreSQL — primary transactional store (feeds, rules, transcripts, feed leases/heartbeats).
  - Client: `asyncpg >= 0.29.0` pool (async path) + `psycopg[binary] >= 3.2.0` (sync path used only in the Echo Cloud Run handler).
  - Connection: direct private IP on VPC. Default port `6432` (managed PgBouncer transaction-mode pooler). Schema migrations connect to `5432` bypassing the pooler — DDL cannot run under transaction-mode.
  - Pool: `backend/pipeline/storage/connection.py::create_pool_with_retry` uses `tenacity` exponential backoff (5 attempts, 2 s → 30 s) for Cloud Run cold-start collisions. `statement_cache_size=0` is mandatory for PgBouncer compatibility.
  - Env vars: `ALLOYDB_HOST`, `ALLOYDB_PORT`, `ALLOYDB_USER`, `ALLOYDB_DB`, `ALLOYDB_PASSWORD`, `ALLOYDB_POOL_MIN_SIZE`, `ALLOYDB_POOL_MAX_SIZE`, `ALLOYDB_COMMAND_TIMEOUT_SEC`, `ALLOYDB_CONNECT_TIMEOUT_SEC`.
  - Schema: SQL files in `terraform/modules/alloydb/sql/ingestion/` applied by a Cloud Run Job (image `postgres:16-alpine`) mounting GCS FUSE. Re-runs are idempotent (`IF NOT EXISTS`/`ON CONFLICT`). Triggered whenever the combined SHA-256 hash of the SQL files changes.
- Cloud Speech-to-Text v2 (Chirp) — primary transcription engine.
  - SDK: `google-cloud-speech >= 2.37.0` (`google.cloud.speech_v2.SpeechClient`).
  - Impl: `backend/pipeline/transcription/transcribers.py`. Constants in `backend/pipeline/transcription/constants.py` and prompts in `chirp_prompt.txt`, `chirp_phrase_hints.txt`.
- Cloud Dataflow (Apache Beam) — the streaming transcription job.
  - Deployed as a Flex Template built from `backend/pipeline/transcription/Dockerfile` (base image `apache/beam_python3.13_sdk:2.71.0`).
  - Launcher: `/opt/google/dataflow/python_template_launcher` copied from `gcr.io/dataflow-templates-base/python313-template-launcher-base:latest`.
  - Pipeline: `backend/pipeline/transcription/orchestration.py`, CLI at `main.py`, DoFns in `stitcher.py`.
- Cloud Logging — centralized logs when running in GCP.
  - SDK: `google-cloud-logging >= 3.14.0`. Setup in `backend/pipeline/common/logging.py::setup_logging` — uses Cloud Logging client handler only when `is_gcp_env()` is true.
- Cloud Monitoring — custom metrics for feed lease/heartbeat state.
  - SDK: `google-cloud-monitoring >= 2.29.1` (`MetricServiceAsyncClient`).
  - Wrapper: `backend/pipeline/common/clients/monitoring_client.py::MonitoringClient.write_time_series` writes `GAUGE INT64` points under resource type `global`.
- Secret Manager — Broadcastify JWT rotation target, AlloyDB password, downstream notification endpoint keys.
  - SDK: `google-cloud-secret-manager >= 2.26.0`.
  - Usage: `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py::_get_jwt_token` reads `projects/{project}/secrets/{BROADCASTIFY_JWT_SECRET_ID}/versions/latest`. The rotator Cloud Function adds new versions and destroys versions older than 6 h.
- Cloud Run — runtime for request-scoped services (Rules Management, Transcripts API, Feeds API, frontend API gateway, Echo ingestion, AlloyDB schema-migration Job).
- Cloud Functions Gen 2 — Broadcastify credential rotator; frontend API gateway packaged with `@google-cloud/functions-framework`.
- GCE Managed Instance Group — long-lived ingestion collector fleet on Container-Optimized OS (`terraform/modules/container_mig/`), with health checks hardcoded to port 8080 and autohealing enabled.
- Artifact Registry — Docker image host. Registry host parsed from `container_image` input in `terraform/modules/container_mig/main.tf`.
- Eventarc — GCS `OBJECT_FINALIZE` events route to the Echo Cloud Run service.
- Cloud Scheduler — assumed driver for the Broadcastify credential rotation Cloud Function (function is HTTP-triggered and designed for scheduled invocation).
- Memorystore for Redis — notification deduplication store (`terraform/modules/memorystore_for_redis/`). SSL + CA cert required in GCP.

**External non-GCP:**
- Wasabi S3 — OpenMHZ audio object host. Accessed via presigned/public URLs in Socket.IO call events; no direct SDK dependency.
- Watchduty in-app notification endpoint — downstream webhook the Notification service POSTs to. Target URL in env `NOTIFICATION_ENDPOINT`, API key in env `NOTIFICATION_ENDPOINT_API_KEY`.

## Data Storage

**Databases:**
- AlloyDB (PostgreSQL-compatible) — the only OLTP store.
  - Connection: private VPC IP. Env-driven config in `backend/pipeline/storage/settings.py::AlloyDBSettings`.
  - Client: `asyncpg` async pool primary; `psycopg[binary]` sync path used only in the Eventarc-triggered Echo handler.
  - Stores: `FeedStore` (`backend/pipeline/storage/feed_store.py`, `feed_queries.py`), `RulesStore` (`rules_store.py`, `rules_queries.py`), `TranscriptStore` (`transcript_store.py`, `transcript_queries.py`).
  - Local-dev: `postgres:15-alpine` with DDL mounted from `terraform/modules/alloydb/sql/ingestion/` in docker-compose.

**File Storage:**
- GCS for audio staging (`AUDIO_STAGING_BUCKET`), canonical audio post-stitcher, playback variants, AlloyDB schema SQL (`{project}-alloydb-schema`), and Dataflow template bundles.
- Lifecycle rules and optional 7-day soft-delete configured via `terraform/modules/gcs_bucket/main.tf`.
- Uniform bucket-level access enforced.

**Caching:**
- Memorystore for Redis — notification dedup (`backend/pipeline/common/storage/redis_service.py`). `set_if_not_exists(key, value, ttl)` with exponential backoff on `BusyLoadingError`, `ConnectionError`, `TimeoutError`.
- `cachetools` TTL caches in-process for feed metadata / JWT refresh.

## Authentication & Identity

**Auth Providers:**
- Google OAuth 2.0 (UI) — `@react-oauth/google` in `frontend/transcription-ui/src/main.tsx` wrapping the app in `<GoogleOAuthProvider clientId={VITE_GOOGLE_AUTH_CLIENT_ID}>`. Token held in `AuthProvider` React context.
- Google OIDC ID tokens (service-to-service) — `backend/pipeline/common/auth.py`. `get_id_token(audience)` fetches from the Cloud Run metadata server; `verify_oidc_token` is the FastAPI `Depends` applied globally on Rules/Feeds/Transcripts services.
- The frontend API gateway decodes (not verifies) incoming JWTs via `jsonwebtoken` in `frontend/api/src/authentication.ts` — it relies on Google API Gateway (GFE) having already verified the token upstream.

**Broadcastify JWT:**
- Self-issued via `pyjwt` in the rotator Cloud Function; persisted to Secret Manager; fetched by the Calls collector at startup.

**Local dev:**
- `is_gcp_env()` returning false causes `verify_oidc_token` to return a stub `local-dev@example.com` claims dict — no credentials required to hit the HTTP APIs on localhost.

## Monitoring & Observability

**Error Tracking:**
- None — errors flow through Cloud Logging (with `logger.exception`) and structured log analysis downstream. No Sentry/Bugsnag/Rollbar found.

**Logs:**
- `backend/pipeline/common/logging.py::setup_logging` — Cloud Logging when `IS_GCP=true`, `basicConfig` locally. `@functools.cache` ensures single initialization.
- All services call `setup_logging()` at module top (notification, evaluation, rules, transcripts-api, feeds-api, ingestion, echo).

**Metrics:**
- `MonitoringClient.write_time_series` for custom gauges. Called from ingestion `quarantine_telemetry.py` and `normalizer_runtime.py`.
- Beam pipeline uses standard Dataflow metrics.

**Health:**
- Ingestion: `backend/pipeline/ingestion/health_server.py` serves `/healthz` on port `HEALTH_CHECK_PORT` (default 8080; must not be changed — GCE health check hardcodes 8080).
- FastAPI services: no explicit `/healthz` found; docker-compose probes a TCP connect to the Uvicorn port.

## CI/CD & Deployment

**Hosting:**
- UI: Firebase Hosting (`frontend/transcription-ui/firebase.json` — SPA rewrite of `**` to `/index.html`).
- API gateway: Cloud Function Gen 2 via `@google-cloud/functions-framework` (target `api`, signature `http`).
- Rules Management / Transcripts API / Feeds API: Cloud Run (Uvicorn on `$PORT=8080`).
- Echo ingestion: Cloud Run (functions-framework CloudEvent).
- Notification + Evaluation: Cloud Run / Cloud Function (functions-framework CloudEvent).
- Ingestion collectors (Icecast, Broadcastify Calls, OpenMHZ): GCE MIG on COS, one container image per source type.
- Transcription: Dataflow streaming Flex Template.
- Broadcastify credential rotator: Cloud Function Gen 2.

**CI Pipeline:**
- `.github/workflows/ci.yml` — runs on every push and is reused via `workflow_call`. Jobs: `setup` (skips if commit already green), `code-quality-checks-python` (ruff check/format, ruff-ignore-list-sorted-check, ty type-check), `code-quality-checks-typescript` (ESLint, Prettier, TSC, tsoa route + OpenAPI verification), `terraform-checks` (fmt + validate), `run-backend-tests` (pytest with apt cache for ffmpeg/libomp5), `run-frontend-tests` (vitest), `docker-smoke-test` (builds ingestion and transcription images, verifies imports and `NormalizerSettings()` with a minimal env).
- `.github/workflows/integration-tests.yml` — `component-tests` (pytest on `integration_tests/storage/`) and `e2e-tests` (docker-compose run). `workflow_dispatch` supports `mxschmitt/action-tmate@v3` SSH debug sessions on failure.
- `.github/workflows/trigger-deploy.yml` — on push to `main`, dispatches `terraform_deploy.yml` or `app_deploy.yml` in a private repo via `PRIVATE_REPO_PAT`. Terraform path is triggered when any `terraform/modules/` file changed; otherwise app-only deploy.
- `.github/workflows/prepend-linear-issue-to-pr-title.yml` — PR title hygiene.
- All tooling bootstrapped by `jdx/mise-action@v3`.

**Local dev stack:**
- `docker-compose.yml` at repo root orchestrates Pub/Sub emulator, Postgres 15, Redis 7, Rules Management, Rules Evaluation, Notification, Transcripts API, Feeds API, frontend API, mock HTTP server, and an integration-tests container.
- `asr-eval-docker-compose.yml` is a separate stack for ASR evaluation (see `ASR_CONTRIBUTING.md`).

## Environment Configuration

**Required env vars (pipeline-wide):**
- Core GCP: `IS_GCP`, `GOOGLE_CLOUD_PROJECT`.
- AlloyDB: `ALLOYDB_HOST`, `ALLOYDB_PORT`, `ALLOYDB_USER`, `ALLOYDB_DB`, `ALLOYDB_PASSWORD`, `ALLOYDB_POOL_MIN_SIZE`, `ALLOYDB_POOL_MAX_SIZE`.
- Pub/Sub topics: `CONTINUOUS_PUBSUB_TOPIC_PATH` (required), `SEGMENTED_PUBSUB_TOPIC_PATH` (optional), `RAW_AUDIO_TOPIC` (echo), `RULES_EVALUATION_RESULTS_TOPIC`, `STAGING_TOPIC`, `CANONICAL_TOPIC`, `TRANSCRIPTION_TOPIC`.
- GCS: `AUDIO_STAGING_BUCKET`.
- Redis: `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`, `REDIS_CERTIFICATE_PATH`.
- Broadcastify: `BROADCASTIFY_USERNAME`, `BROADCASTIFY_PASSWORD`, `BROADCASTIFY_API_KEY`, `BROADCASTIFY_API_APP_ID`, `BROADCASTIFY_API_KEY_ID`, `BROADCASTIFY_JWT_SECRET_ID`.
- Notification: `APP_URL`, `NOTIFICATION_ENDPOINT`, `NOTIFICATION_ENDPOINT_API_KEY`.
- Inter-service URLs: `TRANSCRIPTS_API_URL`, `RULES_API_URL`, `FEEDS_STORE_API_URL` (evaluation, frontend API).
- Worker tuning: `WORKER_ID`, `MAX_FEEDS_PER_WORKER`, `LEASE_POLL_INTERVAL_SEC`, `HEARTBEAT_INTERVAL_SEC`, `HEARTBEAT_STALL_TIMEOUT_SEC`, `FEED_FAILURE_THRESHOLD`, `ABANDONMENT_WINDOW_SEC`, `GCS_UPLOAD_MAX_RETRIES`, `BOOKMARK_MAX_RETRIES`, `HEALTH_CHECK_PORT`, `HEALTH_CHECK_STARTUP_GRACE_SEC`.
- Frontend (UI): `VITE_GOOGLE_AUTH_CLIENT_ID`.
- Frontend (API gateway): `ALLOWED_ORIGIN`, `TRANSCRIPTS_API_URL`, `RULES_API_URL`, `FEEDS_STORE_API_URL`, `PROJECT_ID`, `API_PUBLIC_URL`.

**Secrets location:**
- Google Secret Manager — Broadcastify JWT (rotated), AlloyDB passwords, notification endpoint API keys.
- Schema-migration job reads `password_secret_id` via `value_source.secret_key_ref` in `terraform/modules/alloydb/main.tf`.
- `.env` file loaded by `mise` for local secrets (path set in `.mise.toml`); `local_dev/LOCAL.env` holds non-sensitive local values (Postgres `postgres`/`postgres` on docker-compose). Do not commit real credentials — `.env` is `.gitignore`d.
- GitHub Actions secrets: `PRIVATE_REPO_PAT`, `PRIVATE_REPO_TARGET` for cross-repo deploy dispatch.

## Webhooks & Callbacks

**Incoming:**
- Eventarc OBJECT_FINALIZE → Echo Cloud Run (`backend/pipeline/ingestion/collectors/echo/main.py::handle_notification`).
- Pub/Sub push subscriptions → Evaluation (`evaluate_transcribed_audio_segment`), Notification (`send_notification`), Rules Evaluation. All delivered as CloudEvents via `functions-framework`.
- HTTP REST endpoints (FastAPI): `POST/GET/PUT/DELETE /v1/rules`, `POST/GET/DELETE /v1/feeds`, `POST/GET/DELETE /v1/transcripts`. All guarded by `verify_oidc_token`.
- HTTP REST (frontend API gateway): tsoa-generated routes, spec at `frontend/api/openapi.yaml`.
- HTTP REST (Broadcastify credential rotator): Cloud Scheduler-driven HTTP trigger.

**Outgoing:**
- Watchduty notification webhook — POST `NOTIFICATION_ENDPOINT` with header `X-Api-Key: NOTIFICATION_ENDPOINT_API_KEY`. Payload is `AlertNotification` protobuf serialized as JSON (`google.protobuf.json_format.MessageToJson`). Retries on 500/502/503/504 via `urllib3.util.retry.Retry(total=3, backoff_factor=0.1)`.
- Inter-service HTTP: Evaluation → Rules Management (if `RULES_API_URL` set) via `RemoteTextEvaluator`; Evaluation → Transcripts API via `TranscriptsClient` (`backend/pipeline/common/clients/transcripts_client.py`), authenticated with a Google-issued OIDC ID token.
- Broadcastify Calls API polls and audio downloads (`aiohttp.ClientSession.get`).
- Broadcastify Icecast stream reads (`ffmpeg` subprocess).
- OpenMHZ Socket.IO WebSocket + Wasabi S3 audio GETs (`curl-cffi.AsyncSession`).
- Pub/Sub publishes to all downstream topics.

---

*Integration audit: 2026-04-21*

# External Integrations

**Analysis Date:** 2026-06-14

## APIs & External Services

**Broadcastify / Broadcastify Calls:**
- Used by VM collectors for live stream and calls ingestion.
- Integration method: source-specific HTTP, stream, and API collectors under
  `backend/pipeline/ingestion/collectors/icecast/` and
  `backend/pipeline/ingestion/collectors/bcfy_calls/`.
- Auth: credentials and tokens are supplied through runtime configuration and
  source-specific collector code. Do not store raw credentials in docs.
- Failure semantics: collectors map endpoint evidence into `FeedFailure`
  status reasons and bounded reason tags.

**OpenMHz:**
- Used by VM collector for WebSocket-based call ingestion.
- Integration method: code in
  `backend/pipeline/ingestion/collectors/openmhz/`.
- Failure semantics: transport and WebSocket upgrade failures are retried and
  eventually surfaced as typed feed failures.

**Fire Notifications:**
- Used by VM collector for polling incident audio/file sources.
- Integration method: code in
  `backend/pipeline/ingestion/collectors/fire_notifications/`.
- Auth: required runtime environment settings; missing or rejected auth maps
  to system-owned status reasons.

**Echo:**
- File-based ingestion path that runs outside the VM collector runtime.
- Integration notes live in `backend/pipeline/README.md`; Echo file timestamps
  should come from filenames before GCS object metadata.

## Data Storage

**AlloyDB / PostgreSQL:**
- Primary feed lifecycle, transcript, audio segment, rule, and annotation
  store.
- Access patterns:
  - async runtime/services use `backend/pipeline/storage/*`.
  - Echo-style synchronous paths use `sync_*` storage modules.
- Schema migrations live in
  `terraform/modules/alloydb/sql/ingestion/*.sql`.
- Terraform can apply schema through a Cloud Run job in
  `terraform/modules/alloydb/main.tf`.

**Google Cloud Storage:**
- Stores raw staged chunks, normalized/canonical audio, playback artifacts,
  and model/evaluation data.
- Runtime helpers live in `backend/pipeline/common/gcp_helper.py`.
- Async client wrapper lives in `backend/pipeline/common/clients/gcs_client.py`.
- Local development uses `fsouza/fake-gcs-server` in `docker-compose.yml`.

**Redis / Memorystore:**
- Used for notification deduplication and common cache primitives.
- Terraform module: `terraform/modules/memorystore_for_redis/`.
- Local development uses `redis:7-alpine` in `docker-compose.yml`.

## Messaging

**Google Cloud Pub/Sub:**
- Primary event bus between ingestion, normalization, transcription,
  evaluation, and notification.
- Raw audio claim-check messages use `protos/raw_audio_chunk.proto`.
- Normalized audio messages use `protos/normalized_audio.proto`.
- Transcribed and evaluated messages use `protos/transcribed_audio.proto`,
  `protos/evaluated_transcribed_audio.proto`, and
  `protos/alert_notification.proto`.
- Publish helpers live in `backend/pipeline/common/gcp_helper.py` and
  `backend/pipeline/common/clients/pubsub_client.py`.
- Local development uses the Pub/Sub emulator and `local_dev/pubsub_init.py`.

## Authentication & Identity

**Backend API Auth:**
- FastAPI services use OIDC verification through
  `backend/pipeline/common/auth.py`.
- Frontend API proxy uses Google auth and JOSE dependencies under
  `frontend/api/src/authentication.ts` and `frontend/api/src/auth/`.

**Frontend Auth:**
- React UI auth context lives in `frontend/transcription-ui/src/context/`.
- Login/logout/session services live in `frontend/transcription-ui/src/service/`.

## Monitoring & Observability

**Cloud Logging / Structured Logs:**
- Python services use standard logging plus `extra={"json_fields": ...}` for
  structured event payloads.
- Quarantine/SLO event constants live in
  `backend/pipeline/ingestion/slo_contract.py`.
- Quarantine telemetry lives in
  `backend/pipeline/ingestion/quarantine_telemetry.py`.

**Tracing:**
- OpenTelemetry and Google Cloud trace exporter are configured through
  `backend/pipeline/common/tracing_utils.py` and
  `backend/pipeline/common/fastapi_tracing.py`.
- Pub/Sub and GCS helpers propagate `traceparent` attributes where available.

**Metrics:**
- Google Cloud Monitoring client wrapper lives in
  `backend/pipeline/common/clients/monitoring_client.py`.

## CI/CD & Deployment

**GitHub Actions:**
- Workflows live in `.github/workflows/`.
- CI, integration tests, deploy trigger, image bake, and Linear PR title
  validation are represented there.
- PR titles must use Linear-style prefixes such as `[GOO-123]`,
  `[ENG-ONLY]`, or `[DEV-ONLY]`.

**Terraform / Google Cloud:**
- Reusable modules cover AlloyDB, GCS buckets, Cloud Functions, container MIGs,
  Memorystore, and ASR evaluation.
- Local Docker Compose mirrors many production dependencies with emulators and
  mock servers.

## Environment Configuration

**Development:**
- Local stack configuration is read from `local_dev/LOCAL.env` and optional
  `.env`.
- Mock audio sources live under `local_dev/mock_audio/`.
- Remote UI development helpers live in `local_dev/setup_remote_dev.py` and
  `mise` tasks.

**Production:**
- Secrets should be supplied via cloud secret/config mechanisms and runtime
  environment variables, not committed files.
- Pub/Sub topics, subscriptions, buckets, database connection details, and API
  URLs are environment-specific.

## Webhooks & Callbacks

**Incoming CloudEvents:**
- Cloud Function entry points consume Pub/Sub CloudEvents:
  - `backend/pipeline/transcription/main.py`
  - `backend/pipeline/evaluation/main.py`
  - `backend/pipeline/notification/send_notification.py`

**HTTP APIs:**
- FastAPI service endpoints are served by `backend/services/*/main.py`.
- Express/tsoa frontend proxy endpoints are served by `frontend/api/src/`.

---

*Integration audit: 2026-06-14*
*Update when adding or removing external services*

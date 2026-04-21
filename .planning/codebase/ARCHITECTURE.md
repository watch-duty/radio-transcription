# Architecture

**Analysis Date:** 2026-04-21

## Pattern Overview

**Overall:** Event-driven, message-bus-first pipeline of specialized cloud services. Upstream **stateful MIG workers** (long-lived GCE instances running the `NormalizerRuntime`) fan audio chunks into Pub/Sub, where downstream **stateless Cloud Run / Cloud Function / Dataflow** stages consume and re-publish transformed artifacts. A per-service FastAPI layer guards AlloyDB as the authoritative write store. Frontend is a Vite+React SPA that reads through an OpenAPI-generated TypeScript client.

**Key Characteristics:**
- **Stateful-ingestion / stateless-processing split.** Stateful concerns (active stream connections, fencing tokens, bookmarks, heartbeats) are concentrated in one horizontally-scaled MIG fleet (`backend/pipeline/ingestion/`). Every other stage is pure function over Pub/Sub messages, which makes it trivially replayable and horizontally scalable.
- **DB-level leasing with fencing tokens.** AlloyDB is the source of truth for who owns a feed. `FeedStore` uses `FOR UPDATE SKIP LOCKED` batch acquisition and a monotonic `fencing_token` stamped into every GCS path and every bookmark `UPDATE`'s WHERE clause. Split-brain is impossible because a zombie worker's token will be lower than the current holder's.
- **Contract boundary is the `CapturedChunk` yield.** `backend/pipeline/ingestion/models.py` documents this as an immutable contract: the capture function owns connections/retries/reconnects; the runtime owns GCS upload, Pub/Sub publish, bookmarks, heartbeats, quarantine, and shutdown. A new source = one file in `collectors/` + one registry entry.
- **Protobuf-defined inter-service messages.** All Pub/Sub payloads are protobufs in `protos/`, regenerated into `backend/pipeline/schema_types/` via `mise run generate:protos`. The generated `*_pb2.py` files are `.gitignore`'d so schema changes must flow through `.proto`.
- **Graceful shutdown + prompt fail-fast.** In the ingestion runtime every wait is interruptible by `SIGTERM`; `asyncio.sleep` and `time.sleep` are banned inside `normalizer_runtime.py`. Fence violations or event-loop stalls trip `os._exit(1)` after `logging.shutdown()` so the MIG autohealer restarts the VM.
- **Control-plane / data-plane DB pool separation.** Heartbeat renewal runs against a dedicated 1-connection `asyncpg` pool so 250 concurrent bookmark writes on the main pool can never starve heartbeats and fabricate a stall.

## Layers

**Upstream data sources (external):**
- Purpose: Produce live or recorded audio.
- Location: Third-party services reached via URL bases in `backend/pipeline/ingestion/router.py` (`https://partner.broadcastify.com/`, `https://api.bcfy.io/calls/v1/live/`, `https://api.openmhz.com/`) and the Echo GCS bucket that Eventarc watches.
- Contains: Icecast continuous streams, Broadcastify Calls polling API, OpenMHZ websocket, Echo MP3 object uploads.
- Used by: Collectors in `backend/pipeline/ingestion/collectors/`.

**Collectors (capture layer):**
- Purpose: Per-source async generators that emit `CapturedChunk` objects. Hold all source-specific transport logic (ffmpeg for Icecast, websockets for OpenMHZ, polling for Broadcastify Calls, Eventarc entry for Echo).
- Location: `backend/pipeline/ingestion/collectors/icecast/`, `collectors/openmhz/`, `collectors/bcfy_calls/`, `collectors/echo/`.
- Contains: `capture_icecast_stream`, `capture_bcfy_calls`, `openmhz_collector`, and the Echo Cloud Function `handle_notification`.
- Depends on: `backend.pipeline.ingestion.models.CapturedChunk`, `backend.pipeline.common.constants`, source-specific transport libs (`aiohttp`, `curl_cffi`, `websockets`, ffmpeg subprocess).
- Used by: `backend.pipeline.ingestion.router.route_capturer` (MIG path) or `functions_framework` (Echo Cloud Function path).

**Ingestion runtime (orchestrator):**
- Purpose: Leasing loop, heartbeat thread, fencing-token enforcement, per-feed asyncio task lifecycle, GCS upload, Pub/Sub publish, bookmark write, quarantine telemetry, `/healthz` gates.
- Location: `backend/pipeline/ingestion/normalizer_runtime.py`, `backend/pipeline/ingestion/health_server.py`, `backend/pipeline/ingestion/retry.py`, `backend/pipeline/ingestion/quarantine_telemetry.py`, `backend/pipeline/ingestion/router.py`, `backend/pipeline/ingestion/settings.py`, `backend/pipeline/ingestion/main.py`.
- Contains: `NormalizerRuntime`, `HealthState`, `retry_with_lease_check`, `LeaseExpiredError`, `resolve_topic_path`, `NormalizerSettings`.
- Depends on: `backend.pipeline.storage.feed_store.FeedStore`, `backend.pipeline.common.clients` (GCS, Pub/Sub, monitoring), `asyncpg`, `uvloop`, `aiohttp`.
- Used by: The MIG worker entry point `backend/pipeline/ingestion/main.py`, deployed via `terraform/modules/container_mig/`.

**Storage layer (data access):**
- Purpose: Async and sync `asyncpg` / `psycopg` wrappers around AlloyDB. Encapsulates feed leasing SQL, bookmark updates, heartbeat renewal, transcript and rule CRUD.
- Location: `backend/pipeline/storage/connection.py`, `backend/pipeline/storage/feed_store.py`, `backend/pipeline/storage/feed_queries.py`, `backend/pipeline/storage/sync_connection.py`, `backend/pipeline/storage/sync_feed_store.py`, `backend/pipeline/storage/transcript_store.py`, `backend/pipeline/storage/transcript_queries.py`, `backend/pipeline/storage/rules_store.py`, `backend/pipeline/storage/rules_queries.py`, `backend/pipeline/storage/settings.py`.
- Contains: `FeedStore`, `SyncFeedStore`, `TranscriptStore`, `RulesStore`, `LeasedFeed`, `HeartbeatResult`, `Feed`, `SourceType`, `create_pool_with_retry`, `AlloyDBSettings`.
- Depends on: AlloyDB (via private-IP VPC), raw SQL files under `terraform/modules/alloydb/sql/ingestion/`.
- Used by: `NormalizerRuntime`, FastAPI services in `backend/services/`, evaluation/notification Cloud Functions.

**Transcription pipeline (Dataflow/Beam):**
- Purpose: Consume raw audio chunks from Pub/Sub, download from GCS, run VAD + stitching, call Chirp/Gemini transcribers, write transcripts back via the Transcripts API, and publish transcribed segments to the downstream topic.
- Location: `backend/pipeline/transcription/main.py`, `backend/pipeline/transcription/orchestration.py`, `backend/pipeline/transcription/stitcher.py`, `backend/pipeline/transcription/stitcher_state.py`, `backend/pipeline/transcription/transcribers.py`, `backend/pipeline/transcription/vads.py`, `backend/pipeline/transcription/transforms.py`, `backend/pipeline/transcription/sequence_buffer.py`, `backend/pipeline/transcription/audio_processor.py`, `backend/pipeline/transcription/dsp.py`, `backend/pipeline/transcription/detectors.py`, `backend/pipeline/transcription/orchestration.py`.
- Contains: Apache Beam streaming DAG (`get_pipeline`), DoFns (`ParseAndKeyFn`, `DownloadAudioFn`, `RestoreOrderFn`, `StitchAudioFn`, `TranscribeAudioFn`, `SerializeToPubSubMessageFn`, `BypassStitchingFn`), VAD + stitching state machines, dead-letter queue formatter.
- Depends on: `apache_beam`, `backend.pipeline.schema_types`, Transcripts API service, GCS audio staging bucket, `backend.pipeline.transcription.options.TranscriptionOptions`.
- Used by: Dataflow streaming job (its own `pyproject.toml` at `backend/pipeline/transcription/pyproject.toml`).

**Rules evaluation (Cloud Run Function):**
- Purpose: For each transcribed segment, fetch the relevant rules, evaluate them (locally with `StaticTextEvaluator` or remotely with `RemoteTextEvaluator`), persist evaluations via the Transcripts API, and publish alerts.
- Location: `backend/pipeline/evaluation/main.py`, `backend/pipeline/evaluation/processor.py`, `backend/pipeline/evaluation/service.py`, `backend/pipeline/evaluation/rules_evaluation/evaluator.py`.
- Contains: `EvaluationEventProcessor`, `EvaluationService`, `StaticTextEvaluator`, `RemoteTextEvaluator`, `evaluate_transcribed_audio_segment` entry point.
- Depends on: `functions_framework`, `backend.pipeline.common.clients.pubsub_client`, `backend.pipeline.common.clients.transcripts_client`, Rules API URL, Transcripts API URL.

**Notification (Cloud Run Function):**
- Purpose: Receive `AlertNotification` messages, deduplicate via Redis, POST to the downstream Watch Duty notification endpoint.
- Location: `backend/pipeline/notification/send_notification.py`, `backend/pipeline/notification/request_handler.py`, `backend/pipeline/notification/notification_deduplication.py`.
- Depends on: Redis (`backend/pipeline/common/storage/redis_service.py`), `urllib3`.

**Management services (FastAPI on Cloud Run):**
- Purpose: CRUD REST surfaces over AlloyDB used by both the frontend and internal pipeline services.
- Location: `backend/services/feeds/main.py`, `backend/services/feeds/service.py`, `backend/services/feeds/models.py`, `backend/services/transcripts/main.py`, `backend/services/transcripts/service.py`, `backend/services/transcripts/models.py`, `backend/pipeline/rules/main.py`, `backend/pipeline/rules/service.py`.
- Contains: `FeedService`, `TranscriptService`, `AlloyRulesService`, OIDC-auth gated endpoints under `/v1/feeds`, `/v1/transcripts`, `/v1/rules`.
- Depends on: `fastapi`, `backend.pipeline.common.auth.verify_oidc_token`, `backend.pipeline.storage.*`.

**Broadcastify credential rotation (Cloud Function):**
- Purpose: Periodically refresh scraped Broadcastify credentials used by the BCFY collectors.
- Location: `backend/pipeline/ingestion/broadcastify_credential_rotation/main.py`.

**Frontend SPA:**
- Purpose: Operator UI for browsing transcripts, managing feeds, authoring rules, and reviewing documentation.
- Location: `frontend/transcription-ui/src/main.tsx`, `frontend/transcription-ui/src/App.tsx`, `frontend/transcription-ui/src/components/` (feeds, rules, transcripts, audio, common, docs), `frontend/transcription-ui/src/service/`, `frontend/transcription-ui/src/context/`.
- Depends on: `frontend/api/` (OpenAPI/tsoa-generated TypeScript client), `frontend/common/`, Google OAuth, React Query, React Router.

**Protobuf contracts (cross-cutting):**
- Purpose: Canonical wire format for all Pub/Sub messages.
- Location: `protos/raw_audio_chunk.proto`, `protos/transcribed_audio.proto`, `protos/evaluated_transcribed_audio.proto`, `protos/alert_notification.proto`. Generated modules at `backend/pipeline/schema_types/*_pb2.py`.

**Model training / evaluation notebooks:**
- Purpose: Out-of-band, not part of the runtime serving path. Used by researchers to build ASR models and evaluation datasets.
- Location: `model/colabs/`, `model/data/`, `model/nemo_docker/`, `model/notebook_docker/`.

**Infrastructure:**
- Purpose: Terraform modules that declare production infrastructure.
- Location: `terraform/modules/container_mig/` (MIG + health check for ingestion workers), `terraform/modules/cloud_function/` (Cloud Run Functions for Echo, evaluation, notification, credential rotation), `terraform/modules/alloydb/` (AlloyDB cluster + SQL migrations), `terraform/modules/gcs_bucket/`, `terraform/modules/memorystore_for_redis/`, `terraform/modules/asr_evaluation/`.

## Data Flow

**Primary flow — Icecast/OpenMHZ/BCFY Calls through to notification:**

1. A collector coroutine inside `NormalizerRuntime._process_feed` (`backend/pipeline/ingestion/normalizer_runtime.py:345`) `async for`-iterates a `CapturedChunk` from the per-source capture generator in `backend/pipeline/ingestion/collectors/`.
2. The runtime calls `backend.pipeline.common.gcp_helper.upload_staged_audio` to PUT the raw bytes to `gs://{AUDIO_STAGING_BUCKET}/{source_type}/{feed_id}/token-{N}/{timestamp}_{seq}.{ext}`. Token-qualified pathing + `ifGenerationMatch=0` are the split-brain guardrail. The call is wrapped in `retry_with_lease_check` which races backoffs against `lease_lost` and `shutdown` events.
3. `backend.pipeline.common.gcp_helper.publish_audio_chunk` publishes an `AudioChunk` protobuf (`backend/pipeline/schema_types/raw_audio_chunk_pb2.py`) to the continuous or segmented Pub/Sub topic, selected by `resolve_topic_path(feed['source_type'])` in `backend/pipeline/ingestion/router.py`.
4. `FeedStore.update_feed_progress` writes `last_processed_filename`, `last_bookmark_time`, and validates the `fencing_token` inside the same SQL `UPDATE`. A zero-row result means the lease was lost; `NormalizerRuntime` calls `logging.shutdown()` then `os._exit(1)` so the MIG autohealer replaces the VM.
5. A Dataflow streaming job `backend/pipeline/transcription/orchestration.py:get_pipeline` reads the topic, downloads audio from GCS (`DownloadAudioFn`), restores order (`RestoreOrderFn`), stitches segments (`StitchAudioFn`), transcribes (`TranscribeAudioFn` using Chirp or Gemini), calls Transcripts API to persist, and publishes a `TranscribedAudio` message to the transcription output topic.
6. The evaluation Cloud Function `evaluate_transcribed_audio_segment` (`backend/pipeline/evaluation/main.py`) is triggered by Pub/Sub, loads the relevant rules, runs evaluations, writes results back through the Transcripts API, and publishes an `AlertNotification` when thresholds fire.
7. The notification Cloud Function (`backend/pipeline/notification/send_notification.py`) deduplicates via Redis and POSTs to the Watch Duty notification endpoint.

**Echo side-branch (Eventarc instead of MIG):**

1. An external Echo recorder uploads `.mp3` files to a GCS bucket. Eventarc fires an `OBJECT_FINALIZE` event.
2. `backend/pipeline/ingestion/collectors/echo/main.py:handle_notification` resolves the feed from AlloyDB via `SyncFeedStore.resolve_echo_feed`, re-uploads to the staging bucket under `echo/{feed_id}/...`, publishes an `AudioChunk` to the segmented Pub/Sub topic with a deterministic `session_id` (`uuid.uuid5(NAMESPACE_URL, staging_uri)`), and records a heartbeat. From here it joins the primary flow at step 5.

**Heartbeat control-plane flow (concurrent to per-feed data-plane):**

1. An OS daemon thread (`NormalizerRuntime._heartbeat_loop`, started in `_main`) ticks every `HEARTBEAT_INTERVAL_SEC` on a monotonic ticker.
2. It calls `asyncio.run_coroutine_threadsafe(self._heartbeat_cycle(), self._loop)` and waits up to `HEARTBEAT_STALL_TIMEOUT_SEC` on the resulting future.
3. `_heartbeat_cycle` stamps `HealthState.last_heartbeat_tick` at dispatch (not after DB success — avoids fleet-wide autohealer kill during AlloyDB outages), then batch-renews all active feeds via `FeedStore.renew_heartbeats_batch_diagnostic`.
4. Any feed not renewed (and not `.done()` and not in `_releasing_feeds`) is a genuine fence violation → `os._exit(1)`.
5. A `concurrent.futures.TimeoutError` at the watchdog layer is interpreted as an event-loop stall and also trips `os._exit(1)`. Transient DB errors just set `_lease_lost` and keep the worker alive.

**State Management:**
- **Authoritative state:** AlloyDB (`feeds` table with leasing columns, `transcripts`, `rules`). Every state transition is a single SQL `UPDATE` / `INSERT` guarded by `fencing_token` comparisons.
- **In-flight per-feed state:** Held only inside each `asyncio.Task` in `NormalizerRuntime._feed_tasks`. The runtime never persists intra-chunk state.
- **Cache:** Redis (Cloud Memorystore) used only by the notification deduplication path (`backend/pipeline/notification/notification_deduplication.py`).
- **GCS as log:** Audio bytes are append-only, keyed by fencing-token paths. Because paths are token-qualified, multiple workers holding different tokens write to disjoint namespaces.

## Key Abstractions

**`CapturedChunk`:**
- Purpose: Immutable frozen dataclass that is the contract between every capture function and the runtime. Carries `audio_bytes`, `chunk_start_time`, `chunk_end_time`, `session_id`.
- Examples: `backend/pipeline/ingestion/models.py:75-93`.
- Pattern: Value object / contract type. The yield boundary between `async def collector` and `NormalizerRuntime._process_feed` is where capture-owned vs runtime-owned responsibilities split.

**`LeasedFeed`:**
- Purpose: `TypedDict` returned from `FeedStore.acquire_feeds_batch`. Carries `id`, `name`, `source_type`, `fencing_token`, and capture-resume hints (`last_processed_filename`, `last_bookmark_time`, `source_feed_id`).
- Examples: `backend/pipeline/storage/feed_store.py:52-62`.
- Pattern: Lease / optimistic-lock token. The fencing token flows into every GCS upload path and every bookmark `UPDATE` WHERE clause.

**`NormalizerRuntime`:**
- Purpose: The orchestrator that owns the leasing loop, the heartbeat OS thread, per-feed asyncio tasks, health server, and graceful shutdown sequencing.
- Examples: `backend/pipeline/ingestion/normalizer_runtime.py:45-650+`.
- Pattern: Composition-over-inheritance runtime — the capture function is passed in as a callable (`route_capturer`), not subclassed. Invariant: no `asyncio.sleep` or `time.sleep` anywhere in the file; every wait routes through `_sleep_or_shutdown` or `Event.wait(timeout=)`.

**`HealthState`:**
- Purpose: Shared state between the runtime and the aiohttp `/healthz` handler. Fields: `startup_time`, `last_heartbeat_tick`, `feed_tasks` (held by reference so `len()` reflects live leasing).
- Examples: `backend/pipeline/ingestion/health_server.py:18-41`.
- Pattern: Read-only view struct living on the event-loop thread (no lock).

**`FeedStore` / `SyncFeedStore`:**
- Purpose: Repository pattern over AlloyDB. `FeedStore` is async (used by the runtime and management services); `SyncFeedStore` is the sync mirror used by the Echo Cloud Function where `functions_framework` forbids asyncio.
- Examples: `backend/pipeline/storage/feed_store.py:90`, `backend/pipeline/storage/sync_feed_store.py`.
- Pattern: Repository. SQL lives in sibling `_queries.py` files so SQL review is separate from Python control flow.

**`SourceType`:**
- Purpose: `StrEnum` of supported source kinds (`bcfy_feeds`, `bcfy_calls`, `echo`, `openmhz`). Must be kept in sync with `terraform/modules/alloydb/sql/ingestion/002_source_types.sql` and `006_seed_source_types.sql`.
- Examples: `backend/pipeline/storage/feed_store.py:30-49`.

**`retry_with_lease_check` / `LeaseExpiredError`:**
- Purpose: Generic retry helper that aborts immediately if the lease is lost during a backoff. Every ingestion I/O (GCS upload, bookmark write) uses it.
- Examples: `backend/pipeline/ingestion/retry.py:18-124`.
- Pattern: Decorator-style wrapper with cooperative lease + shutdown cancellation. Preserves the runtime's `SIGTERM`-interruptibility invariant.

**`AudioChunk` / `TranscribedAudio` / `EvaluatedTranscribedAudio` / `AlertNotification` protobufs:**
- Purpose: Wire format for Pub/Sub messages between stages.
- Examples: `protos/raw_audio_chunk.proto`, `protos/transcribed_audio.proto`, `protos/evaluated_transcribed_audio.proto`, `protos/alert_notification.proto`.
- Pattern: Protobuf schema contract. Generated Python bindings live under `backend/pipeline/schema_types/` and are regenerated with `mise run generate:protos`.

**Beam DoFn chain (`ParseAndKeyFn` → `DownloadAudioFn` → `RestoreOrderFn` → `StitchAudioFn` → `TranscribeAudioFn` → `SerializeToPubSubMessageFn`):**
- Purpose: Streaming transcription DAG with keyed windowing and a dead-letter queue.
- Examples: `backend/pipeline/transcription/orchestration.py`, `backend/pipeline/transcription/transforms.py`, `backend/pipeline/transcription/stitcher.py`.
- Pattern: Apache Beam DoFn composition. `MAIN_TAG` / `DEAD_LETTER_QUEUE_TAG` (`backend/pipeline/transcription/constants.py`) split successful outputs from failures.

## Entry Points

**`backend/pipeline/ingestion/main.py:main`:**
- Location: `backend/pipeline/ingestion/main.py`.
- Triggers: The Docker container started on each MIG instance by the cloud-init template in `terraform/modules/container_mig/cloud_config.yaml.tftpl`.
- Responsibilities: Calls `setup_logging()`, resolves topic paths for every supported source type at startup (fail-fast on misconfiguration), constructs `NormalizerRuntime(route_capturer, NormalizerSettings(...))`, and calls `runtime.run()` which blocks until graceful shutdown.

**`backend/pipeline/ingestion/collectors/echo/main.py:handle_notification`:**
- Location: `backend/pipeline/ingestion/collectors/echo/main.py`.
- Triggers: Eventarc-delivered `OBJECT_FINALIZE` CloudEvent on the Echo recordings bucket. Decorated with `@functions_framework.cloud_event`.
- Responsibilities: Guards on env vars, lazily initializes GCS / Pub/Sub / `SyncFeedStore` singletons across warm invocations, and delegates to `_handle(cloud_event)` which runs the upload + publish sync pipeline.

**`backend/pipeline/evaluation/main.py:evaluate_transcribed_audio_segment`:**
- Location: `backend/pipeline/evaluation/main.py`.
- Triggers: Pub/Sub-delivered CloudEvent on the transcription output topic.
- Responsibilities: Delegates to `EvaluationEventProcessor.process_event`.

**`backend/pipeline/notification/send_notification.py` (Cloud Function):**
- Location: `backend/pipeline/notification/send_notification.py`.
- Triggers: Pub/Sub-delivered CloudEvent on the alerts topic.
- Responsibilities: Deduplicate, forward to external notification endpoint.

**`backend/pipeline/transcription/main.py:main`:**
- Location: `backend/pipeline/transcription/main.py`.
- Triggers: Invoked by Dataflow when launching the streaming job.
- Responsibilities: Parse Beam `PipelineOptions` and `TranscriptionOptions`, build the DAG via `get_pipeline`, call `pipeline.run().wait_until_finish()`.

**`backend/services/feeds/main.py` (FastAPI app):**
- Location: `backend/services/feeds/main.py`.
- Triggers: HTTP requests on `/v1/feeds*`. Cloud Run container.
- Responsibilities: OIDC-authenticated CRUD over the `feeds` table via `FeedService`. Lifespan creates/tears down the AlloyDB pool.

**`backend/services/transcripts/main.py` (FastAPI app):**
- Location: `backend/services/transcripts/main.py`.
- Triggers: HTTP requests on `/v1/transcripts*`.
- Responsibilities: OIDC-authenticated CRUD + keyset-paginated listing over the `transcripts` table.

**`backend/pipeline/rules/main.py` (FastAPI app):**
- Location: `backend/pipeline/rules/main.py`.
- Triggers: HTTP requests on `/v1/rules*`.
- Responsibilities: OIDC-authenticated CRUD over the `rules` table. Captures `created_by` from the verified OIDC claim.

**`GET /healthz` (aiohttp on the MIG worker):**
- Location: `backend/pipeline/ingestion/health_server.py`.
- Triggers: GCP health check at `0.0.0.0:8080/healthz` (the port is hardcoded in both `terraform/modules/container_mig/cloud_config.yaml.tftpl` and `google_compute_health_check` — do not override `HEALTH_CHECK_PORT` in production).
- Responsibilities: Three gates — (0) startup grace, (1) heartbeat-tick freshness (2× `HEARTBEAT_INTERVAL_SEC`), and an intentional no-op on "zero active feeds" to avoid fleet-wide autohealer kills when upstream idles.

**`backend/pipeline/ingestion/broadcastify_credential_rotation/main.py`:**
- Location: `backend/pipeline/ingestion/broadcastify_credential_rotation/main.py`.
- Triggers: Scheduled invocation (Cloud Scheduler → Pub/Sub → Cloud Function).
- Responsibilities: Rotate scraped Broadcastify credentials used by BCFY collectors.

**Frontend SPA `frontend/transcription-ui/src/main.tsx`:**
- Location: `frontend/transcription-ui/src/main.tsx`.
- Triggers: Browser load of the Firebase-hosted SPA.
- Responsibilities: Wraps `App` with `GoogleOAuthProvider`, `AuthProvider`, `BrowserRouter`, and `QueryClientProvider`. Routes in `App.tsx` map `/`, `/rules`, `/feeds`, `/docs`.

## Error Handling

**Strategy:** Fail-fast for invariant violations (fence violations, event-loop stalls), fail-soft with retry for transient I/O, fail-closed with quarantine for persistently-broken feeds. Always preserve enough state in AlloyDB that the next worker can resume exactly where the old one stopped.

**Patterns:**
- **Retry with lease check:** `retry_with_lease_check` (`backend/pipeline/ingestion/retry.py`) is the single chokepoint for all retryable I/O in the runtime. Exponential backoff with jitter, bounded by `max_delay_sec`, interruptible by `lease_lost` or `shutdown` events.
- **Fencing token + `os._exit(1)`:** When `FeedStore.update_feed_progress` returns zero rows, the runtime calls `logging.shutdown()` then `os._exit(1)`. Rationale documented at `backend/pipeline/ingestion/normalizer_runtime.py:458-475`: batched heartbeats mean a single stolen lease implies systemic heartbeat failure; cancelling one task among 249 compromised peers is unsafe.
- **Graceful-shutdown vs hard-kill distinction:** `SIGTERM` / `SIGINT` set `_shutdown` and `_thread_stop`. All waits inside the runtime poll both. A stuck event loop is detected by `_heartbeat_loop`'s `concurrent.futures.TimeoutError` watchdog — the comment explicitly warns that Python 3.11+ aliases `asyncio.TimeoutError` to the built-in `TimeoutError`, which is a *different* class from `concurrent.futures.TimeoutError` (a bare `except TimeoutError` would silently defeat the watchdog).
- **Quarantine on failure threshold:** `FeedStore.report_feed_failure` increments `failure_count`; when it crosses `FEED_FAILURE_THRESHOLD`, status becomes `quarantined`. `quarantine_telemetry.emit_quarantine_event` emits a structured log and a `custom.googleapis.com/feeds/quarantine_events` Cloud Monitoring metric.
- **60s abandonment window safety net:** If any release/failure-reporting write fails, the runtime returns cleanly and the abandonment window in the DB lets another worker re-lease the feed after `ABANDONMENT_WINDOW_SEC`.
- **Dead-letter queue in transcription:** Beam pipeline tags failed elements with `DEAD_LETTER_QUEUE_TAG` and writes them to a DLQ Pub/Sub topic with `error_type` attribute (`backend/pipeline/transcription/orchestration.py`).
- **Tenacity retry on AlloyDB pool init:** `create_pool_with_retry` uses `tenacity` with exponential backoff, stop after 5 attempts, for Cloud Run cold-start pool contention.
- **CancelledError discipline:** The runtime always `raise`s `CancelledError` from the shutdown path rather than suppressing it. Capture functions are contractually forbidden (see `backend/pipeline/ingestion/models.py:52-60`) from swallowing `CancelledError`.
- **HTTP error mapping in FastAPI services:** Service-layer exceptions (`AlreadyExistsError`, `ValueError`) map to `HTTPException` with 409 / 400 codes in `backend/services/*/main.py`.

## Cross-Cutting Concerns

**Logging:** `backend/pipeline/common/logging.py:setup_logging` is `@functools.cache`-memoized and switches between `google.cloud.logging` (when `is_gcp_env()` is true) and stdlib `basicConfig` otherwise. Before any `os._exit(1)`, the runtime calls `logging.shutdown()` to flush the Cloud Logging background thread. `is_gcp_env` lives in `backend/pipeline/common/env.py`.

**Monitoring:** `backend/pipeline/common/clients/monitoring_client.py` writes custom metrics lazily (gRPC channel opens on first write). Used today by `quarantine_telemetry` (`backend/pipeline/ingestion/quarantine_telemetry.py`).

**Validation:** Pydantic models for REST payloads (`backend/services/feeds/models.py`, `backend/services/transcripts/models.py`, `backend/pipeline/common/rules/models.py`). Protobufs for Pub/Sub payloads. `SourceType` enum is DB-invariant and must match the seed SQL at `terraform/modules/alloydb/sql/ingestion/006_seed_source_types.sql`.

**Authentication:** `backend/pipeline/common/auth.py:verify_oidc_token` is a FastAPI `Depends` that returns OIDC claims. In local dev (`is_gcp_env()==False`) it returns a stub `local-dev@example.com` claim. Service-to-service calls use `get_id_token(audience)` via the GCE metadata server. The frontend authenticates users with Google OAuth (`@react-oauth/google`).

**Concurrency model:** `asyncio` on `uvloop` as the event loop implementation (`asyncio.run(self._main(), loop_factory=uvloop.new_event_loop)`). One OS daemon thread only — the heartbeat thread — to stay immune to event-loop starvation. All inter-thread signaling uses `threading.Event` for the thread side and `asyncio.Event` for the loop side; `call_soon_threadsafe` bridges OS → loop (`asyncio.Event.set()` is not thread-safe).

**DB pooling:** Main data pool sized to the feed concurrency. A dedicated 1-connection `heartbeat_pool` separates control-plane from data-plane so bookmark contention cannot starve heartbeats. `statement_cache_size=0` is required for PgBouncer transaction-mode pooling (AlloyDB on port 6432).

**HTTP connection pooling:** `GcsClient` wraps `aiohttp.ClientSession` with a `TCPConnector` sized to `max_feeds_per_worker` to prevent uploads from queuing behind a 100-connection default cap.

**Secrets:** `google-cloud-secret-manager` for BCFY credentials and any downstream endpoint API keys; env-var configuration elsewhere. Never read `.env*`, `*secret*`, `*credential*`, `*.pem`, `*.key`, `id_rsa*` files directly — pyproject.toml declares `google-cloud-secret-manager>=2.26.0` as a runtime dependency.

**Configuration:** Per-service dataclass settings pattern (`NormalizerSettings`, `AlloyDBSettings`) loaded from env vars with `default_factory` closures and fail-fast `_require_env` for mandatory values.

**Tests:** Pytest with `pytest-asyncio`. `testcontainers[postgres]` + `fakeredis` + `httpx` for integration; module-local `tests/` folders for unit tests (see STRUCTURE.md for the full map).

---

*Architecture analysis: 2026-04-21*

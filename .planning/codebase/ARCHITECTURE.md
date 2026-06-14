# Architecture

**Analysis Date:** 2026-06-14

## Pattern Overview

**Overall:** Event-driven radio transcription monorepo with VM collectors,
serverless processing functions, FastAPI management services, React UI, and
Terraform-managed Google Cloud infrastructure.

**Key Characteristics:**
- Audio ingestion is feed-leased and stateful; most downstream stages are
  event-driven and claim-check GCS objects through Pub/Sub.
- Feed lifecycle state is centralized in AlloyDB through
  `backend/pipeline/storage/feed_store.py`.
- Collector code owns source-specific capture and classification; runtime code
  owns leases, upload, bookmark, publish, heartbeats, and quarantine telemetry.
- Protocol buffers are the stable message contract between pipeline stages.
- Frontend API proxy and UI consume management services rather than touching
  storage directly.

## Layers

**Source Collector Layer:**
- Purpose: Convert source-specific radio/audio APIs into `CapturedChunk`,
  `SourceObservation`, or typed `FeedFailure` events.
- Contains: collectors in `backend/pipeline/ingestion/collectors/`.
- Depends on: `aiohttp`, source-specific APIs, shared classifiers, and
  ingestion models.
- Used by: `CollectorRuntime` through `router.py`.

**Collector Runtime Layer:**
- Purpose: Lease feeds, spawn per-feed tasks, maintain heartbeats, process
  capture events, upload audio, write bookmarks, publish Pub/Sub messages, and
  record failure state.
- Contains: `backend/pipeline/ingestion/collector_runtime.py`,
  `settings.py`, `router.py`, `retry.py`, `health_server.py`.
- Depends on: storage, GCS/PubSub helpers, runtime settings, and collectors.
- Used by: `backend/pipeline/ingestion/main.py` and source-specific images.

**Storage Layer:**
- Purpose: Provide atomic database operations for feeds, audio segments,
  transcripts, and rules.
- Contains: `backend/pipeline/storage/*_store.py` and `*_queries.py`.
- Depends on: asyncpg/psycopg and SQL schema in Terraform migrations.
- Used by: runtime workers, FastAPI services, and some pipeline functions.

**Pipeline Processing Layer:**
- Purpose: Normalize raw chunks, transcribe normalized transmissions, evaluate
  rules, and send notifications.
- Contains:
  - `backend/pipeline/normalization/`
  - `backend/pipeline/transcription/`
  - `backend/pipeline/evaluation/`
  - `backend/pipeline/notification/`
- Depends on: Pub/Sub, GCS, protobufs, audio libraries, Speech API, Redis, and
  management APIs.
- Used by: Cloud Functions, Beam pipelines, and local Docker services.

**Management API Layer:**
- Purpose: CRUD/list/reset operations for feeds, transcripts, rules, and audio
  segments.
- Contains: FastAPI apps under `backend/services/*`.
- Depends on: storage layer and common OIDC/tracing helpers.
- Used by: frontend API proxy, integration tests, operators.

**Frontend Layer:**
- Purpose: Browser UI and API proxy for feeds, transcripts, rules, audio, and
  docs.
- Contains: `frontend/api`, `frontend/common`, and
  `frontend/transcription-ui`.
- Depends on: backend HTTP services, Google auth, React, Material UI, and
  shared TypeScript types.

**Infrastructure Layer:**
- Purpose: Define cloud resources and local development environment.
- Contains: `terraform/modules/`, `docker-compose.yml`, local init scripts,
  and `.github/workflows/`.
- Depends on: Google Cloud resources and container images.

## Data Flow

**VM Audio Ingestion:**

1. `CollectorRuntime` leases feeds through `FeedStore.acquire_feeds_batch`.
2. Router dispatches each feed to a source-specific collector.
3. Collector yields `CapturedChunk` for audio or `SourceObservation` for a
   successful non-audio check.
4. Runtime uploads raw audio to GCS with `upload_staged_audio`.
5. Runtime updates feed progress/bookmark with fencing protection.
6. Runtime publishes `AudioChunk` protobuf to the continuous or segmented
   Pub/Sub topic with ordering key `feed_id`.
7. Runtime emits structured SLO logs after publish success.

**Post-Capture Pipeline:**

1. Normalization reads raw chunk claim-check messages.
2. Beam/audio transforms stitch, classify, and emit normalized audio.
3. Transcription Cloud Function reads normalized claim-checks and calls the
   configured transcriber.
4. Evaluation applies rules and writes annotations.
5. Notification sends alert notifications with Redis-backed deduplication.

**Feed Management:**

1. React UI calls frontend proxy services in `frontend/api/src`.
2. Proxy calls backend FastAPI services.
3. FastAPI service delegates to a service class such as `FeedService`.
4. Service calls storage operations such as `FeedStore.reset_feed`.
5. Response models map backend states into frontend-friendly types.

**State Management:**
- Feed lifecycle, bookmarks, status reasons, and failure counts live in
  AlloyDB.
- Raw and canonical audio live in GCS.
- Pub/Sub carries ordered claim-check messages.
- Runtime feed leases are protected by worker IDs, fencing tokens, heartbeat
  renewal, and abandonment recovery.

## Key Abstractions

**Feed / LeasedFeed:**
- Purpose: A configured source and a runtime-owned lease on that source.
- Examples: `Feed`, `LeasedFeed`, `FeedStatus`, and `FeedStatusReason` in
  `backend/pipeline/storage/feed_store.py`.
- Pattern: typed dicts/enums over AlloyDB rows.

**CaptureEvent:**
- Purpose: Union of audio progress and successful non-audio source contact.
- Examples: `CapturedChunk`, `SourceObservation`, and `FeedFailure` in
  `backend/pipeline/ingestion/models.py`.
- Pattern: collector/runtime boundary object.

**FeedStore:**
- Purpose: Repository-like storage facade for feed lifecycle operations.
- Examples: `report_feed_failure`, `record_source_observation`,
  `update_feed_progress`, `acquire_feeds_batch`.
- Pattern: async store backed by query constants.

**ItemBatchOutcome / FailureClassification:**
- Purpose: Keep item-scoped failures from becoming feed-level failures unless
  an observation boundary fails completely.
- Location: `backend/pipeline/ingestion/collectors/failure_classification.py`.
- Pattern: explicit classification and promotion helper.

**Protobuf Claim Checks:**
- Purpose: Move small structured messages through Pub/Sub while audio lives in
  GCS.
- Examples: `AudioChunk`, `NormalizedAudio`, `TranscribedAudio`,
  `EvaluatedTranscribedAudio`, `AlertNotification`.

## Entry Points

**VM Ingestion:**
- `backend/pipeline/ingestion/main.py` - starts collector runtime and verifies
  registry/cap invariants.
- `backend/pipeline/ingestion/health_server.py` - health endpoint for worker
  readiness/liveness.

**Cloud Functions / Pipelines:**
- `backend/pipeline/transcription/main.py`
- `backend/pipeline/evaluation/main.py`
- `backend/pipeline/notification/send_notification.py`
- `backend/pipeline/normalization/main.py`

**FastAPI Services:**
- `backend/services/feeds/main.py`
- `backend/services/transcripts/main.py`
- `backend/services/rules/main.py`
- `backend/services/audio_segments/main.py`

**Frontend:**
- `frontend/api/src/index.ts` - Express/tsoa API app.
- `frontend/transcription-ui/src/main.tsx` and `App.tsx` - React app entry.

## Error Handling

**Strategy:**
- Collectors raise typed `FeedFailure` only for classified feed-level
  source/system evidence.
- Runtime raises private `_PipelineFailure` for post-capture side-effect
  stages such as GCS upload, bookmark write, and Pub/Sub publish.
- Unexpected runtime or collector exceptions fall back to
  `system_unexpected_error`.
- FastAPI services convert validation/storage errors to HTTP exceptions.

**Important Current Behavior:**
- `FeedFailure`, `_PipelineFailure`, and unexpected exceptions currently route
  through `_record_feed_failure` and `FeedStore.report_feed_failure`, so all
  can consume the same persisted quarantine threshold. This is the main design
  pressure behind the quarantine policy redesign.

## Cross-Cutting Concerns

**Logging:**
- Python uses structured `json_fields` where logs are consumed by SLOs or
  operational workflows.
- Frontend API centralizes Express error handling in `frontend/api/src/index.ts`.

**Validation:**
- Pydantic models validate backend service inputs/outputs.
- TypeScript shared types live in `frontend/common/src/types/`.
- Protobuf schemas define inter-stage message compatibility.

**Authentication:**
- Backend APIs depend on OIDC verification.
- Frontend proxy and UI manage session/auth separately from backend storage.

**Concurrency:**
- Ingestion workers use asyncio tasks, a separate heartbeat thread, fenced DB
  writes, per-type feed caps, and cgroup-aware RSS backpressure.

---

*Architecture analysis: 2026-06-14*
*Update when major patterns change*

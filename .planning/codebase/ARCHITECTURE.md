<!-- refreshed: 2026-05-24 -->
# Architecture

**Analysis Date:** 2026-05-24

## System Overview

```text
radio-transcription/

  Audio sources
  `backend/pipeline/ingestion/collectors/`
        |
        v
  Feed leasing and capture runtime
  `backend/pipeline/ingestion/normalizer_runtime.py`
        |
        | staged audio in GCS + `AudioChunk` Pub/Sub claim check
        v
  Streaming normalization and stitching
  `backend/pipeline/normalization/orchestration.py`
        |
        | `NormalizedAudio` Pub/Sub claim check
        v
  Transcription function
  `backend/pipeline/transcription/main.py`
        |
        | `TranscribedAudio` Pub/Sub
        v
  Rules evaluation function
  `backend/pipeline/evaluation/main.py`
        |
        | writes transcript + publishes alert candidates
        v
  Notification function and UI/query services
  `backend/pipeline/notification/send_notification.py`
  `backend/services/*/main.py`
        |
        v
  AlloyDB, GCS, Pub/Sub, Redis, frontend API, React UI
  `backend/pipeline/storage/`
  `frontend/api/src/`
  `frontend/transcription-ui/src/`
```

## Component Responsibilities

| Component | Responsibility | File |
|-----------|----------------|------|
| Ingestion runtime | Lease feeds from AlloyDB, run one async capture task per feed, upload captured chunks, publish raw audio claim-check messages, heartbeat leases, quarantine repeated failures. | `backend/pipeline/ingestion/normalizer_runtime.py` |
| Ingestion router | Map `SourceType` values to collector functions and continuous vs segmented Pub/Sub topics. | `backend/pipeline/ingestion/router.py` |
| Source collectors | Connect to source-specific upstreams and yield `CapturedChunk` values without owning DB writes, GCS uploads, or Pub/Sub publishing. | `backend/pipeline/ingestion/collectors/` |
| Echo collector | Handle object notification events for Echo recordings and publish raw audio chunks through the shared helper path. | `backend/pipeline/ingestion/collectors/echo/main.py` |
| Normalization pipeline | Define the Apache Beam streaming DAG for parse, ordering, stitching, normalization, serialization, and DLQ routing. | `backend/pipeline/normalization/orchestration.py` |
| Normalization transforms | Implement stateless parsing/serialization and stateful continuous/segmented stitching and audio normalization. | `backend/pipeline/normalization/transforms/stateless.py`, `backend/pipeline/normalization/transforms/stateful.py` |
| Transcription function | Decode `NormalizedAudio`, invoke the selected transcriber, and publish `TranscribedAudio`. | `backend/pipeline/transcription/main.py`, `backend/pipeline/transcription/processor.py` |
| Transcriber implementations | Provide the pluggable speech-to-text interface and concrete Google Chirp/mock implementations. | `backend/pipeline/transcription/transcribers/base.py`, `backend/pipeline/transcription/transcribers/factory.py` |
| Evaluation function | Decode `TranscribedAudio`, evaluate rules, write evaluated transcripts through the Transcripts API, and publish alert candidates. | `backend/pipeline/evaluation/main.py`, `backend/pipeline/evaluation/processor.py` |
| Rules evaluators | Evaluate text against static rules or rules fetched from the Rules API. | `backend/pipeline/evaluation/rules_evaluation/evaluator.py` |
| Notification function | Deduplicate alert candidates, enrich with feed tags, build `AlertNotification`, and POST it to the configured downstream endpoint. | `backend/pipeline/notification/send_notification.py` |
| Management services | Expose FastAPI CRUD/read APIs for feeds, rules, and transcripts over the shared storage layer. | `backend/services/feeds/main.py`, `backend/services/rules/main.py`, `backend/services/transcripts/main.py` |
| Storage layer | Own asyncpg/psycopg access, SQL constants, row-to-model/protobuf mapping, feed leasing, transcripts, rules, and audio segment persistence. | `backend/pipeline/storage/` |
| Frontend API facade | Expose public TSOA/Express routes, perform Google auth/session work, translate camelCase UI DTOs to backend snake_case APIs, and call backend services with ID tokens. | `frontend/api/src/index.ts`, `frontend/api/src/**/*Controller.ts` |
| React UI | Provide authenticated transcript search/playback, feed management, rules view, and API docs views. | `frontend/transcription-ui/src/main.tsx`, `frontend/transcription-ui/src/App.tsx` |
| Shared frontend types | Export TypeScript DTOs used by the API facade and React UI. | `frontend/common/src/index.ts`, `frontend/common/src/types/` |
| Model evaluation workspace | Hold notebooks, common evaluation helpers, manifests, and source-fetch scripts for ASR model work. | `model/colabs/`, `model/colabs/common/`, `model/data_sources/` |
| Infrastructure modules | Define reusable Terraform modules for AlloyDB, Redis, GCS buckets, Cloud Functions, MIGs, and ASR evaluation VMs. | `terraform/modules/` |

## Pattern Overview

**Overall:** Event-driven claim-check pipeline with HTTP management services and a frontend facade.

**Key Characteristics:**
- Use Pub/Sub payloads as protobuf claim checks and put audio bytes in GCS (`backend/pipeline/common/gcp_helper.py:40`, `backend/pipeline/common/gcp_helper.py:214`, `backend/pipeline/common/gcp_helper.py:267`).
- Keep capture functions source-specific and side-effect-light; the runtime owns upload, publish, bookmarks, failures, quarantine, heartbeats, lease release, and timeouts (`backend/pipeline/ingestion/models.py:1`).
- Keep database ownership in stores under `backend/pipeline/storage/`; service layers compose stores and HTTP handlers translate errors (`backend/pipeline/storage/feed_store.py:126`, `backend/services/feeds/main.py:26`).
- Use generated protobuf schemas from `protos/` as pipeline contracts and regenerate Python bindings into `backend/pipeline/schema_types/` (`backend/pipeline/README.md`, `.mise.toml:140`).
- Use warmed module-level or container-level clients in Cloud Function entry points for cold-start mitigation (`backend/pipeline/transcription/main.py:27`, `backend/pipeline/evaluation/main.py:20`, `backend/pipeline/notification/send_notification.py:72`).

## Layers

**Schema Layer:**
- Purpose: Define wire contracts for raw, normalized, transcribed, evaluated, alert, and state payloads.
- Location: `protos/`, `backend/pipeline/schema_types/`
- Contains: `.proto` files and generated Python modules such as `backend/pipeline/schema_types/streaming_state.py`.
- Depends on: Protocol Buffers and generated code.
- Used by: `backend/pipeline/common/gcp_helper.py`, `backend/pipeline/normalization/`, `backend/pipeline/transcription/`, `backend/pipeline/evaluation/`, `backend/pipeline/notification/`, `backend/services/transcripts/`.

**Ingestion Layer:**
- Purpose: Lease configured feeds, run source capture loops, upload staged audio, and publish raw audio claim checks.
- Location: `backend/pipeline/ingestion/`
- Contains: `NormalizerRuntime`, source collectors, health server, retry helpers, SLO telemetry, settings, source routing.
- Depends on: `backend/pipeline/storage/feed_store.py`, `backend/pipeline/common/gcp_helper.py`, `backend/pipeline/common/clients/`, `backend/pipeline/storage/settings.py`.
- Used by: `backend/pipeline/ingestion/main.py` and containerized local/prod runners.

**Normalization Layer:**
- Purpose: Convert raw captured chunks into normalized transmission-level audio claim checks.
- Location: `backend/pipeline/normalization/`
- Contains: Beam options, orchestration DAG, stateful and stateless transforms, audio DSP/VAD, stitching state machines.
- Depends on: Apache Beam, GCS, Pub/Sub, generated protobufs, audio models under `backend/pipeline/normalization/audio/models/`.
- Used by: `backend/pipeline/normalization/main.py` and the normalization container.

**Serverless Processing Layer:**
- Purpose: Process Pub/Sub events after normalization: transcription, rules evaluation, notification.
- Location: `backend/pipeline/transcription/`, `backend/pipeline/evaluation/`, `backend/pipeline/notification/`
- Contains: Functions Framework entry points, processor classes, transcriber/evaluator interfaces, notification dedup/request handling.
- Depends on: Google Cloud Speech, Pub/Sub, Transcripts/Rules/Feeds APIs, Redis, external notification endpoint.
- Used by: Cloud Functions or local `functions-framework` commands in `docker-compose.yml`.

**Persistence Layer:**
- Purpose: Encapsulate AlloyDB and Redis access behind typed stores/clients.
- Location: `backend/pipeline/storage/`, `backend/pipeline/common/storage/`
- Contains: `FeedStore`, `TranscriptStore`, `RulesStore`, `AudioSegmentStore`, async/sync connection factories, SQL query modules, Redis cache providers.
- Depends on: asyncpg, psycopg, Redis, SQL schema under `terraform/modules/alloydb/sql/ingestion/`.
- Used by: ingestion runtime, FastAPI services, notification deduplication.

**Backend API Layer:**
- Purpose: Provide authenticated internal CRUD/read HTTP APIs for persisted feeds, rules, and transcripts.
- Location: `backend/services/`
- Contains: FastAPI apps, Pydantic models, service classes.
- Depends on: `backend/pipeline/storage/`, `backend/pipeline/common/auth.py`.
- Used by: frontend API facade and pipeline processors.

**Frontend Facade Layer:**
- Purpose: Provide public Express/TSOA routes, auth session endpoints, OpenAPI docs, and typed translation into backend service APIs.
- Location: `frontend/api/src/`
- Contains: `index.ts`, TSOA controllers, config validation, auth middleware, generated route target configured by `frontend/api/tsoa.json`.
- Depends on: `@transcription/common`, Google auth libraries, backend service URLs, API Gateway.
- Used by: React UI and API Gateway backend config.

**Frontend UI Layer:**
- Purpose: Provide interactive views for transcripts, feeds, rules, docs, login, and audio playback.
- Location: `frontend/transcription-ui/src/`
- Contains: React routes, context providers, service functions, MUI components, query/cache logic, audio playback logic.
- Depends on: React, React Router, TanStack Query, MUI, Howler/Wavesurfer, `@transcription/common`.
- Used by: browser users.

**Model Evaluation Layer:**
- Purpose: Support ASR evaluation notebooks, reusable inference/scoring helpers, manifests, and data source selection scripts.
- Location: `model/`
- Contains: notebooks in `model/colabs/`, importable helpers in `model/colabs/common/`, manifests in `model/data/`, source scripts in `model/data_sources/`.
- Depends on: optional model extras in `model/pyproject.toml`, notebook Docker images, GCS.
- Used by: ASR evaluation workflows described in `ASR_CONTRIBUTING.md`.

**Infrastructure Layer:**
- Purpose: Define reusable deployable resources for storage, compute, functions, Redis, GCS, and ASR evaluation machines.
- Location: `terraform/modules/`
- Contains: Terraform modules and AlloyDB SQL migration files.
- Depends on: Google Cloud Terraform providers and deployment orchestration outside this repo.
- Used by: deployment repositories or operators composing these modules.

## Data Flow

### Primary Audio Processing Path

1. Feed capture starts in `backend/pipeline/ingestion/main.py:14`, loads `NormalizerSettings`, verifies router/cap registries, and runs `NormalizerRuntime`.
2. `NormalizerRuntime` initializes DB pools, feed stores, heartbeat/watchdog threads, a shared `aiohttp.ClientSession`, GCS and Pub/Sub clients, and the `/healthz` server (`backend/pipeline/ingestion/normalizer_runtime.py:178`, `backend/pipeline/ingestion/normalizer_runtime.py:204`, `backend/pipeline/ingestion/normalizer_runtime.py:273`, `backend/pipeline/ingestion/normalizer_runtime.py:290`).
3. The leasing loop acquires feeds through `FeedStore.acquire_feeds_batch` and recovery SQL, then spawns per-feed tasks (`backend/pipeline/ingestion/normalizer_runtime.py:410`, `backend/pipeline/ingestion/normalizer_runtime.py:490`, `backend/pipeline/storage/feed_store.py:419`).
4. `route_capturer` dispatches each leased feed by `SourceType` to a collector (`backend/pipeline/ingestion/router.py:41`, `backend/pipeline/ingestion/router.py:80`).
5. Collectors yield `CapturedChunk`; the runtime uploads staged bytes to GCS, publishes an `AudioChunk` Pub/Sub message, and writes a fenced bookmark (`backend/pipeline/ingestion/models.py:108`, `backend/pipeline/ingestion/normalizer_runtime.py:778`, `backend/pipeline/ingestion/normalizer_runtime.py:824`, `backend/pipeline/ingestion/normalizer_runtime.py:853`, `backend/pipeline/ingestion/normalizer_runtime.py:871`).
6. Beam reads continuous and segmented raw-audio topics, parses and keys messages, stitches/normalizes audio, and writes `NormalizedAudio` claim checks to an output topic plus DLQ messages (`backend/pipeline/normalization/orchestration.py:112`, `backend/pipeline/normalization/orchestration.py:122`, `backend/pipeline/normalization/orchestration.py:130`, `backend/pipeline/normalization/orchestration.py:176`, `backend/pipeline/normalization/orchestration.py:189`, `backend/pipeline/normalization/orchestration.py:207`, `backend/pipeline/normalization/orchestration.py:221`, `backend/pipeline/normalization/orchestration.py:224`, `backend/pipeline/normalization/orchestration.py:237`).
7. The transcription CloudEvent entry point decodes `NormalizedAudio`, calls the configured transcriber, builds `TranscribedAudio`, and publishes it ordered by feed ID (`backend/pipeline/transcription/main.py:117`, `backend/pipeline/transcription/processor.py:49`, `backend/pipeline/transcription/processor.py:63`, `backend/pipeline/transcription/processor.py:95`, `backend/pipeline/transcription/processor.py:107`, `backend/pipeline/transcription/processor.py:134`).
8. The evaluation CloudEvent entry point decodes `TranscribedAudio`, evaluates rules, writes the evaluated transcript to the Transcripts API, and publishes alert candidates when decisions or errors exist (`backend/pipeline/evaluation/main.py:54`, `backend/pipeline/evaluation/processor.py:55`, `backend/pipeline/evaluation/processor.py:90`, `backend/pipeline/evaluation/processor.py:103`, `backend/pipeline/evaluation/processor.py:111`).
9. The notification CloudEvent entry point parses evaluated messages, deduplicates with Redis, fetches feed tags from the Feeds API, builds `AlertNotification`, and sends it to the configured notification endpoint (`backend/pipeline/notification/send_notification.py:143`, `backend/pipeline/notification/send_notification.py:151`, `backend/pipeline/notification/send_notification.py:156`, `backend/pipeline/notification/send_notification.py:163`, `backend/pipeline/notification/send_notification.py:166`, `backend/pipeline/notification/request_handler.py:32`).

### Management API/UI Path

1. React mounts with Google OAuth, auth context, router, and TanStack Query providers (`frontend/transcription-ui/src/main.tsx:16`).
2. `App` gates authenticated routes, centralizes API error presentation, and routes to feed search, transcripts, rules, feeds, docs, and login views (`frontend/transcription-ui/src/App.tsx:31`, `frontend/transcription-ui/src/App.tsx:154`, `frontend/transcription-ui/src/App.tsx:187`).
3. UI service functions call the frontend API base URL with bearer tokens and common `apiFetch` error handling (`frontend/transcription-ui/src/service/listTranscripts.ts:5`, `frontend/transcription-ui/src/utils/apiUtils.ts:3`).
4. The frontend API Express app registers generated TSOA routes and a centralized error handler (`frontend/api/src/index.ts:10`, `frontend/api/src/index.ts:28`, `frontend/api/src/index.ts:30`).
5. TSOA controllers call backend FastAPI services with Google ID token clients and translate snake_case backend DTOs into UI/common TypeScript shapes (`frontend/api/src/transcripts/transcriptsController.ts:37`, `frontend/api/src/transcripts/transcriptsController.ts:90`, `frontend/api/src/feeds/feedsController.ts:94`, `frontend/api/src/feeds/feedsController.ts:123`, `frontend/api/src/rules/rulesController.ts:85`, `frontend/api/src/rules/rulesController.ts:220`).
6. FastAPI services create an AlloyDB pool during lifespan startup, attach service instances to `app.state`, and route requests through service/store classes (`backend/services/transcripts/main.py:22`, `backend/services/feeds/main.py:26`, `backend/services/rules/main.py:22`).
7. Stores execute SQL through asyncpg pools and map rows to Pydantic or protobuf models (`backend/pipeline/storage/transcript_store.py:31`, `backend/pipeline/storage/rules_store.py:15`, `backend/pipeline/storage/feed_store.py:126`).

**State Management:**
- Feed processing state lives in AlloyDB feed rows, fencing tokens, heartbeats, bookmarks, failure counts, and quarantine fields managed by `FeedStore` (`backend/pipeline/storage/feed_store.py:68`, `backend/pipeline/storage/feed_store.py:86`, `backend/pipeline/storage/feed_store.py:126`).
- In-memory worker state lives in `NormalizerRuntime` task maps, shutdown events, heartbeat/watchdog threads, and shared clients (`backend/pipeline/ingestion/normalizer_runtime.py:103`, `backend/pipeline/ingestion/normalizer_runtime.py:124`, `backend/pipeline/ingestion/normalizer_runtime.py:130`, `backend/pipeline/ingestion/normalizer_runtime.py:149`).
- Beam state lives in stateful DoFns and streaming state proto helpers (`backend/pipeline/normalization/transforms/stateful.py`, `backend/pipeline/normalization/state/`, `protos/streaming_state.proto`).
- HTTP API process state lives in FastAPI `app.state` service instances and module-level Cloud Function clients (`backend/services/transcripts/main.py:27`, `backend/pipeline/evaluation/main.py:20`, `backend/pipeline/notification/send_notification.py:72`).
- UI state lives in React context, component state, and TanStack Query caches (`frontend/transcription-ui/src/context/AuthProvider.tsx:14`, `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx:235`, `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx:455`).

## Key Abstractions

**Protobuf Contracts:**
- Purpose: Represent all pipeline event payloads and streaming state.
- Examples: `protos/raw_audio_chunk.proto`, `protos/normalized_audio.proto`, `protos/transcribed_audio.proto`, `protos/evaluated_transcribed_audio.proto`, `protos/alert_notification.proto`, `protos/streaming_state.proto`.
- Pattern: Generate Python modules into `backend/pipeline/schema_types/` and import them from processors, stores, and helpers (`backend/pipeline/README.md`).

**CapturedChunk and CollectorFn:**
- Purpose: Define the boundary between source collectors and the runtime-owned upload/publish/bookmark pipeline.
- Examples: `backend/pipeline/ingestion/models.py:1`, `backend/pipeline/ingestion/models.py:108`, `backend/pipeline/ingestion/models.py:167`.
- Pattern: Add collectors as async generators returning `CapturedChunk`, then register them in `backend/pipeline/ingestion/router.py`.

**NormalizerRuntime:**
- Purpose: Own feed leasing, task lifecycle, heartbeat, memory back-pressure, GCS upload, Pub/Sub publish, progress bookmarks, shutdown, and quarantine.
- Examples: `backend/pipeline/ingestion/normalizer_runtime.py:61`, `backend/pipeline/ingestion/normalizer_runtime.py:410`, `backend/pipeline/ingestion/normalizer_runtime.py:745`, `backend/pipeline/ingestion/normalizer_runtime.py:1018`.
- Pattern: Inject a capture function rather than subclassing the runtime (`backend/pipeline/ingestion/normalizer_runtime.py:94`).

**Store Classes:**
- Purpose: Keep persistence and SQL mapping separate from HTTP handlers and pipeline processors.
- Examples: `backend/pipeline/storage/feed_store.py:126`, `backend/pipeline/storage/transcript_store.py:31`, `backend/pipeline/storage/rules_store.py:15`, `backend/pipeline/storage/audio_segment_store.py:22`.
- Pattern: Use stores from services/runtime; add SQL constants in sibling query modules under `backend/pipeline/storage/`.

**Transcriber Interface and Factory:**
- Purpose: Select a speech-to-text implementation from configuration.
- Examples: `backend/pipeline/transcription/transcribers/base.py`, `backend/pipeline/transcription/transcribers/factory.py:18`, `backend/pipeline/transcription/main.py:35`.
- Pattern: Add a transcriber class, config model, enum value, and factory branch.

**TextEvaluator Interface:**
- Purpose: Evaluate transcribed text against active rules from static or remote sources.
- Examples: `backend/pipeline/evaluation/rules_evaluation/evaluator.py:31`, `backend/pipeline/evaluation/rules_evaluation/evaluator.py:127`, `backend/pipeline/evaluation/rules_evaluation/evaluator.py:160`.
- Pattern: Implement `BaseTextEvaluator.evaluate()` and return `EvaluationResult`.

**TSOA Frontend API Controllers:**
- Purpose: Generate OpenAPI/routes from decorated controllers and bridge the React UI to backend services.
- Examples: `frontend/api/tsoa.json:1`, `frontend/api/src/transcripts/transcriptsController.ts:66`, `frontend/api/src/feeds/feedsController.ts:119`, `frontend/api/src/rules/rulesController.ts:216`.
- Pattern: Add a `*Controller.ts` file under `frontend/api/src/<domain>/`, define `@Route`, `@Security`, DTO conversion, and regenerate routes/spec.

**React Query Views:**
- Purpose: Keep server state and polling behavior localized to view components.
- Examples: `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx:235`, `frontend/transcription-ui/src/components/feeds/FeedSearchView.tsx`, `frontend/transcription-ui/src/components/rules/RulesView.tsx`.
- Pattern: Put fetch wrappers under `frontend/transcription-ui/src/service/`, render state in components, and share DTOs through `frontend/common/src/types/`.

## Entry Points

**Feed ingestion worker:**
- Location: `backend/pipeline/ingestion/main.py:14`
- Triggers: Container command or local Docker service.
- Responsibilities: Load settings, enforce source registry invariants, run `NormalizerRuntime`.

**Echo ingestion CloudEvent function:**
- Location: `backend/pipeline/ingestion/collectors/echo/main.py:68`
- Triggers: CloudEvent object notification.
- Responsibilities: Convert Echo files into raw audio messages through sync helper/store paths.

**Oldest feed publisher HTTP function:**
- Location: `backend/pipeline/ingestion/oldest_feed_publisher/main.py:151`
- Triggers: HTTP function request.
- Responsibilities: Publish oldest-feed metrics through monitoring/Pub/Sub support code.

**Broadcastify credential rotation HTTP function:**
- Location: `backend/pipeline/ingestion/broadcastify_credential_rotation/main.py:216`
- Triggers: HTTP function request.
- Responsibilities: Rotate Broadcastify credential material using Secret Manager.

**Normalization Beam job:**
- Location: `backend/pipeline/normalization/main.py:20`
- Triggers: Container command.
- Responsibilities: Parse Beam options, build the streaming DAG, and wait for pipeline completion.

**Transcription CloudEvent function:**
- Location: `backend/pipeline/transcription/main.py:117`
- Triggers: Pub/Sub push CloudEvent carrying `NormalizedAudio`.
- Responsibilities: Transcribe canonical audio and publish `TranscribedAudio`.

**Rules evaluation CloudEvent function:**
- Location: `backend/pipeline/evaluation/main.py:54`
- Triggers: Pub/Sub push CloudEvent carrying `TranscribedAudio`.
- Responsibilities: Evaluate rules, persist evaluated transcripts, and publish alert candidates.

**Notification CloudEvent function:**
- Location: `backend/pipeline/notification/send_notification.py:143`
- Triggers: Pub/Sub push CloudEvent carrying `EvaluatedTranscribedAudio`.
- Responsibilities: Deduplicate, enrich, convert, and POST notifications.

**FastAPI services:**
- Location: `backend/services/feeds/main.py:36`, `backend/services/rules/main.py:32`, `backend/services/transcripts/main.py:32`
- Triggers: Uvicorn/Cloud Run HTTP requests.
- Responsibilities: Serve authenticated CRUD/read APIs backed by AlloyDB stores.

**Frontend API function:**
- Location: `frontend/api/src/index.ts:50`
- Triggers: Google Functions Framework target `api`.
- Responsibilities: Serve TSOA routes, auth/session endpoints, and backend service proxy requests.

**React UI:**
- Location: `frontend/transcription-ui/src/main.tsx:16`
- Triggers: Browser load.
- Responsibilities: Mount providers, route views, query APIs, and play audio.

**Model notebooks and helpers:**
- Location: `model/colabs/`, `model/colabs/common/inference_pipeline_runner.py`, `model/colabs/common/scoring.py`
- Triggers: Jupyter notebooks and local ASR evaluation scripts.
- Responsibilities: Run model inference/evaluation loops and score manifests.

## Architectural Constraints

- **Threading:** Ingestion uses asyncio with uvloop plus daemon OS threads for heartbeat and RSS watchdog; heartbeat deliberately runs outside the event loop to detect stalls (`backend/pipeline/ingestion/normalizer_runtime.py:160`, `backend/pipeline/ingestion/normalizer_runtime.py:226`, `backend/pipeline/ingestion/normalizer_runtime.py:248`, `backend/pipeline/ingestion/normalizer_runtime.py:1018`).
- **Streaming:** Normalization is forced into Beam streaming mode and reads from two unbounded Pub/Sub subscriptions (`backend/pipeline/normalization/orchestration.py:65`, `backend/pipeline/normalization/orchestration.py:71`, `backend/pipeline/normalization/orchestration.py:112`, `backend/pipeline/normalization/orchestration.py:122`).
- **Lease safety:** Feed writes and progress updates are fenced by worker ID and fencing token, with `os._exit(1)` used for lease-compromise paths (`backend/pipeline/storage/feed_store.py:229`, `backend/pipeline/ingestion/normalizer_runtime.py:893`, `backend/pipeline/ingestion/normalizer_runtime.py:1049`).
- **Global state:** Cloud Function modules cache clients/processor instances for warm starts (`backend/pipeline/transcription/main.py:112`, `backend/pipeline/evaluation/main.py:20`, `backend/pipeline/notification/send_notification.py:72`, `frontend/api/src/docs/docsController.ts:12`).
- **Generated code:** `backend/pipeline/schema_types/` is generated from `protos/`; run `.mise.toml` task `generate:protos` after schema changes (`backend/pipeline/README.md`, `.mise.toml:140`).
- **Authentication:** FastAPI services require `verify_oidc_token` in GCP and return a local-dev identity outside GCP (`backend/pipeline/common/auth.py:34`, `backend/services/feeds/main.py:36`, `backend/services/rules/main.py:32`, `backend/services/transcripts/main.py:32`).
- **Frontend backend boundary:** React calls only the frontend API; the frontend API calls backend service URLs with Google ID token clients (`frontend/transcription-ui/src/service/listTranscripts.ts:14`, `frontend/api/src/transcripts/transcriptsController.ts:90`, `frontend/api/src/feeds/feedsController.ts:123`, `frontend/api/src/rules/rulesController.ts:220`).
- **Circular imports:** No explicit circular import chains were detected by file-system import scanning; keep shared schemas/types in `protos/`, `backend/pipeline/common/`, `backend/pipeline/storage/`, and `frontend/common/` to preserve this property.
- **External-service commands:** Integration/e2e test commands and service-exercising commands are excluded from mapping by request; architecture notes are based on file inspection only.

## Anti-Patterns

### Collector Owns Runtime Side Effects

**What happens:** A collector writes directly to AlloyDB, uploads to GCS, publishes Pub/Sub messages, or handles feed lease release instead of yielding `CapturedChunk`.
**Why it's wrong:** It bypasses the runtime contract that centralizes upload, publish, bookmarks, failure counting, quarantine, heartbeats, lease release, and timeouts.
**Do this instead:** Keep collector functions under `backend/pipeline/ingestion/collectors/` as async generators yielding `CapturedChunk`, and let `NormalizerRuntime._process_feed()` own side effects (`backend/pipeline/ingestion/models.py:1`, `backend/pipeline/ingestion/normalizer_runtime.py:745`).

### Partial Source Type Registration

**What happens:** A source type is added only to `SourceType` or only to the router, leaving caps or DB seed SQL out of sync.
**Why it's wrong:** Workers can silently never claim the new type or claim a type with no collector.
**Do this instead:** Update `backend/pipeline/storage/feed_store.py:36`, `backend/pipeline/ingestion/router.py:41`, `backend/pipeline/ingestion/settings.py:26`, and source-type SQL under `terraform/modules/alloydb/sql/ingestion/` together.

### Direct UI-To-Backend Service Calls

**What happens:** React service functions call `backend/services/*` URLs directly instead of going through `frontend/api`.
**Why it's wrong:** It bypasses the TSOA route surface, auth/session handling, DTO conversion, and centralized UI error mapping.
**Do this instead:** Add or extend a TSOA controller in `frontend/api/src/<domain>/` and call it from `frontend/transcription-ui/src/service/` (`frontend/api/src/index.ts:28`, `frontend/transcription-ui/src/utils/apiUtils.ts:3`).

### Database Logic In HTTP Handlers

**What happens:** FastAPI handlers or TSOA controllers execute SQL or map SQL rows directly.
**Why it's wrong:** It duplicates the storage layer and weakens the existing service/store boundary.
**Do this instead:** Add methods to stores under `backend/pipeline/storage/`, call them from service classes under `backend/services/<domain>/service.py`, and keep HTTP handlers focused on routing and status translation (`backend/services/feeds/main.py:51`, `backend/services/feeds/service.py:15`, `backend/pipeline/storage/feed_store.py:126`).

## Error Handling

**Strategy:** Isolate failures at component boundaries, retry transient infrastructure operations, make feed leases fail closed, and surface HTTP failures with typed status codes.

**Patterns:**
- Ingestion reports per-feed failures to AlloyDB and emits quarantine telemetry after threshold-based quarantine (`backend/pipeline/ingestion/normalizer_runtime.py:959`, `backend/pipeline/ingestion/normalizer_runtime.py:977`, `backend/pipeline/ingestion/normalizer_runtime.py:984`).
- Lease loss and heartbeat stalls avoid unsafe continued work; bookmark fence violations and heartbeat stall timeouts terminate the worker process (`backend/pipeline/ingestion/normalizer_runtime.py:893`, `backend/pipeline/ingestion/normalizer_runtime.py:947`, `backend/pipeline/ingestion/normalizer_runtime.py:1049`).
- GCS upload and bookmark writes use retry helpers tied to lease/shutdown checks (`backend/pipeline/ingestion/retry.py`, `backend/pipeline/ingestion/normalizer_runtime.py:824`, `backend/pipeline/ingestion/normalizer_runtime.py:871`).
- Beam routes parser/stitch/normalize/serialize failures to a DLQ Pub/Sub topic (`backend/pipeline/normalization/orchestration.py:145`, `backend/pipeline/normalization/orchestration.py:237`).
- FastAPI services translate `ValueError`, not-found, and duplicate conflicts into HTTP exceptions (`backend/services/transcripts/main.py:52`, `backend/services/feeds/main.py:57`, `backend/services/rules/main.py:86`).
- Frontend API controllers translate Gaxios/backend errors with `handleBackendError` and raise `HttpError` for centralized Express error handling (`frontend/api/src/utils.ts:17`, `frontend/api/src/index.ts:30`).
- UI wraps failed fetches in `ApiError` and displays route-level alerts/snackbars (`frontend/transcription-ui/src/utils/apiUtils.ts:3`, `frontend/transcription-ui/src/App.tsx:72`).

## Cross-Cutting Concerns

**Logging:** Python pipeline/service code uses standard logging and `backend/pipeline/common/logging.py`; Cloud Functions call `setup_logging()` at import time (`backend/pipeline/transcription/main.py:21`, `backend/pipeline/evaluation/main.py:15`, `backend/pipeline/notification/send_notification.py:29`).

**Tracing:** Trace context is propagated through Pub/Sub attributes and restored in processors; OpenTelemetry setup is gated on GCP environment (`backend/pipeline/common/tracing_utils.py:31`, `backend/pipeline/common/tracing_utils.py:65`, `backend/pipeline/common/tracing_utils.py:93`, `backend/pipeline/common/tracing_utils.py:108`).

**Validation:** Pydantic models validate HTTP DTOs in backend services; protobuf parsing validates pipeline payload shapes; TSOA and TypeScript types validate frontend API surfaces (`backend/services/feeds/models.py`, `backend/pipeline/transcription/processor.py:63`, `frontend/api/tsoa.json:1`, `frontend/common/src/types/`).

**Authentication:** Backend service-to-service calls use Google ID tokens in GCP; UI auth uses Google OAuth, refresh-token cookies, and bearer ID tokens (`backend/pipeline/common/auth.py:19`, `frontend/api/src/auth/authController.ts:31`, `frontend/transcription-ui/src/context/AuthProvider.tsx:14`).

**Configuration:** Backend Python config is environment-driven through dataclasses and module-level checks; frontend API config validates required env vars at import time; UI uses Vite env vars (`backend/pipeline/storage/settings.py:17`, `backend/pipeline/ingestion/settings.py:51`, `frontend/api/src/config.ts:6`, `frontend/transcription-ui/src/main.tsx:18`).

**Local Development:** Docker Compose defines local services and emulators, while `.mise.toml` wraps dev, format, lint, test, and proto-generation tasks (`docker-compose.yml`, `.mise.toml:144`, `.mise.toml:170`).

---

*Architecture analysis: 2026-05-24*

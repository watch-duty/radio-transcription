<!-- refreshed: 2026-05-27 -->
# Architecture

**Analysis Date:** 2026-05-27

## System Overview

```text
+-------------------------------------------------------------+
|                   External Audio Sources                    |
| Broadcastify | OpenMHz / Fire | Echo GCS Notify             |
| `backend/...` | `backend/...` | `backend/.../echo`          |
+-----------------------------+-------------------------------+
                              |
                              v
+-------------------------------------------------------------+
| Ingestion and Claim-Check Publication                       |
| `backend/pipeline/ingestion`                                |
| `backend/pipeline/common/gcp_helper.py`                     |
| writes staged audio to GCS and publishes `AudioChunk`        |
+-----------------------------+-------------------------------+
                              |
                              v
+-------------------------------------------------------------+
| Streaming Normalization                                     |
| `backend/pipeline/normalization`                            |
| reads `AudioChunk`, stitches, normalizes, publishes          |
| `NormalizedAudio`                                           |
+-----------------------------+-------------------------------+
                              |
                              v
+-------------------------------------------------------------+
| Transcription                                                |
| `backend/pipeline/transcription`                            |
| reads `NormalizedAudio`, calls transcriber, publishes        |
| `TranscribedAudio`                                          |
+-----------------------------+-------------------------------+
                              |
                              v
+-------------------------------------------------------------+
| Evaluation, Storage, and Notification                       |
| `backend/pipeline/evaluation`                               |
| `backend/services/transcripts`                              |
| `backend/pipeline/notification`                             |
+-------------+-------------------------------+---------------+
              |                               |
              v                               v
+-----------------------------+   +---------------------------+
| AlloyDB / GCS / Redis       |   | Frontend API and React UI |
| `backend/pipeline/storage`  |   | `frontend/api`            |
| `terraform/modules/alloydb` |   | `frontend/transcription-ui` |
+-----------------------------+   +---------------------------+

+-------------------------------------------------------------+
| Offline Model and SFT Tooling                               |
| `model/scripts/sft`, `model/colabs/common`                  |
| builds/evaluates datasets against GCS and Vertex AI          |
+-------------------------------------------------------------+
```

## Component Responsibilities

| Component | Responsibility | File |
|-----------|----------------|------|
| Ingestion CLI | Bootstraps settings, validates source topic routing, and runs the ingestion runtime. | `backend/pipeline/ingestion/main.py` |
| Ingestion runtime | Owns feed leasing, heartbeat renewal, collector task lifecycle, GCS upload, Pub/Sub publication, bookmarks, quarantine reporting, health state, and shutdown. | `backend/pipeline/ingestion/normalizer_runtime.py` |
| Source router | Maps `SourceType` values to collector functions and segmented or continuous Pub/Sub topics. | `backend/pipeline/ingestion/router.py` |
| Collector contract | Defines the `CapturedChunk`, `CaptureResources`, and `CollectorFn` interface used by all source collectors. | `backend/pipeline/ingestion/models.py` |
| Broadcastify feed collector | Captures continuous Icecast audio and segments it through ffmpeg. | `backend/pipeline/ingestion/collectors/icecast/icecast_collector.py` |
| OpenMHz collector | Consumes OpenMHz websocket events, downloads referenced M4A audio, and yields staged chunks. | `backend/pipeline/ingestion/collectors/openmhz/collector.py` |
| Echo ingestion function | Handles Echo bucket notifications as a separate Cloud Function and publishes `AudioChunk` messages. | `backend/pipeline/ingestion/collectors/echo/main.py` |
| Oldest feed publisher | Emits oldest-start-time metrics for alerting and scheduler visibility. | `backend/pipeline/ingestion/oldest_feed_publisher/main.py` |
| Normalization DAG | Defines the Beam streaming pipeline from Pub/Sub input through parse, stitch, normalize, serialize, and DLQ output. | `backend/pipeline/normalization/orchestration.py` |
| Normalization transforms | Implement Pub/Sub parsing, protobuf serialization, ordered stitching, VAD flushing, and derivative audio export. | `backend/pipeline/normalization/transforms/stateless.py`, `backend/pipeline/normalization/transforms/stateful.py` |
| Audio processor | Downloads audio, detects speech, converts/export normalized FLAC/M4A outputs. | `backend/pipeline/normalization/audio/audio_processor.py` |
| Transcription function | CloudEvent entry point with cached transcriber, publisher, and processor instances. | `backend/pipeline/transcription/main.py` |
| Transcription processor | Parses `NormalizedAudio`, invokes a transcriber, builds `TranscribedAudio`, and publishes ordered Pub/Sub output. | `backend/pipeline/transcription/processor.py` |
| Transcriber abstraction | Defines and constructs concrete transcription backends such as Google Chirp v3 and mock transcribers. | `backend/pipeline/transcription/transcribers/base.py`, `backend/pipeline/transcription/transcribers/factory.py` |
| Evaluation function | Parses transcription events, evaluates rules, stores transcripts, and publishes alerts. | `backend/pipeline/evaluation/main.py`, `backend/pipeline/evaluation/processor.py` |
| Rules evaluator | Applies static or remote rulesets against transcript text. | `backend/pipeline/evaluation/rules_evaluation/evaluator.py` |
| Notification function | Converts alert events to notification payloads, deduplicates with Redis, and sends outbound notifications. | `backend/pipeline/notification/send_notification.py` |
| Storage layer | Centralizes AlloyDB connection pooling and query-backed stores for feeds, transcripts, rules, and audio segments. | `backend/pipeline/storage/connection.py`, `backend/pipeline/storage/feed_store.py`, `backend/pipeline/storage/transcript_store.py`, `backend/pipeline/storage/rules_store.py`, `backend/pipeline/storage/audio_segment_store.py` |
| Feed service | FastAPI CRUD API over feed records and ingestion control operations. | `backend/services/feeds/main.py`, `backend/services/feeds/service.py` |
| Rules service | FastAPI CRUD API over alert/evaluation rules. | `backend/services/rules/main.py`, `backend/services/rules/service.py` |
| Transcripts service | FastAPI API for transcript creation, listing, lookup, and deletion. | `backend/services/transcripts/main.py`, `backend/services/transcripts/service.py` |
| Frontend API facade | Express/TSOA gateway that handles browser auth, generated OpenAPI routes, backend ID-token clients, and response conversion. | `frontend/api/src/index.ts`, `frontend/api/src/authentication.ts` |
| React UI | Browser application for transcript review, feed/rules management, API docs, and authenticated routes. | `frontend/transcription-ui/src/main.tsx`, `frontend/transcription-ui/src/App.tsx` |
| Shared frontend types | TypeScript contract package shared by the UI and API facade. | `frontend/common/src/index.ts`, `frontend/common/src/types/feeds.ts`, `frontend/common/src/types/transcripts.ts`, `frontend/common/src/types/rules.ts` |
| Protobuf contracts | Pub/Sub message schemas for raw chunks, normalized audio, transcribed audio, evaluated audio, alerts, and Beam state. | `protos/raw_audio_chunk.proto`, `protos/normalized_audio.proto`, `protos/transcribed_audio.proto`, `protos/evaluated_transcribed_audio.proto`, `protos/alert_notification.proto`, `protos/streaming_state.proto` |
| SFT/model tooling | Builds ASR training/evaluation JSONL, validates datasets, shares scoring and Vertex helpers. | `model/scripts/sft/pipeline.py`, `model/scripts/sft/preflight.py`, `model/colabs/common` |
| Infrastructure | Defines deployment building blocks for AlloyDB, GCS, Redis, Cloud Functions, container MIGs, and ASR evaluation. | `terraform/modules` |

## Pattern Overview

**Overall:** Event-driven claim-check pipeline with management services, a browser API facade, and offline model tooling.

**Key Characteristics:**
- Use GCS for audio payloads and Pub/Sub protobuf messages for claim metadata. Message schemas live in `protos/*.proto`; generated Python bindings are consumed from `backend/pipeline/schema_types`.
- Keep ingestion source-specific logic behind `CollectorFn` implementations in `backend/pipeline/ingestion/collectors`, while `NormalizerRuntime` owns leasing, upload, publication, bookmarks, health, and failures in `backend/pipeline/ingestion/normalizer_runtime.py`.
- Use AlloyDB as the durable control plane for feeds, transcripts, rules, and audio segments through stores in `backend/pipeline/storage`.
- Put browser-facing auth/session behavior in `frontend/api`; React code in `frontend/transcription-ui` calls the facade instead of direct backend service URLs.
- Keep offline dataset, scoring, and Vertex AI operations in `model/scripts/sft` and `model/colabs/common` rather than in serving code.

## Layers

**UI Layer:**
- Purpose: Browser workflows for transcript review, feed administration, rules, docs, and login.
- Location: `frontend/transcription-ui`
- Contains: React routes, MUI components, TanStack Query hooks, browser services, auth context.
- Depends on: `frontend/api`, `frontend/common`, Google OAuth browser client.
- Used by: End users and local development workflows.

**Frontend API Facade Layer:**
- Purpose: Convert browser requests into authenticated calls to backend services and expose generated TSOA routes/docs.
- Location: `frontend/api`
- Contains: Express app, TSOA controllers, auth/session endpoints, OpenAPI generation, backend error mapping.
- Depends on: `frontend/common`, Google auth libraries, backend FastAPI services.
- Used by: `frontend/transcription-ui`.

**Management Service Layer:**
- Purpose: Provide internal CRUD APIs for feeds, rules, and transcripts.
- Location: `backend/services`
- Contains: FastAPI apps, service classes, Pydantic request/response models.
- Depends on: `backend/pipeline/storage`, `backend/pipeline/common/auth.py`, generated protobuf classes.
- Used by: `frontend/api`, evaluation functions, local and integration tests.

**Ingestion Layer:**
- Purpose: Lease active feeds, capture source audio, stage audio in GCS, and publish raw audio claim messages.
- Location: `backend/pipeline/ingestion`
- Contains: Runtime, settings, routing, health server, collectors, Echo Cloud Function, oldest-feed publisher.
- Depends on: `backend/pipeline/storage`, `backend/pipeline/common`, `backend/pipeline/schema_types`, GCS, Pub/Sub, AlloyDB.
- Used by: Deployment entry points and integration tests.

**Normalization Layer:**
- Purpose: Parse raw chunk events, enforce ordering, stitch audio, detect speech, normalize outputs, and publish normalized claim messages.
- Location: `backend/pipeline/normalization`
- Contains: Beam pipeline assembly, stateful/stateless DoFns, audio processor, options.
- Depends on: Apache Beam, GCS, Pub/Sub, protobuf contracts, VAD/audio processing libraries.
- Used by: Dataflow or local Beam execution.

**Transcription Layer:**
- Purpose: Convert normalized audio into transcript protobuf events through configurable transcriber backends.
- Location: `backend/pipeline/transcription`
- Contains: Cloud Function entry point, processor, publisher, transcriber interface/factory, Chirp and mock implementations.
- Depends on: Google Speech APIs, Pub/Sub, GCS, protobuf contracts.
- Used by: Pub/Sub-triggered Cloud Functions and tests.

**Evaluation and Notification Layer:**
- Purpose: Evaluate transcripts against rules, persist transcript results, publish alert decisions, deduplicate notifications, and call outbound notification endpoints.
- Location: `backend/pipeline/evaluation`, `backend/pipeline/notification`
- Contains: Cloud Function entry points, evaluation service, rule evaluators, notification request handler.
- Depends on: Rules API, Transcripts API, Pub/Sub, Redis, outbound notification endpoint.
- Used by: Pub/Sub-triggered Cloud Functions.

**Storage and Contract Layer:**
- Purpose: Own durable database access and typed message contracts shared across services.
- Location: `backend/pipeline/storage`, `protos`, `backend/pipeline/schema_types`
- Contains: AsyncPG pools, stores, SQL query helpers, generated protobuf modules.
- Depends on: AlloyDB/PostgreSQL, generated protobuf code.
- Used by: Backend pipeline code, FastAPI services, tests.

**Model and Evaluation Tooling Layer:**
- Purpose: Build training/evaluation datasets, score ASR outputs, submit Vertex AI jobs, and share prompt/manifest contracts.
- Location: `model/scripts/sft`, `model/colabs/common`
- Contains: CLI pipeline, dataset adapters, preflight validation, scoring, GCS helpers, Vertex helpers.
- Depends on: GCS, Vertex AI, optional model/scoring dependencies.
- Used by: Offline workflows and model development.

**Infrastructure and Local/Test Layer:**
- Purpose: Define deployment modules, local emulators/mock services, and end-to-end/integration tests.
- Location: `terraform`, `local_dev`, `integration_tests`
- Contains: Terraform modules, Docker/local support files, pytest suites.
- Depends on: GCP, local environment configuration, service containers.
- Used by: Deployment, CI, local development, and regression testing.

## Data Flow

### Primary Audio-to-Alert Path

1. Ingestion starts at `backend/pipeline/ingestion/main.py:14`, loads `NormalizerSettings` from `backend/pipeline/ingestion/settings.py:52`, verifies topic routing through `backend/pipeline/ingestion/router.py:66`, and constructs `NormalizerRuntime` from `backend/pipeline/ingestion/normalizer_runtime.py:61`.
2. The runtime enters `_leasing_loop` in `backend/pipeline/ingestion/normalizer_runtime.py:410`, acquires work through `FeedStore.acquire_feeds_batch` in `backend/pipeline/storage/feed_store.py:423`, and creates per-feed tasks at `backend/pipeline/ingestion/normalizer_runtime.py:580`.
3. Source collectors registered in `_COLLECTORS` at `backend/pipeline/ingestion/router.py:41` yield `CapturedChunk` objects defined at `backend/pipeline/ingestion/models.py:108`. Echo bucket notifications enter separately through `backend/pipeline/ingestion/collectors/echo/main.py:68`.
4. The runtime uploads staged audio through `upload_staged_audio` in `backend/pipeline/common/gcp_helper.py:40`, publishes `AudioChunk` messages through `publish_audio_chunk` in `backend/pipeline/common/gcp_helper.py:267`, and writes fenced feed progress through `backend/pipeline/ingestion/normalizer_runtime.py:871`.
5. The Beam normalization pipeline reads continuous and segmented Pub/Sub subscriptions in `backend/pipeline/normalization/orchestration.py:112` and `backend/pipeline/normalization/orchestration.py:122`, then parses and keys messages with `ParseAndKeyFn` in `backend/pipeline/normalization/transforms/stateless.py:60`.
6. Stateful Beam transforms stitch chunks with `OrderedContinuousStitchAudioFn` in `backend/pipeline/normalization/transforms/stateful.py:206`, normalize/export audio with `NormalizeAudioFn` in `backend/pipeline/normalization/transforms/stateful.py:957`, and serialize `NormalizedAudio` with `SerializeNormalizationClaimFn` in `backend/pipeline/normalization/transforms/stateless.py:243`.
7. The transcription Cloud Function enters at `transcribe_claim_check` in `backend/pipeline/transcription/main.py:117`, processes events through `TranscriptionEventProcessor.process_event` in `backend/pipeline/transcription/processor.py:49`, calls the configured transcriber such as `GoogleChirpV3Transcriber.transcribe` in `backend/pipeline/transcription/transcribers/chirp.py:128`, and publishes `TranscribedAudio` at `backend/pipeline/transcription/processor.py:124`.
8. Evaluation enters at `evaluate_transcription` in `backend/pipeline/evaluation/main.py:54`, parses and validates events in `backend/pipeline/evaluation/processor.py:55`, evaluates transcript text through `EvaluationService.evaluate` in `backend/pipeline/evaluation/service.py:34`, writes transcripts through `TranscriptsClient.create_transcript` in `backend/pipeline/common/clients/transcripts_client.py:36`, and publishes alert events at `backend/pipeline/evaluation/processor.py:111`.
9. Notification enters at `send_notification` in `backend/pipeline/notification/send_notification.py:143`, deduplicates alerts at `backend/pipeline/notification/send_notification.py:156`, fetches feed tags at `backend/pipeline/notification/send_notification.py:163`, and sends outbound requests through `RequestHandler.send_notification` in `backend/pipeline/notification/request_handler.py:32`.

### Browser Management Path

1. The UI boots React providers in `frontend/transcription-ui/src/main.tsx:16`, configures routes in `frontend/transcription-ui/src/App.tsx:188`, and loads transcript data from `TranscriptView` in `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx:250`.
2. Browser services call the API facade through helpers such as `apiFetch` in `frontend/transcription-ui/src/utils/apiUtils.ts:3` and `listTranscripts` in `frontend/transcription-ui/src/service/listTranscripts.ts:5`.
3. Express registers generated TSOA routes in `frontend/api/src/index.ts:28` and applies TSOA authentication through `expressAuthentication` in `frontend/api/src/authentication.ts:13`.
4. Controllers convert request/response shapes and call backend services with ID-token clients, for example `TranscriptsController.listTranscripts` in `frontend/api/src/transcripts/transcriptsController.ts:74`, `FeedsController.getIdTokenClient` in `frontend/api/src/feeds/feedsController.ts:139`, and `RulesController` routes in `frontend/api/src/rules/rulesController.ts:216`.
5. FastAPI services initialize stores in lifespan handlers such as `backend/services/transcripts/main.py:22`, delegate to service classes such as `TranscriptService` in `backend/services/transcripts/service.py:21`, and persist through stores such as `TranscriptStore` in `backend/pipeline/storage/transcript_store.py:31`.
6. Stores use AsyncPG pools created by `create_pool` in `backend/pipeline/storage/connection.py:17` against AlloyDB schemas managed by SQL files in `terraform/modules/alloydb/sql/ingestion`.

### SFT Dataset Path

1. The SFT CLI dispatches subcommands from `model/scripts/sft/pipeline.py:461`.
2. The build command loads dataset registry and prompt configuration in `_build` at `model/scripts/sft/pipeline.py:227`.
3. Dataset adapters such as `GcsManifestAdapter` in `model/scripts/sft/adapters/gcs_manifest.py:18` yield `CanonicalRow` records defined in `model/colabs/common/manifest.py:22`.
4. `build_example` in `model/colabs/common/sft.py:17` and `validate_example` in `model/colabs/common/sft.py:66` produce Vertex JSONL examples, then `_build_split_jsonl` uploads generated files at `model/scripts/sft/pipeline.py:193`.
5. Preflight validation runs through `run_preflight` in `model/scripts/sft/preflight.py:168`. Vertex helpers for tuning and batch inference live in `model/colabs/common/vertex.py:143` and `model/colabs/common/vertex.py:226`; current `_tune` and `_eval` CLI bodies in `model/scripts/sft/pipeline.py:332` and `model/scripts/sft/pipeline.py:338` are stubs.

**State Management:**
- Ingestion runtime state is held on `NormalizerRuntime` instance fields and support threads in `backend/pipeline/ingestion/normalizer_runtime.py`.
- Feed ownership and progress are durable in AlloyDB through `backend/pipeline/storage/feed_store.py`, with fenced updates and heartbeat renewal.
- Beam ordering and flush state are held in state specs and timers in `backend/pipeline/normalization/transforms/stateful.py`.
- FastAPI services create per-process connection pools in lifespan handlers such as `backend/services/feeds/main.py:29`.
- Cloud Function modules cache warm clients/processors in module globals such as `backend/pipeline/transcription/main.py:112`, `backend/pipeline/evaluation/main.py:21`, `backend/pipeline/notification/send_notification.py:75`, and `backend/pipeline/ingestion/collectors/echo/main.py:60`.
- Frontend server state is managed through TanStack Query in `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx` and auth context in `frontend/transcription-ui/src/context/AuthProvider.tsx`.

## Key Abstractions

**Collector Contract:**
- Purpose: Keep source capture implementations interchangeable while preventing collectors from owning runtime responsibilities.
- Examples: `CapturedChunk`, `CaptureResources`, and `CollectorFn` in `backend/pipeline/ingestion/models.py`.
- Pattern: Async generator interface that yields local staged audio plus metadata to `NormalizerRuntime`.

**NormalizerRuntime:**
- Purpose: Coordinate feed leasing, capture task orchestration, upload/publication, progress bookmarks, heartbeat, health, and shutdown.
- Examples: `backend/pipeline/ingestion/normalizer_runtime.py`.
- Pattern: Composition root around collector functions, store clients, GCS/Pub/Sub clients, and watchdog threads.

**Store Classes:**
- Purpose: Encapsulate SQL and row/protobuf/domain mapping for each durable resource.
- Examples: `backend/pipeline/storage/feed_store.py`, `backend/pipeline/storage/transcript_store.py`, `backend/pipeline/storage/rules_store.py`, `backend/pipeline/storage/audio_segment_store.py`.
- Pattern: AsyncPG-backed repositories used by FastAPI services and pipeline runtimes.

**Protobuf Claim Messages:**
- Purpose: Carry typed metadata across Pub/Sub while audio bytes remain in GCS.
- Examples: `protos/raw_audio_chunk.proto`, `protos/normalized_audio.proto`, `protos/transcribed_audio.proto`, `protos/evaluated_transcribed_audio.proto`, `protos/alert_notification.proto`.
- Pattern: Claim-check contracts with generated Python bindings in `backend/pipeline/schema_types`.

**Transcriber Interface:**
- Purpose: Allow transcription backends to be selected by configuration while preserving a common processor path.
- Examples: `backend/pipeline/transcription/transcribers/base.py`, `backend/pipeline/transcription/transcribers/factory.py`, `backend/pipeline/transcription/transcribers/chirp.py`, `backend/pipeline/transcription/transcribers/mock.py`.
- Pattern: ABC plus factory selected by `TRANSCRIBER_TYPE`.

**Evaluation Service and Evaluators:**
- Purpose: Separate transcript event processing from ruleset loading and text matching.
- Examples: `backend/pipeline/evaluation/service.py`, `backend/pipeline/evaluation/rules_evaluation/evaluator.py`.
- Pattern: Service object delegates to static or remote evaluator implementations.

**Frontend Shared Types:**
- Purpose: Keep browser UI and API facade request/response contracts aligned.
- Examples: `frontend/common/src/types/feeds.ts`, `frontend/common/src/types/transcripts.ts`, `frontend/common/src/types/rules.ts`.
- Pattern: Shared package imported as `@transcription/common`.

**Dataset Adapter and CanonicalRow:**
- Purpose: Convert source-specific dataset manifests into a canonical ASR training/evaluation row format.
- Examples: `model/colabs/common/manifest.py`, `model/scripts/sft/adapters/gcs_manifest.py`.
- Pattern: Adapter interface plus registry-driven SFT pipeline.

## Entry Points

**Ingestion runtime:**
- Location: `backend/pipeline/ingestion/main.py`
- Triggers: Container process or local CLI execution.
- Responsibilities: Validate settings/topic routing and run `NormalizerRuntime`.

**Echo ingestion Cloud Function:**
- Location: `backend/pipeline/ingestion/collectors/echo/main.py`
- Triggers: CloudEvent bucket notification.
- Responsibilities: Resolve feed metadata, upload/copy staged Echo audio, publish `AudioChunk`, record failures.

**Oldest feed publisher:**
- Location: `backend/pipeline/ingestion/oldest_feed_publisher/main.py`
- Triggers: HTTP Cloud Function.
- Responsibilities: Query oldest active feed start time and publish monitoring metric data.

**Normalization pipeline:**
- Location: `backend/pipeline/normalization/main.py`
- Triggers: Beam/Dataflow process.
- Responsibilities: Parse pipeline options and run `get_pipeline`.

**Transcription Cloud Function:**
- Location: `backend/pipeline/transcription/main.py`
- Triggers: Pub/Sub CloudEvent containing `NormalizedAudio`.
- Responsibilities: Transcribe normalized audio and publish `TranscribedAudio`.

**Evaluation Cloud Function:**
- Location: `backend/pipeline/evaluation/main.py`
- Triggers: Pub/Sub CloudEvent containing `TranscribedAudio`.
- Responsibilities: Evaluate transcript rules, write transcript service records, publish alert events.

**Notification Cloud Function:**
- Location: `backend/pipeline/notification/send_notification.py`
- Triggers: Pub/Sub CloudEvent containing alert notification data.
- Responsibilities: Deduplicate and send outbound notification requests.

**Feed service API:**
- Location: `backend/services/feeds/main.py`
- Triggers: HTTP requests through FastAPI/Uvicorn.
- Responsibilities: Feed CRUD, deactivate, and reset operations.

**Rules service API:**
- Location: `backend/services/rules/main.py`
- Triggers: HTTP requests through FastAPI/Uvicorn.
- Responsibilities: Rules CRUD operations.

**Transcripts service API:**
- Location: `backend/services/transcripts/main.py`
- Triggers: HTTP requests through FastAPI/Uvicorn.
- Responsibilities: Transcript creation, lookup, listing, and deletion.

**Frontend API:**
- Location: `frontend/api/src/index.ts`
- Triggers: Express/Functions Framework HTTP execution.
- Responsibilities: Register TSOA routes, handle browser auth, proxy backend calls, expose OpenAPI docs.

**React application:**
- Location: `frontend/transcription-ui/src/main.tsx`
- Triggers: Browser load through Vite/build output.
- Responsibilities: Mount providers, route views, manage browser workflows.

**SFT CLI:**
- Location: `model/scripts/sft/pipeline.py`
- Triggers: Python CLI subcommands.
- Responsibilities: Build SFT JSONL datasets, preflight data, and provide tuning/evaluation command surface.

**Bulk feed import:**
- Location: `backend/scripts/bulk_import_feeds.py`
- Triggers: Operator script.
- Responsibilities: Import feed definitions into feed storage.

## Architectural Constraints

- **Threading:** Ingestion is an asyncio/uvloop runtime with additional OS threads for heartbeat and RSS watchdog behavior in `backend/pipeline/ingestion/normalizer_runtime.py`. Collector implementations must not block the event loop and should yield control through async operations as specified by `backend/pipeline/ingestion/models.py`.
- **Beam state:** Normalization uses Beam keyed state and timers in `backend/pipeline/normalization/transforms/stateful.py`; ordering and flushing changes must preserve state schema compatibility in `protos/streaming_state.proto`.
- **Global state:** Warm module-level caches exist in Cloud Function modules and docs helpers: `backend/pipeline/transcription/main.py`, `backend/pipeline/evaluation/main.py`, `backend/pipeline/notification/send_notification.py`, `backend/pipeline/ingestion/collectors/echo/main.py`, and `frontend/api/src/docs/docsController.ts`.
- **Generated contracts:** Edit `protos/*.proto` and regenerate Python bindings rather than hand-editing `backend/pipeline/schema_types`. Edit TSOA controllers and regenerate routes/spec rather than hand-editing generated TSOA output.
- **Source type registry:** Adding or changing a claimable source type spans `backend/pipeline/storage/feed_store.py`, SQL seed/migration files in `terraform/modules/alloydb/sql/ingestion`, `_DEFAULT_CAPS` in `backend/pipeline/ingestion/settings.py`, `_COLLECTORS` in `backend/pipeline/ingestion/router.py`, and shared UI/API types in `frontend/common/src/types/feeds.ts` when browser-facing.
- **Circular imports:** Not detected during architecture inspection. Keep shared concerns in `backend/pipeline/common`, `backend/pipeline/storage`, `frontend/common`, or `model/colabs/common` instead of importing across feature entry points.

## Anti-Patterns

### Updating a Source Type in One Place

**What happens:** A new feed source is added only to a collector or only to an enum.
**Why it's wrong:** Source routing, database seed values, ingestion concurrency caps, UI types, and feed store validation can drift and make feeds unclaimable or invisible.
**Do this instead:** Update `backend/pipeline/storage/feed_store.py`, `terraform/modules/alloydb/sql/ingestion`, `backend/pipeline/ingestion/settings.py`, `backend/pipeline/ingestion/router.py`, and `frontend/common/src/types/feeds.ts` together.

### Letting Collectors Own Runtime Side Effects

**What happens:** A collector writes bookmarks, publishes Pub/Sub messages, uploads final objects, or mutates feed status directly.
**Why it's wrong:** `NormalizerRuntime` centralizes fencing, failure/quarantine behavior, upload naming, trace propagation, and health accounting.
**Do this instead:** Return `CapturedChunk` objects from collector modules under `backend/pipeline/ingestion/collectors` and let `backend/pipeline/ingestion/normalizer_runtime.py` perform runtime side effects.

### Bypassing the Frontend API Facade

**What happens:** React components call backend FastAPI service URLs directly.
**Why it's wrong:** Browser auth/session handling, backend ID-token clients, error conversion, and shape conversion live in `frontend/api`.
**Do this instead:** Add UI calls under `frontend/transcription-ui/src/service`, facade routes/controllers under `frontend/api/src`, and shared contract types under `frontend/common/src/types`.

### Editing Generated Files by Hand

**What happens:** Generated protobuf or TSOA output is modified directly.
**Why it's wrong:** Regeneration overwrites manual edits and can desynchronize contracts from source definitions.
**Do this instead:** Update `protos/*.proto` then run `mise run generate:protos`; update TSOA controllers in `frontend/api/src` then run the API package route/spec generation scripts.

## Error Handling

**Strategy:** Fail fast on invalid configuration, isolate per-feed/per-message failures where the pipeline can continue, and route recoverable streaming transform errors to DLQ outputs.

**Patterns:**
- Ingestion validates configuration at startup in `backend/pipeline/ingestion/main.py` and `backend/pipeline/ingestion/settings.py`.
- Ingestion records per-feed failures and quarantines repeated failures through `FeedStore.report_feed_failure` in `backend/pipeline/storage/feed_store.py:309`.
- Ingestion treats feed-fence violations as process-fatal in `backend/pipeline/ingestion/normalizer_runtime.py:893`.
- Beam parsing/serialization/normalization errors are tagged to DLQ outputs in `backend/pipeline/normalization/transforms/stateless.py` and `backend/pipeline/normalization/transforms/stateful.py`.
- Transcription and evaluation processors parse Pub/Sub payloads explicitly and raise processing errors from `backend/pipeline/transcription/processor.py` and `backend/pipeline/evaluation/processor.py`.
- The frontend API converts backend HTTP errors through `frontend/api/src/utils.ts` and centralizes Express error handling in `frontend/api/src/index.ts`.
- The React UI converts failed fetches to `ApiError` in `frontend/transcription-ui/src/utils/apiUtils.ts` and displays route-level/application-level feedback in `frontend/transcription-ui/src/App.tsx`.

## Cross-Cutting Concerns

**Logging:** Use `setup_logging` from `backend/pipeline/common/logging.py` for Python services and structured logs around ingestion, normalization, transcription, evaluation, and notification events. Local execution falls back to standard logging configuration.

**Tracing:** Use helpers in `backend/pipeline/common/tracing_utils.py` to set up tracing, propagate `traceparent`, and attach trace context to Pub/Sub messages.

**Validation:** Use Pydantic for HTTP request/response models in `backend/services`, protobuf parsing for Pub/Sub contracts in pipeline processors, TSOA decorators/specs in `frontend/api`, and explicit SFT example validation in `model/colabs/common/sft.py`.

**Authentication:** Backend FastAPI services use Google OIDC verification in `backend/pipeline/common/auth.py`. The frontend API handles browser auth/session flows in `frontend/api/src/auth` and calls backend services with Google ID-token clients from controllers.

**Configuration:** Python configuration comes from dataclasses and environment variables such as `backend/pipeline/ingestion/settings.py` and `backend/pipeline/storage/settings.py`. TypeScript API configuration is centralized in `frontend/api/src/config.ts`. SFT configuration is registry-driven from `model/scripts/sft/datasets.toml`.

**Persistence:** Store audio payloads in GCS, operational state and records in AlloyDB through `backend/pipeline/storage`, alert deduplication state in Redis through `backend/pipeline/notification`, and offline SFT artifacts under GCS/result paths managed by `model/scripts/sft`.

---

*Architecture analysis: 2026-05-27*

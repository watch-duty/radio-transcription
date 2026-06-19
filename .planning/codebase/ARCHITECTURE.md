<!-- refreshed: 2026-06-19 -->
# Architecture

**Analysis Date:** 2026-06-19

## System Overview

```text
+----------------------------------------------------------------------------------+
|                    Radio Transcription Monorepo                                  |
+----------------------+-----------------------+-----------------------------------+
| Audio source capture | Operator/data APIs    | Operator UI and model tooling     |
| `backend/pipeline/`  | `backend/services/`   | `frontend/`, `model/src/`         |
+----------+-----------+-----------+-----------+-------------------+---------------+
           |                       |                               |
           v                       v                               v
+----------------------------------------------------------------------------------+
|              Typed contracts, claim-check messages, and stores                   |
|              `protos/`, `backend/pipeline/schema_types/`,                        |
|              `backend/pipeline/storage/`                                         |
+----------+-----------------------+-------------------------------+---------------+
           |                       |                               |
           v                       v                               v
+----------------------+-----------------------+-----------------------------------+
| GCS audio objects    | Pub/Sub topics        | AlloyDB and Redis state           |
| `gcs_uri` fields     | protobuf payloads     | `terraform/modules/alloydb/`,     |
|                      |                       | `backend/pipeline/storage/`       |
+----------------------+-----------------------+-----------------------------------+
```

The repository is a monorepo for emergency radio transcription. The production path is an event-driven audio pipeline under `backend/pipeline/`, data APIs under `backend/services/`, a TypeScript BFF and React UI under `frontend/`, and ASR/model workflow code under `model/src/`.

## Component Responsibilities

| Component | Responsibility | File |
|-----------|----------------|------|
| Ingestion runtime | Lease feeds, run source collectors, upload chunks, publish initial Pub/Sub messages, record observations and failures | `backend/pipeline/ingestion/collector_runtime.py` |
| Source routing | Map `source_type` values to collector implementations and reject unsupported capture modes | `backend/pipeline/ingestion/router.py` |
| Runtime source specs | Define source capture mode, claimability, runtime caps, and default lease durations | `backend/pipeline/ingestion/source_runtime_specs.py` |
| Collector contracts | Define `CapturedChunk`, `SourceObservation`, `FeedFailure`, and runtime-owned side-effect boundaries | `backend/pipeline/ingestion/models.py` |
| Echo ingestion function | Receive external echo notifications and publish segmented audio for the `echo` source type | `backend/pipeline/ingestion/collectors/echo/main.py` |
| GCP helpers | Upload audio objects and publish typed `ContinuousAudio` or `SegmentedAudio` protobuf messages | `backend/pipeline/common/gcp_helper.py` |
| Segmentation pipeline | Consume continuous audio, stitch ordered chunks, run VAD, upload raw segments, publish `SegmentedAudio` | `backend/pipeline/segmentation/orchestration.py` |
| Normalization stage | Transcode raw audio, write canonical/playback/transcription objects, persist audio segment metadata, publish `NormalizedAudio` | `backend/pipeline/normalization/processor.py` |
| Transcription stage | Select a transcriber, write transcript annotations, publish `TranscribedAudio` | `backend/pipeline/transcription/processor.py` |
| Evaluation stage | Evaluate transcripts against rules, write transcript/evaluation records, publish notification candidates | `backend/pipeline/evaluation/processor.py` |
| Notification stage | Deduplicate notifications, enrich feed metadata, call the outbound notification endpoint | `backend/pipeline/notification/send_notification.py` |
| FastAPI services | Expose feed, audio segment, transcript, and rule CRUD APIs with OIDC auth | `backend/services/feeds/main.py`, `backend/services/audio_segments/main.py`, `backend/services/transcripts/main.py`, `backend/services/rules/main.py` |
| Storage layer | Encapsulate AlloyDB access and operational SQL for feeds, transcripts, rules, and audio segments | `backend/pipeline/storage/feed_store.py`, `backend/pipeline/storage/transcript_store.py`, `backend/pipeline/storage/rules_store.py`, `backend/pipeline/storage/audio_segment_store.py` |
| BFF API | Expose TSOA/Express endpoints, handle user auth, proxy service calls with ID tokens | `frontend/api/src/index.ts`, `frontend/api/src/utils.ts`, `frontend/api/src/authentication.ts` |
| Shared frontend types | Share API/domain contracts between the BFF and React UI | `frontend/common/src/index.ts`, `frontend/common/src/types/` |
| React UI | Render feed search, transcripts, rules, feed configuration, docs, auth, and audio playback workflows | `frontend/transcription-ui/src/App.tsx`, `frontend/transcription-ui/src/main.tsx` |
| Gemini SFT CLI | Prepare data, launch Vertex tuning, and evaluate ASR fine-tuning runs | `model/src/gemini_sft/cli.py` |
| Manifest helpers | Validate and merge canonical ASR JSONL manifests | `model/src/common/manifest.py`, `model/data/manifests/README.md` |
| Infrastructure | Define AlloyDB, Cloud Functions, buckets, Redis, MIG collectors, and supporting cloud resources | `terraform/modules/` |

## Pattern Overview

**Overall:** Event-driven claim-check pipeline with service/store APIs and generated contracts.

**Key Characteristics:**
- Audio payloads move by object reference. Pipeline messages carry GCS URIs and metadata defined in `protos/*.proto`; large audio bytes are stored through helpers in `backend/pipeline/common/gcp_helper.py`.
- Source ingestion is split between capture logic and runtime side effects. Collectors return typed events from `backend/pipeline/ingestion/models.py`; `backend/pipeline/ingestion/collector_runtime.py` owns leases, uploads, publishing, bookmarks, and failure policy.
- Continuous and pre-segmented sources converge on the `SegmentedAudio` contract. `bcfy_feeds` emits `ContinuousAudio` through `backend/pipeline/common/gcp_helper.py`; `backend/pipeline/segmentation/orchestration.py` converts it to `SegmentedAudio`. Other sources publish `SegmentedAudio` directly.
- Backend services use a thin HTTP layer, domain service classes, and store classes. `backend/services/feeds/main.py` delegates to `backend/services/feeds/service.py`, which delegates to `backend/pipeline/storage/feed_store.py`.
- The frontend uses a BFF boundary. React services in `frontend/transcription-ui/src/service/` call TSOA controllers in `frontend/api/src/`, and the BFF calls backend services through `frontend/api/src/utils.ts`.
- Model and research workflows share manifest contracts with the product pipeline through `model/src/common/manifest.py` and `model/data/manifests/README.md`.

## Layers

**Source Capture Layer:**
- Purpose: Convert external audio source events into `CapturedChunk`, `SourceObservation`, or `FeedFailure` values.
- Location: `backend/pipeline/ingestion/collectors/`
- Contains: Source-specific clients, parsers, polling loops, event handlers, and collector tests.
- Depends on: Contracts in `backend/pipeline/ingestion/models.py` and runtime specs in `backend/pipeline/ingestion/source_runtime_specs.py`.
- Used by: `backend/pipeline/ingestion/collector_runtime.py` and direct Cloud Function entrypoints such as `backend/pipeline/ingestion/collectors/echo/main.py`.

**Ingestion Runtime Layer:**
- Purpose: Lease feeds, run collectors, upload audio, publish the first pipeline message, heartbeat active work, and apply failure/quarantine policy.
- Location: `backend/pipeline/ingestion/`
- Contains: Runtime orchestration, source routing, feed scheduling, source caps, failure classification, and ingestion settings.
- Depends on: `backend/pipeline/storage/feed_store.py`, `backend/pipeline/common/gcp_helper.py`, `backend/pipeline/common/settings.py`, and collector contracts in `backend/pipeline/ingestion/models.py`.
- Used by: VM collector entrypoint `backend/pipeline/ingestion/main.py`.

**Message Contract Layer:**
- Purpose: Define the typed data exchanged between pipeline stages.
- Location: `protos/`, generated Python package `backend/pipeline/schema_types/`
- Contains: `ContinuousAudio`, `SegmentedAudio`, `NormalizedAudio`, `TranscribedAudio`, `EvaluatedTranscribedAudio`, `AlertNotification`, and Beam streaming state messages.
- Depends on: Protobuf generation configured by `.mise.toml` and `backend/pipeline/README.md`.
- Used by: All pipeline stages under `backend/pipeline/`.

**Streaming Segmentation Layer:**
- Purpose: Convert ordered continuous chunks into speech segments with stateful stitching and VAD.
- Location: `backend/pipeline/segmentation/`
- Contains: Apache Beam orchestration in `backend/pipeline/segmentation/orchestration.py`, transforms in `backend/pipeline/segmentation/transforms/`, audio/VAD helpers in `backend/pipeline/segmentation/audio/`, and state models in `backend/pipeline/segmentation/state/`.
- Depends on: Pub/Sub `ContinuousAudio`, generated `backend/pipeline/schema_types/streaming_state.py`, and GCS helpers in `backend/pipeline/common/gcp_helper.py`.
- Used by: Dataflow entrypoint `backend/pipeline/segmentation/main.py`.

**Function Pipeline Layer:**
- Purpose: Process claim-check CloudEvents for normalization, transcription, evaluation, and notification.
- Location: `backend/pipeline/{normalization,transcription,evaluation,notification}/`
- Contains: `main.py` entrypoints, `processor.py` orchestration, settings, helper clients, and stage tests.
- Depends on: Generated protobufs in `backend/pipeline/schema_types/`, backend service APIs, GCS, Pub/Sub, and storage helpers.
- Used by: Cloud Functions defined by Terraform modules in `terraform/modules/`.

**Data API Layer:**
- Purpose: Expose authenticated CRUD/query endpoints over operational data.
- Location: `backend/services/`
- Contains: FastAPI `main.py`, pydantic request/response `models.py`, domain `service.py`, and unit tests for each service.
- Depends on: OIDC auth in `backend/pipeline/common/auth.py`, tracing in `backend/pipeline/common/tracing.py`, AlloyDB stores in `backend/pipeline/storage/`, and settings in `backend/pipeline/common/settings.py`.
- Used by: Pipeline stages and the BFF in `frontend/api/src/`.

**Storage Layer:**
- Purpose: Centralize SQL, asyncpg pools, retries, and state transitions against AlloyDB.
- Location: `backend/pipeline/storage/`
- Contains: Store classes, SQL query modules, connection helpers, and storage tests.
- Depends on: AlloyDB schema migrations in `terraform/modules/alloydb/sql/ingestion/`.
- Used by: `backend/services/*`, ingestion runtime, and pipeline processors.

**BFF Layer:**
- Purpose: Provide a browser-facing API, validate user identity/admin access, generate OpenAPI, and proxy backend services with Google ID tokens.
- Location: `frontend/api/src/`
- Contains: TSOA controllers, authentication middleware, service client utilities, config validation, and generated route/OpenAPI configuration.
- Depends on: Shared types in `frontend/common/src/`, `tsoa` config in `frontend/api/tsoa.json`, and backend service URLs from `frontend/api/src/config.ts`.
- Used by: React services in `frontend/transcription-ui/src/service/`.

**React UI Layer:**
- Purpose: Render operator workflows for feeds, transcripts, rules, docs, auth, and audio playback.
- Location: `frontend/transcription-ui/src/`
- Contains: `App.tsx` routes, feature components under `src/components/`, service clients under `src/service/`, auth context under `src/context/`, and hooks under `src/hooks/`.
- Depends on: Shared types in `frontend/common/src/`, BFF endpoints, Google OAuth, TanStack Query, and MUI.
- Used by: Browser users and local Vite development.

**Model Tooling Layer:**
- Purpose: Provide ASR dataset preparation, Gemini supervised fine-tuning, evaluation, and shared manifest utilities.
- Location: `model/src/`
- Contains: Common manifest and scoring helpers in `model/src/common/`, Gemini helpers in `model/src/common/gemini/`, and the `gemini-sft` CLI package in `model/src/gemini_sft/`.
- Depends on: Manifest rules in `model/data/manifests/README.md` and package metadata in `model/pyproject.toml`.
- Used by: Researchers, notebooks in `model/colabs/`, and model scripts in `model/scripts/`.

## Data Flow

### Primary Audio Processing Path

1. VM ingestion starts from `backend/pipeline/ingestion/main.py:15`, validates the selected Pub/Sub topics and source registry invariants, and constructs `CollectorRuntime` from `backend/pipeline/ingestion/collector_runtime.py:91`.
2. The runtime acquires eligible feed leases through `backend/pipeline/storage/feed_store.py:612` and routes each feed using `_COLLECTORS` in `backend/pipeline/ingestion/router.py:35`.
3. Source collectors return typed objects from `backend/pipeline/ingestion/models.py:1`. Runtime owns GCS upload, Pub/Sub publish, bookmarks, source observations, heartbeat, and failure release.
4. `backend/pipeline/common/gcp_helper.py:308` inspects `source_type`. `bcfy_feeds` publishes `ContinuousAudio`; pre-segmented sources publish `SegmentedAudio` with source metadata.
5. Continuous `bcfy_feeds` messages enter Beam in `backend/pipeline/segmentation/orchestration.py:60`. `ReadFromPubSub` loads `ContinuousAudio` at `backend/pipeline/segmentation/orchestration.py:98`.
6. Beam parses and keys messages at `backend/pipeline/segmentation/orchestration.py:105`, then `OrderedStitchAudioFn` stitches ordered chunks and runs VAD at `backend/pipeline/segmentation/orchestration.py:125`.
7. Beam uploads raw speech segments at `backend/pipeline/segmentation/orchestration.py:141` and publishes `SegmentedAudio` at `backend/pipeline/segmentation/orchestration.py:149`.
8. Normalization CloudEvents enter `backend/pipeline/normalization/main.py:152`, and `backend/pipeline/normalization/processor.py:89` downloads raw audio, transcodes it, uploads canonical/playback/transcription objects, writes audio segment metadata, and publishes `NormalizedAudio`.
9. Transcription CloudEvents enter `backend/pipeline/transcription/main.py:150`, and `backend/pipeline/transcription/processor.py:62` selects a transcriber, writes transcript annotations, and publishes ordered `TranscribedAudio`.
10. Evaluation CloudEvents enter `backend/pipeline/evaluation/main.py:162`, and `backend/pipeline/evaluation/processor.py:63` evaluates transcript text, writes transcript/evaluation state, and publishes notification candidates.
11. Notification CloudEvents enter `backend/pipeline/notification/send_notification.py:195`. Redis dedupe runs at `backend/pipeline/notification/send_notification.py:213`, feed enrichment runs at `backend/pipeline/notification/send_notification.py:222`, and outbound webhook delivery runs through `backend/pipeline/notification/request_handler.py`.
12. Persistent state is queryable through stores in `backend/pipeline/storage/` and service APIs in `backend/services/`.

### Direct Segmented Source Path

1. Segmented collectors such as `echo`, `openmhz`, `bcfy_calls`, and `fire_notifications` use specs in `backend/pipeline/ingestion/source_runtime_specs.py:41`.
2. `backend/pipeline/ingestion/collectors/echo/main.py:83` handles an external CloudEvent-style notification without the VM lease loop.
3. `backend/pipeline/ingestion/collectors/echo/main.py:235` publishes a `SegmentedAudio` claim-check message with `source_type="echo"` and an external segment identifier.
4. The message joins the common normalization path at `backend/pipeline/normalization/processor.py:89`.

### Operator UI and API Path

1. The React application mounts providers and routing in `frontend/transcription-ui/src/main.tsx:16` and `frontend/transcription-ui/src/App.tsx:31`.
2. Feature views call service clients in `frontend/transcription-ui/src/service/`, which use the BFF API surface under `/api/v1`.
3. TSOA controllers in `frontend/api/src/feeds/feedsController.ts:167`, `frontend/api/src/transcripts/transcriptsController.ts`, `frontend/api/src/audio/audioController.ts`, and `frontend/api/src/rules/rulesController.ts` validate request shape and auth.
4. `frontend/api/src/utils.ts` chooses unauthenticated local calls or Google ID-token service clients based on `AUTH_BACKEND`.
5. Backend FastAPI services handle CRUD/query work through `backend/services/feeds/main.py`, `backend/services/audio_segments/main.py`, `backend/services/transcripts/main.py`, and `backend/services/rules/main.py`.
6. Service classes delegate persistence to store classes in `backend/pipeline/storage/`.

### Gemini SFT Workflow

1. The CLI entrypoint `gemini-sft` is declared in `model/pyproject.toml` and implemented by `model/src/gemini_sft/cli.py:51`.
2. `model/src/gemini_sft/config.py` validates TOML run configuration and derives run paths.
3. `model/src/gemini_sft/prepare.py` validates manifests, creates artifacts, and uploads data.
4. `model/src/gemini_sft/tune.py` submits or resumes Vertex tuning jobs.
5. `model/src/gemini_sft/evaluate.py` runs batch inference and scoring using shared manifest helpers in `model/src/common/manifest.py`.

**State Management:**
- Feed lifecycle, leases, failure episodes, source observations, rules, transcripts, audio segments, and annotations persist in AlloyDB through `backend/pipeline/storage/` and migrations in `terraform/modules/alloydb/sql/ingestion/`.
- In-flight audio bytes and normalized artifacts persist in GCS through URIs carried in protobuf messages from `protos/`.
- Pipeline ordering and retries rely on Pub/Sub, CloudEvent retry semantics, and Beam state in `backend/pipeline/segmentation/state/`.
- Notification idempotency uses Redis through `backend/pipeline/notification/notification_deduplication.py`.
- UI state is request/cache oriented through TanStack Query providers in `frontend/transcription-ui/src/main.tsx`; durable UI data lives behind backend service APIs.

## Key Abstractions

**Feed and Lease:**
- Purpose: Represent an audio source plus operational state, routing metadata, and lease ownership.
- Examples: `backend/pipeline/storage/feed_store.py`, `backend/services/feeds/models.py`, `frontend/common/src/types/feeds.ts`.
- Pattern: Enum-backed source/status models with store-owned state transitions.

**Capture Event Contract:**
- Purpose: Keep source collectors pure from infrastructure side effects.
- Examples: `backend/pipeline/ingestion/models.py`, `backend/pipeline/ingestion/collector_runtime.py`.
- Pattern: Collectors yield `CapturedChunk`, `SourceObservation`, or `FeedFailure`; the runtime performs persistence and publishing.

**Source Runtime Spec:**
- Purpose: Centralize source capture mode, lease eligibility, concurrency caps, and lease duration.
- Examples: `backend/pipeline/ingestion/source_runtime_specs.py`, `backend/pipeline/ingestion/router.py`.
- Pattern: One registry entry per source type; runtime validates registry consistency at startup.

**Pipeline Protobuf Message:**
- Purpose: Provide explicit stage contracts for Pub/Sub and generated Python types.
- Examples: `protos/continuous_audio.proto`, `protos/segmented_audio.proto`, `protos/normalized_audio.proto`, `protos/transcribed_audio.proto`, `protos/evaluated_transcribed_audio.proto`, `protos/alert_notification.proto`.
- Pattern: Edit proto definitions, generate code through `mise run generate:protos`, and consume generated classes from `backend/pipeline/schema_types/`.

**Store Class:**
- Purpose: Encapsulate SQL and transactional behavior for a domain aggregate.
- Examples: `backend/pipeline/storage/feed_store.py`, `backend/pipeline/storage/audio_segment_store.py`, `backend/pipeline/storage/transcript_store.py`, `backend/pipeline/storage/rules_store.py`.
- Pattern: FastAPI services and processors call typed store methods instead of embedding SQL in request handlers.

**Processor Container:**
- Purpose: Lazily construct reusable clients/settings for Cloud Function invocations.
- Examples: `backend/pipeline/normalization/main.py`, `backend/pipeline/transcription/main.py`, `backend/pipeline/evaluation/main.py`, `backend/pipeline/notification/send_notification.py`.
- Pattern: `main.py` handles CloudEvent parsing and dependency container setup; `processor.py` owns stage behavior.

**Transcriber:**
- Purpose: Hide speech-to-text provider differences from the transcription processor.
- Examples: `backend/pipeline/transcription/transcribers/factory.py`, `backend/pipeline/transcription/transcribers/chirp.py`, `backend/pipeline/transcription/transcribers/local_api.py`, `backend/pipeline/transcription/transcribers/mock.py`.
- Pattern: Select implementation via transcription settings and call a provider-neutral interface.

**Text Evaluator:**
- Purpose: Evaluate transcript text against static or remote rules.
- Examples: `backend/pipeline/evaluation/rules_evaluation/evaluator.py`, `backend/pipeline/evaluation/service.py`.
- Pattern: `EvaluationService` builds `EvaluatedTranscribedAudio`; evaluator implementations provide rules.

**TSOA Controller:**
- Purpose: Define browser-facing routes, OpenAPI metadata, auth requirements, and request/response types.
- Examples: `frontend/api/src/feeds/feedsController.ts`, `frontend/api/src/rules/rulesController.ts`, `frontend/api/tsoa.json`.
- Pattern: Add controller methods and regenerate TSOA routes instead of editing generated output.

**React Feature Module:**
- Purpose: Group UI components, service calls, hooks, and shared types around operator workflows.
- Examples: `frontend/transcription-ui/src/components/feeds/`, `frontend/transcription-ui/src/components/transcripts/`, `frontend/transcription-ui/src/service/feeds.ts`, `frontend/common/src/types/feeds.ts`.
- Pattern: Shared types live in `frontend/common/src/types/`; UI-specific behavior lives under `frontend/transcription-ui/src/`.

## Entry Points

**VM Collector Runtime:**
- Location: `backend/pipeline/ingestion/main.py`
- Triggers: Collector VM or local process startup.
- Responsibilities: Load ingestion settings, validate source registry/topic consistency, run `CollectorRuntime`.

**Echo Ingestion Function:**
- Location: `backend/pipeline/ingestion/collectors/echo/main.py`
- Triggers: External echo audio notification event.
- Responsibilities: Validate notification payload, fetch/upload audio, publish `SegmentedAudio`.

**Segmentation Dataflow Job:**
- Location: `backend/pipeline/segmentation/main.py`
- Triggers: Dataflow/Beam job invocation.
- Responsibilities: Build and run the streaming segmentation graph from `backend/pipeline/segmentation/orchestration.py`.

**Normalization Cloud Function:**
- Location: `backend/pipeline/normalization/main.py`
- Triggers: Pub/Sub CloudEvent containing `SegmentedAudio`.
- Responsibilities: Normalize audio objects, persist segment metadata, publish `NormalizedAudio`.

**Transcription Cloud Function:**
- Location: `backend/pipeline/transcription/main.py`
- Triggers: Pub/Sub CloudEvent containing `NormalizedAudio`.
- Responsibilities: Transcribe audio and publish `TranscribedAudio`.

**Evaluation Cloud Function:**
- Location: `backend/pipeline/evaluation/main.py`
- Triggers: Pub/Sub CloudEvent containing `TranscribedAudio`.
- Responsibilities: Evaluate transcript text, persist transcript/evaluation data, publish alert candidates.

**Notification Cloud Function:**
- Location: `backend/pipeline/notification/send_notification.py`
- Triggers: Pub/Sub CloudEvent containing `AlertNotification`.
- Responsibilities: Deduplicate and deliver outbound notifications.

**Backend Services:**
- Location: `backend/services/feeds/main.py`, `backend/services/audio_segments/main.py`, `backend/services/transcripts/main.py`, `backend/services/rules/main.py`
- Triggers: HTTP requests from BFF or pipeline stages.
- Responsibilities: Authenticated CRUD/query operations over AlloyDB-backed domain data.

**BFF HTTP Server:**
- Location: `frontend/api/src/index.ts`
- Triggers: Node process startup.
- Responsibilities: Register TSOA routes, auth middleware, CORS, error handling, and docs endpoints.

**React Application:**
- Location: `frontend/transcription-ui/src/main.tsx`
- Triggers: Browser page load through Vite/build assets.
- Responsibilities: Mount providers, configure routing, and render operator workflows.

**Model CLI:**
- Location: `model/src/gemini_sft/cli.py`
- Triggers: `gemini-sft` console command from `model/pyproject.toml`.
- Responsibilities: Dispatch `prepare`, `tune`, and `eval` ASR workflow commands.

## Architectural Constraints

- **Threading:** Ingestion uses a long-running runtime plus heartbeat behavior in `backend/pipeline/ingestion/collector_runtime.py`; Beam segmentation uses stateful streaming transforms in `backend/pipeline/segmentation/orchestration.py`; FastAPI services and stores use async IO through `backend/pipeline/storage/connection.py`.
- **Global state:** Cloud Function modules cache dependency containers or clients in stage entrypoints such as `backend/pipeline/normalization/main.py`, `backend/pipeline/transcription/main.py`, `backend/pipeline/evaluation/main.py`, and `backend/pipeline/notification/send_notification.py`.
- **Circular imports:** No intentional circular dependency chain is represented in the inspected layers. Preserve one-way dependencies from entrypoints to processors/services to stores, and from UI to BFF to backend services.
- **Generated code:** Do not edit generated protobuf files under `backend/pipeline/schema_types/` or TSOA route output configured by `frontend/api/tsoa.json`. Edit `protos/*.proto` or `frontend/api/src/*Controller.ts` and regenerate through the configured tasks.
- **Source registry consistency:** A source type is represented across storage enum/seed SQL, runtime specs, routing, collector code, and shared frontend feed types. Keep `backend/pipeline/storage/feed_store.py`, `terraform/modules/alloydb/sql/ingestion/`, `backend/pipeline/ingestion/source_runtime_specs.py`, `backend/pipeline/ingestion/router.py`, and `frontend/common/src/types/feeds.ts` aligned.
- **Auth boundary:** Browser requests terminate at `frontend/api/src/`; backend service auth uses OIDC verification in `backend/pipeline/common/auth.py`. React components do not call `backend/services/*` directly.
- **Local development secrets:** Environment files such as `local_dev/LOCAL.env` are configuration inputs only and are not source of architectural truth.

## Anti-Patterns

### Collector-Owned Infrastructure Side Effects

**What happens:** Source collector code uploads to GCS, writes feed state, or publishes Pub/Sub directly while also returning capture results.
**Why it's wrong:** It bypasses the runtime ownership boundary documented in `backend/pipeline/ingestion/models.py:1` and creates duplicate side effects outside lease/failure handling in `backend/pipeline/ingestion/collector_runtime.py`.
**Do this instead:** Return `CapturedChunk`, `SourceObservation`, or `FeedFailure` from collector code under `backend/pipeline/ingestion/collectors/`; let `backend/pipeline/ingestion/collector_runtime.py` upload, publish, bookmark, and release leases.

### Generated Contract Edits

**What happens:** Code is changed directly under `backend/pipeline/schema_types/` or TSOA-generated output rather than changing source definitions.
**Why it's wrong:** Generated files are overwritten by `mise run generate:protos` from `.mise.toml` or TSOA generation from `frontend/api/tsoa.json`.
**Do this instead:** Edit protobuf contracts under `protos/` or controllers under `frontend/api/src/`, then run the configured generation task.

### Partial Source-Type Registration

**What happens:** A new source appears only in one of `backend/pipeline/ingestion/router.py`, `backend/pipeline/ingestion/source_runtime_specs.py`, `backend/pipeline/storage/feed_store.py`, Terraform seed SQL, or `frontend/common/src/types/feeds.ts`.
**Why it's wrong:** Runtime validation, feed CRUD, lease acquisition, UI configuration, and database constraints rely on the same source vocabulary.
**Do this instead:** Add source types across the storage enum, SQL seeds, runtime specs, router, collector tests, and shared frontend feed types in one change set.

### Diagnostic Text as Control Flow

**What happens:** Code branches on freeform `quarantine_reason` or failure text.
**Why it's wrong:** Operational policy is encoded by stable enums and failure handling under `backend/pipeline/storage/feed_store.py` and `backend/pipeline/ingestion/failure_policy.py`; diagnostic strings are for operators.
**Do this instead:** Branch on typed status/reason enums and keep diagnostic strings as display/observability details.

### UI Direct-to-Service Calls

**What happens:** React code calls URLs for `backend/services/*` directly or duplicates backend service auth.
**Why it's wrong:** `frontend/api/src/authentication.ts` and `frontend/api/src/utils.ts` are the browser auth and service-token boundary.
**Do this instead:** Add a BFF endpoint in `frontend/api/src/<domain>/`, shared types in `frontend/common/src/types/`, and React service calls in `frontend/transcription-ui/src/service/`.

## Error Handling

**Strategy:** Convert low-level failures into stage-specific retry, dead-letter, quarantine, or API error behavior while preserving typed state transitions.

**Patterns:**
- Ingestion classifies source failures through `backend/pipeline/ingestion/failure_policy.py` and records failure episodes through `backend/pipeline/storage/feed_store.py`.
- Beam segmentation publishes malformed or failed records to DLQ output in `backend/pipeline/segmentation/orchestration.py:157`.
- Function processors publish DLQ messages or raise retryable errors from stage code such as `backend/pipeline/normalization/processor.py`, `backend/pipeline/transcription/processor.py`, and `backend/pipeline/evaluation/processor.py`.
- FastAPI services raise HTTP exceptions from `backend/services/*/main.py` and keep persistence errors inside service/store boundaries.
- BFF error handling is centralized in `frontend/api/src/index.ts:30`, converting thrown errors into API responses.
- Notification retryability is explicit in `backend/pipeline/notification/send_notification.py`; dedupe keys are cleared for retryable outbound failures.

## Cross-Cutting Concerns

**Logging:** Backend code uses Python logging and structured stage logs in `backend/pipeline/*`; Node BFF errors are handled through `frontend/api/src/index.ts`. Keep stage identifiers and feed/segment IDs in logs.

**Validation:** Pydantic validates backend service requests in `backend/services/*/models.py`, protobufs define pipeline message shape in `protos/`, TSOA validates BFF contracts from `frontend/api/src/*Controller.ts`, and model manifests are validated by `model/src/common/manifest.py`.

**Authentication:** Backend services use OIDC dependency injection from `backend/pipeline/common/auth.py`; the BFF validates browser identity and admin membership in `frontend/api/src/authentication.ts`; local unauthenticated BFF mode is controlled by `frontend/api/src/config.ts`.

**Tracing:** FastAPI services call tracing setup from `backend/pipeline/common/tracing.py`; keep new service entrypoints consistent with `backend/services/feeds/main.py`.

**Configuration:** Backend runtime settings live under `backend/pipeline/common/settings.py` and service-specific settings modules; frontend config is validated in `frontend/api/src/config.ts` and Vite config is in `frontend/transcription-ui/vite.config.ts`; root workflow tasks live in `.mise.toml`.

**Schema Evolution:** Database schema changes are migrations under `terraform/modules/alloydb/sql/ingestion/`; protobuf changes start in `protos/`; shared API type changes start in `frontend/common/src/types/`.

---

*Architecture analysis: 2026-06-19*

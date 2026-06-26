<!-- refreshed: 2026-06-26 -->
# Architecture

**Analysis Date:** 2026-06-26

## System Overview

```text
┌───────────────────────────────────────────────────────────────────────────────┐
│                         External Inputs And Operators                         │
├────────────────────┬────────────────────┬────────────────────┬───────────────┤
│ Radio source feeds │ Echo GCS recordings│ Admin / operator UI│ Model operator │
│ `backend/pipeline/ │ `backend/pipeline/ │ `frontend/transcrip│ `model/src/    │
│  ingestion`        │  ingestion/        │  tion-ui/src`      │  gemini_sft`   │
│                    │  collectors/echo`  │                    │               │
└─────────┬──────────┴─────────┬──────────┴─────────┬──────────┴───────┬───────┘
          │                    │                    │                  │
          ▼                    ▼                    ▼                  ▼
┌───────────────────────┐ ┌────────────────┐ ┌────────────────┐ ┌──────────────┐
│ VM collector runtime  │ │ Echo function  │ │ TypeScript BFF │ │ Gemini SFT   │
│ `backend/pipeline/    │ │ `backend/      │ │ `frontend/api/ │ │ CLI          │
│  ingestion/main.py`   │ │ pipeline/      │ │ src`           │ │ `model/src/  │
│                       │ │ ingestion/...` │ │                │ │ gemini_sft`  │
└─────────┬─────────────┘ └────────┬───────┘ └───────┬────────┘ └──────┬───────┘
          │                        │                 │                 │
          ▼                        ▼                 ▼                 ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                         Event And Service Layer                               │
│ Pub/Sub claim-check pipeline: ingestion -> segmentation -> normalization ->   │
│ transcription -> evaluation -> notification                                   │
│ `backend/pipeline/*`, `protos/*.proto`                                        │
│                                                                               │
│ FastAPI backend services: feeds, audio segments, rules                        │
│ `backend/services/*/main.py`                                                  │
└───────────────────────────────────┬───────────────────────────────────────────┘
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                      Shared Persistence And Infrastructure                     │
│ AlloyDB schema and stores: `backend/pipeline/storage`,                        │
│ `terraform/modules/alloydb/sql/ingestion`                                     │
│ GCS audio/artifact buckets, Pub/Sub topics, Redis notification dedupe,         │
│ Terraform modules in `terraform/modules`                                      │
└───────────────────────────────────────────────────────────────────────────────┘
```

## Component Responsibilities

| Component | Responsibility | File |
|-----------|----------------|------|
| VM ingestion entry point | Starts the collector runtime, validates source registries, and blocks until graceful shutdown. | `backend/pipeline/ingestion/main.py` |
| Ingestion router | Maps `SourceType` values to collector functions and Pub/Sub topic families. | `backend/pipeline/ingestion/router.py` |
| Collector runtime | Leases feeds, runs async collector tasks, uploads captured audio to GCS, bookmarks progress, publishes Pub/Sub, renews heartbeats, and records failures. | `backend/pipeline/ingestion/collector_runtime.py` |
| Source runtime registry | Defines claimable source types, per-worker caps, URL bases, and continuous versus segmented topic routing. | `backend/pipeline/ingestion/source_runtime_specs.py` |
| Echo ingestion | Handles GCS object finalize events for Echo MP3 recordings through a synchronous Cloud Function path. | `backend/pipeline/ingestion/collectors/echo/main.py` |
| Segmentation pipeline | Runs the streaming Apache Beam/Dataflow DAG that orders, stitches, VAD-classifies, uploads raw segments, and emits `SegmentedAudio`. | `backend/pipeline/segmentation/orchestration.py` |
| Normalization function | Converts `SegmentedAudio` claim-checks into canonical/playback audio, persists segment metadata, and emits `NormalizedAudio`. | `backend/pipeline/normalization/processor.py` |
| Transcription service | Consumes `NormalizedAudio`, invokes a selected transcriber, writes transcript annotations, and emits `TranscribedAudio`. | `backend/pipeline/transcription/processor.py` |
| Transcriber factory | Selects Chirp, Gemini, local Whisper, or mock transcriber implementations from runtime config. | `backend/pipeline/transcription/transcribers/factory.py` |
| Evaluation function | Evaluates transcripts against rules, writes evaluation annotations, and publishes evaluated alert payloads. | `backend/pipeline/evaluation/processor.py` |
| Notification function | Deduplicates evaluated alerts, fetches feed tags, builds notification protobufs, and posts outbound notifications. | `backend/pipeline/notification/send_notification.py` |
| Feeds service | Exposes feed lifecycle CRUD/reset/deactivate routes and resolves trusted actor headers for audited mutations. | `backend/services/feeds/main.py` |
| Audio segments service | Exposes segment listing, segment creation, and annotation creation over FastAPI. | `backend/services/audio_segments/main.py` |
| Rules service | Exposes rule CRUD routes and delegates to AlloyDB-backed rule storage. | `backend/services/rules/main.py` |
| Storage layer | Owns AlloyDB access, SQL contracts, feed lifecycle/audit SQL, pagination, and service stores. | `backend/pipeline/storage` |
| Protobuf contracts | Define Pub/Sub message boundaries between pipeline stages. | `protos` |
| Frontend BFF | Provides TSOA routes, Google auth integration, service-to-service ID-token clients, and backend response conversion. | `frontend/api/src` |
| React UI | Provides operator views for feeds, transcripts, rules, docs, auth, playback, and polling. | `frontend/transcription-ui/src` |
| Shared frontend types | Publishes UI/BFF domain types and status conversion helpers. | `frontend/common/src/index.ts` |
| Model package | Provides shared ASR/model helpers and the `gemini-sft` CLI workflow. | `model/src` |
| Infrastructure modules | Define reusable GCP resources and AlloyDB SQL migrations. | `terraform/modules` |

## Pattern Overview

**Overall:** Event-driven claim-check pipeline plus service/store APIs in a Python/TypeScript monorepo.

**Key Characteristics:**
- Audio bytes move through GCS claim-check URIs; Pub/Sub carries protobuf metadata from `protos/*.proto`.
- Backend services use a controller/service/store split: FastAPI route in `backend/services/*/main.py`, domain service in `backend/services/*/service.py`, SQL store in `backend/pipeline/storage/*_store.py`.
- Ingestion separates source-specific collection from runtime-owned side effects. Collectors emit `CapturedChunk`, `SourceObservation`, or `FeedFailure`; `CollectorRuntime` owns GCS, Pub/Sub, bookmarks, heartbeats, and failure budgeting.
- The frontend separates browser views in `frontend/transcription-ui/src`, BFF controllers in `frontend/api/src`, and shared TypeScript contracts in `frontend/common/src`.
- Research/model workflows are packaged separately under `model/src` and use GCS-authoritative run state instead of production service state.

## Layers

**Domain Contracts:**
- Purpose: Define message and type boundaries used by production pipeline, API, UI, and model workflows.
- Location: `protos`, `frontend/common/src`, `backend/services/*/models.py`, `backend/pipeline/common/rules/models.py`, `model/src/common`
- Contains: Protobuf schemas, Pydantic models, TypeScript interfaces, manifest helpers, scoring helpers.
- Depends on: Protobuf tooling, Pydantic, TypeScript packages, Python standard libraries.
- Used by: `backend/pipeline/*`, `backend/services/*`, `frontend/api/src`, `frontend/transcription-ui/src`, `model/src/gemini_sft`.

**Ingestion:**
- Purpose: Claim feeds, collect source audio, upload staged audio, publish source-specific claim-check messages, and maintain feed lifecycle state.
- Location: `backend/pipeline/ingestion`
- Contains: VM runtime, source collector registry, source runtime specs, collector implementations, failure classifiers, health/memory watchdogs, retry policy.
- Depends on: `backend/pipeline/storage`, `backend/pipeline/common`, GCS, Pub/Sub, AlloyDB.
- Used by: VM capturer deployment and Echo Cloud Function deployment.

**Segmentation:**
- Purpose: Transform continuous audio chunks into ordered speech/non-speech segments with VAD and stateful stitching.
- Location: `backend/pipeline/segmentation`
- Contains: Apache Beam pipeline assembly, stateful transforms, pure-Python stitching engine, VAD/audio utilities, Beam coders.
- Depends on: Pub/Sub, GCS, Beam/Dataflow, ONNX VAD models under `backend/pipeline/segmentation/audio/models`.
- Used by: Continuous ingestion topics before normalization.

**Normalization:**
- Purpose: Convert staged/raw segment audio into canonical FLAC, playback M4A, optional mono transcription FLAC, persisted audio segment rows, and downstream normalized claim-checks.
- Location: `backend/pipeline/normalization`
- Contains: Functions Framework entry point, warm client container, `NormalizationEventProcessor`, audio transcode helpers.
- Depends on: `backend/pipeline/common/storage/gcs_uploader.py`, `backend/pipeline/schema_types`, `backend/pipeline/common/clients/audio_segments_client.py`, GCS, Pub/Sub.
- Used by: Cloud Function or Cloud Run triggered from segmented Pub/Sub.

**Transcription:**
- Purpose: Invoke ASR, persist transcript annotation data, and publish `TranscribedAudio`.
- Location: `backend/pipeline/transcription`
- Contains: FastAPI Pub/Sub push endpoint, warm service container, processor, transcriber interface and implementations.
- Depends on: Google Chirp/Gemini/local Whisper/mock transcribers, `backend/pipeline/common/clients/audio_segments_client.py`, Pub/Sub.
- Used by: Cloud Run ASGI service triggered by Pub/Sub push.

**Evaluation And Notification:**
- Purpose: Evaluate transcript text against configured rules, persist evaluation annotations, publish alert candidates, deduplicate, and send outbound notifications.
- Location: `backend/pipeline/evaluation`, `backend/pipeline/notification`
- Contains: Evaluation processor/service, remote/static rule evaluators, notification conversion, Redis-backed dedupe, outbound request handler.
- Depends on: Rules API, Audio Segments API, Feeds API, Pub/Sub, Redis, outbound notification endpoint.
- Used by: Cloud Function/Cloud Run functions triggered by transcribed/evaluated Pub/Sub topics.

**Backend Services:**
- Purpose: Provide internal HTTP APIs for feed lifecycle, audio segments/annotations, and rules.
- Location: `backend/services`
- Contains: FastAPI apps, Pydantic models, thin service classes, tests.
- Depends on: `backend/pipeline/storage`, `backend/pipeline/common/auth.py`, AlloyDB.
- Used by: Pipeline functions through service clients and by the frontend BFF.

**Storage:**
- Purpose: Centralize AlloyDB access, feed lifecycle invariants, audit event SQL, keyset pagination, and sync/async store variants.
- Location: `backend/pipeline/storage`
- Contains: `FeedStore`, `SyncFeedStore`, `AudioSegmentStore`, `RulesStore`, `TranscriptStore`, SQL query modules, connection helpers, pagination.
- Depends on: `terraform/modules/alloydb/sql/ingestion` schema, `asyncpg`, `psycopg`, Pydantic service models.
- Used by: FastAPI services, ingestion runtime, Echo ingestion, integration tests.

**Frontend BFF:**
- Purpose: Authenticate users, expose UI-facing REST routes, translate casing/shape between common UI types and backend service APIs, and call backend services with Google ID-token clients.
- Location: `frontend/api/src`
- Contains: Express app, TSOA controllers, generated route registration, auth handler, config, downstream HTTP utilities.
- Depends on: `frontend/common/src`, Google auth libraries, downstream backend service URLs.
- Used by: `frontend/transcription-ui/src` browser services.

**React UI:**
- Purpose: Render operator views, manage auth context, poll transcript/audio segment data, and call BFF endpoints.
- Location: `frontend/transcription-ui/src`
- Contains: Routes, MUI shell, auth provider, React Query hooks, service wrappers, components, playback utilities.
- Depends on: `frontend/common/src`, BFF API base URL, Google OAuth client ID.
- Used by: Browser users.

**Model And Evaluation Tooling:**
- Purpose: Package reusable ASR/model utilities and the Gemini supervised fine-tuning workflow.
- Location: `model/src`
- Contains: `common` helpers, Gemini Vertex helpers, manifest/scoring utilities, `gemini_sft` CLI.
- Depends on: GCS, Vertex Gemini APIs, optional audio/scoring/HF extras from `model/pyproject.toml`.
- Used by: Researchers/operators running `gemini-sft prepare`, `gemini-sft tune`, and `gemini-sft eval`.

**Infrastructure:**
- Purpose: Define deployable GCP resources and database schema.
- Location: `terraform/modules`
- Contains: AlloyDB, Cloud Function, container MIG, GCS bucket, Memorystore Redis, ASR evaluation modules, SQL migrations.
- Depends on: Terraform module consumers outside this subtree and SQL migrations under `terraform/modules/alloydb/sql/ingestion`.
- Used by: Deployment automation and local/integration setup.

## Data Flow

### Primary Audio Processing Path

1. VM ingestion starts from `backend/pipeline/ingestion/main.py:15`, validates source registry/cap parity at `backend/pipeline/ingestion/main.py:27`, and starts `CollectorRuntime` at `backend/pipeline/ingestion/main.py:50`.
2. `route_capturer` selects a collector from `_COLLECTORS` in `backend/pipeline/ingestion/router.py:35` and resolves continuous versus segmented topic routing through `backend/pipeline/ingestion/router.py:50`.
3. `CollectorRuntime` creates AlloyDB stores and clients in `backend/pipeline/ingestion/collector_runtime.py:219`, leases feeds through `backend/pipeline/ingestion/collector_runtime.py:343`, uploads/bookmarks/publishes captured chunks in `backend/pipeline/ingestion/collector_runtime.py:739`, and handles feed-level events in `backend/pipeline/ingestion/collector_runtime.py:1178`.
4. Continuous chunks enter the Beam DAG assembled by `get_pipeline` in `backend/pipeline/segmentation/orchestration.py:60`, read from Pub/Sub at `backend/pipeline/segmentation/orchestration.py:98`, pass through `OrderedStitchAudioFn` at `backend/pipeline/segmentation/orchestration.py:125`, upload raw segments at `backend/pipeline/segmentation/orchestration.py:142`, and publish `SegmentedAudio` at `backend/pipeline/segmentation/orchestration.py:149`.
5. Segmented source chunks, including Echo and polling collectors, feed normalization directly through the `SegmentedAudio` Pub/Sub topic defined by the same source runtime/topic routing.
6. Normalization receives Pub/Sub CloudEvents at `backend/pipeline/normalization/main.py:151`, downloads raw audio and transcodes derivatives in `backend/pipeline/normalization/processor.py:124`, persists segment metadata in `backend/pipeline/normalization/processor.py:183`, and publishes `NormalizedAudio` in `backend/pipeline/normalization/processor.py:191`.
7. Transcription receives Pub/Sub push requests at `backend/pipeline/transcription/main.py:190`, parses `NormalizedAudio` in `backend/pipeline/transcription/processor.py:67`, invokes the selected transcriber in `backend/pipeline/transcription/processor.py:162`, writes transcript annotations in `backend/pipeline/transcription/processor.py:258`, and publishes `TranscribedAudio` in `backend/pipeline/transcription/processor.py:235`.
8. Evaluation receives transcribed CloudEvents at `backend/pipeline/evaluation/main.py:144`, evaluates transcript text in `backend/pipeline/evaluation/service.py:106`, writes evaluation annotations in `backend/pipeline/evaluation/processor.py:110`, and publishes evaluated alert payloads in `backend/pipeline/evaluation/processor.py:142`.
9. Notification receives evaluated CloudEvents at `backend/pipeline/notification/send_notification.py:196`, deduplicates with Redis at `backend/pipeline/notification/send_notification.py:215`, fetches feed tags at `backend/pipeline/notification/send_notification.py:223`, converts the message at `backend/pipeline/notification/send_notification.py:162`, and posts outbound notifications at `backend/pipeline/notification/send_notification.py:237`.

### Echo Ingestion Path

1. Eventarc invokes `handle_notification` on GCS object finalize events at `backend/pipeline/ingestion/collectors/echo/main.py:91`.
2. Echo lazily initializes GCS, Pub/Sub, and sync feed store clients at `backend/pipeline/ingestion/collectors/echo/main.py:102`.
3. `_handle` validates MP3 object shape, resolves the feed, skips deactivated/quarantined feeds, downloads audio, stages it, and publishes to the segmented Pub/Sub topic from `backend/pipeline/ingestion/collectors/echo/main.py:118`.
4. Echo uses `SyncFeedStore` in `backend/pipeline/storage/sync_feed_store.py` and `failure_policy` in `backend/pipeline/ingestion/failure_policy.py` instead of the VM leasing runtime.

### UI And Admin Request Path

1. Browser startup creates providers and the router in `frontend/transcription-ui/src/main.tsx:16`.
2. `App` gates routes on auth state and maps `/`, `/transcripts`, `/rules`, `/feeds`, `/docs`, and `/login` in `frontend/transcription-ui/src/App.tsx:197`.
3. React Query hooks call service wrappers, for example `useAudioSegments` calls `listAudioSegments` at `frontend/transcription-ui/src/hooks/useAudioSegments.ts:98`.
4. Browser service wrappers call the BFF with bearer tokens, for example `frontend/transcription-ui/src/service/listAudioSegments.ts:15`.
5. The BFF registers generated TSOA routes in `frontend/api/src/index.ts:28`, authenticates Google user metadata in `frontend/api/src/authentication.ts:14`, and creates service-to-service clients in `frontend/api/src/utils.ts:99`.
6. TSOA controllers call downstream backend services and convert shapes, for example feeds controller routes start at `frontend/api/src/feeds/feedsController.ts:191` and call `FEEDS_STORE_API_URL` in `frontend/api/src/feeds/feedsController.ts:222`.
7. Backend FastAPI services expose domain APIs and delegate to service/store layers, for example feed routes in `backend/services/feeds/main.py:82`, audio segment routes in `backend/services/audio_segments/main.py:56`, and rules routes in `backend/services/rules/main.py:48`.

### Gemini SFT Operator Path

1. The packaged CLI entry point is `gemini-sft = "gemini_sft.cli:main"` in `model/pyproject.toml:42` and dispatches subcommands in `model/src/gemini_sft/cli.py:13`.
2. `prepare` loads a TOML config, validates run prefix state, downloads canonical manifests, creates Gemini JSONL, runs preflight, and writes local/GCS artifacts from `model/src/gemini_sft/prepare.py:47`.
3. `tune` uses GCS `config.json` as durable state, resumes existing jobs or submits Vertex tuning jobs, and writes status/config artifacts from `model/src/gemini_sft/tune.py:77`.
4. `eval` downloads canonical eval rows, runs Vertex batch inference, computes WER/CER/keyword metrics, writes summaries, and appends the ledger from `model/src/gemini_sft/evaluate.py:53`.

**State Management:**
- AlloyDB is the system of record for feeds, feed lifecycle state, feed audit events, audio segments, annotations, rules, and legacy transcript storage through `backend/pipeline/storage`.
- GCS stores staged raw chunks, canonical/playback/transcription audio, and model/SFT artifacts; pipeline messages carry GCS URIs instead of large audio payloads.
- Pub/Sub carries protobuf claim-check messages and uses feed IDs as ordering keys where order matters.
- Apache Beam/Dataflow owns state and timers for continuous audio ordering/stitching in `backend/pipeline/segmentation/transforms/stateful.py`.
- Redis backs notification deduplication through `backend/pipeline/common/storage/redis_service.py` and `backend/pipeline/notification/notification_deduplication.py`.
- React Query owns browser-side request caches in hooks under `frontend/transcription-ui/src/hooks`.
- Serverless entry points cache clients/processors in module-level or lifespan containers such as `NormalizationServiceContainer`, `TranscriptionServiceContainer`, `EvaluationServiceContainer`, and `NotificationServiceContainer`.

## Key Abstractions

**Source Runtime Spec:**
- Purpose: Data-only source metadata for source type, topic kind, claimability, caps, and URL base.
- Examples: `backend/pipeline/ingestion/source_runtime_specs.py`, `backend/pipeline/storage/feed_store.py`
- Pattern: Registry plus startup invariant; update `SourceType`, SQL seed data, `SourceRuntimeSpec`, router, and tests together.

**Collector Contract:**
- Purpose: Isolate source-specific collection from runtime-owned side effects and feed lifecycle state.
- Examples: `backend/pipeline/ingestion/models.py`, `backend/pipeline/ingestion/collectors/README.md`, `backend/pipeline/ingestion/router.py`
- Pattern: Async generator yields `CapturedChunk` or `SourceObservation`; typed failures use `FeedFailure`.

**Feed Store And Audit SQL:**
- Purpose: Centralize lifecycle transitions, lease fencing, failure budgeting, recovery, hard delete, reset, and audit event inserts.
- Examples: `backend/pipeline/storage/feed_store.py`, `backend/pipeline/storage/feed_queries.py`, `backend/pipeline/storage/feed_audit_sql.py`, `backend/pipeline/storage/sync_feed_store.py`
- Pattern: Store methods call SQL constants/fragments; services pass explicit `actor_id`; audit JSON allowlists live in SQL helper fragments.

**Claim-Check Protobufs:**
- Purpose: Define durable Pub/Sub boundaries for audio chunks and stage outputs.
- Examples: `protos/continuous_audio.proto`, `protos/segmented_audio.proto`, `protos/normalized_audio.proto`, `protos/transcribed_audio.proto`, `protos/evaluated_transcribed_audio.proto`, `protos/alert_notification.proto`
- Pattern: Protobuf metadata plus GCS URIs, with generated Python bindings under `backend/pipeline/schema_types`.

**Processor Classes:**
- Purpose: Keep cloud/function entry point code small and make stage logic testable.
- Examples: `backend/pipeline/normalization/processor.py`, `backend/pipeline/transcription/processor.py`, `backend/pipeline/evaluation/processor.py`
- Pattern: Entry point parses/wires clients; processor owns parsing, business logic, persistence, and publishing.

**Transcriber Interface:**
- Purpose: Select ASR backend at runtime while keeping transcription processor independent of implementation details.
- Examples: `backend/pipeline/transcription/transcribers/base.py`, `backend/pipeline/transcription/transcribers/factory.py`, `backend/pipeline/transcription/enums.py`
- Pattern: Factory returns an implementation from `TRANSCRIBER_TYPE` and `TRANSCRIBER_CONFIG`.

**FastAPI Service Boundary:**
- Purpose: Expose internal HTTP APIs over store-backed domain services.
- Examples: `backend/services/feeds/main.py`, `backend/services/audio_segments/main.py`, `backend/services/rules/main.py`
- Pattern: Lifespan creates AlloyDB pool and service; route handlers translate validation/store errors into HTTP responses.

**TSOA BFF Controller:**
- Purpose: Present UI-friendly API contracts and handle auth/admin checks before calling backend services.
- Examples: `frontend/api/src/feeds/feedsController.ts`, `frontend/api/src/audio/audioController.ts`, `frontend/api/src/rules/rulesController.ts`, `frontend/api/src/auth/authController.ts`
- Pattern: Controller decorators generate routes; controller methods convert common types to backend wire format and use `getServiceClient`.

**React Query Hook Plus Service Wrapper:**
- Purpose: Keep UI components declarative while service files own HTTP calls.
- Examples: `frontend/transcription-ui/src/hooks/useAudioSegments.ts`, `frontend/transcription-ui/src/service/listAudioSegments.ts`, `frontend/transcription-ui/src/utils/apiUtils.ts`
- Pattern: Hooks own cache keys and pagination/polling; `service/*.ts` owns endpoint construction and bearer tokens.

**Gemini SFT Run Config:**
- Purpose: External TOML config drives repeatable model prepare/tune/eval workflows with GCS as source of truth.
- Examples: `model/src/gemini_sft/config.py`, `model/src/gemini_sft/prepare.py`, `model/src/gemini_sft/tune.py`, `model/src/gemini_sft/evaluate.py`
- Pattern: CLI subcommands share `RunConfig`, persist `config.json`, and mirror artifacts locally under `results/`.

## Entry Points

**VM Ingestion Worker:**
- Location: `backend/pipeline/ingestion/main.py`
- Triggers: Container process start.
- Responsibilities: Initialize logging/tracing/settings, validate source routing and caps, start `CollectorRuntime`.

**Echo Ingestion Function:**
- Location: `backend/pipeline/ingestion/collectors/echo/main.py`
- Triggers: Eventarc GCS object finalize events.
- Responsibilities: Resolve Echo feed, download MP3, stage audio, publish segmented claim-check, update sync feed state.

**Segmentation Dataflow Job:**
- Location: `backend/pipeline/segmentation/main.py`
- Triggers: CLI/container process start with Beam options.
- Responsibilities: Build and run the streaming Beam DAG from `backend/pipeline/segmentation/orchestration.py`.

**Normalization Cloud Function:**
- Location: `backend/pipeline/normalization/main.py`
- Triggers: Pub/Sub CloudEvent containing `SegmentedAudio`.
- Responsibilities: Get/warm `NormalizationEventProcessor` and process the event.

**Transcription ASGI Service:**
- Location: `backend/pipeline/transcription/main.py`
- Triggers: Pub/Sub push HTTP POST to `/`.
- Responsibilities: Warm transcriber/publisher/API clients and process `NormalizedAudio`.

**Evaluation Cloud Function:**
- Location: `backend/pipeline/evaluation/main.py`
- Triggers: Pub/Sub CloudEvent containing `TranscribedAudio`.
- Responsibilities: Create evaluator and process transcript evaluation.

**Notification Cloud Function:**
- Location: `backend/pipeline/notification/send_notification.py`
- Triggers: Pub/Sub CloudEvent containing `EvaluatedTranscribedAudio`.
- Responsibilities: Deduplicate, fetch tags, convert to `AlertNotification`, and send outbound request.

**Backend FastAPI Services:**
- Location: `backend/services/audio_segments/main.py`, `backend/services/feeds/main.py`, `backend/services/rules/main.py`
- Triggers: Internal HTTP requests from pipeline clients or BFF.
- Responsibilities: Authenticate OIDC, validate requests, call service/store classes, return JSON.

**Frontend BFF:**
- Location: `frontend/api/src/index.ts`
- Triggers: Node/Cloud Run process serving Express.
- Responsibilities: Configure middleware, register TSOA routes, centralize error handling.

**React UI:**
- Location: `frontend/transcription-ui/src/main.tsx`
- Triggers: Browser load of Vite bundle.
- Responsibilities: Create OAuth, React Query, auth, and routing providers.

**Model CLI:**
- Location: `model/src/gemini_sft/cli.py`
- Triggers: `gemini-sft` console script.
- Responsibilities: Dispatch `prepare`, `tune`, and `eval` workflows.

## Architectural Constraints

- **Threading:** `CollectorRuntime` uses `uvloop` for async feed tasks and separate OS threads for heartbeat and memory watchdog behavior in `backend/pipeline/ingestion/collector_runtime.py`. Serverless processors use warm cached clients in module-level or FastAPI lifespan containers. Beam/Dataflow uses serialized state/timer APIs in `backend/pipeline/segmentation/transforms/stateful.py`.
- **Global state:** Warm containers are module-level in `backend/pipeline/normalization/main.py`, `backend/pipeline/evaluation/main.py`, and `backend/pipeline/notification/send_notification.py`; Echo caches `gcs_client`, `pubsub_client`, and `feed_store` in `backend/pipeline/ingestion/collectors/echo/main.py`; React Query uses a process-level `QueryClient` in `frontend/transcription-ui/src/main.tsx`.
- **Circular imports:** Not detected in the architecture scan. Keep storage imports below services where possible: stores may import service models for validation, but FastAPI services should not build SQL or audit rows directly.
- **Generated protobufs:** Generated Python protobuf bindings live under `backend/pipeline/schema_types` and are excluded by lint config in `pyproject.toml`. Regenerate from `protos/*.proto` after schema changes using the command documented in `backend/pipeline/README.md`.
- **Source type changes:** Adding a claimable source type requires coordinated updates to `backend/pipeline/storage/feed_store.py`, `terraform/modules/alloydb/sql/ingestion/002_source_types.sql`, `terraform/modules/alloydb/sql/ingestion/006_seed_source_types.sql`, `backend/pipeline/ingestion/source_runtime_specs.py`, `backend/pipeline/ingestion/router.py`, and tests.
- **Audit writes:** Audited feed lifecycle mutations require explicit actor IDs through `backend/services/feeds/main.py`, `backend/services/feeds/service.py`, and `backend/pipeline/storage/feed_store.py`. Audit event SQL belongs in `backend/pipeline/storage/feed_audit_sql.py` and query modules.
- **Hot feed table:** The feed leasing table has hot-path protections and SQL/index constraints under `terraform/modules/alloydb/sql/ci/hot_protection_check.sql`; avoid adding indexes on frequently updated feed columns without checking that contract.
- **Secrets:** Environment configuration is referenced by config files and code, but secret values must stay outside docs and source maps. Do not read `.env`, `.env.*`, `*.env`, credential, key, or secret files.

## Anti-Patterns

### Collector Writes Lifecycle State Directly

**What happens:** A collector changes feed rows, bookmarks, failure counts, or quarantine state inside source-specific code.
**Why it's wrong:** It bypasses lease fencing, failure budgeting, heartbeat diagnostics, audit event generation, and runtime policy in `backend/pipeline/ingestion/collector_runtime.py` and `backend/pipeline/storage/feed_store.py`.
**Do this instead:** Emit `CapturedChunk`, `SourceObservation`, or raise `FeedFailure` from collector code under `backend/pipeline/ingestion/collectors`; let `CollectorRuntime` and `FeedStore` own side effects.

### Source Type Registry Drift

**What happens:** A source type is added to only `SourceType`, only `_COLLECTORS`, or only SQL seed data.
**Why it's wrong:** VM workers can claim feeds they cannot process or ship collectors that never receive leases.
**Do this instead:** Update `backend/pipeline/storage/feed_store.py`, `backend/pipeline/ingestion/source_runtime_specs.py`, `backend/pipeline/ingestion/router.py`, `terraform/modules/alloydb/sql/ingestion/006_seed_source_types.sql`, and related tests together.

### Services Build Audit Rows

**What happens:** `backend/services/feeds` constructs `feed_audit_events` inserts, snapshots, or feed revision SQL.
**Why it's wrong:** It duplicates storage-owned SQL contracts and risks inconsistent audit shape or actor enforcement.
**Do this instead:** Pass `actor_id` from `backend/services/feeds/main.py` through `FeedService` to `FeedStore`; keep audit SQL in `backend/pipeline/storage/feed_audit_sql.py` and `backend/pipeline/storage/feed_queries.py`.

### UI Calls Backend Services Directly

**What happens:** React components or hooks call `FEEDS_STORE_API_URL`, `RULES_API_URL`, or `AUDIO_SEGMENTS_API_URL` directly.
**Why it's wrong:** It bypasses BFF auth, admin checks, shape conversion, CORS policy, and actor header creation in `frontend/api/src`.
**Do this instead:** Add or update a BFF controller in `frontend/api/src`, shared types in `frontend/common/src`, and a browser service wrapper in `frontend/transcription-ui/src/service`.

### Editing Generated Routes Or Protobuf Outputs By Hand

**What happens:** Generated TSOA routes or protobuf Python outputs are edited manually.
**Why it's wrong:** Generated files are overwritten by tooling and are excluded or derived from source contracts.
**Do this instead:** Edit TSOA controller sources under `frontend/api/src/*/*Controller.ts` or protobuf sources under `protos/*.proto`, then regenerate outputs.

## Error Handling

**Strategy:** Stage-specific processors classify errors into acknowledge/drop, retry, DLQ, annotation-with-error, feed-budgeted failure, or HTTP response categories at the boundary that has the needed context.

**Patterns:**
- Ingestion source failures use `FeedFailure` and `failure_policy` under `backend/pipeline/ingestion`; runtime pipeline failures use `_PipelineFailure` in `backend/pipeline/ingestion/collector_runtime.py`.
- Normalization treats malformed inputs and permanent processing failures as no-retry or DLQ conditions in `backend/pipeline/normalization/processor.py`.
- Transcription re-raises transient ASR/API failures and writes transcript annotations with error details for permanent failures in `backend/pipeline/transcription/processor.py`.
- Evaluation validates required fields before evaluating and writes evaluation annotations through `backend/pipeline/evaluation/processor.py`.
- FastAPI services translate `ValueError`, missing rows, conflicts, and store exceptions into `HTTPException` in `backend/services/*/main.py`.
- The BFF wraps downstream service errors with `HttpError` through `frontend/api/src/utils.ts` and centralizes Express error responses in `frontend/api/src/index.ts`.

## Cross-Cutting Concerns

**Logging:** Python code uses `backend/pipeline/common/log_helper.py`, `setup_logging`, stage metric helpers, and structured `json_fields`; frontend BFF uses `console.error` with structured JSON in `frontend/api/src/utils.ts`.

**Validation:** Backend service models use Pydantic in `backend/services/*/models.py`; source and feed enums live in `backend/pipeline/storage/feed_store.py`; frontend type contracts live in `frontend/common/src`; protobuf schema contracts live in `protos`.

**Authentication:** Internal FastAPI services depend on OIDC verification in `backend/pipeline/common/auth.py`; BFF user auth flows through `frontend/api/src/authentication.ts`; BFF downstream calls use Google ID-token clients in `frontend/api/src/utils.ts`; React OAuth provider is initialized in `frontend/transcription-ui/src/main.tsx`.

**Tracing:** Pipeline stages call `setup_tracing`, parse/inject Pub/Sub trace context, and wrap spans through `backend/pipeline/common/tracing_utils.py`; FastAPI tracing is installed with `backend/pipeline/common/fastapi_tracing.py`.

**Configuration:** Python service config is environment-driven through `backend/pipeline/ingestion/settings.py`, stage entry points, and `backend/pipeline/storage/settings.py`; BFF config is centralized in `frontend/api/src/config.ts`; model workflow config is TOML-driven through `model/src/gemini_sft/config.py`.

**Schema Evolution:** Database changes belong in ordered SQL migrations under `terraform/modules/alloydb/sql/ingestion`; message schema changes belong in `protos`; frontend shared type changes belong in `frontend/common/src/types`.

---

*Architecture analysis: 2026-06-26*

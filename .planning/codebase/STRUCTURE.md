# Codebase Structure

**Analysis Date:** 2026-05-27

## Directory Layout

```text
radio-transcription/
|-- .agents/                    # Agent instructions used by repo automation
|-- .github/instructions/       # Python and TypeScript style guidance
|-- .planning/codebase/         # Generated GSD codebase maps
|-- backend/
|   |-- pipeline/
|   |   |-- common/             # Shared auth, logging, tracing, GCP helpers, clients
|   |   |-- ingestion/          # Feed leasing runtime, collectors, health, ingestion functions
|   |   |-- normalization/      # Beam streaming normalization pipeline
|   |   |-- transcription/      # Pub/Sub Cloud Function transcription pipeline
|   |   |-- evaluation/         # Transcript evaluation and alert publication
|   |   |-- notification/       # Alert notification Cloud Function
|   |   |-- storage/            # AlloyDB connection pools, stores, query helpers
|   |   `-- schema_types/       # Generated/checked protobuf Python modules
|   |-- services/
|   |   |-- feeds/              # Feed FastAPI service
|   |   |-- rules/              # Rules FastAPI service
|   |   |-- transcripts/        # Transcripts FastAPI service
|   |   `-- audio_segments/     # Audio segment domain models/store support
|   `-- scripts/                # Operator scripts such as feed imports
|-- frontend/
|   |-- api/                    # Express/TSOA browser API facade
|   |-- common/                 # Shared TypeScript types package
|   `-- transcription-ui/       # React/Vite browser application
|-- integration_tests/
|   |-- api/                    # API integration tests
|   |-- e2e/                    # End-to-end pipeline tests
|   `-- storage/                # Store integration tests
|-- local_dev/                  # Local mock services and scripts
|-- model/
|   |-- colabs/common/          # Shared manifest, scoring, SFT, GCS, Vertex helpers
|   |-- scripts/sft/            # SFT dataset/tune/eval CLI and adapters
|   |-- data/                   # Sample/reference model data
|   `-- data_sources/           # Offline source datasets and metadata
|-- protos/                     # Pub/Sub and Beam state protobuf source contracts
|-- terraform/modules/          # Deployment modules and AlloyDB SQL
|-- pyproject.toml              # Root Python project and uv workspace
|-- uv.lock                     # Python lockfile
|-- package.json                # Root Node workspace scripts
|-- .mise.toml                  # Task runner and development commands
`-- README.md                   # Top-level repo overview
```

## Directory Purposes

**Root:**
- Purpose: Own repository-level workspace configuration, task definitions, lockfiles, and overview documentation.
- Contains: Python workspace config, Node workspace scripts, mise tasks, pre-commit config, README, Docker/local compose files.
- Key files: `pyproject.toml`, `uv.lock`, `package.json`, `.mise.toml`, `.pre-commit-config.yaml`, `README.md`.

**`.agents`:**
- Purpose: Repository-specific agent and workflow instructions.
- Contains: High-level coding workflow guidance.
- Key files: `.agents/instructions.md`.

**`.github/instructions`:**
- Purpose: Language-specific style rules for Python and TypeScript/JavaScript changes.
- Contains: Style guides referenced by automation and agents.
- Key files: `.github/instructions/PYTHON_STYLE.instructions.md`, `.github/instructions/JS_TS_STYLE.instructions.md`.

**`backend/pipeline/common`:**
- Purpose: Shared Python support code for pipeline entry points and backend services.
- Contains: GCP helper functions, auth helpers, logging/tracing setup, Pub/Sub/GCS/transcripts clients, constants.
- Key files: `backend/pipeline/common/gcp_helper.py`, `backend/pipeline/common/auth.py`, `backend/pipeline/common/logging.py`, `backend/pipeline/common/tracing_utils.py`, `backend/pipeline/common/clients/pubsub_client.py`, `backend/pipeline/common/clients/gcs_client.py`, `backend/pipeline/common/clients/transcripts_client.py`.

**`backend/pipeline/ingestion`:**
- Purpose: Convert external audio sources into staged GCS audio plus raw Pub/Sub claim messages.
- Contains: CLI entry point, async runtime, settings, router, collector interface, collectors, health server, oldest-feed publisher, tests.
- Key files: `backend/pipeline/ingestion/main.py`, `backend/pipeline/ingestion/normalizer_runtime.py`, `backend/pipeline/ingestion/settings.py`, `backend/pipeline/ingestion/router.py`, `backend/pipeline/ingestion/models.py`, `backend/pipeline/ingestion/health_server.py`.

**`backend/pipeline/ingestion/collectors`:**
- Purpose: Hold source-specific capture logic behind the common collector contract.
- Contains: Icecast/Broadcastify, Broadcastify calls, OpenMHz, fire notification, Echo, and related collector tests.
- Key files: `backend/pipeline/ingestion/collectors/icecast/icecast_collector.py`, `backend/pipeline/ingestion/collectors/openmhz/collector.py`, `backend/pipeline/ingestion/collectors/echo/main.py`.

**`backend/pipeline/normalization`:**
- Purpose: Run the Beam streaming audio normalization pipeline.
- Contains: Pipeline options, orchestration, audio processors, stateful/stateless Beam transforms, tests.
- Key files: `backend/pipeline/normalization/main.py`, `backend/pipeline/normalization/orchestration.py`, `backend/pipeline/normalization/options.py`, `backend/pipeline/normalization/audio/audio_processor.py`, `backend/pipeline/normalization/transforms/stateless.py`, `backend/pipeline/normalization/transforms/stateful.py`.

**`backend/pipeline/transcription`:**
- Purpose: Transcribe normalized audio claim events through configurable transcription backends.
- Contains: Cloud Function entry point, event processor, publisher, transcriber interface/factory, Chirp/mock backends, tests.
- Key files: `backend/pipeline/transcription/main.py`, `backend/pipeline/transcription/processor.py`, `backend/pipeline/transcription/publisher.py`, `backend/pipeline/transcription/transcribers/base.py`, `backend/pipeline/transcription/transcribers/factory.py`, `backend/pipeline/transcription/transcribers/chirp.py`, `backend/pipeline/transcription/transcribers/mock.py`.

**`backend/pipeline/evaluation`:**
- Purpose: Evaluate transcribed audio against rules, persist transcript results, and publish alert events.
- Contains: Cloud Function entry point, event processor, evaluation service, text evaluators.
- Key files: `backend/pipeline/evaluation/main.py`, `backend/pipeline/evaluation/processor.py`, `backend/pipeline/evaluation/service.py`, `backend/pipeline/evaluation/rules_evaluation/evaluator.py`.

**`backend/pipeline/notification`:**
- Purpose: Deduplicate and send alert notification requests.
- Contains: Cloud Function entry point, Redis-backed dedupe helper, request handler, tests.
- Key files: `backend/pipeline/notification/send_notification.py`, `backend/pipeline/notification/request_handler.py`, `backend/pipeline/notification/deduplication.py`.

**`backend/pipeline/storage`:**
- Purpose: Encapsulate AlloyDB connection management and SQL-backed stores.
- Contains: Settings, async connection pool helpers, feed/transcript/rules/audio segment stores, query helpers, tests.
- Key files: `backend/pipeline/storage/connection.py`, `backend/pipeline/storage/settings.py`, `backend/pipeline/storage/feed_store.py`, `backend/pipeline/storage/transcript_store.py`, `backend/pipeline/storage/rules_store.py`, `backend/pipeline/storage/audio_segment_store.py`.

**`backend/pipeline/schema_types`:**
- Purpose: Provide generated Python protobuf modules used by pipeline code.
- Contains: Generated message classes and package init files.
- Key files: `backend/pipeline/schema_types/raw_audio_chunk_pb2.py`, `backend/pipeline/schema_types/normalized_audio_pb2.py`, `backend/pipeline/schema_types/transcribed_audio_pb2.py`, `backend/pipeline/schema_types/evaluated_transcribed_audio_pb2.py`, `backend/pipeline/schema_types/alert_notification_pb2.py`, `backend/pipeline/schema_types/streaming_state.py`.

**`backend/services`:**
- Purpose: Host internal FastAPI service applications that expose CRUD/control APIs over pipeline storage.
- Contains: Domain service packages for feeds, rules, transcripts, and audio segment support.
- Key files: `backend/services/feeds/main.py`, `backend/services/feeds/service.py`, `backend/services/rules/main.py`, `backend/services/rules/service.py`, `backend/services/transcripts/main.py`, `backend/services/transcripts/service.py`, `backend/services/audio_segments/models.py`.

**`backend/scripts`:**
- Purpose: Hold one-off/operator scripts that act on backend data.
- Contains: Feed import utility.
- Key files: `backend/scripts/bulk_import_feeds.py`.

**`frontend/api`:**
- Purpose: Browser-facing API facade with session/auth routes, generated TSOA routes, and backend service proxying.
- Contains: Express app, TSOA config, OpenAPI generation scripts, controllers, auth code, tests.
- Key files: `frontend/api/src/index.ts`, `frontend/api/src/config.ts`, `frontend/api/src/authentication.ts`, `frontend/api/tsoa.json`, `frontend/api/scripts/post-process-spec.js`, `frontend/api/src/feeds/feedsController.ts`, `frontend/api/src/rules/rulesController.ts`, `frontend/api/src/transcripts/transcriptsController.ts`, `frontend/api/src/auth/authController.ts`, `frontend/api/src/docs/docsController.ts`.

**`frontend/common`:**
- Purpose: Shared TypeScript package for UI/API contract types.
- Contains: Type exports for API errors, feeds, rules, and transcripts.
- Key files: `frontend/common/src/index.ts`, `frontend/common/src/types/apiError.ts`, `frontend/common/src/types/feeds.ts`, `frontend/common/src/types/rules.ts`, `frontend/common/src/types/transcripts.ts`.

**`frontend/transcription-ui`:**
- Purpose: Vite/React browser application.
- Contains: App bootstrap, routes, auth context, service calls, components, MUI theme, tests.
- Key files: `frontend/transcription-ui/src/main.tsx`, `frontend/transcription-ui/src/App.tsx`, `frontend/transcription-ui/src/context/AuthProvider.tsx`, `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`, `frontend/transcription-ui/src/utils/apiUtils.ts`, `frontend/transcription-ui/src/service/listTranscripts.ts`.

**`model`:**
- Purpose: Offline ASR model evaluation, dataset, scoring, and SFT tooling.
- Contains: Python package config, common Colab helpers, SFT pipeline, source data directories, sample/reference data.
- Key files: `model/pyproject.toml`, `model/scripts/sft/pipeline.py`, `model/scripts/sft/preflight.py`, `model/scripts/sft/datasets.toml`, `model/colabs/common/manifest.py`, `model/colabs/common/sft.py`, `model/colabs/common/scoring.py`, `model/colabs/common/vertex.py`, `model/colabs/common/gcs_utils.py`, `model/colabs/common/prompts.py`.

**`protos`:**
- Purpose: Source-of-truth message contracts for Pub/Sub payloads and Beam state.
- Contains: Raw, normalized, transcribed, evaluated, alert, and streaming-state proto definitions.
- Key files: `protos/raw_audio_chunk.proto`, `protos/normalized_audio.proto`, `protos/transcribed_audio.proto`, `protos/evaluated_transcribed_audio.proto`, `protos/alert_notification.proto`, `protos/streaming_state.proto`.

**`terraform/modules`:**
- Purpose: Reusable deployment modules and database schema SQL.
- Contains: Modules for AlloyDB, GCS buckets, Redis/Memorystore, Cloud Functions, container MIGs, and ASR evaluation.
- Key files: `terraform/modules/alloydb/sql/ingestion`, `terraform/modules/cloud_function`, `terraform/modules/gcs_bucket`, `terraform/modules/memorystore_for_redis`, `terraform/modules/container_mig`, `terraform/modules/asr_evaluation`.

**`integration_tests`:**
- Purpose: Cross-component tests against storage, APIs, and end-to-end workflows.
- Contains: Pytest suites for storage stores, API endpoints, ingestion, transcription, rules evaluation, and notifications.
- Key files: `integration_tests/storage/test_feed_store_integration.py`, `integration_tests/storage/test_rules_store_integration.py`, `integration_tests/storage/test_transcript_store_integration.py`, `integration_tests/api/test_transcripts_api.py`, `integration_tests/e2e/test_ingestion.py`, `integration_tests/e2e/test_transcription_pipeline.py`, `integration_tests/e2e/test_rules_creation_evaluation_publish.py`, `integration_tests/e2e/test_notifications.py`.

**`local_dev`:**
- Purpose: Local development support for mock services, initialization, and environment-specific scripts.
- Contains: Mock API/server scripts, local DB init scripts, local environment config file presence.
- Key files: `local_dev/mock_feeds_api.py`, `local_dev/mock_openmhz_server.py`, `local_dev/init-local-db.sh`, `local_dev/init-gcloud.sh`, `local_dev/LOCAL.env`.

## Key File Locations

**Entry Points:**
- `backend/pipeline/ingestion/main.py`: Container/CLI entry for feed ingestion.
- `backend/pipeline/ingestion/collectors/echo/main.py`: Echo GCS notification Cloud Function.
- `backend/pipeline/ingestion/oldest_feed_publisher/main.py`: HTTP function for oldest-feed metrics.
- `backend/pipeline/normalization/main.py`: Beam/Dataflow normalization runner.
- `backend/pipeline/transcription/main.py`: Transcription Pub/Sub Cloud Function.
- `backend/pipeline/evaluation/main.py`: Evaluation Pub/Sub Cloud Function.
- `backend/pipeline/notification/send_notification.py`: Notification Pub/Sub Cloud Function.
- `backend/services/feeds/main.py`: Feed FastAPI app.
- `backend/services/rules/main.py`: Rules FastAPI app.
- `backend/services/transcripts/main.py`: Transcripts FastAPI app.
- `frontend/api/src/index.ts`: Express/TSOA API app.
- `frontend/transcription-ui/src/main.tsx`: React app bootstrap.
- `model/scripts/sft/pipeline.py`: SFT CLI.
- `backend/scripts/bulk_import_feeds.py`: Operator feed import script.

**Configuration:**
- `pyproject.toml`: Root Python project, dependencies, and uv workspace members.
- `uv.lock`: Locked Python dependency graph.
- `.mise.toml`: Development task definitions, test/lint/format tasks, and `PYTHONPATH`.
- `model/pyproject.toml`: Model tooling package metadata and optional extras.
- `frontend/api/package.json`: API facade scripts and TypeScript/Express dependencies.
- `frontend/api/tsoa.json`: TSOA route/spec generation config and OpenAPI gateway extensions.
- `frontend/api/tsconfig.json`: TypeScript settings for API facade.
- `frontend/transcription-ui/package.json`: React UI scripts and dependencies.
- `frontend/transcription-ui/vite.config.ts`: Vite configuration.
- `frontend/common/package.json`: Shared TypeScript package metadata.
- `.github/instructions/PYTHON_STYLE.instructions.md`: Python coding conventions.
- `.github/instructions/JS_TS_STYLE.instructions.md`: TypeScript/JavaScript coding conventions.

**Core Logic:**
- `backend/pipeline/ingestion/normalizer_runtime.py`: Async ingestion orchestration.
- `backend/pipeline/ingestion/router.py`: Source type to collector/topic routing.
- `backend/pipeline/normalization/orchestration.py`: Beam DAG assembly.
- `backend/pipeline/normalization/transforms/stateful.py`: Stateful stitching and normalization transforms.
- `backend/pipeline/transcription/processor.py`: Normalized audio to transcript processing.
- `backend/pipeline/evaluation/processor.py`: Transcription evaluation and alert processing.
- `backend/pipeline/storage/feed_store.py`: Feed leasing, heartbeats, progress, and CRUD.
- `backend/pipeline/storage/transcript_store.py`: Transcript persistence and pagination.
- `frontend/api/src/*/*Controller.ts`: Browser API route implementations.
- `frontend/transcription-ui/src/components`: UI component implementations.
- `model/scripts/sft/pipeline.py`: Offline SFT workflow orchestration.

**Data Contracts:**
- `protos/raw_audio_chunk.proto`: Raw audio claim message.
- `protos/normalized_audio.proto`: Normalized audio claim message.
- `protos/transcribed_audio.proto`: Transcription result message.
- `protos/evaluated_transcribed_audio.proto`: Evaluation result message.
- `protos/alert_notification.proto`: Alert notification message.
- `protos/streaming_state.proto`: Beam state payloads.
- `frontend/common/src/types`: Shared browser/API TypeScript contracts.
- `model/colabs/common/manifest.py`: Offline dataset row contract.
- `model/colabs/common/sft.py`: Vertex SFT JSONL example contract.

**Testing:**
- `backend/pipeline/ingestion/tests`: Ingestion unit tests.
- `backend/pipeline/normalization/tests`: Normalization unit tests.
- `backend/pipeline/transcription/tests`: Transcription unit tests.
- `backend/pipeline/notification/tests`: Notification unit tests.
- `backend/pipeline/storage/tests`: Storage unit tests.
- `frontend/api/src/**/*.test.ts`: API facade tests.
- `frontend/transcription-ui/src/**/*.test.tsx`: React UI tests.
- `integration_tests/storage`: Store integration tests.
- `integration_tests/api`: API integration tests.
- `integration_tests/e2e`: End-to-end pipeline tests.
- `model/colabs/common/test_*.py`: Model helper tests.

## Naming Conventions

**Files:**
- Python modules use lowercase snake_case: `backend/pipeline/storage/feed_store.py`, `backend/pipeline/common/gcp_helper.py`.
- Python tests use `test_*.py`: `integration_tests/e2e/test_transcription_pipeline.py`, `model/colabs/common/test_scoring.py`.
- Backend service packages use domain directories with `main.py`, `service.py`, `models.py`, and optional tests: `backend/services/transcripts/main.py`, `backend/services/transcripts/service.py`.
- Storage files use `<domain>_store.py` and related query/helper modules: `backend/pipeline/storage/transcript_store.py`, `backend/pipeline/storage/feed_queries.py`.
- Pipeline Cloud Function packages use `main.py` for the deployed entry and `processor.py` for event processing when processing logic is non-trivial: `backend/pipeline/transcription/main.py`, `backend/pipeline/transcription/processor.py`.
- Beam transforms are grouped by behavior under `backend/pipeline/normalization/transforms`: `stateless.py`, `stateful.py`.
- React component files use PascalCase where the exported component is central: `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`.
- React service/util files commonly use camelCase: `frontend/transcription-ui/src/service/listTranscripts.ts`, `frontend/transcription-ui/src/utils/apiUtils.ts`.
- TypeScript tests are co-located and use `.test.ts` or `.test.tsx`: `frontend/api/src/auth/authController.test.ts`, `frontend/transcription-ui/src/App.test.tsx`.
- Proto files use snake_case filenames and PascalCase message names: `protos/raw_audio_chunk.proto` defines `AudioChunk`.
- Terraform module directories use snake_case: `terraform/modules/memorystore_for_redis`.

**Directories:**
- Python package directories use lowercase snake_case: `backend/pipeline/schema_types`, `backend/pipeline/ingestion/oldest_feed_publisher`.
- Backend feature directories are domain-oriented: `backend/services/feeds`, `backend/services/rules`, `backend/services/transcripts`.
- Frontend feature directories are domain-oriented under `src/components` and `src/service`: `frontend/transcription-ui/src/components/transcripts`.
- Model tooling directories separate reusable helpers from workflow CLIs: `model/colabs/common` for shared code, `model/scripts/sft` for SFT commands.

## Where to Add New Code

**New Audio Source Collector:**
- Source enum and feed model: `backend/pipeline/storage/feed_store.py`.
- Database seed/migration: `terraform/modules/alloydb/sql/ingestion`.
- Collector implementation: `backend/pipeline/ingestion/collectors/<source>/`.
- Collector registration and topic route: `backend/pipeline/ingestion/router.py`.
- Source concurrency cap: `backend/pipeline/ingestion/settings.py`.
- UI/API shared source type if browser-facing: `frontend/common/src/types/feeds.ts`.
- Controller/type conversion if browser-facing: `frontend/api/src/feeds/feedsController.ts`.
- Tests: `backend/pipeline/ingestion/tests` or a collector-specific `tests` directory under `backend/pipeline/ingestion/collectors/<source>/`.

**New Pub/Sub Contract or Message Field:**
- Source schema: `protos/*.proto`.
- Generated Python bindings: `backend/pipeline/schema_types` via `mise run generate:protos`.
- Pipeline usage: nearest processor/transform under `backend/pipeline/ingestion`, `backend/pipeline/normalization`, `backend/pipeline/transcription`, `backend/pipeline/evaluation`, or `backend/pipeline/notification`.
- Tests: affected package tests plus integration tests under `integration_tests/e2e` when the contract crosses components.

**New Backend Service Domain:**
- FastAPI app: `backend/services/<domain>/main.py`.
- Service class: `backend/services/<domain>/service.py`.
- Request/response models: `backend/services/<domain>/models.py`.
- Persistence: `backend/pipeline/storage/<domain>_store.py` and optional query helper in `backend/pipeline/storage`.
- Database SQL: `terraform/modules/alloydb/sql/ingestion`.
- Tests: `backend/services/<domain>/tests` or `integration_tests/api` for API-level behavior.

**New Feed/Rules/Transcript API Endpoint:**
- Backend route: matching `backend/services/<domain>/main.py`.
- Business logic: matching `backend/services/<domain>/service.py`.
- Browser facade route: `frontend/api/src/<domain>/<domain>Controller.ts`.
- Shared request/response type: `frontend/common/src/types/<domain>.ts`.
- API facade tests: co-located `.test.ts` file in `frontend/api/src/<domain>`.

**New UI View or Workflow:**
- Route registration: `frontend/transcription-ui/src/App.tsx`.
- Components: `frontend/transcription-ui/src/components/<domain>/`.
- API calls: `frontend/transcription-ui/src/service/`.
- Shared browser/API types: `frontend/common/src/types`.
- Tests: co-located `.test.tsx` files under `frontend/transcription-ui/src`.

**New Normalization Transform:**
- Beam transform implementation: `backend/pipeline/normalization/transforms`.
- Pipeline wiring: `backend/pipeline/normalization/orchestration.py`.
- Pipeline options: `backend/pipeline/normalization/options.py` when new runtime configuration is required.
- Tests: `backend/pipeline/normalization/tests`.

**New Transcriber Backend:**
- Implementation: `backend/pipeline/transcription/transcribers/<backend>.py`.
- Factory registration: `backend/pipeline/transcription/transcribers/factory.py`.
- Configuration handling: `backend/pipeline/transcription/main.py` or backend-specific config class.
- Tests: `backend/pipeline/transcription/tests`.

**New Evaluator Backend or Rule Behavior:**
- Evaluator implementation: `backend/pipeline/evaluation/rules_evaluation`.
- Service integration: `backend/pipeline/evaluation/service.py`.
- Rules API model changes: `backend/services/rules` and `frontend/common/src/types/rules.ts` when user-facing.
- Tests: `backend/pipeline/evaluation` tests and `integration_tests/e2e/test_rules_creation_evaluation_publish.py`.

**New SFT Dataset Adapter:**
- Adapter implementation: `model/scripts/sft/adapters/<adapter>.py`.
- Dataset registry entry: `model/scripts/sft/datasets.toml`.
- Shared row/manifest contract changes: `model/colabs/common/manifest.py`.
- Example validation changes: `model/colabs/common/sft.py`.
- Tests: `model/scripts/sft` tests or `model/colabs/common/test_*.py`.

**New Infrastructure Module:**
- Terraform module: `terraform/modules/<module>`.
- Database SQL, when schema-related: `terraform/modules/alloydb/sql/ingestion`.
- Local development support, when required: `local_dev`.

**Utilities:**
- Pipeline-wide Python helpers: `backend/pipeline/common`.
- Backend database helpers: `backend/pipeline/storage`.
- Shared frontend types: `frontend/common/src/types`.
- UI-only helpers: `frontend/transcription-ui/src/utils`.
- API-facade-only helpers: `frontend/api/src`.
- Model/offline helpers: `model/colabs/common`.

## Special Directories

**`backend/pipeline/schema_types`:**
- Purpose: Generated protobuf Python modules consumed by pipeline code.
- Generated: Yes.
- Committed: Partially present in the working tree; regenerate from `protos` with `mise run generate:protos`.

**`frontend/api/src/generated`:**
- Purpose: Generated TSOA routes when API generation is run.
- Generated: Yes.
- Committed: Not guaranteed to exist until generated; do not hand-edit generated route output.

**`model/scripts/sft/results`:**
- Purpose: Per-round SFT build/tune/eval records described by the SFT README.
- Generated: Yes.
- Committed: No for generated JSONL/result artifacts.

**`model/data`:**
- Purpose: Sample/reference model data and manifests.
- Generated: Mixed.
- Committed: Yes for selected sample/reference assets.

**`local_dev`:**
- Purpose: Local mocks and initialization scripts.
- Generated: No.
- Committed: Yes, except local environment values. `local_dev/LOCAL.env` exists as environment configuration and must not be read or quoted.

**`frontend/api/.env.example` and `frontend/transcription-ui/.env.example`:**
- Purpose: Example environment configuration files.
- Generated: No.
- Committed: Yes. Do not quote environment values from env-like files.

**`.planning/codebase`:**
- Purpose: Generated codebase mapping documents used by GSD planning/execution commands.
- Generated: Yes.
- Committed: Project-dependent.

---

*Structure analysis: 2026-05-27*

# Codebase Structure

**Analysis Date:** 2026-06-26

## Directory Layout

```text
radio-transcription/
├── .github/                         # GitHub Actions, PR templates, and language instructions
├── .planning/codebase/              # GSD codebase maps and planning artifacts
├── backend/                         # Python production pipeline, services, storage, and scripts
│   ├── pipeline/                    # Event-driven ingestion/transcription pipeline stages
│   │   ├── common/                  # Shared clients, auth, tracing, logging, GCS/Redis helpers
│   │   ├── evaluation/              # Rule evaluation Cloud Function processor
│   │   ├── ingestion/               # Collector runtime, collectors, source specs, failure policy
│   │   ├── normalization/           # Audio normalization Cloud Function processor
│   │   ├── notification/            # Alert notification sender and deduplication
│   │   ├── schema_types/            # Generated protobuf Python modules
│   │   ├── segmentation/            # Apache Beam/Dataflow VAD and stitching pipeline
│   │   ├── storage/                 # AlloyDB connection, stores, SQL query constants
│   │   └── transcription/           # Transcription service and transcriber implementations
│   ├── scripts/                     # Backend operational scripts
│   └── services/                    # FastAPI services for audio segments, feeds, rules, local Whisper
├── documentation/                   # Supplemental project docs
├── frontend/                        # TypeScript BFF, shared types, and React app
│   ├── api/                         # Express/TSOA backend-for-frontend
│   ├── common/                      # Shared TypeScript domain types and helpers
│   └── transcription-ui/            # Vite React operator UI
├── integration_tests/               # Cross-service, API, storage, and end-to-end tests
├── local_dev/                       # Local emulators, mock servers, setup scripts, seed data
├── model/                           # Research/model package, notebooks, data, SFT workflows
│   ├── colabs/                      # Notebook-adjacent helper code
│   ├── data/                        # Manifests, label exports, inference manifests
│   ├── data_sources/                # Source-specific data import tooling
│   ├── src/                         # Packaged model helpers and Gemini SFT CLI
│   └── tests/                       # Model package tests
├── protos/                          # Source protobuf schemas for Pub/Sub contracts
├── scripts/                         # Repository-level utility scripts
└── terraform/                       # Reusable Terraform modules and AlloyDB SQL migrations
```

## Directory Purposes

**`backend/pipeline/common`:**
- Purpose: Share cross-stage helpers for auth, tracing, logging, GCP clients, GCS upload, Redis cache, evaluation annotations, and container fork safety.
- Contains: Python packages such as `backend/pipeline/common/clients`, `backend/pipeline/common/storage`, `backend/pipeline/common/auth.py`, `backend/pipeline/common/tracing_utils.py`, `backend/pipeline/common/log_helper.py`.
- Key files: `backend/pipeline/common/clients/audio_segments_client.py`, `backend/pipeline/common/clients/feeds_client.py`, `backend/pipeline/common/clients/pubsub_client.py`, `backend/pipeline/common/storage/redis_service.py`, `backend/pipeline/common/fastapi_tracing.py`.

**`backend/pipeline/ingestion`:**
- Purpose: Own source collection, VM feed leasing, source runtime metadata, retry/failure/quarantine policy, and Echo ingestion.
- Contains: Runtime entry point, collector runtime, router, collector implementations, failure classifiers, settings, health/memory watchdogs, source runtime specs.
- Key files: `backend/pipeline/ingestion/main.py`, `backend/pipeline/ingestion/collector_runtime.py`, `backend/pipeline/ingestion/router.py`, `backend/pipeline/ingestion/source_runtime_specs.py`, `backend/pipeline/ingestion/failure_policy.py`, `backend/pipeline/ingestion/collectors/README.md`.

**`backend/pipeline/segmentation`:**
- Purpose: Run streaming Apache Beam/Dataflow segmentation, VAD, ordering, state/timer handling, and raw segment upload.
- Contains: Beam DAG assembly, stateful/stateless transforms, stitcher engine, state models, ONNX VAD model assets, tests, local architecture docs.
- Key files: `backend/pipeline/segmentation/main.py`, `backend/pipeline/segmentation/orchestration.py`, `backend/pipeline/segmentation/transforms/stateful.py`, `backend/pipeline/segmentation/transforms/stitcher_engine.py`, `backend/pipeline/segmentation/state/stitcher_state.py`, `backend/pipeline/segmentation/README.md`.

**`backend/pipeline/normalization`:**
- Purpose: Normalize segmented audio into canonical/playback/transcription derivatives, write audio segment metadata, and publish normalized claim-checks.
- Contains: Functions Framework entry point, processor, audio processor helpers, tests, Dockerfile.
- Key files: `backend/pipeline/normalization/main.py`, `backend/pipeline/normalization/processor.py`, `backend/pipeline/normalization/audio_processor.py`.

**`backend/pipeline/transcription`:**
- Purpose: Receive normalized claim-checks, run configurable ASR, write transcript annotations, and publish transcribed claim-checks.
- Contains: FastAPI ASGI entry point, processor, transcriber interface, Chirp/Gemini/local/mock transcribers, prompts, tests.
- Key files: `backend/pipeline/transcription/main.py`, `backend/pipeline/transcription/processor.py`, `backend/pipeline/transcription/transcribers/base.py`, `backend/pipeline/transcription/transcribers/factory.py`, `backend/pipeline/transcription/enums.py`.

**`backend/pipeline/evaluation`:**
- Purpose: Evaluate transcribed audio against configured rules and publish evaluated alert candidates.
- Contains: Functions Framework entry point, processor, evaluation service, rule evaluator implementations, tests.
- Key files: `backend/pipeline/evaluation/main.py`, `backend/pipeline/evaluation/processor.py`, `backend/pipeline/evaluation/service.py`, `backend/pipeline/evaluation/rules_evaluation/evaluator.py`.

**`backend/pipeline/notification`:**
- Purpose: Convert evaluated alert candidates into outbound alert notifications with Redis deduplication.
- Contains: Functions Framework entry point, request handler, dedupe helper, tests.
- Key files: `backend/pipeline/notification/send_notification.py`, `backend/pipeline/notification/request_handler.py`, `backend/pipeline/notification/notification_deduplication.py`.

**`backend/pipeline/storage`:**
- Purpose: Own AlloyDB connection management, SQL queries, lifecycle transitions, audit event writes, and store classes.
- Contains: Store classes, query modules, sync store variants, pagination helpers, settings, storage tests.
- Key files: `backend/pipeline/storage/feed_store.py`, `backend/pipeline/storage/feed_queries.py`, `backend/pipeline/storage/feed_audit_sql.py`, `backend/pipeline/storage/audio_segment_store.py`, `backend/pipeline/storage/rules_store.py`, `backend/pipeline/storage/transcript_store.py`, `backend/pipeline/storage/connection.py`.

**`backend/services`:**
- Purpose: Expose internal HTTP APIs backed by storage stores.
- Contains: FastAPI apps, service classes, Pydantic models, service-specific tests.
- Key files: `backend/services/audio_segments/main.py`, `backend/services/audio_segments/service.py`, `backend/services/feeds/main.py`, `backend/services/feeds/service.py`, `backend/services/rules/main.py`, `backend/services/rules/service.py`, `backend/services/local-whisper-api/main.py`.

**`frontend/api`:**
- Purpose: Serve the TypeScript BFF using Express and TSOA-generated routing.
- Contains: Controller folders, generated route file, auth/config/util modules, tests, package config.
- Key files: `frontend/api/src/index.ts`, `frontend/api/src/authentication.ts`, `frontend/api/src/config.ts`, `frontend/api/src/utils.ts`, `frontend/api/src/generated/routes.ts`, `frontend/api/src/feeds/feedsController.ts`, `frontend/api/src/audio/audioController.ts`, `frontend/api/src/rules/rulesController.ts`, `frontend/api/src/auth/authController.ts`.

**`frontend/common`:**
- Purpose: Publish shared TypeScript domain types and conversion helpers consumed by BFF and React UI.
- Contains: `types` and `utils` modules plus package entry point.
- Key files: `frontend/common/src/index.ts`, `frontend/common/src/types/audio.ts`, `frontend/common/src/types/feeds.ts`, `frontend/common/src/types/rules.ts`, `frontend/common/src/types/auth.ts`, `frontend/common/src/utils/statusUtils.ts`.

**`frontend/transcription-ui`:**
- Purpose: Provide the browser operator interface.
- Contains: Vite app entry, route shell, MUI components, auth context, React Query hooks, service wrappers, playback/audio utilities, tests.
- Key files: `frontend/transcription-ui/src/main.tsx`, `frontend/transcription-ui/src/App.tsx`, `frontend/transcription-ui/src/components/AppContainer.tsx`, `frontend/transcription-ui/src/context/AuthProvider.tsx`, `frontend/transcription-ui/src/hooks/useAudioSegments.ts`, `frontend/transcription-ui/src/service/listAudioSegments.ts`.

**`model`:**
- Purpose: Keep research/model package code, ASR helpers, manifests, data import tooling, notebooks, and Gemini SFT workflows separate from production services.
- Contains: Packaged Python code under `model/src`, tests under `model/tests`, manifests under `model/data`, notebooks/helpers under `model/colabs`, source importers under `model/data_sources`.
- Key files: `model/pyproject.toml`, `model/src/gemini_sft/cli.py`, `model/src/gemini_sft/prepare.py`, `model/src/gemini_sft/tune.py`, `model/src/gemini_sft/evaluate.py`, `model/src/common/manifest.py`, `model/src/common/scoring.py`.

**`protos`:**
- Purpose: Define protobuf message contracts used across Pub/Sub pipeline stages.
- Contains: `.proto` schemas for continuous, segmented, normalized, transcribed, evaluated, alert, and streaming state messages.
- Key files: `protos/continuous_audio.proto`, `protos/segmented_audio.proto`, `protos/normalized_audio.proto`, `protos/transcribed_audio.proto`, `protos/evaluated_transcribed_audio.proto`, `protos/alert_notification.proto`, `protos/streaming_state.proto`.

**`terraform`:**
- Purpose: Provide reusable GCP infrastructure modules and AlloyDB SQL schema migrations.
- Contains: Module directories under `terraform/modules`, SQL migrations under `terraform/modules/alloydb/sql/ingestion`, CI SQL checks under `terraform/modules/alloydb/sql/ci`.
- Key files: `terraform/modules/alloydb/main.tf`, `terraform/modules/alloydb/sql/ingestion/003_feeds.sql`, `terraform/modules/alloydb/sql/ingestion/022_audio_segments_annotations.sql`, `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql`, `terraform/modules/cloud_function/main.tf`, `terraform/modules/container_mig/main.tf`, `terraform/modules/memorystore_for_redis/main.tf`.

**`integration_tests`:**
- Purpose: Exercise cross-service API, storage, and end-to-end workflows.
- Contains: Test helpers, API tests, storage integration tests, E2E pipeline tests.
- Key files: `integration_tests/conftest.py`, `integration_tests/api/test_feeds_api.py`, `integration_tests/api/test_audio_segments_api.py`, `integration_tests/e2e/test_transcription_pipeline.py`, `integration_tests/e2e/test_notifications.py`, `integration_tests/storage/test_feed_store_integration.py`.

**`local_dev`:**
- Purpose: Provide local emulators, mock audio servers, setup helpers, and seed data for development.
- Contains: Python mock servers, GCS/Pub/Sub init scripts, Docker Postgres init, local test data, environment configuration files.
- Key files: `local_dev/mock_audio_server.py`, `local_dev/mock_server.py`, `local_dev/gcs_init.py`, `local_dev/pubsub_init.py`, `local_dev/test_data.sql`.

## Key File Locations

**Entry Points:**
- `backend/pipeline/ingestion/main.py`: VM ingestion worker process.
- `backend/pipeline/ingestion/collectors/echo/main.py`: Echo GCS Eventarc ingestion function.
- `backend/pipeline/ingestion/oldest_feed_publisher/main.py`: HTTP function for publishing oldest feed work.
- `backend/pipeline/ingestion/broadcastify_credential_rotation/main.py`: HTTP credential rotation function.
- `backend/pipeline/normalization/main.py`: Normalization CloudEvent function.
- `backend/pipeline/segmentation/main.py`: Beam/Dataflow segmentation job CLI.
- `backend/pipeline/transcription/main.py`: FastAPI Pub/Sub push transcription service.
- `backend/pipeline/evaluation/main.py`: Evaluation CloudEvent function.
- `backend/pipeline/notification/send_notification.py`: Notification CloudEvent function.
- `backend/services/audio_segments/main.py`: Audio Segments FastAPI app.
- `backend/services/feeds/main.py`: Feeds FastAPI app.
- `backend/services/rules/main.py`: Rules FastAPI app.
- `backend/services/local-whisper-api/main.py`: Local Whisper FastAPI app.
- `frontend/api/src/index.ts`: Express/TSOA BFF app.
- `frontend/transcription-ui/src/main.tsx`: React browser app entry.
- `model/src/gemini_sft/cli.py`: `gemini-sft` CLI entry.

**Configuration:**
- `pyproject.toml`: Root Python package, dependencies, Ruff config, pytest/development groups.
- `model/pyproject.toml`: Model package dependencies, extras, and `gemini-sft` script.
- `frontend/api/package.json`: BFF package scripts/dependencies.
- `frontend/common/package.json`: Shared TypeScript package scripts/dependencies.
- `frontend/transcription-ui/package.json`: UI package scripts/dependencies.
- `backend/pipeline/ingestion/settings.py`: Collector runtime settings and required env handling.
- `backend/pipeline/storage/settings.py`: AlloyDB/storage settings.
- `frontend/api/src/config.ts`: BFF environment validation and admin group lookup cache.
- `terraform/modules/*/variables.tf`: Terraform module inputs.
- `backend/pipeline/README.md`: Protobuf generation command and backend notes.

**Core Logic:**
- `backend/pipeline/ingestion/collector_runtime.py`: VM runtime lifecycle and side effects.
- `backend/pipeline/ingestion/router.py`: Source type to collector routing.
- `backend/pipeline/ingestion/source_runtime_specs.py`: Source runtime metadata.
- `backend/pipeline/segmentation/orchestration.py`: Beam topology assembly.
- `backend/pipeline/segmentation/transforms/stateful.py`: Stateful Beam DoFn.
- `backend/pipeline/segmentation/transforms/stitcher_engine.py`: Framework-independent stitching orchestration.
- `backend/pipeline/segmentation/state/stitcher_state.py`: Audio stitching FSM.
- `backend/pipeline/normalization/processor.py`: Normalization stage behavior.
- `backend/pipeline/transcription/processor.py`: Transcription stage behavior.
- `backend/pipeline/transcription/transcribers/factory.py`: Transcriber selection.
- `backend/pipeline/evaluation/processor.py`: Evaluation stage orchestration.
- `backend/pipeline/evaluation/service.py`: Evaluation business logic.
- `backend/pipeline/notification/send_notification.py`: Notification conversion and send flow.
- `backend/pipeline/storage/feed_store.py`: Feed lifecycle store.
- `backend/pipeline/storage/feed_queries.py`: Feed lifecycle SQL.
- `backend/pipeline/storage/feed_audit_sql.py`: Feed audit SQL fragments.
- `frontend/api/src/*/*Controller.ts`: BFF API resources.
- `frontend/transcription-ui/src/components`: React view and UI components.
- `frontend/transcription-ui/src/hooks`: React Query/data and playback hooks.
- `frontend/transcription-ui/src/service`: Browser HTTP service wrappers.
- `model/src/gemini_sft`: Gemini SFT workflow package.

**Testing:**
- `backend/pipeline/*/tests`: Pipeline stage unit tests.
- `backend/services/*/tests`: FastAPI service tests.
- `backend/pipeline/storage/tests`: Store and SQL contract tests.
- `frontend/api/src/**/*.test.ts`: BFF unit tests.
- `frontend/transcription-ui/src/**/*.test.tsx`: UI component/hook tests.
- `model/tests`: Model package tests.
- `integration_tests`: Cross-service and E2E integration tests.

## Naming Conventions

**Files:**
- Python modules use snake_case: `backend/pipeline/ingestion/source_runtime_specs.py`, `backend/pipeline/storage/feed_audit_sql.py`.
- Python tests use `test_*.py` under the owning package: `backend/pipeline/ingestion/tests/test_router.py`.
- FastAPI service packages group `main.py`, `service.py`, `models.py`, and `tests`: `backend/services/feeds/main.py`.
- TypeScript React components use PascalCase: `frontend/transcription-ui/src/components/feeds/FeedConfigurationView.tsx`.
- React hooks use `use*.ts` or `use*.tsx`: `frontend/transcription-ui/src/hooks/useAudioSegments.ts`.
- Browser service wrappers use action names: `frontend/transcription-ui/src/service/listFeeds.ts`, `frontend/transcription-ui/src/service/updateRule.ts`.
- BFF controllers use `*Controller.ts`: `frontend/api/src/feeds/feedsController.ts`.
- Shared TypeScript type files are lower camel domain nouns: `frontend/common/src/types/feeds.ts`, `frontend/common/src/types/audio.ts`.
- Protobuf schemas use snake_case message domain names: `protos/normalized_audio.proto`.
- SQL migrations use ordered numeric prefixes: `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql`.

**Directories:**
- Backend pipeline stages are nouns under `backend/pipeline`: `ingestion`, `segmentation`, `normalization`, `transcription`, `evaluation`, `notification`.
- Backend services are resource names under `backend/services`: `audio_segments`, `feeds`, `rules`.
- Frontend feature components are grouped by screen/domain: `frontend/transcription-ui/src/components/transcripts`, `frontend/transcription-ui/src/components/feeds`, `frontend/transcription-ui/src/components/rules`.
- Source-specific collectors live under `backend/pipeline/ingestion/collectors/<source>`: `backend/pipeline/ingestion/collectors/openmhz`, `backend/pipeline/ingestion/collectors/fire_notifications`.
- Terraform modules live under `terraform/modules/<resource_kind>`: `terraform/modules/container_mig`, `terraform/modules/cloud_function`.

## Where to Add New Code

**New VM Audio Source:**
- Source enum and lifecycle model: `backend/pipeline/storage/feed_store.py`.
- Source runtime metadata: `backend/pipeline/ingestion/source_runtime_specs.py`.
- Collector implementation: `backend/pipeline/ingestion/collectors/<source_name>/`.
- Runtime route: `backend/pipeline/ingestion/router.py`.
- DB seed/schema: `terraform/modules/alloydb/sql/ingestion/006_seed_source_types.sql` and related migration if schema changes.
- Tests: `backend/pipeline/ingestion/collectors/tests`, `backend/pipeline/ingestion/tests/test_router.py`, `backend/pipeline/storage/tests`.

**New Echo-like Push Ingestion Source:**
- Function entry point: `backend/pipeline/ingestion/collectors/<source_name>/main.py`.
- Sync store support if feed state writes are needed: `backend/pipeline/storage/sync_feed_store.py` and `backend/pipeline/storage/sync_feed_queries.py`.
- Source runtime metadata and source enum: `backend/pipeline/ingestion/source_runtime_specs.py`, `backend/pipeline/storage/feed_store.py`.
- Tests: `backend/pipeline/ingestion/collectors/<source_name>/tests`.

**New Pipeline Stage:**
- Implementation package: `backend/pipeline/<stage_name>`.
- Entry point: `backend/pipeline/<stage_name>/main.py`.
- Processor/business logic: `backend/pipeline/<stage_name>/processor.py` or `service.py`.
- Shared schemas: `protos/<message_name>.proto` and generated bindings in `backend/pipeline/schema_types`.
- Tests: `backend/pipeline/<stage_name>/tests`.
- Deployment module usage: `terraform/modules/cloud_function` or an existing container module as appropriate.

**New Backend API Resource:**
- FastAPI app or route: `backend/services/<resource>/main.py`.
- Domain service: `backend/services/<resource>/service.py`.
- Request/response models: `backend/services/<resource>/models.py` or shared models under `backend/pipeline/common`.
- Store and queries: `backend/pipeline/storage/<resource>_store.py`, `backend/pipeline/storage/<resource>_queries.py`.
- Schema migration: `terraform/modules/alloydb/sql/ingestion/<next_number>_<description>.sql`.
- Tests: `backend/services/<resource>/tests`, `backend/pipeline/storage/tests`, and `integration_tests/api`.

**New Feed Lifecycle Mutation:**
- Route and actor resolution: `backend/services/feeds/main.py`.
- Service method: `backend/services/feeds/service.py`.
- Store method and SQL: `backend/pipeline/storage/feed_store.py`, `backend/pipeline/storage/feed_queries.py`.
- Audit fragments if the mutation is audited: `backend/pipeline/storage/feed_audit_sql.py`.
- Tests: `backend/pipeline/storage/tests/test_feed_store.py`, `backend/services/feeds/tests`, `backend/pipeline/storage/tests/test_feed_audit_contract.py`.

**New BFF Endpoint:**
- Controller: `frontend/api/src/<resource>/<resource>Controller.ts`.
- Shared types: `frontend/common/src/types/<resource>.ts` and export from `frontend/common/src/index.ts`.
- Auth/security: Use `@Security('google_id_token')` and `AuthenticatedRequest` from `frontend/api/src/authentication.ts` when user context is needed.
- Downstream client: Use `getServiceClient` from `frontend/api/src/utils.ts`.
- Generated routes: Regenerate `frontend/api/src/generated/routes.ts` through the frontend API build workflow; do not edit it by hand.
- Tests: `frontend/api/src/<resource>/<resource>Controller.test.ts`.

**New UI Screen:**
- Route: `frontend/transcription-ui/src/App.tsx`.
- Navigation item: `frontend/transcription-ui/src/components/AppContainer.tsx`.
- Screen component: `frontend/transcription-ui/src/components/<domain>/<ScreenName>.tsx`.
- Data hook: `frontend/transcription-ui/src/hooks/use<Domain>.ts`.
- HTTP wrapper: `frontend/transcription-ui/src/service/<action><Domain>.ts`.
- Shared types: `frontend/common/src/types/<domain>.ts` when the type crosses BFF/UI.
- Tests: Co-located `*.test.tsx` or `*.test.ts`.

**New Protobuf Message Or Field:**
- Schema source: `protos/<message>.proto`.
- Regeneration target: `backend/pipeline/schema_types`.
- Producer/consumer updates: Stage processors under `backend/pipeline/*/processor.py` and tests under each affected stage.
- Documentation: Update `backend/pipeline/README.md` only when generation workflow changes.

**New Model Workflow Code:**
- Shared helpers: `model/src/common`.
- Gemini SFT command logic: `model/src/gemini_sft`.
- CLI subcommand registration: `model/src/gemini_sft/cli.py`.
- Tests: `model/tests`.
- Data contracts: `model/data/manifests` and related README files.

**New Terraform Module Or Resource:**
- Reusable module: `terraform/modules/<module_name>`.
- Module variables/outputs: `terraform/modules/<module_name>/variables.tf`, `terraform/modules/<module_name>/outputs.tf`.
- AlloyDB migration: `terraform/modules/alloydb/sql/ingestion/<next_number>_<description>.sql`.
- SQL CI checks: `terraform/modules/alloydb/sql/ci` when adding database invariants.

**Utilities:**
- Backend operational scripts: `backend/scripts`.
- Repo-wide scripts: `scripts`.
- Local development helpers: `local_dev`.
- Shared Python helper used by multiple pipeline stages: `backend/pipeline/common`.
- Shared frontend helper used by BFF and UI: `frontend/common/src`.

## Special Directories

**`.planning/codebase`:**
- Purpose: Stores generated codebase maps for GSD planning/execution.
- Generated: Yes.
- Committed: Yes.

**`backend/pipeline/schema_types`:**
- Purpose: Holds generated Python protobuf bindings from `protos`.
- Generated: Yes.
- Committed: Partially; generated `*_pb2.py`, `*_pb2.pyi`, and gRPC outputs are excluded by lint config and should be regenerated locally as needed.

**`frontend/api/src/generated`:**
- Purpose: Holds TSOA-generated route registration.
- Generated: Yes.
- Committed: Yes.

**`frontend/*/dist`:**
- Purpose: Holds built JavaScript/type/static outputs for frontend packages.
- Generated: Yes.
- Committed: Present in this worktree; do not edit generated build output by hand.

**`frontend/*/node_modules`:**
- Purpose: Installed JavaScript dependencies.
- Generated: Yes.
- Committed: No.

**`.venv`, `.pytest_cache`, `.ruff_cache`, `__pycache__`:**
- Purpose: Local Python virtualenv, test cache, lint cache, and bytecode cache.
- Generated: Yes.
- Committed: No.

**`model/data`:**
- Purpose: Stores manifests, label studio exports, and inference manifests for research/evaluation workflows.
- Generated: Mixed.
- Committed: Mixed; treat large/raw/generated data carefully and follow existing file patterns.

**`model/trained_checkpoints`:**
- Purpose: Stores trained model checkpoints when present.
- Generated: Yes.
- Committed: No for large checkpoint artifacts unless explicitly required.

**`backend/pipeline/segmentation/audio/models`:**
- Purpose: Stores ONNX VAD model assets used by segmentation.
- Generated: No.
- Committed: Yes.

**`terraform/modules/alloydb/sql/ingestion`:**
- Purpose: Ordered AlloyDB schema migrations and seed data.
- Generated: No.
- Committed: Yes.

**`local_dev`:**
- Purpose: Local emulator setup, mock audio servers, and seed data.
- Generated: Mixed.
- Committed: Yes for scripts and seed data. Environment files such as `local_dev/LOCAL.env` contain configuration and must not be quoted in generated docs.

**`.github`:**
- Purpose: GitHub workflows, custom actions, PR templates, and language-specific contribution instructions.
- Generated: No.
- Committed: Yes.

---

*Structure analysis: 2026-06-26*

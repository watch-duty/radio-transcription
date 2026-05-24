# Codebase Structure

**Analysis Date:** 2026-05-24

## Directory Layout

```text
radio-transcription/
├── .agents/                  # Agent guidance for this workspace
├── .github/                  # CI workflows and language style instructions
├── .planning/                # GSD planning artifacts, including this codebase map
├── backend/                  # Python ingestion, processing pipeline, services, storage
│   ├── pipeline/             # Pub/Sub/Dataflow/functions pipeline and shared backend code
│   │   ├── common/           # Auth, logging, tracing, clients, GCS/Pub/Sub helpers, common models
│   │   ├── evaluation/       # Rules evaluation CloudEvent function and evaluator strategies
│   │   ├── ingestion/        # Feed leasing runtime, source collectors, settings, health, retries
│   │   ├── normalization/    # Apache Beam streaming normalization/stitching pipeline
│   │   ├── notification/     # Alert notification CloudEvent function and dedupe/request helpers
│   │   ├── schema_types/     # Generated protobuf Python modules
│   │   ├── storage/          # AlloyDB/Redis access stores, SQL query modules, connection factories
│   │   └── transcription/    # Transcription CloudEvent function and transcriber implementations
│   └── services/             # FastAPI services for feeds, rules, transcripts, audio segments
├── frontend/                 # TypeScript API facade, shared types, React UI
│   ├── api/                  # Express/TSOA frontend API function
│   ├── common/               # Shared TypeScript DTO package
│   └── transcription-ui/     # Vite React UI
├── integration_tests/        # API, storage, and e2e integration test suites
├── local_dev/                # Local Docker/emulator init scripts and mock services
├── model/                    # ASR notebooks, reusable evaluation helpers, data artifacts/scripts
├── protos/                   # Protobuf source schemas for pipeline contracts
├── terraform/                # Reusable Terraform modules and AlloyDB schema SQL
├── pyproject.toml            # Root Python package/workspace/lint/test config
├── uv.lock                   # Root Python lockfile
├── docker-compose.yml        # Local multi-service pipeline stack
├── asr-eval-docker-compose.yml # Model evaluation notebook/NeMo containers
└── .mise.toml                # Project task runner definitions
```

## Directory Purposes

**`backend/`:**
- Purpose: Python backend code for the pipeline and HTTP services.
- Contains: `backend/pipeline/`, `backend/services/`, package marker `backend/__init__.py`.
- Key files: `backend/pipeline/ingestion/main.py`, `backend/pipeline/normalization/main.py`, `backend/pipeline/transcription/main.py`, `backend/pipeline/evaluation/main.py`, `backend/pipeline/notification/send_notification.py`.

**`backend/pipeline/common/`:**
- Purpose: Shared runtime support for backend pipeline code.
- Contains: Auth helpers, GCP helpers, clients, logging/tracing, constants, common rule models, storage cache abstractions.
- Key files: `backend/pipeline/common/gcp_helper.py`, `backend/pipeline/common/auth.py`, `backend/pipeline/common/tracing_utils.py`, `backend/pipeline/common/clients/pubsub_client.py`, `backend/pipeline/common/storage/redis_service.py`.

**`backend/pipeline/ingestion/`:**
- Purpose: Feed capture orchestration and source-specific ingestion.
- Contains: `NormalizerRuntime`, source collectors, health server, settings, retries, SLO/quarantine telemetry, special HTTP/functions helpers.
- Key files: `backend/pipeline/ingestion/main.py`, `backend/pipeline/ingestion/normalizer_runtime.py`, `backend/pipeline/ingestion/router.py`, `backend/pipeline/ingestion/models.py`, `backend/pipeline/ingestion/settings.py`.

**`backend/pipeline/ingestion/collectors/`:**
- Purpose: Source adapters for Broadcastify feeds/calls, OpenMHZ, FireNotifications, Icecast, and Echo.
- Contains: Source-specific capture functions and collector tests.
- Key files: `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py`, `backend/pipeline/ingestion/collectors/openmhz/collector.py`, `backend/pipeline/ingestion/collectors/fire_notifications/collector.py`, `backend/pipeline/ingestion/collectors/icecast/icecast_collector.py`, `backend/pipeline/ingestion/collectors/echo/main.py`.

**`backend/pipeline/normalization/`:**
- Purpose: Streaming audio stitching/normalization and claim-check serialization.
- Contains: Beam entry point/options/orchestration, stateful/stateless transforms, DSP/VAD, state machines, packaged ONNX models.
- Key files: `backend/pipeline/normalization/main.py`, `backend/pipeline/normalization/orchestration.py`, `backend/pipeline/normalization/transforms/stateful.py`, `backend/pipeline/normalization/transforms/stateless.py`, `backend/pipeline/normalization/audio/audio_processor.py`.

**`backend/pipeline/transcription/`:**
- Purpose: Convert normalized audio claim checks into transcribed audio events.
- Contains: Functions Framework entry point, processor, transcriber interface/factory, Google Chirp and mock implementations.
- Key files: `backend/pipeline/transcription/main.py`, `backend/pipeline/transcription/processor.py`, `backend/pipeline/transcription/transcribers/base.py`, `backend/pipeline/transcription/transcribers/factory.py`, `backend/pipeline/transcription/transcribers/chirp.py`.

**`backend/pipeline/evaluation/`:**
- Purpose: Evaluate transcribed text against rules and persist evaluated transcripts.
- Contains: Functions Framework entry point, processor, evaluation service, evaluator strategies, unit tests.
- Key files: `backend/pipeline/evaluation/main.py`, `backend/pipeline/evaluation/processor.py`, `backend/pipeline/evaluation/service.py`, `backend/pipeline/evaluation/rules_evaluation/evaluator.py`.

**`backend/pipeline/notification/`:**
- Purpose: Convert evaluated alert candidates into outbound notifications.
- Contains: CloudEvent entry point, notification conversion, Redis-backed dedupe, HTTP request handler, tests.
- Key files: `backend/pipeline/notification/send_notification.py`, `backend/pipeline/notification/notification_deduplication.py`, `backend/pipeline/notification/request_handler.py`.

**`backend/pipeline/storage/`:**
- Purpose: Persistence layer for AlloyDB and SQL-backed entities.
- Contains: Connection factories, settings, store classes, SQL query constants, sync feed store.
- Key files: `backend/pipeline/storage/connection.py`, `backend/pipeline/storage/settings.py`, `backend/pipeline/storage/feed_store.py`, `backend/pipeline/storage/transcript_store.py`, `backend/pipeline/storage/rules_store.py`, `backend/pipeline/storage/audio_segment_store.py`.

**`backend/pipeline/schema_types/`:**
- Purpose: Generated protobuf Python bindings.
- Contains: Generated modules such as `backend/pipeline/schema_types/streaming_state.py`; other generated `_pb2.py` files are produced locally.
- Key files: `backend/pipeline/schema_types/__init__.py`, `backend/pipeline/schema_types/streaming_state.py`.

**`backend/services/`:**
- Purpose: Authenticated FastAPI services over the storage layer.
- Contains: Domain folders for `feeds`, `rules`, `transcripts`, and `audio_segments`.
- Key files: `backend/services/feeds/main.py`, `backend/services/feeds/service.py`, `backend/services/rules/main.py`, `backend/services/rules/service.py`, `backend/services/transcripts/main.py`, `backend/services/transcripts/service.py`.

**`frontend/api/`:**
- Purpose: Public TypeScript API facade deployed as a Google function/API Gateway backend.
- Contains: Express entry point, TSOA controllers, auth/session handling, config, OpenAPI output, generated route target configured by `frontend/api/tsoa.json`.
- Key files: `frontend/api/src/index.ts`, `frontend/api/src/auth/authController.ts`, `frontend/api/src/authentication.ts`, `frontend/api/src/transcripts/transcriptsController.ts`, `frontend/api/src/feeds/feedsController.ts`, `frontend/api/src/rules/rulesController.ts`, `frontend/api/src/docs/docsController.ts`.

**`frontend/common/`:**
- Purpose: Shared TypeScript type package consumed by `frontend/api` and `frontend/transcription-ui`.
- Contains: DTOs and an index barrel.
- Key files: `frontend/common/src/index.ts`, `frontend/common/src/types/transcripts.ts`, `frontend/common/src/types/feeds.ts`, `frontend/common/src/types/rules.ts`, `frontend/common/src/types/apiError.ts`.

**`frontend/transcription-ui/`:**
- Purpose: Browser UI for authenticated transcript search/playback, feeds, rules, docs, and login.
- Contains: Vite/React app, components, service wrappers, auth context, utilities, tests.
- Key files: `frontend/transcription-ui/src/main.tsx`, `frontend/transcription-ui/src/App.tsx`, `frontend/transcription-ui/src/context/AuthProvider.tsx`, `frontend/transcription-ui/src/service/listTranscripts.ts`, `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`.

**`model/`:**
- Purpose: ASR model evaluation workspace and training/evaluation data organization.
- Contains: Jupyter notebooks, common helpers, Docker images, manifests, label exports, inference outputs, data source fetch scripts.
- Key files: `model/pyproject.toml`, `model/colabs/common/scoring.py`, `model/colabs/common/manifest.py`, `model/colabs/common/inference_pipeline_runner.py`, `model/data/README.md`, `model/data_sources/broadcastify/bcfy_api.py`.

**`protos/`:**
- Purpose: Source schemas for all pipeline message contracts.
- Contains: Six `.proto` schemas.
- Key files: `protos/raw_audio_chunk.proto`, `protos/normalized_audio.proto`, `protos/transcribed_audio.proto`, `protos/evaluated_transcribed_audio.proto`, `protos/alert_notification.proto`, `protos/streaming_state.proto`.

**`terraform/`:**
- Purpose: Reusable infrastructure modules and AlloyDB schema migration SQL.
- Contains: Modules for AlloyDB, Cloud Function, GCS bucket, Redis, container MIG, ASR evaluation VM, plus SQL under the AlloyDB module.
- Key files: `terraform/modules/alloydb/main.tf`, `terraform/modules/alloydb/sql/ingestion/`, `terraform/modules/cloud_function/main.tf`, `terraform/modules/container_mig/main.tf`, `terraform/modules/memorystore_for_redis/main.tf`.

**`integration_tests/`:**
- Purpose: Integration/e2e tests for API, storage, and end-to-end pipeline behavior.
- Contains: `api`, `storage`, `e2e` test directories and helpers.
- Key files: `integration_tests/api/test_transcripts_api.py`, `integration_tests/storage/test_feed_store_integration.py`, `integration_tests/e2e/test_transcription_pipeline.py`.

**`local_dev/`:**
- Purpose: Local emulator initialization, mock servers, and seed data.
- Contains: Pub/Sub/GCS/Postgres init scripts, mock audio/server scripts, test data, local env file.
- Key files: `local_dev/pubsub_init.py`, `local_dev/gcs_init.py`, `local_dev/mock_server.py`, `local_dev/mock_audio_server.py`, `local_dev/test_data.sql`, `local_dev/LOCAL.env`.

**`.agents/`:**
- Purpose: Workspace agent guidance.
- Contains: `instructions.md` pointing agents to repo style guides.
- Key files: `.agents/instructions.md`.

**`.github/`:**
- Purpose: CI workflows and language style instructions.
- Contains: Workflows and instructions for Python and JS/TS style.
- Key files: `.github/workflows/ci.yml`, `.github/workflows/integration-tests.yml`, `.github/instructions/PYTHON_STYLE.instructions.md`, `.github/instructions/JS_TS_STYLE.instructions.md`.

## Key File Locations

**Entry Points:**
- `backend/pipeline/ingestion/main.py`: Ingestion worker startup.
- `backend/pipeline/ingestion/collectors/echo/main.py`: Echo CloudEvent collector.
- `backend/pipeline/ingestion/oldest_feed_publisher/main.py`: Oldest-feed HTTP function.
- `backend/pipeline/ingestion/broadcastify_credential_rotation/main.py`: Broadcastify credential rotation HTTP function.
- `backend/pipeline/normalization/main.py`: Beam normalization pipeline startup.
- `backend/pipeline/transcription/main.py`: Transcription CloudEvent function.
- `backend/pipeline/evaluation/main.py`: Rules evaluation CloudEvent function.
- `backend/pipeline/notification/send_notification.py`: Notification CloudEvent function.
- `backend/services/feeds/main.py`: Feeds FastAPI app.
- `backend/services/rules/main.py`: Rules FastAPI app.
- `backend/services/transcripts/main.py`: Transcripts FastAPI app.
- `frontend/api/src/index.ts`: Express/TSOA API function export.
- `frontend/transcription-ui/src/main.tsx`: React app mount.

**Configuration:**
- `pyproject.toml`: Root Python package, uv workspace, ruff, ty, pytest config.
- `backend/pipeline/normalization/pyproject.toml`: Normalization workspace package dependencies.
- `backend/pipeline/transcription/pyproject.toml`: Transcription function workspace package dependencies.
- `model/pyproject.toml`: Model common helper package and optional dependency extras.
- `.mise.toml`: Task runner definitions for lint, format, tests, proto generation, and local dev.
- `frontend/api/package.json`: Frontend API scripts/dependencies.
- `frontend/api/tsoa.json`: TSOA route/OpenAPI generation config.
- `frontend/transcription-ui/package.json`: React UI scripts/dependencies.
- `frontend/transcription-ui/vite.config.ts`: Vite UI config.
- `frontend/common/package.json`: Shared TypeScript package config.
- `docker-compose.yml`: Local pipeline stack and emulators.
- `docker-compose.override.yml`: Local Docker override.
- `asr-eval-docker-compose.yml`: ASR evaluation containers.
- `frontend/api/.env.example`: Frontend API environment example only.
- `frontend/transcription-ui/.env.example`: UI environment example only.
- `local_dev/LOCAL.env`: Local development environment file present; contents not read or quoted.

**Core Logic:**
- `backend/pipeline/ingestion/normalizer_runtime.py`: Feed leasing and capture orchestration.
- `backend/pipeline/ingestion/router.py`: Source-type routing.
- `backend/pipeline/ingestion/models.py`: Collector/runtime contract.
- `backend/pipeline/normalization/orchestration.py`: Beam streaming DAG.
- `backend/pipeline/transcription/processor.py`: Normalized-audio event processing.
- `backend/pipeline/evaluation/processor.py`: Transcribed-audio evaluation orchestration.
- `backend/pipeline/evaluation/service.py`: Evaluation business logic.
- `backend/pipeline/evaluation/rules_evaluation/evaluator.py`: Rule matching strategies.
- `backend/pipeline/notification/send_notification.py`: Notification conversion and send path.
- `backend/pipeline/storage/feed_store.py`: Feed lifecycle/lease storage.
- `backend/pipeline/storage/transcript_store.py`: Transcript storage and pagination.
- `backend/pipeline/storage/rules_store.py`: Rules storage.
- `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`: Main transcript query/playback view.
- `frontend/api/src/**/*Controller.ts`: Public API route controllers.

**Testing:**
- `backend/pipeline/**/tests/`: Python unit tests for pipeline modules.
- `backend/services/*/tests/`: Python unit tests for FastAPI service APIs.
- `frontend/api/src/**/*.test.ts`: Vitest tests for frontend API controllers.
- `frontend/transcription-ui/src/**/*.test.tsx`: Vitest/Testing Library UI tests.
- `model/colabs/common/tests/`: Pytest tests for model common helpers.
- `integration_tests/api/`: API integration tests.
- `integration_tests/storage/`: Store integration tests.
- `integration_tests/e2e/`: End-to-end Docker/local pipeline tests.

**Schemas and Data Contracts:**
- `protos/*.proto`: Source protobuf contracts.
- `backend/pipeline/schema_types/`: Generated Python schema modules.
- `frontend/common/src/types/`: Shared TypeScript frontend DTOs.
- `backend/services/*/models.py`: Pydantic HTTP models.
- `backend/pipeline/common/rules/models.py`: Shared rule Pydantic models.

**Infrastructure and SQL:**
- `terraform/modules/alloydb/sql/ingestion/`: AlloyDB schema SQL migrations.
- `terraform/modules/alloydb/main.tf`: AlloyDB cluster, user, and migration job module.
- `terraform/modules/container_mig/main.tf`: Containerized managed instance group module.
- `terraform/modules/cloud_function/main.tf`: Cloud Function source archive/function module.
- `terraform/modules/memorystore_for_redis/main.tf`: Redis module.
- `terraform/modules/gcs_bucket/main.tf`: GCS bucket module.

## Naming Conventions

**Files:**
- Python modules use snake_case: `backend/pipeline/ingestion/normalizer_runtime.py`, `backend/pipeline/storage/transcript_store.py`.
- Python tests use `test_*.py`: `backend/pipeline/storage/tests/test_feed_store.py`.
- React component files use PascalCase for component modules: `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`, `frontend/transcription-ui/src/components/feeds/FeedTable.tsx`.
- Frontend service files use camelCase named after the operation: `frontend/transcription-ui/src/service/listTranscripts.ts`, `frontend/transcription-ui/src/service/authSession.ts`.
- Frontend API controllers use `<domain>Controller.ts`: `frontend/api/src/rules/rulesController.ts`.
- Frontend tests are co-located with `.test.ts` or `.test.tsx`: `frontend/api/src/rules/rulesController.test.ts`, `frontend/transcription-ui/src/components/Login.test.tsx`.
- Protobuf files use snake_case: `protos/evaluated_transcribed_audio.proto`.
- Terraform modules use standard `main.tf`, `variables.tf`, `outputs.tf`, and optional `versions.tf`: `terraform/modules/alloydb/main.tf`.

**Directories:**
- Backend pipeline domains use snake_case or lower-case domain names: `backend/pipeline/ingestion/`, `backend/pipeline/schema_types/`.
- Source collectors live under one directory per source: `backend/pipeline/ingestion/collectors/bcfy_calls/`, `backend/pipeline/ingestion/collectors/openmhz/`.
- Backend services live under plural domain folders: `backend/services/feeds/`, `backend/services/rules/`, `backend/services/transcripts/`.
- React components are grouped by feature/domain: `frontend/transcription-ui/src/components/transcripts/`, `frontend/transcription-ui/src/components/audio/`, `frontend/transcription-ui/src/components/common/`.
- Model data artifacts and fetch code are separated: `model/data/` for artifacts and `model/data_sources/` for scripts/clients.

**Python Symbols:**
- Classes use `CapWords`: `NormalizerRuntime`, `FeedStore`, `TranscriptService`, `EvaluationEventProcessor`.
- Functions and methods use snake_case: `route_capturer`, `create_pool_with_retry`, `list_transcripts_by_feed_id`.
- Constants use upper snake case: `DEFAULT_REFRESH_INTERVAL`, `OUTPUT_TOPIC_PATH`, `CHIRP_UNINTELLIGIBLE_MARKER`.
- Private helpers use a leading underscore: `_require_env`, `_build_app_url`, `_resolve_container_memory_bytes`.

**TypeScript Symbols:**
- React components use PascalCase exports: `TranscriptView`, `FeedTable`, `AuthProvider`.
- Service functions use camelCase verbs: `listTranscripts`, `authSession`, `getFeed`.
- Shared DTO interfaces/types use PascalCase: `Transcript`, `Feed`, `RuleCreate`, `ListTranscriptsResponse`.
- API route classes use PascalCase controller names: `TranscriptsController`, `FeedsController`, `RulesController`.

## Where to Add New Code

**New Source Collector:**
- Primary code: `backend/pipeline/ingestion/collectors/<source>/`
- Register source: `backend/pipeline/ingestion/router.py`
- Add source enum: `backend/pipeline/storage/feed_store.py`
- Add claim caps: `backend/pipeline/ingestion/settings.py`
- Add DB seed/migration: `terraform/modules/alloydb/sql/ingestion/`
- Tests: `backend/pipeline/ingestion/collectors/tests/` or `backend/pipeline/ingestion/collectors/<source>/tests/`

**New Ingestion Runtime Behavior:**
- Primary code: `backend/pipeline/ingestion/normalizer_runtime.py`
- Contract/model changes: `backend/pipeline/ingestion/models.py`
- Settings: `backend/pipeline/ingestion/settings.py`
- Store operations: `backend/pipeline/storage/feed_store.py` and `backend/pipeline/storage/feed_queries.py`
- Tests: `backend/pipeline/ingestion/tests/`, `backend/pipeline/storage/tests/`

**New Normalization Transform:**
- Primary code: `backend/pipeline/normalization/transforms/`
- Wire into DAG: `backend/pipeline/normalization/orchestration.py`
- State/data models: `backend/pipeline/normalization/common/datatypes.py`, `backend/pipeline/normalization/state/`
- Options: `backend/pipeline/normalization/options.py`
- Tests: `backend/pipeline/normalization/tests/`

**New Pipeline Message Contract:**
- Schema: `protos/<name>.proto`
- Generated code: `backend/pipeline/schema_types/`
- Generation task: `.mise.toml` task `generate:protos`
- Python usage: Import generated modules from pipeline processors/stores.
- TypeScript usage: Add UI/API DTOs to `frontend/common/src/types/` when the payload crosses the frontend boundary.

**New Transcriber:**
- Implementation: `backend/pipeline/transcription/transcribers/<name>.py`
- Factory branch: `backend/pipeline/transcription/transcribers/factory.py`
- Interface: `backend/pipeline/transcription/transcribers/base.py`
- Enum: `backend/pipeline/normalization/common/enums.py`
- Tests: `backend/pipeline/transcription/tests/`

**New Rule Evaluation Strategy:**
- Implementation: `backend/pipeline/evaluation/rules_evaluation/`
- Service wiring: `backend/pipeline/evaluation/main.py`
- Business logic: `backend/pipeline/evaluation/service.py`
- Tests: `backend/pipeline/evaluation/tests/`

**New Notification Output Behavior:**
- Conversion/entry logic: `backend/pipeline/notification/send_notification.py`
- Dedup logic: `backend/pipeline/notification/notification_deduplication.py`
- HTTP request behavior: `backend/pipeline/notification/request_handler.py`
- Schema changes: `protos/alert_notification.proto`
- Tests: `backend/pipeline/notification/test_*.py`

**New Backend Service Domain:**
- FastAPI app: `backend/services/<domain>/main.py`
- Service class: `backend/services/<domain>/service.py`
- Pydantic models: `backend/services/<domain>/models.py`
- Store: `backend/pipeline/storage/<domain>_store.py`
- SQL constants: `backend/pipeline/storage/<domain>_queries.py`
- Schema SQL: `terraform/modules/alloydb/sql/ingestion/`
- Tests: `backend/services/<domain>/tests/`, `backend/pipeline/storage/tests/`

**New Frontend API Route:**
- Controller: `frontend/api/src/<domain>/<domain>Controller.ts`
- Shared DTOs: `frontend/common/src/types/<domain>.ts`, exported from `frontend/common/src/index.ts`
- Config env vars: `frontend/api/src/config.ts`
- Generated routes/spec: `frontend/api/tsoa.json`, `frontend/api/openapi.yaml`
- Tests: `frontend/api/src/<domain>/<domain>Controller.test.ts`

**New React UI Feature:**
- View/component: `frontend/transcription-ui/src/components/<domain>/`
- Fetch wrapper: `frontend/transcription-ui/src/service/<operation>.ts`
- Shared utilities: `frontend/transcription-ui/src/utils/`
- Auth-dependent code: `frontend/transcription-ui/src/context/`
- Route registration: `frontend/transcription-ui/src/App.tsx`
- Tests: Co-located `*.test.tsx` beside components or `*.test.ts` beside service/util files.

**New Shared Frontend Type:**
- Type file: `frontend/common/src/types/<name>.ts`
- Export barrel: `frontend/common/src/index.ts`
- Consumers: `frontend/api/src/` and `frontend/transcription-ui/src/`

**New Model Evaluation Notebook:**
- Notebook: `model/colabs/evaluate_<model_name>.ipynb`
- Shared helper code: `model/colabs/common/`
- Manifests/results: `model/data/manifests/` and `model/data/inference_manifests/`
- Dependencies: `model/notebook_docker/requirements.txt` or `model/nemo_docker/requirements.txt`
- Tests for common helpers: `model/colabs/common/tests/`

**New Data Source Script for Model Work:**
- Implementation: `model/data_sources/<source>/`
- Data artifacts: `model/data/`
- README guidance: `model/data_sources/<source>/README.md` and `model/data/README.md`

**New Infrastructure Module:**
- Module code: `terraform/modules/<module>/main.tf`
- Inputs/outputs: `terraform/modules/<module>/variables.tf`, `terraform/modules/<module>/outputs.tf`
- Versions when needed: `terraform/modules/<module>/versions.tf`
- SQL migrations for AlloyDB: `terraform/modules/alloydb/sql/ingestion/`

**New Local Development Support:**
- Scripts and seed data: `local_dev/`
- Compose service definitions: `docker-compose.yml`
- Project tasks: `.mise.toml`

## Special Directories

**`backend/pipeline/schema_types/`:**
- Purpose: Generated Python protobuf modules.
- Generated: Yes.
- Committed: Partially; regenerate from `protos/` with `.mise.toml` task `generate:protos`.

**`frontend/api/src/generated/`:**
- Purpose: TSOA generated Express routes.
- Generated: Yes.
- Committed: Not present in file scan; generated by `frontend/api` script `generate-routes`.

**`frontend/api/openapi.yaml`:**
- Purpose: Generated OpenAPI spec consumed by API Gateway/docs workflows.
- Generated: Yes.
- Committed: Yes.

**`model/data/`:**
- Purpose: Data artifacts, manifests, label exports, inference results, and segmentation samples.
- Generated: Mixed; contains curated/generated artifacts.
- Committed: Yes for selected artifacts.

**`model/colabs/`:**
- Purpose: ASR evaluation notebooks and shared helpers.
- Generated: No for helper code; notebooks may contain generated outputs.
- Committed: Yes.

**`local_dev/`:**
- Purpose: Local emulator/mock setup and seed files.
- Generated: No for scripts; contains local environment file `local_dev/LOCAL.env`.
- Committed: Mixed; do not quote environment values from `local_dev/LOCAL.env`.

**`integration_tests/`:**
- Purpose: Service-exercising integration and e2e tests.
- Generated: No.
- Committed: Yes.

**`.planning/`:**
- Purpose: GSD planning state and codebase maps.
- Generated: Yes.
- Committed: Workflow-dependent.

**`radio_transcription.egg-info/`:**
- Purpose: Python package metadata from local install/build.
- Generated: Yes.
- Committed: Present in workspace; avoid adding new source code here.

---

*Structure analysis: 2026-05-24*

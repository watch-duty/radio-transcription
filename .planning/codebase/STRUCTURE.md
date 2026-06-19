# Codebase Structure

**Analysis Date:** 2026-06-19

## Directory Layout

```text
radio-transcription/
|-- .agents/                    # Repository agent instructions
|-- .github/                    # CI workflows and coding instructions
|-- .planning/codebase/         # Generated codebase mapping documents
|-- backend/                    # Python pipeline stages and backend services
|   |-- pipeline/               # Event-driven audio processing pipeline
|   |   |-- common/             # Shared GCP, auth, settings, tracing helpers
|   |   |-- ingestion/          # Source collectors and VM runtime
|   |   |-- normalization/      # Raw audio normalization Cloud Function
|   |   |-- transcription/      # Speech-to-text Cloud Function
|   |   |-- evaluation/         # Transcript evaluation Cloud Function
|   |   |-- notification/       # Alert notification Cloud Function
|   |   |-- segmentation/       # Apache Beam/Dataflow continuous segmentation
|   |   |-- schema_types/       # Generated protobuf Python types
|   |   `-- storage/            # AlloyDB stores and SQL query modules
|   |-- scripts/                # Backend utility scripts
|   `-- services/               # FastAPI feed/audio/transcript/rule services
|-- documentation/              # Project documentation and local-dev notes
|-- frontend/                   # BFF API, shared TypeScript types, React UI
|   |-- api/                    # Express/TSOA browser-facing API
|   |-- common/                 # Shared TypeScript package
|   `-- transcription-ui/       # React/Vite operator UI
|-- integration_tests/          # Python integration test suites
|-- local_dev/                  # Local emulators, mock servers, seed data
|-- model/                      # ASR/model research, manifests, notebooks, CLI
|   |-- colabs/                 # Notebooks and notebook shared code
|   |-- data/                   # Dataset manifests and model data docs
|   |-- data_sources/           # Data-source specific model helpers
|   |-- nemo_docker/            # NeMo container resources
|   |-- notebook_docker/        # Notebook container resources
|   |-- scripts/                # Model workflow scripts
|   |-- src/                    # Model Python packages
|   |-- tests/                  # Model package tests
|   `-- trained_checkpoints/    # Model checkpoint workspace
|-- protos/                     # Source protobuf contracts
|-- scripts/                    # Repository-level utility scripts
|-- terraform/                  # Cloud infrastructure modules and SQL migrations
|-- ASR_CONTRIBUTING.md         # Model contribution guide
|-- CONTEXT.md                  # Domain terminology and architectural context
|-- README.md                   # Repository overview
|-- pyproject.toml              # Root Python package and tool configuration
|-- uv.lock                     # Root Python lockfile
`-- .mise.toml                  # Developer tasks for lint, format, protos, dev
```

## Directory Purposes

**Repository Root:**
- Purpose: Holds monorepo configuration, high-level docs, lockfiles, and orchestration tasks.
- Contains: `README.md`, `CONTEXT.md`, `ASR_CONTRIBUTING.md`, `pyproject.toml`, `uv.lock`, `.mise.toml`, `.pre-commit-config.yaml`.
- Key files: `README.md`, `CONTEXT.md`, `.mise.toml`, `pyproject.toml`.

**`.agents/`:**
- Purpose: Repository-specific agent guidance.
- Contains: Supplemental instructions for agent behavior in this repository.
- Key files: `.agents/instructions.md`.

**`.github/`:**
- Purpose: CI workflows and checked-in coding instructions.
- Contains: GitHub Actions workflow YAML and language style guides.
- Key files: `.github/workflows/ci.yml`, `.github/workflows/integration-tests.yml`, `.github/instructions/PYTHON_STYLE.instructions.md`, `.github/instructions/JS_TS_STYLE.instructions.md`.

**`.planning/codebase/`:**
- Purpose: Generated architecture, structure, stack, testing, and concern maps for GSD workflows.
- Contains: Mapper output documents.
- Key files: `.planning/codebase/ARCHITECTURE.md`, `.planning/codebase/STRUCTURE.md`.

**`backend/`:**
- Purpose: Python backend package for the production pipeline and data APIs.
- Contains: `backend/pipeline/`, `backend/services/`, and `backend/scripts/`.
- Key files: `backend/pipeline/README.md`, `backend/pipeline/ingestion/main.py`, `backend/services/feeds/main.py`.

**`backend/pipeline/common/`:**
- Purpose: Shared backend helpers used across stages and services.
- Contains: GCP publishing/upload helpers, auth, settings, tracing, Pub/Sub/CloudEvent helpers, and shared tests.
- Key files: `backend/pipeline/common/gcp_helper.py`, `backend/pipeline/common/auth.py`, `backend/pipeline/common/settings.py`, `backend/pipeline/common/tracing.py`.

**`backend/pipeline/ingestion/`:**
- Purpose: Audio source ingestion runtime, source routing, collector contracts, and source-specific collectors.
- Contains: Runtime modules in `backend/pipeline/ingestion/*.py`, collectors in `backend/pipeline/ingestion/collectors/`, collector docs, and ingestion tests.
- Key files: `backend/pipeline/ingestion/main.py`, `backend/pipeline/ingestion/collector_runtime.py`, `backend/pipeline/ingestion/models.py`, `backend/pipeline/ingestion/router.py`, `backend/pipeline/ingestion/source_runtime_specs.py`.

**`backend/pipeline/ingestion/collectors/`:**
- Purpose: Source-specific adapters for Broadcastify feeds, Broadcastify calls, OpenMHz, fire notifications, and echo notifications.
- Contains: One source folder per collector plus shared collector tests and docs.
- Key files: `backend/pipeline/ingestion/collectors/README.md`, `backend/pipeline/ingestion/collectors/echo/main.py`, `backend/pipeline/ingestion/collectors/tests/`.

**`backend/pipeline/segmentation/`:**
- Purpose: Streaming segmentation for continuous audio feeds.
- Contains: Apache Beam orchestration, transforms, VAD/audio helpers, state models, and segmentation tests.
- Key files: `backend/pipeline/segmentation/main.py`, `backend/pipeline/segmentation/orchestration.py`, `backend/pipeline/segmentation/transforms/`, `backend/pipeline/segmentation/state/`.

**`backend/pipeline/normalization/`:**
- Purpose: Cloud Function stage that transcodes and persists audio segment artifacts.
- Contains: Function entrypoint, processor, settings, helper modules, and tests.
- Key files: `backend/pipeline/normalization/main.py`, `backend/pipeline/normalization/processor.py`.

**`backend/pipeline/transcription/`:**
- Purpose: Cloud Function stage that runs speech-to-text and publishes transcript messages.
- Contains: Function entrypoint, processor, transcriber factory, provider implementations, enums, settings, and tests.
- Key files: `backend/pipeline/transcription/main.py`, `backend/pipeline/transcription/processor.py`, `backend/pipeline/transcription/transcribers/factory.py`, `backend/pipeline/transcription/transcribers/chirp.py`.

**`backend/pipeline/evaluation/`:**
- Purpose: Cloud Function stage that evaluates transcripts against alerting rules.
- Contains: Function entrypoint, processor, evaluation service, rules evaluator implementations, settings, and tests.
- Key files: `backend/pipeline/evaluation/main.py`, `backend/pipeline/evaluation/processor.py`, `backend/pipeline/evaluation/service.py`, `backend/pipeline/evaluation/rules_evaluation/evaluator.py`.

**`backend/pipeline/notification/`:**
- Purpose: Cloud Function stage that deduplicates and sends outbound alert notifications.
- Contains: Function entrypoint, webhook request handler, Redis dedupe helper, settings, and tests.
- Key files: `backend/pipeline/notification/send_notification.py`, `backend/pipeline/notification/request_handler.py`, `backend/pipeline/notification/notification_deduplication.py`.

**`backend/pipeline/schema_types/`:**
- Purpose: Python types generated from protobuf contracts and Beam state definitions.
- Contains: Generated `*_pb2.py`, `*_pb2_grpc.py`, `*.pyi`, and betterproto state types.
- Key files: `backend/pipeline/schema_types/continuous_audio_pb2.py`, `backend/pipeline/schema_types/segmented_audio_pb2.py`, `backend/pipeline/schema_types/streaming_state.py`.

**`backend/pipeline/storage/`:**
- Purpose: AlloyDB connection management, store classes, and SQL query modules.
- Contains: Store implementations, query constants, connection helpers, migrations-facing domain models, and storage tests.
- Key files: `backend/pipeline/storage/connection.py`, `backend/pipeline/storage/feed_store.py`, `backend/pipeline/storage/audio_segment_store.py`, `backend/pipeline/storage/transcript_store.py`, `backend/pipeline/storage/rules_store.py`.

**`backend/services/`:**
- Purpose: Authenticated FastAPI services around backend domain data.
- Contains: One service folder per domain with `main.py`, `models.py`, `service.py`, and tests.
- Key files: `backend/services/feeds/main.py`, `backend/services/audio_segments/main.py`, `backend/services/transcripts/main.py`, `backend/services/rules/main.py`.

**`frontend/api/`:**
- Purpose: TypeScript BFF for browser-facing routes, auth, OpenAPI generation, and backend service proxying.
- Contains: Express app, TSOA controllers, auth utilities, config, generated route config, and API package metadata.
- Key files: `frontend/api/src/index.ts`, `frontend/api/src/config.ts`, `frontend/api/src/authentication.ts`, `frontend/api/src/utils.ts`, `frontend/api/tsoa.json`.

**`frontend/common/`:**
- Purpose: Shared TypeScript package for API/domain types and helpers consumed by the BFF and UI.
- Contains: `frontend/common/src/index.ts`, typed modules under `frontend/common/src/types/`, and build config.
- Key files: `frontend/common/src/index.ts`, `frontend/common/src/types/feeds.ts`, `frontend/common/src/types/audio.ts`, `frontend/common/src/types/transcripts.ts`, `frontend/common/src/types/rules.ts`.

**`frontend/transcription-ui/`:**
- Purpose: React/Vite operator UI.
- Contains: Route composition, feature components, service clients, auth context, hooks, assets, and Vite config.
- Key files: `frontend/transcription-ui/src/main.tsx`, `frontend/transcription-ui/src/App.tsx`, `frontend/transcription-ui/src/service/`, `frontend/transcription-ui/src/components/`, `frontend/transcription-ui/vite.config.ts`.

**`model/`:**
- Purpose: ASR research, Gemini fine-tuning, dataset manifests, notebook workflows, and shared model utilities.
- Contains: Model package config, source packages, tests, notebooks, data manifests, scripts, Docker resources, and checkpoints.
- Key files: `model/pyproject.toml`, `model/src/common/manifest.py`, `model/src/gemini_sft/cli.py`, `model/data/manifests/README.md`, `model/tests/`.

**`protos/`:**
- Purpose: Source-of-truth protobuf contracts for pipeline Pub/Sub messages and Beam state.
- Contains: `.proto` files for each pipeline message family.
- Key files: `protos/continuous_audio.proto`, `protos/segmented_audio.proto`, `protos/normalized_audio.proto`, `protos/transcribed_audio.proto`, `protos/evaluated_transcribed_audio.proto`, `protos/alert_notification.proto`, `protos/streaming_state.proto`.

**`terraform/`:**
- Purpose: Cloud infrastructure and database schema definitions.
- Contains: Reusable modules for AlloyDB, Cloud Functions, GCS, Redis, MIG collectors, ASR evaluation, and SQL migrations.
- Key files: `terraform/modules/alloydb/sql/ingestion/003_feeds.sql`, `terraform/modules/alloydb/sql/ingestion/010_rules.sql`, `terraform/modules/alloydb/sql/ingestion/011_transcripts.sql`, `terraform/modules/alloydb/sql/ingestion/022_audio_segments_annotations.sql`.

**`integration_tests/`:**
- Purpose: Cross-service and storage integration coverage.
- Contains: Python integration test modules and helpers.
- Key files: `integration_tests/storage/`.

**`local_dev/`:**
- Purpose: Local emulator setup, mock services, seed data, and development helpers.
- Contains: Mock servers, Pub/Sub/GCS setup scripts, test SQL, and local environment file placeholders.
- Key files: `local_dev/mock_server.py`, `local_dev/mock_audio_server.py`, `local_dev/gcs_init.py`, `local_dev/pubsub_init.py`, `local_dev/test_data.sql`. `local_dev/LOCAL.env` is present and is treated as secret-bearing configuration.

## Key File Locations

**Entry Points:**
- `backend/pipeline/ingestion/main.py`: VM ingestion runtime startup.
- `backend/pipeline/ingestion/collectors/echo/main.py`: Echo source notification Cloud Function.
- `backend/pipeline/segmentation/main.py`: Beam/Dataflow segmentation job entrypoint.
- `backend/pipeline/normalization/main.py`: Normalization Cloud Function entrypoint.
- `backend/pipeline/transcription/main.py`: Transcription Cloud Function entrypoint.
- `backend/pipeline/evaluation/main.py`: Evaluation Cloud Function entrypoint.
- `backend/pipeline/notification/send_notification.py`: Notification Cloud Function entrypoint.
- `backend/services/feeds/main.py`: Feed service FastAPI app.
- `backend/services/audio_segments/main.py`: Audio segment service FastAPI app.
- `backend/services/transcripts/main.py`: Transcript service FastAPI app.
- `backend/services/rules/main.py`: Rules service FastAPI app.
- `frontend/api/src/index.ts`: Express/TSOA BFF app.
- `frontend/transcription-ui/src/main.tsx`: React app mount.
- `model/src/gemini_sft/cli.py`: Gemini SFT CLI dispatcher.

**Configuration:**
- `pyproject.toml`: Root Python dependencies and lint/type tool settings.
- `uv.lock`: Root Python dependency lockfile.
- `model/pyproject.toml`: Model package dependencies and `gemini-sft` script.
- `.mise.toml`: Developer tasks for lint, format, proto generation, build, and local dev.
- `.pre-commit-config.yaml`: Pre-commit hooks for Python, proto, notebook, and frontend checks.
- `frontend/api/package.json`: BFF package scripts and dependencies.
- `frontend/api/tsoa.json`: TSOA route/OpenAPI generation config.
- `frontend/transcription-ui/package.json`: React UI package scripts and dependencies.
- `frontend/transcription-ui/vite.config.ts`: Vite dev/build and local proxy behavior.
- `frontend/transcription-ui/tsconfig.json`: UI TypeScript configuration.
- `local_dev/LOCAL.env`: Local environment configuration file present; contents are not read or quoted.

**Core Logic:**
- `backend/pipeline/ingestion/collector_runtime.py`: Feed leasing and source runtime side effects.
- `backend/pipeline/ingestion/router.py`: Source-type to collector dispatch.
- `backend/pipeline/ingestion/source_runtime_specs.py`: Capture mode and lease policy registry.
- `backend/pipeline/common/gcp_helper.py`: GCS upload and Pub/Sub publish helpers.
- `backend/pipeline/segmentation/orchestration.py`: Beam graph for continuous segmentation.
- `backend/pipeline/normalization/processor.py`: Raw-to-normalized audio processing.
- `backend/pipeline/transcription/processor.py`: Transcription stage orchestration.
- `backend/pipeline/evaluation/processor.py`: Transcript evaluation and notification publishing.
- `backend/pipeline/notification/send_notification.py`: Notification orchestration and dedupe.
- `backend/pipeline/storage/feed_store.py`: Feed lifecycle, leasing, status, and failure persistence.
- `backend/pipeline/storage/audio_segment_store.py`: Audio segment and annotation persistence.
- `backend/pipeline/storage/transcript_store.py`: Transcript persistence.
- `backend/pipeline/storage/rules_store.py`: Alert rule persistence.
- `frontend/api/src/feeds/feedsController.ts`: Browser-facing feed operations.
- `frontend/transcription-ui/src/App.tsx`: UI route structure.
- `model/src/common/manifest.py`: Canonical ASR manifest validation and merge helpers.

**Testing:**
- `backend/pipeline/ingestion/collectors/tests/`: Source collector and ingestion runtime tests.
- `backend/pipeline/storage/tests/`: Store and storage helper tests.
- `backend/services/feeds/tests/`, `backend/services/audio_segments/tests/`, `backend/services/transcripts/tests/`, `backend/services/rules/tests/`: FastAPI service unit tests.
- `integration_tests/storage/`: AlloyDB-backed storage integration tests.
- `model/tests/`: Model package tests.
- `frontend/api/src/**/*.test.ts`: BFF unit tests when present.
- `frontend/transcription-ui/src/**/*.test.tsx`: UI unit tests when present.

## Naming Conventions

**Files:**
- Python modules use `snake_case.py`, such as `backend/pipeline/notification/request_handler.py` and `model/src/common/inference_manifest.py`.
- Pipeline stage folders use domain nouns, such as `backend/pipeline/normalization/`, `backend/pipeline/transcription/`, `backend/pipeline/evaluation/`, and `backend/pipeline/notification/`.
- Backend services use one folder per domain with `main.py`, `models.py`, `service.py`, and tests, such as `backend/services/feeds/main.py`.
- Store files use `<domain>_store.py`, such as `backend/pipeline/storage/feed_store.py` and `backend/pipeline/storage/transcript_store.py`.
- SQL query modules use `<domain>_queries.py`, such as `backend/pipeline/storage/rules_queries.py`.
- Protobuf source files use lower snake case domain names, such as `protos/normalized_audio.proto` and `protos/evaluated_transcribed_audio.proto`.
- Generated protobuf Python files use `*_pb2.py`, `*_pb2_grpc.py`, and `*.pyi` under `backend/pipeline/schema_types/`.
- TypeScript React components and views use PascalCase filenames, such as `frontend/transcription-ui/src/App.tsx`.
- TypeScript service/type modules use lower camel or domain filenames, such as `frontend/transcription-ui/src/service/feeds.ts` and `frontend/common/src/types/transcripts.ts`.
- Terraform SQL migrations use numeric prefixes, such as `terraform/modules/alloydb/sql/ingestion/022_audio_segments_annotations.sql`.

**Directories:**
- Backend pipeline directories are domain-aligned under `backend/pipeline/<stage>/`.
- Source collector directories are source-aligned under `backend/pipeline/ingestion/collectors/<source>/`.
- Backend service directories are API-domain aligned under `backend/services/<domain>/`.
- React feature components are grouped by domain under `frontend/transcription-ui/src/components/<feature>/`.
- Shared TypeScript types are grouped under `frontend/common/src/types/`.
- Model package code lives under `model/src/<package>/`; notebooks and notebook helpers stay under `model/colabs/`.

## Where to Add New Code

**New VM-Managed Audio Source:**
- Primary code: `backend/pipeline/ingestion/collectors/<source>/`
- Runtime registration: `backend/pipeline/ingestion/router.py`, `backend/pipeline/ingestion/source_runtime_specs.py`
- Storage enum and SQL seeds: `backend/pipeline/storage/feed_store.py`, `terraform/modules/alloydb/sql/ingestion/002_source_types.sql`, `terraform/modules/alloydb/sql/ingestion/006_seed_source_types.sql`
- API/UI types: `backend/services/feeds/models.py`, `frontend/common/src/types/feeds.ts`
- Tests: `backend/pipeline/ingestion/collectors/tests/`

**New Direct Notification Source:**
- Primary code: `backend/pipeline/ingestion/collectors/<source>/main.py`
- Message publish helper use: `backend/pipeline/common/gcp_helper.py`
- Runtime/source vocabulary alignment: `backend/pipeline/ingestion/source_runtime_specs.py`, `backend/pipeline/storage/feed_store.py`, `terraform/modules/alloydb/sql/ingestion/`
- Tests: `backend/pipeline/ingestion/collectors/tests/`

**New Pipeline Message or Stage:**
- Contract: `protos/<message>.proto`
- Generated types: `backend/pipeline/schema_types/`
- Stage entrypoint and processor: `backend/pipeline/<stage>/main.py`, `backend/pipeline/<stage>/processor.py`
- Infrastructure: `terraform/modules/cloud_function/` and related Terraform module wiring.
- Tests: `backend/pipeline/<stage>/tests/`

**New Backend API Service:**
- Implementation: `backend/services/<domain>/main.py`, `backend/services/<domain>/models.py`, `backend/services/<domain>/service.py`
- Persistence: `backend/pipeline/storage/<domain>_store.py`, `backend/pipeline/storage/<domain>_queries.py`
- Schema: `terraform/modules/alloydb/sql/ingestion/<nnn>_<domain>.sql`
- Tests: `backend/services/<domain>/tests/`, `backend/pipeline/storage/tests/`, `integration_tests/storage/`

**New Storage Operation:**
- Store method: `backend/pipeline/storage/<domain>_store.py`
- SQL constant: `backend/pipeline/storage/<domain>_queries.py`
- Service use: `backend/services/<domain>/service.py`
- Tests: `backend/pipeline/storage/tests/`

**New BFF Endpoint:**
- Controller: `frontend/api/src/<domain>/<domain>Controller.ts`
- Shared types: `frontend/common/src/types/<domain>.ts`
- Generation config: `frontend/api/tsoa.json`
- Error/auth behavior: `frontend/api/src/authentication.ts`, `frontend/api/src/index.ts`
- Tests: `frontend/api/src/<domain>/**/*.test.ts`

**New React View or Workflow:**
- Route: `frontend/transcription-ui/src/App.tsx`
- View/component code: `frontend/transcription-ui/src/components/<feature>/`
- API client: `frontend/transcription-ui/src/service/<feature>.ts`
- Shared types: `frontend/common/src/types/<feature>.ts`
- Hooks/context: `frontend/transcription-ui/src/hooks/`, `frontend/transcription-ui/src/context/`
- Tests: `frontend/transcription-ui/src/**/*.test.tsx`

**New Shared Frontend Type:**
- Implementation: `frontend/common/src/types/<domain>.ts`
- Export: `frontend/common/src/index.ts`
- BFF use: `frontend/api/src/<domain>/`
- UI use: `frontend/transcription-ui/src/`

**New Model Helper or CLI Command:**
- Common helper: `model/src/common/`
- Gemini SFT command: `model/src/gemini_sft/cli.py` plus a module under `model/src/gemini_sft/`
- Manifest rules: `model/data/manifests/README.md`
- Tests: `model/tests/`

**New Infrastructure Resource:**
- Module code: `terraform/modules/<resource>/`
- Database schema: `terraform/modules/alloydb/sql/ingestion/`
- Local dev support: `local_dev/`
- Documentation: `documentation/`

**Utilities:**
- Backend shared helpers: `backend/pipeline/common/`
- Repository scripts: `scripts/`
- Backend scripts: `backend/scripts/`
- Model scripts: `model/scripts/`
- Local emulator/mock helpers: `local_dev/`

## Special Directories

**`backend/pipeline/schema_types/`:**
- Purpose: Generated Python protobuf and betterproto state types.
- Generated: Yes, from `protos/*.proto` through `.mise.toml` task `generate:protos`.
- Committed: Source proto files are committed in `protos/`; generated output is regenerated by task and should not be edited by hand.

**`frontend/api/src/generated/`:**
- Purpose: TSOA route output configured by `frontend/api/tsoa.json`.
- Generated: Yes, from controller metadata in `frontend/api/src/*Controller.ts`.
- Committed: Treat generated route files as build artifacts; edit controllers and regenerate.

**`terraform/modules/alloydb/sql/ingestion/`:**
- Purpose: Ordered database migrations and seed data for ingestion, feeds, transcripts, rules, audio segments, and annotations.
- Generated: No.
- Committed: Yes.

**`model/colabs/`:**
- Purpose: Notebook workflows and notebook-oriented helpers for ASR/model experiments.
- Generated: No for maintained notebooks and helper code; notebook output cells are removed by pre-commit.
- Committed: Yes for source notebooks and helpers.

**`model/trained_checkpoints/`:**
- Purpose: Workspace for trained model checkpoint artifacts.
- Generated: Yes, by training/evaluation workflows.
- Committed: No for large generated checkpoint artifacts.

**`local_dev/`:**
- Purpose: Local emulators, mock services, seed data, and local developer configuration.
- Generated: Mixed; scripts and seed SQL are source, emulator data and local env are environment-specific.
- Committed: Yes for scripts and seed data. `local_dev/LOCAL.env` is present and must not be read or quoted.

**`.planning/codebase/`:**
- Purpose: Codebase mapping documents used by GSD planning/execution commands.
- Generated: Yes, by mapper agents.
- Committed: Project-dependent; mapper writes should stay limited to assigned documents such as `.planning/codebase/ARCHITECTURE.md` and `.planning/codebase/STRUCTURE.md`.

---

*Structure analysis: 2026-06-19*

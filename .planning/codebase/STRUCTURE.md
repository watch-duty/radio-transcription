# Codebase Structure

**Analysis Date:** 2026-06-14

## Directory Layout

```text
radio-transcription/
|-- backend/            # Pipeline workers, common code, storage, APIs
|-- frontend/           # API proxy, shared TS types, React UI
|-- model/              # ASR evaluation, Gemini SFT, notebooks, datasets
|-- protos/             # Pub/Sub protobuf message contracts
|-- terraform/          # Google Cloud infrastructure modules and SQL schema
|-- integration_tests/  # Docker/local-stack integration and E2E tests
|-- local_dev/          # Local emulators, mock servers, seed data
|-- documentation/      # Developer docs
|-- scripts/            # Repo scripts such as notebook formatting
|-- .github/            # Style guides, PR template, CI workflows
|-- .mise.toml          # Repo task runner configuration
|-- pyproject.toml      # Root Python workspace config
|-- docker-compose.yml  # Local full-stack development environment
`-- CONTEXT.md          # Domain glossary
```

## Directory Purposes

**`backend/`:**
- Purpose: production backend pipeline, APIs, and storage access.
- Contains: `pipeline/`, `services/`, and scripts.
- Key files: `backend/pipeline/README.md`,
  `backend/pipeline/ingestion/collector_runtime.py`,
  `backend/pipeline/storage/feed_store.py`.
- Subdirectories:
  - `pipeline/common/` - shared clients, auth, GCP helpers, tracing.
  - `pipeline/ingestion/` - VM collector runtime and source collectors.
  - `pipeline/normalization/` - Beam/audio normalization pipeline.
  - `pipeline/transcription/` - transcription Cloud Function.
  - `pipeline/evaluation/` - rules evaluation function.
  - `pipeline/notification/` - alert notification function.
  - `services/*` - FastAPI management APIs.

**`frontend/`:**
- Purpose: operator-facing UI and API proxy.
- Contains:
  - `api/` - Express/tsoa Google Cloud Function proxy.
  - `common/` - shared TypeScript types and status utilities.
  - `transcription-ui/` - React/Vite app.
- Key files: `frontend/common/src/types/feeds.ts`,
  `frontend/common/src/utils/statusUtils.ts`,
  `frontend/transcription-ui/src/App.tsx`.

**`model/`:**
- Purpose: ASR research/evaluation and Gemini SFT workflows.
- Contains: package code in `model/src/`, tests, notebooks, data sources, and
  Dockerfiles.
- Key files: `model/src/gemini_sft/cli.py`,
  `model/src/common/scoring.py`, `ASR_CONTRIBUTING.md`.

**`protos/`:**
- Purpose: pipeline message schemas.
- Contains: raw, normalized, transcribed, evaluated, notification, and
  streaming-state proto files.
- Generated Python files go under `backend/pipeline/schema_types/`.

**`terraform/`:**
- Purpose: reusable Google Cloud resource modules and AlloyDB schema.
- Contains: modules for AlloyDB, Cloud Functions, container MIG, GCS,
  Memorystore, and ASR evaluation.
- Key files: `terraform/modules/alloydb/sql/ingestion/*.sql`.

**`integration_tests/`:**
- Purpose: tests that exercise the local Docker/resource stack.
- Contains: API, storage, and E2E flows.
- These are resource-heavy and should not be run by default.

**`local_dev/`:**
- Purpose: local emulators, mock servers, seeded data, and helper scripts.
- Contains: Pub/Sub and GCS initialization, mock audio server, mock source
  server, SQL test data, and local env file.

## Key File Locations

**Entry Points:**
- `backend/pipeline/ingestion/main.py` - VM ingestion worker.
- `backend/pipeline/normalization/main.py` - normalization pipeline.
- `backend/pipeline/transcription/main.py` - transcription function.
- `backend/pipeline/evaluation/main.py` - evaluation function.
- `backend/pipeline/notification/send_notification.py` - notification
  function.
- `backend/services/*/main.py` - FastAPI apps.
- `frontend/api/src/index.ts` - Express proxy app.
- `frontend/transcription-ui/src/main.tsx` - React app.

**Configuration:**
- `pyproject.toml` - Python workspace, Ruff, ty, and package config.
- `.mise.toml` - canonical repo tasks.
- `docker-compose.yml` - local stack.
- `frontend/*/package.json` - frontend package scripts/dependencies.
- `.github/instructions/*.instructions.md` - style guides.

**Core Logic:**
- `backend/pipeline/ingestion/models.py` - collector/runtime contract.
- `backend/pipeline/ingestion/collectors/README.md` - collector authoring and
  failure-classification policy.
- `backend/pipeline/ingestion/collector_runtime.py` - leasing, capture,
  upload, publish, and failure handling.
- `backend/pipeline/storage/feed_store.py` and `feed_queries.py` - feed
  lifecycle persistence.
- `backend/pipeline/common/gcp_helper.py` - GCS and Pub/Sub helpers.

**Testing:**
- Backend unit tests live next to modules under `backend/**/tests/` or as
  `backend/pipeline/notification/test_*.py`.
- Ingestion runtime tests live in `backend/pipeline/ingestion/tests/`.
- Collector tests live in `backend/pipeline/ingestion/collectors/tests/`.
- Storage tests live in `backend/pipeline/storage/tests/`.
- Frontend tests live beside components/services as `*.test.ts(x)`.
- Heavy integration/E2E tests live under `integration_tests/`.

**Documentation:**
- `README.md` - repo overview.
- `CONTEXT.md` - domain glossary.
- `backend/pipeline/README.md` - backend pipeline notes.
- `documentation/local-dev-mock-audio.md` - local mock audio guide.
- `ASR_CONTRIBUTING.md` - model/evaluation guidance.

## Naming Conventions

**Files:**
- Python modules use `snake_case.py`.
- Python tests use `test_*.py`.
- React components commonly use `PascalCase.tsx`.
- Existing frontend tests use `*.test.ts` and `*.test.tsx`, even though the
  style guide says new test names should use `_test.ts`.
- Terraform files are conventional `main.tf`, `variables.tf`, `outputs.tf`,
  and `versions.tf`.

**Directories:**
- Backend packages use descriptive snake_case or plural names.
- Source-specific collectors live under
  `backend/pipeline/ingestion/collectors/{source}/`.
- Frontend feature areas live under `components/{area}/` and `service/`.

**Special Patterns:**
- Generated proto output under `backend/pipeline/schema_types/` should not be
  edited directly.
- Package-local `.egg-info` directories exist in some subpackages as generated
  metadata and should not be treated as hand-authored source.
- `__pycache__`, `.pytest_cache`, and `.ruff_cache` are local artifacts.

## Where to Add New Code

**New VM Collector:**
- Implementation: `backend/pipeline/ingestion/collectors/{source}/`.
- Registry: `backend/pipeline/ingestion/router.py`.
- Caps/settings: `backend/pipeline/ingestion/settings.py`.
- Source type enum: `backend/pipeline/storage/feed_store.py`.
- DB seeds: `terraform/modules/alloydb/sql/ingestion/`.
- Tests: `backend/pipeline/ingestion/collectors/tests/`.

**Feed Lifecycle / Quarantine Policy:**
- Runtime routing: `backend/pipeline/ingestion/collector_runtime.py`.
- Contract models: `backend/pipeline/ingestion/models.py`.
- Store methods: `backend/pipeline/storage/feed_store.py`.
- SQL: `backend/pipeline/storage/feed_queries.py`.
- API/UI status propagation: `backend/services/feeds/`,
  `frontend/common/src/types/feeds.ts`, and
  `frontend/common/src/utils/statusUtils.ts`.
- Tests: `backend/pipeline/ingestion/tests/test_collector_runtime.py` and
  `backend/pipeline/storage/tests/test_feed_store.py`.

**New Backend API Capability:**
- FastAPI route: `backend/services/{domain}/main.py`.
- Service logic: `backend/services/{domain}/service.py`.
- Pydantic models: `backend/services/{domain}/models.py`.
- Store/query methods: `backend/pipeline/storage/`.
- Tests: `backend/services/{domain}/tests/`.

**New Pipeline Message Field:**
- Schema: `protos/*.proto`.
- Generate bindings with `mise run generate:protos`.
- Update producer and consumer tests in the relevant pipeline packages.

**New Frontend Feature:**
- Shared type: `frontend/common/src/types/`.
- API proxy: `frontend/api/src/{domain}/`.
- UI component/view: `frontend/transcription-ui/src/components/{domain}/`.
- UI service wrapper: `frontend/transcription-ui/src/service/`.

## Special Directories

**`.planning/codebase/`:**
- Purpose: GSD-generated codebase map.
- Source: This mapping workflow.
- Committed: Yes if `commit_docs` is true.

**`backend/pipeline/schema_types/`:**
- Purpose: generated protobuf Python modules and schema helpers.
- Source: `mise run generate:protos`.
- Committed: wrapper/helper files may exist, but generated outputs should not
  be edited directly.

**`local_dev/mock_audio/`:**
- Purpose: local mock source fixture data for collector workflows.
- Source: developer-maintained sample files.

---

*Structure analysis: 2026-06-14*
*Update when directory structure changes*

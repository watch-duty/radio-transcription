# Technology Stack

**Analysis Date:** 2026-06-14

## Languages

**Primary:**
- Python 3.13 - backend pipeline, FastAPI services, storage, ingestion
  workers, Cloud Functions, local tooling, and integration tests.
- TypeScript - frontend API proxy, shared frontend types, and React UI.
- Terraform HCL - Google Cloud infrastructure modules.
- Protocol Buffers - Pub/Sub message contracts in `protos/`.

**Secondary:**
- SQL - AlloyDB schema migrations in
  `terraform/modules/alloydb/sql/ingestion/`.
- Shell - Docker entrypoints and local initialization scripts.
- Python 3.11+ - model package under `model/` has its own lower runtime
  floor for ASR research and Gemini SFT tooling.

## Runtime

**Environment:**
- Root package requires `>=3.13,<3.14` in `pyproject.toml`.
- Backend runtime uses `uv`, `uvloop`, `asyncio`, `asyncpg`, `aiohttp`,
  Google Cloud clients, and containerized services.
- Frontend packages target Node/TypeScript with Yarn lockfiles.
- Local development uses Docker Compose services for Pub/Sub emulator, fake
  GCS, Postgres, Redis, pipeline services, APIs, and mock audio sources.

**Package Manager:**
- Python: `uv` with root `uv.lock` and workspace members in `pyproject.toml`.
- Model subtree: separate `model/pyproject.toml` and `model/uv.lock`.
- Frontend: Yarn per package under `frontend/api`,
  `frontend/common`, and `frontend/transcription-ui`.

## Frameworks

**Core Backend:**
- FastAPI and Uvicorn - management APIs under `backend/services/*`.
- Functions Framework and CloudEvents - transcription, evaluation, and
  notification Cloud Function entry points.
- Apache Beam - normalization pipeline in `backend/pipeline/normalization`.
- asyncpg and psycopg - AlloyDB/PostgreSQL storage access.
- aiohttp and curl-cffi - collector/runtime HTTP integrations.

**Core Frontend:**
- React 19, React Router 7, Vite 8, TypeScript 6.
- Material UI 9 and Toolpad Core for UI structure.
- TanStack Query, Wavesurfer, Howler, and date-fns for interactive data and
  audio workflows.
- Express 5, tsoa, jose, and google-auth-library in `frontend/api`.

**Testing:**
- Python: `pytest`, `pytest-asyncio`, `pytest-cov`, `pytest-xdist`,
  `testcontainers`, `fakeredis`, and focused service/pipeline tests.
- Frontend: Vitest, Testing Library, jsdom, and ESLint.
- Infrastructure: Terraform validate/fmt and SQL guard checks.

**Build/Dev:**
- `mise` orchestrates repo-level format, lint, dev, and proto-generation
  tasks in `.mise.toml`.
- Ruff formats and lints Python; `ty` type-checks Python.
- Prettier, ESLint, and TypeScript type checking cover frontend packages.
- `grpcio-tools` and `betterproto` generate schema bindings from `protos/`.

## Key Dependencies

**Critical:**
- `google-cloud-pubsub` - ordered publish and event transport between
  pipeline stages.
- `google-cloud-storage` and `gcloud-aio-storage` - GCS claim-check storage.
- `asyncpg` - async feed lease, progress, and lifecycle state mutations.
- `pydantic` and `pydantic-settings` - API models and settings.
- `redis` - notification deduplication and shared cache support.
- `opentelemetry-*` - tracing across services and functions.

**Domain-Specific:**
- `apache-beam[gcp]`, `onnxruntime`, `pedalboard`, `numba`, `soundfile` -
  audio normalization and VAD pipeline.
- `google-cloud-speech` - CHIRP transcription path.
- `google-genai` under `model[vertex]` - Gemini SFT and Vertex workflows.
- `faster-whisper` - optional local Whisper API service.

## Configuration

**Environment:**
- Runtime settings are mostly environment-variable driven; local values live
  in `.env` and `local_dev/LOCAL.env`.
- Important environment surfaces include database settings, GCP project,
  topic/subscription names, bucket names, source credentials, and frontend API
  URLs.
- Do not copy values from local env files into docs or logs.

**Build:**
- Root `pyproject.toml` defines workspace packages and Ruff/ty settings.
- `.mise.toml` defines canonical repo tasks.
- `backend/pipeline/README.md` documents proto generation.
- Frontend package scripts define build, lint, format, test, and typecheck
  tasks per package.

## Platform Requirements

**Development:**
- Docker is needed for full local stack workflows.
- `safe-run -- <command>` should wrap agent-run tests, builds, installs, and
  other potentially heavy local commands.
- Broad local E2E/API/component/integration tests are explicitly discouraged
  unless the user asks and confirms the machine is prepared.

**Production:**
- Google Cloud Pub/Sub, GCS, AlloyDB, Redis/Memorystore, Cloud Functions,
  Cloud Run jobs, and Managed Instance Groups.
- Terraform modules under `terraform/modules/` define reusable cloud resources.

---

*Stack analysis: 2026-06-14*
*Update after major dependency or runtime changes*

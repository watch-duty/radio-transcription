# Codebase Structure

**Analysis Date:** 2026-04-21

## Directory Layout

```
radio-transcription/
├── backend/                                  # All Python runtime code
│   ├── __init__.py
│   ├── pipeline/                             # Ingestion / transcription / evaluation / notification / rules / storage / common
│   │   ├── __init__.py
│   │   ├── README.md                         # Protobuf regeneration instructions
│   │   ├── common/                           # Shared helpers (logging, auth, GCP clients, audio, env detection)
│   │   ├── ingestion/                        # NormalizerRuntime MIG worker + collectors + Echo Cloud Function + credential rotation
│   │   │   ├── main.py                       # MIG worker entry (asyncio.run → NormalizerRuntime)
│   │   │   ├── normalizer_runtime.py         # Leasing loop, heartbeat OS thread, per-feed pipeline
│   │   │   ├── models.py                     # CapturedChunk contract dataclass
│   │   │   ├── router.py                     # SourceType → collector dispatch table
│   │   │   ├── retry.py                      # retry_with_lease_check + LeaseExpiredError
│   │   │   ├── health_server.py              # aiohttp /healthz on :8080
│   │   │   ├── quarantine_telemetry.py       # Structured log + Cloud Monitoring metric
│   │   │   ├── settings.py                   # NormalizerSettings (env-driven dataclass)
│   │   │   ├── Dockerfile                    # MIG worker container
│   │   │   ├── collectors/                   # One subdir per source type
│   │   │   │   ├── icecast/                  # capture_icecast_stream (ffmpeg subprocess)
│   │   │   │   ├── openmhz/                  # openmhz_collector + websocket transport
│   │   │   │   ├── bcfy_calls/               # capture_bcfy_calls (polling)
│   │   │   │   ├── echo/                     # Eventarc Cloud Function (separate entry path)
│   │   │   │   └── tests/                    # Per-collector unit + integration tests
│   │   │   ├── broadcastify_credential_rotation/  # Cloud Function for BCFY credential refresh
│   │   │   └── tests/                        # Runtime + router + retry + health + settings tests
│   │   ├── transcription/                    # Apache Beam streaming pipeline (Dataflow)
│   │   │   ├── main.py                       # CLI entry, launches Beam DAG
│   │   │   ├── orchestration.py              # get_pipeline — DAG composition
│   │   │   ├── transforms.py                 # ParseAndKeyFn, DownloadAudioFn, RestoreOrderFn, etc.
│   │   │   ├── stitcher.py, stitcher_state.py# StitchAudioFn + TranscribeAudioFn + state machine
│   │   │   ├── transcribers.py               # Chirp / Gemini transcription backends
│   │   │   ├── vads.py, detectors.py         # Voice activity + onset detection
│   │   │   ├── audio_processor.py, dsp.py    # Audio resampling + DSP helpers
│   │   │   ├── sequence_buffer.py            # Ordered buffering for out-of-order messages
│   │   │   ├── options.py, datatypes.py      # Beam PipelineOptions + config dataclasses
│   │   │   ├── constants.py, enums.py        # DAG tags + timeout defaults
│   │   │   ├── pyproject.toml                # Separate build for Dataflow worker image
│   │   │   ├── chirp_prompt.txt, chirp_phrase_hints.txt  # Model prompts
│   │   │   ├── Dockerfile, entrypoint.sh
│   │   │   └── tests/
│   │   ├── evaluation/                       # Cloud Run Function — rules evaluation
│   │   │   ├── main.py                       # functions_framework CloudEvent entry
│   │   │   ├── processor.py                  # EvaluationEventProcessor
│   │   │   ├── service.py                    # EvaluationService
│   │   │   ├── rules_evaluation/evaluator.py # StaticTextEvaluator, RemoteTextEvaluator
│   │   │   ├── Dockerfile
│   │   │   └── tests/
│   │   ├── notification/                     # Cloud Run Function — outbound alerts with Redis dedup
│   │   │   ├── send_notification.py          # Entry
│   │   │   ├── request_handler.py            # urllib3 POST to Watch Duty endpoint
│   │   │   ├── notification_deduplication.py # Redis-backed dedup
│   │   │   ├── Dockerfile
│   │   │   └── test_*.py                     # Tests colocated with source (no tests/ subdir)
│   │   ├── rules/                            # FastAPI rules-management service
│   │   │   ├── main.py                       # /v1/rules endpoints
│   │   │   ├── service.py                    # AlloyRulesService
│   │   │   ├── Dockerfile
│   │   │   └── tests/
│   │   ├── storage/                          # AlloyDB data-access layer (async + sync mirrors)
│   │   │   ├── connection.py                 # asyncpg pool + create_pool_with_retry
│   │   │   ├── sync_connection.py            # psycopg sync mirror (for Cloud Functions)
│   │   │   ├── feed_store.py                 # FeedStore, LeasedFeed, HeartbeatResult, SourceType
│   │   │   ├── feed_queries.py               # SQL strings for feeds
│   │   │   ├── sync_feed_store.py            # SyncFeedStore (Echo CF)
│   │   │   ├── transcript_store.py, transcript_queries.py
│   │   │   ├── rules_store.py, rules_queries.py
│   │   │   ├── settings.py                   # AlloyDBSettings
│   │   │   └── tests/
│   │   └── schema_types/                     # Generated protobuf bindings (gitignored — regenerate via mise)
│   └── services/
│       ├── feeds/                            # FastAPI feeds-management service
│       │   ├── main.py, service.py, models.py, Dockerfile, tests/
│       └── transcripts/                      # FastAPI transcripts-management service
│           ├── main.py, service.py, models.py, Dockerfile, tests/
├── frontend/
│   ├── transcription-ui/                     # Vite + React + TypeScript SPA (Firebase-hosted)
│   │   ├── src/
│   │   │   ├── main.tsx                      # React root + providers
│   │   │   ├── App.tsx                       # Router
│   │   │   ├── components/                   # feeds, rules, transcripts, audio, common, docs
│   │   │   ├── context/                      # AuthContext + AuthProvider
│   │   │   ├── service/                      # listFeeds / listRules / listTranscripts wrappers
│   │   │   ├── utils/, test/, assets/
│   │   ├── index.html, package.json, vite.config.ts, firebase.json, tsconfig*.json
│   ├── api/                                  # OpenAPI/tsoa-generated TypeScript client (shared)
│   │   ├── src/                              # feeds, rules, transcripts, docs TS clients
│   │   ├── openapi.yaml, tsoa.json, Dockerfile
│   └── common/                               # Shared TS utilities across frontend packages
├── protos/                                   # Canonical protobuf schemas
│   ├── raw_audio_chunk.proto
│   ├── transcribed_audio.proto
│   ├── evaluated_transcribed_audio.proto
│   └── alert_notification.proto
├── terraform/
│   └── modules/
│       ├── container_mig/                    # MIG + health check for ingestion workers
│       │   ├── main.tf, variables.tf, outputs.tf, versions.tf
│       │   └── cloud_config.yaml.tftpl       # cloud-init: hardcodes :8080 docker -p mapping
│       ├── cloud_function/                   # Cloud Run Functions for Echo, eval, notification, credentials
│       ├── alloydb/                          # AlloyDB cluster + SQL migrations
│       │   └── sql/ingestion/                # 002_source_types.sql, 006_seed_source_types.sql, ...
│       ├── gcs_bucket/
│       ├── memorystore_for_redis/
│       └── asr_evaluation/
├── integration_tests/                        # Cross-service tests using testcontainers + fakeredis + httpx
│   ├── api/                                  # Management-service API tests
│   ├── storage/                              # FeedStore / RulesStore / TranscriptStore against real Postgres
│   ├── e2e/                                  # End-to-end flows (notification Redis, rules→eval→publish)
│   ├── conftest.py, test_utils.py, utils.py
├── local_dev/                                # Local pubsub-emulator-driven docker-compose rig
│   ├── LOCAL.env                             # Local env overrides (DO NOT commit secrets; gitignored if applicable)
│   ├── pubsub_init.py                        # Pre-creates topics + subscriptions in the emulator
│   ├── mock_server.py, run_evaluation_publish.py
├── model/                                    # Out-of-band ML research (not in runtime path)
│   ├── colabs/                               # Notebooks for Chirp, Gemini, Gemma3N experiments
│   ├── data/                                 # Training + inference manifests
│   ├── nemo_docker/Dockerfile, notebook_docker/Dockerfile
├── docker-compose.yml                        # Local dev stack (pubsub emulator, redis, postgres, services)
├── asr-eval-docker-compose.yml               # Separate compose for ASR evaluation
├── pyproject.toml                            # Root Python project (backend + services)
├── uv.lock                                   # uv-managed lockfile
├── yarn.lock                                 # Root yarn workspace lockfile
├── .mise.toml                                # mise task definitions (generate:protos, etc.)
├── .pre-commit-config.yaml                   # ruff + prettier + other lints
├── .ruff_cache/                              # Generated (not committed)
├── .venv/                                    # Generated (not committed)
├── CONTRIBUTING.md, ASR_CONTRIBUTING.md, README.md, LICENSE
├── .github/workflows/                        # CI: ci.yml, integration-tests.yml, trigger-deploy.yml, prepend-linear-issue-to-pr-title.yml
└── .planning/                                # GSD planning directory (this document lives under codebase/)
```

## Directory Purposes

**`backend/pipeline/ingestion/`:**
- Purpose: The stateful MIG worker that holds audio-source connections, leases feeds from AlloyDB, uploads bytes to GCS, publishes Pub/Sub messages, and maintains fence-token-enforced bookmarks.
- Contains: Runtime orchestrator, collectors (one per source type), Echo Cloud Function (an Eventarc-triggered side branch), health server, retry helper, quarantine telemetry, BCFY credential rotation.
- Key files: `backend/pipeline/ingestion/normalizer_runtime.py`, `backend/pipeline/ingestion/models.py`, `backend/pipeline/ingestion/main.py`, `backend/pipeline/ingestion/router.py`, `backend/pipeline/ingestion/health_server.py`, `backend/pipeline/ingestion/retry.py`, `backend/pipeline/ingestion/settings.py`.

**`backend/pipeline/ingestion/collectors/`:**
- Purpose: Per-source capture functions. Each exports an `async def` that conforms to `CollectorFn` and yields `CapturedChunk`.
- Contains: `icecast/` (ffmpeg-based continuous stream), `openmhz/` (websocket + download), `bcfy_calls/` (polling API), `echo/` (Cloud Function — does not use the runtime).
- Key files: `backend/pipeline/ingestion/collectors/icecast/icecast_collector.py`, `collectors/openmhz/collector.py`, `collectors/bcfy_calls/bcfy_calls_collector.py`, `collectors/echo/main.py`.

**`backend/pipeline/transcription/`:**
- Purpose: Apache Beam streaming pipeline deployed to Dataflow. Consumes raw audio chunks, stitches adjacent segments, transcribes via Chirp/Gemini, publishes transcripts.
- Contains: DAG composition, DoFns, stitching state machine, VAD, DSP, transcription backends, Beam options.
- Key files: `backend/pipeline/transcription/orchestration.py`, `backend/pipeline/transcription/main.py`, `backend/pipeline/transcription/stitcher.py`, `backend/pipeline/transcription/transcribers.py`, `backend/pipeline/transcription/transforms.py`.

**`backend/pipeline/evaluation/`:**
- Purpose: Rules-evaluation Cloud Run Function. Scores each transcribed segment against configured rules and publishes alerts.
- Key files: `backend/pipeline/evaluation/main.py`, `backend/pipeline/evaluation/processor.py`, `backend/pipeline/evaluation/rules_evaluation/evaluator.py`.

**`backend/pipeline/notification/`:**
- Purpose: Outbound alert forwarding with Redis-backed dedup.
- Key files: `backend/pipeline/notification/send_notification.py`, `backend/pipeline/notification/request_handler.py`, `backend/pipeline/notification/notification_deduplication.py`.

**`backend/pipeline/rules/`:**
- Purpose: FastAPI rules-management service (Cloud Run).
- Key files: `backend/pipeline/rules/main.py`, `backend/pipeline/rules/service.py`.

**`backend/pipeline/storage/`:**
- Purpose: All AlloyDB access. Async `asyncpg` for hot paths; sync `psycopg` mirrors for Cloud Function contexts where asyncio is forbidden.
- Contains: Connection pool setup (with tenacity retry), per-entity stores, raw SQL strings, settings.
- Key files: `backend/pipeline/storage/feed_store.py`, `backend/pipeline/storage/connection.py`, `backend/pipeline/storage/transcript_store.py`, `backend/pipeline/storage/rules_store.py`.

**`backend/pipeline/schema_types/`:**
- Purpose: Generated protobuf Python bindings. Checked into `.gitignore` — regenerate with `mise run generate:protos` before first run or after `.proto` edits.
- Key files: `raw_audio_chunk_pb2.py`, `transcribed_audio_pb2.py`, `evaluated_transcribed_audio_pb2.py`, `alert_notification_pb2.py` plus `.pyi` stubs.

**`backend/pipeline/common/`:**
- Purpose: Cross-cutting helpers.
- Contains: `logging.py` (setup_logging), `auth.py` (verify_oidc_token + get_id_token), `env.py` (is_gcp_env), `gcp_helper.py` (upload_staged_audio, publish_audio_chunk, publish_audio_chunk_sync), `audio.py` (get_audio_duration), `clients/` (GCS, Pub/Sub, monitoring, transcripts HTTP client), `storage/` (Redis cache provider + mock), `rules/models.py`, `constants.py`, `exceptions.py`.
- Key files: `backend/pipeline/common/logging.py`, `backend/pipeline/common/auth.py`, `backend/pipeline/common/gcp_helper.py`, `backend/pipeline/common/clients/gcs_client.py`, `backend/pipeline/common/clients/pubsub_client.py`.

**`backend/services/feeds/` and `backend/services/transcripts/`:**
- Purpose: FastAPI management services on Cloud Run.
- Key files: `backend/services/feeds/main.py`, `backend/services/feeds/service.py`, `backend/services/transcripts/main.py`, `backend/services/transcripts/service.py`.

**`protos/`:**
- Purpose: Canonical wire-format schemas for Pub/Sub messages.
- Key files: `protos/raw_audio_chunk.proto`, `protos/transcribed_audio.proto`, `protos/evaluated_transcribed_audio.proto`, `protos/alert_notification.proto`.

**`frontend/transcription-ui/`:**
- Purpose: Vite + React + TypeScript operator SPA. Three main routes: transcripts browser, feeds manager, rules editor, plus a lazy-loaded docs viewer.
- Key files: `frontend/transcription-ui/src/main.tsx`, `frontend/transcription-ui/src/App.tsx`, `frontend/transcription-ui/src/components/transcripts/TranscriptView.tsx`, `frontend/transcription-ui/src/components/feeds/FeedsView.tsx`, `frontend/transcription-ui/src/components/rules/RulesView.tsx`, `frontend/transcription-ui/src/context/AuthProvider.tsx`.

**`frontend/api/`:**
- Purpose: Shared TypeScript API client generated from `openapi.yaml` via tsoa. Imported by `transcription-ui`.
- Key files: `frontend/api/openapi.yaml`, `frontend/api/src/index.ts`, `frontend/api/src/feeds/`, `frontend/api/src/transcripts/`, `frontend/api/src/rules/`, `frontend/api/src/authentication.ts`.

**`frontend/common/`:**
- Purpose: Shared TypeScript utilities across frontend packages.
- Key files: `frontend/common/src/`.

**`terraform/modules/`:**
- Purpose: Infrastructure-as-code definitions for each deployable component.
- Contains: `container_mig/` (ingestion workers), `cloud_function/` (all Cloud Run Functions), `alloydb/` (cluster + SQL migrations), `gcs_bucket/`, `memorystore_for_redis/`, `asr_evaluation/`.
- Key files: `terraform/modules/container_mig/main.tf`, `terraform/modules/container_mig/cloud_config.yaml.tftpl`, `terraform/modules/alloydb/main.tf`, `terraform/modules/alloydb/sql/ingestion/002_source_types.sql`, `terraform/modules/alloydb/sql/ingestion/006_seed_source_types.sql`.

**`integration_tests/`:**
- Purpose: Tests that cross service boundaries, run against ephemeral Postgres via `testcontainers` and `fakeredis`.
- Contains: `api/` (FastAPI endpoint tests), `storage/` (store integration with real Postgres), `e2e/` (full flows through Pub/Sub emulator).
- Key files: `integration_tests/conftest.py`, `integration_tests/utils.py`, `integration_tests/storage/test_feed_store_integration.py`, `integration_tests/e2e/test_rules_creation_evaluation_publish.py`, `integration_tests/e2e/test_notification_redis.py`.

**`local_dev/`:**
- Purpose: Local-development scaffolding used by `docker-compose.yml`. Pre-seeds the Pub/Sub emulator with topics + subscriptions and provides helper scripts.
- Key files: `local_dev/pubsub_init.py`, `local_dev/run_evaluation_publish.py`, `local_dev/mock_server.py`, `local_dev/LOCAL.env`.

**`model/`:**
- Purpose: Out-of-band ML research area. Not part of the runtime serving path. Used for training, inference experiments, and dataset curation.
- Contains: Jupyter notebooks, dataset manifests, Docker build files for NeMo + notebook environments.

**`.planning/codebase/`:**
- Purpose: This directory — GSD-managed codebase-mapping artifacts.
- Key files: `.planning/codebase/ARCHITECTURE.md`, `.planning/codebase/STRUCTURE.md`.

## Key File Locations

**Entry Points:**
- `backend/pipeline/ingestion/main.py`: MIG worker process entry — constructs `NormalizerRuntime` and blocks until graceful shutdown.
- `backend/pipeline/ingestion/collectors/echo/main.py`: Eventarc CloudEvent handler (`handle_notification`) for Echo MP3 uploads.
- `backend/pipeline/evaluation/main.py`: Pub/Sub CloudEvent handler (`evaluate_transcribed_audio_segment`) for rules evaluation.
- `backend/pipeline/notification/send_notification.py`: Pub/Sub CloudEvent handler for outbound alerts.
- `backend/pipeline/transcription/main.py`: Beam / Dataflow pipeline CLI entry.
- `backend/services/feeds/main.py`, `backend/services/transcripts/main.py`, `backend/pipeline/rules/main.py`: FastAPI app objects (`app`) for Cloud Run.
- `backend/pipeline/ingestion/broadcastify_credential_rotation/main.py`: Scheduled Cloud Function.
- `backend/pipeline/ingestion/health_server.py`: aiohttp `/healthz` on port 8080.
- `frontend/transcription-ui/src/main.tsx`: Browser SPA root.

**Configuration:**
- `pyproject.toml`: Root Python project (dependencies, ruff, ty, pytest, uv groups).
- `uv.lock`: `uv`-managed lockfile.
- `.mise.toml`: Task definitions, including `generate:protos`.
- `.pre-commit-config.yaml`: Ruff + Prettier + pre-commit hooks.
- `.prettierrc`, `.prettierignore`, `frontend/transcription-ui/eslint.config.js`: Frontend lint + format.
- `backend/pipeline/ingestion/settings.py`: `NormalizerSettings` env-driven dataclass.
- `backend/pipeline/storage/settings.py`: `AlloyDBSettings`.
- `backend/pipeline/transcription/options.py`, `backend/pipeline/transcription/datatypes.py`: Beam `TranscriptionOptions` and config dataclasses.
- `terraform/modules/container_mig/cloud_config.yaml.tftpl`: cloud-init template that hardcodes the `/healthz` port and container run args. `HEALTH_CHECK_PORT` env override must NOT diverge from this.
- `local_dev/LOCAL.env`: Local dev env vars consumed by `docker-compose.yml`.
- `docker-compose.yml`, `asr-eval-docker-compose.yml`: Local stacks.
- `frontend/transcription-ui/vite.config.ts`, `frontend/transcription-ui/tsconfig.json`, `frontend/transcription-ui/firebase.json`.

**Core Logic:**
- `backend/pipeline/ingestion/normalizer_runtime.py`: Leasing loop, heartbeat OS thread, per-feed asyncio pipeline, fence-violation handling.
- `backend/pipeline/ingestion/models.py`: `CapturedChunk` + capture-function contract documentation.
- `backend/pipeline/ingestion/router.py`: `SourceType → (capture_fn, url_base)` registry.
- `backend/pipeline/ingestion/retry.py`: `retry_with_lease_check` + `LeaseExpiredError`.
- `backend/pipeline/storage/feed_store.py`: `FeedStore`, `LeasedFeed`, `SourceType`, `HeartbeatResult`.
- `backend/pipeline/storage/feed_queries.py`: SQL strings (`ACQUIRE_FEEDS_BATCH_SQL`, `UPDATE_PROGRESS_SQL`, `RENEW_HEARTBEATS_BATCH_DIAGNOSTIC_SQL`, etc.).
- `backend/pipeline/common/gcp_helper.py`: `upload_staged_audio`, `publish_audio_chunk`, `publish_audio_chunk_sync`.
- `backend/pipeline/transcription/orchestration.py`: Beam `get_pipeline` DAG composition.
- `backend/pipeline/transcription/stitcher.py`, `backend/pipeline/transcription/stitcher_state.py`: Audio stitching state machine.
- `backend/pipeline/evaluation/processor.py`: `EvaluationEventProcessor`.
- `backend/pipeline/common/auth.py`: OIDC verification + token fetch.
- `backend/pipeline/common/logging.py`: Cloud Logging vs local `basicConfig` switch.

**Testing:**
- `backend/pipeline/ingestion/tests/`: Unit tests for runtime, router, retry, health server, settings, quarantine telemetry, collector contract.
- `backend/pipeline/ingestion/collectors/tests/`: Per-collector unit + integration tests.
- `backend/pipeline/ingestion/collectors/echo/tests/`: Echo CF unit + integration tests.
- `backend/pipeline/storage/tests/`: Connection + FeedStore + TranscriptStore unit tests.
- `backend/pipeline/transcription/tests/`: Orchestration, stitcher, VAD, transforms, DSP, audio processor, sequence buffer tests.
- `backend/pipeline/evaluation/tests/`: Processor, service, evaluator tests.
- `backend/pipeline/notification/test_*.py`: Tests colocated with source (no `tests/` subdir).
- `backend/pipeline/rules/tests/`, `backend/services/feeds/tests/`, `backend/services/transcripts/tests/`: FastAPI endpoint tests.
- `backend/pipeline/common/tests/`: Shared helper tests.
- `integration_tests/`: Cross-service tests using testcontainers.
- `frontend/transcription-ui/src/**/*.test.ts(x)`: Vitest frontend tests (e.g., `listFeeds.test.ts`, `TranscriptView.test.tsx`).

## Naming Conventions

**Files:**
- **Python modules**: `snake_case.py`. Example: `normalizer_runtime.py`, `feed_store.py`, `retry_with_lease_check` inside `retry.py`.
- **Test modules**: `test_<subject>.py`. Example: `backend/pipeline/ingestion/tests/test_runtime.py`, `backend/pipeline/storage/tests/test_feed_store.py`. Notification is the exception — tests sit next to source as `test_<name>.py` with no `tests/` dir.
- **Integration tests**: prefixed `test_<subject>_integration.py` when they need `testcontainers`. Example: `backend/pipeline/ingestion/collectors/tests/test_openmhz_collector_integration.py`.
- **SQL query modules**: `<entity>_queries.py` alongside `<entity>_store.py`. Example: `feed_queries.py` + `feed_store.py`, `rules_queries.py` + `rules_store.py`.
- **Protobuf generated files**: `<name>_pb2.py`, `<name>_pb2.pyi`, `<name>_pb2_grpc.py` under `backend/pipeline/schema_types/`. Never edit by hand.
- **Dockerfile**: One `Dockerfile` per deployable service directory.
- **Terraform modules**: `main.tf`, `variables.tf`, `outputs.tf`, `versions.tf`, `cloud_config.yaml.tftpl` where a cloud-init template is needed.
- **TypeScript**: `PascalCase.tsx` for React components, `camelCase.ts` for services/helpers. Example: `TranscriptView.tsx`, `listTranscripts.ts`.
- **Frontend tests**: `<name>.test.ts(x)` colocated with source.
- **SQL migrations**: `NNN_description.sql` under `terraform/modules/alloydb/sql/ingestion/`. Example: `002_source_types.sql`, `006_seed_source_types.sql`.

**Directories:**
- **Per-source-type collectors**: `backend/pipeline/ingestion/collectors/<slug>/`. Slug matches `SourceType` value (e.g., `openmhz`, `bcfy_calls`, `icecast`, `echo`).
- **Per-service code + tests**: service implementation files at the top of the dir, a `tests/` subdir for pytest, a `Dockerfile` alongside.
- **Shared helpers**: `backend/pipeline/common/` plus `backend/pipeline/common/clients/` for GCP clients and `backend/pipeline/common/storage/` for cache providers.
- **Generated code**: segregated under `backend/pipeline/schema_types/` (Python protos) and `frontend/api/src/` (TS client). Both are regenerated rather than hand-edited.

**Classes / Types:**
- `PascalCase` Python classes: `NormalizerRuntime`, `FeedStore`, `CapturedChunk`, `LeaseExpiredError`, `HealthState`.
- `StrEnum` for slug-valued enums that must match SQL seed data: `SourceType`.
- `TypedDict` for DB-row shapes: `LeasedFeed`, `Feed`, `HeartbeatResult`.
- Dataclasses with `frozen=True, kw_only=True` for settings: `NormalizerSettings`, `AlloyDBSettings`.
- Callable type aliases: `CollectorFn`, `CaptureFn` (see `backend/pipeline/ingestion/models.py` and `backend/pipeline/ingestion/normalizer_runtime.py`).

## Where to Add New Code

**New ingestion source (new `SourceType`):**
- Primary code: create `backend/pipeline/ingestion/collectors/<slug>/` with the async capture function that conforms to `CollectorFn` (see `backend/pipeline/ingestion/models.py`). Add an `__init__.py` re-exporting the capture function.
- Registry: add `SourceType.<NAME>` to `backend/pipeline/storage/feed_store.py:SourceType` AND update `terraform/modules/alloydb/sql/ingestion/002_source_types.sql` + `006_seed_source_types.sql`.
- Router: add an entry to the `_COLLECTORS` dict in `backend/pipeline/ingestion/router.py` with the capture function + URL base.
- Extension/content-type: if the new source yields non-FLAC audio, extend the branch in `NormalizerRuntime._process_feed` at `backend/pipeline/ingestion/normalizer_runtime.py:359-368`.
- Tests: unit tests in `backend/pipeline/ingestion/collectors/<slug>/` or `backend/pipeline/ingestion/collectors/tests/test_<slug>_collector.py`; integration tests go in the same directory with `_integration.py` suffix.

**New Pub/Sub message type:**
- Primary code: add `protos/<name>.proto`. Run `mise run generate:protos` to regenerate `backend/pipeline/schema_types/<name>_pb2.py`.
- Publisher side: add a helper in `backend/pipeline/common/gcp_helper.py` or the relevant service.
- Subscriber side: add a Cloud Function entry (`@functions_framework.cloud_event`) under `backend/pipeline/<stage>/main.py`, plus Terraform wiring in `terraform/modules/cloud_function/`.

**New FastAPI management endpoint (existing service):**
- Primary code: add a route in the service's `main.py` and business logic in `service.py`.
- Models: add Pydantic models in `models.py`.
- Tests: extend `tests/test_api.py` in the service directory.
- If new DB access is needed: extend the corresponding `<entity>_store.py` and `<entity>_queries.py` under `backend/pipeline/storage/`.

**New FastAPI management service:**
- Primary code: create `backend/services/<name>/main.py` + `service.py` + `models.py` + `Dockerfile` + `tests/test_api.py`. Model on `backend/services/feeds/`.
- Auth: reuse `backend.pipeline.common.auth.verify_oidc_token` as a FastAPI `Depends`.
- Terraform: add a Cloud Run resource to `terraform/modules/cloud_function/` or an equivalent module.

**New Beam DoFn in the transcription pipeline:**
- Implementation: add the DoFn class to `backend/pipeline/transcription/transforms.py` (or a dedicated module if complex, e.g., `stitcher.py`).
- DAG wiring: insert it into `get_pipeline` in `backend/pipeline/transcription/orchestration.py`.
- Config: add an option to `backend/pipeline/transcription/options.py` + defaults in `backend/pipeline/transcription/constants.py`.
- Tests: add a unit test to `backend/pipeline/transcription/tests/test_<dofn>.py`.

**New shared helper:**
- Cross-cutting: `backend/pipeline/common/<name>.py` with a unit test in `backend/pipeline/common/tests/test_<name>.py`.
- GCP client wrapper: `backend/pipeline/common/clients/<name>_client.py`.
- Cache provider: `backend/pipeline/common/storage/<name>.py`.

**New frontend feature:**
- Primary code: create a component folder `frontend/transcription-ui/src/components/<feature>/` with one or more `PascalCase.tsx` files.
- Data fetching: add a `camelCase.ts` in `frontend/transcription-ui/src/service/` that wraps the generated client from `frontend/api/src/`.
- Routing: add a `<Route>` entry to `frontend/transcription-ui/src/App.tsx`.
- Tests: colocate `<name>.test.tsx` or `<name>.test.ts` next to source.

**New Terraform module:**
- Create `terraform/modules/<name>/` with `main.tf`, `variables.tf`, `outputs.tf`, `versions.tf`. Consumers live in the root Terraform config (not yet visible in this repo — the per-env root is expected to reference modules here).

**New integration test:**
- Cross-service flow: `integration_tests/e2e/test_<flow>.py`.
- API test against a running service: `integration_tests/api/test_<service>_api.py`.
- Store test against real Postgres: `integration_tests/storage/test_<entity>_store_integration.py`.

**New protobuf field:**
- Edit the `.proto` file under `protos/`. Regenerate with `mise run generate:protos`. Ensure downstream consumers handle the new field tolerantly (protobuf's default = unknown-field skip).

## Special Directories

**`backend/pipeline/schema_types/`:**
- Purpose: Generated protobuf bindings for `backend/pipeline/` code.
- Generated: Yes — regenerate with `mise run generate:protos` or the manual `uv run python -m grpc_tools.protoc ...` command in `backend/pipeline/README.md`.
- Committed: No — contents are `.gitignore`d. Must be regenerated after `git clone` or any `.proto` edit.

**`frontend/api/src/`:**
- Purpose: Generated TypeScript API client from `frontend/api/openapi.yaml` via `tsoa`.
- Generated: Yes.
- Committed: Check `frontend/api/.gitignore` (not inspected here). Rebuild with the scripts in `frontend/api/scripts/`.

**`backend/pipeline/transcription/transcription_pipeline.egg-info/`:**
- Purpose: Dataflow worker package metadata from `pip install -e` against `backend/pipeline/transcription/pyproject.toml`.
- Generated: Yes.
- Committed: No (by convention — `.gitignore` handles).

**`radio_transcription.egg-info/`:**
- Purpose: Package metadata for the root `pyproject.toml`.
- Generated: Yes.
- Committed: No.

**`.venv/`, `.ruff_cache/`, `.pytest_cache/`, `__pycache__/`:**
- Generated: Yes.
- Committed: No (`.gitignore`).

**`local_dev/LOCAL.env`:**
- Purpose: Env vars for the local docker-compose stack. Should NOT contain production secrets — treat as dev-only.
- Generated: No.
- Committed: Depends on `.gitignore` content — inspect before committing any file matching `*.env`.

**`model/`:**
- Purpose: Research artifacts. Not on the runtime serving path. Notebooks and manifests for ASR model development.
- Generated: Manifests are generated by notebooks.
- Committed: Yes for notebooks + small manifests; large datasets live in GCS and are referenced by manifest.

**`.planning/`:**
- Purpose: GSD workflow artifacts (phases, plans, codebase maps, etc.).
- Generated: Partially — humans and GSD agents both write here.
- Committed: Project-dependent. Normally yes for decision records (`codebase/`, `milestones/`); tool-managed state may be gitignored.

**`terraform/modules/alloydb/sql/ingestion/`:**
- Purpose: Raw SQL migrations applied during the AlloyDB module apply. Ordering is lexicographic via the `NNN_` prefix.
- Generated: No.
- Committed: Yes. Must be kept in lockstep with `backend/pipeline/storage/feed_store.py:SourceType` — changing one without the other breaks ingestion at startup.

**`protos/`:**
- Purpose: Canonical schema source of truth. Every Pub/Sub message type is defined here.
- Generated: No.
- Committed: Yes.

---

*Structure analysis: 2026-04-21*

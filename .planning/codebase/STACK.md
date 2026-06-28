# Stack

## Summary

Radio Transcription is a monorepo for emergency radio audio ingestion,
segmentation, transcription, rules evaluation, notifications, model research,
and a web UI for feed/rule/transcript operations.

Tracked repository scale at mapping time:

- 754 tracked files.
- 271 backend Python files.
- 67 model-subtree Python files outside `model/.venv`.
- 141 frontend TypeScript/TSX files.
- 165 Python/TypeScript test files across backend, model, frontend, and
  integration tests.

## Runtime Platforms

- Backend services target Python 3.13 and are managed by `uv` from the root
  `pyproject.toml`.
- Model and ASR research code targets Python 3.11 through the separate
  `model/pyproject.toml` package.
- Frontend packages target Node.js 22 and Yarn.
- Local orchestration is Docker Compose.
- Infrastructure is Terraform.
- Cloud runtime is Google Cloud: Pub/Sub, Cloud Storage, Cloud Run or Cloud
  Functions style HTTP/event services, AlloyDB/Postgres, Memorystore/Redis,
  Vertex AI/Gemini, and Google auth.

Tool versions are pinned through `.tool-versions`:

- `uv 0.9.28`
- `python 3.13.2`
- `nodejs 22.14.0`
- `terraform 1.14.5`
- `jq latest`

## Backend Python

The root package is `radio-transcription` and discovers `backend*` packages.
The root `uv` workspace has these members:

- `backend/pipeline/common`
- `backend/pipeline/normalization`
- `backend/pipeline/segmentation`
- `backend/pipeline/transcription`
- `backend/pipeline/evaluation`
- `backend/pipeline/notification`
- `backend/services/audio_segments`
- `backend/services/feeds`
- `backend/services/rules`

Important backend dependencies:

- Web and service layer: FastAPI, Uvicorn, Functions Framework, Pydantic.
- Cloud APIs: `google-cloud-storage`, `google-cloud-pubsub`,
  `google-cloud-logging`, `google-cloud-monitoring`, `google-cloud-speech`,
  `google-genai`.
- Data and messaging: `asyncpg`, `psycopg[binary]`, Redis, protobuf,
  CloudEvents.
- Streaming segmentation: Apache Beam with GCP extras, ONNX Runtime,
  Pedalboard, NumPy, SoundFile, AV.
- Reliability and observability: Tenacity, OpenTelemetry exporters,
  structured Google Cloud logging.

## Model Python

`model/` is a separate package named `radio-transcription-model`. The packaged
source lives under `model/src` and exposes:

- `common`: shared manifest, scoring, GCS, audio, inference, and Gemini helper
  modules.
- `common.gemini`: prompt constants, prior-context construction, Vertex request
  construction, batch inference, tuning data, and Vertex job helpers.
- `gemini_sft`: the config-driven Gemini SFT CLI.

Optional extras keep heavyweight dependencies isolated:

- `audio`: `torchaudio`, `soundfile`
- `scoring`: `jiwer`, `nemo_text_processing`
- `hf`: Hugging Face dataset/evaluation dependencies
- `vertex`: `google-genai`
- `dev`: pytest and xdist
- `all`: convenience bundle of all model extras

The installed console script is `gemini-sft`.

## Frontend TypeScript

`frontend/` contains three Yarn packages:

- `frontend/common`: shared API/client types consumed by UI and proxy.
- `frontend/api`: Express 5 proxy/BFF, Functions Framework target,
  TSOA route/spec generation, Google auth, cookie parsing, CORS, and
  OpenAPI output.
- `frontend/transcription-ui`: Vite React app using MUI, Toolpad, React Query,
  React Router, Wavesurfer, Google OAuth, and Vitest.

The UI exposes feed search/configuration, transcripts, rule management, and API
docs routes.

## Local Development

Primary tasks live in `.mise.toml`.

Common commands:

- `mise install`
- `mise run dev:start`
- `mise run dev`
- `mise run dev:whisper`
- `mise run dev:remote`
- `mise run dev:stop`
- `mise run lint`
- `mise run format`

`docker-compose.yml` starts emulators and services for local development:

- Pub/Sub emulator
- Fake GCS server
- Postgres
- Redis
- Audio ingestion
- Normalization
- Segmentation
- Transcription
- Rules evaluation
- Notification
- Rules management API
- Feeds API
- Audio segments API
- Mock source servers
- Frontend API
- Integration test runner

`asr-eval-docker-compose.yml` is the preferred model/operator runtime for
notebooks, ASR CLI work, and Gemini SFT.

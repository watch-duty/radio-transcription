# Integrations

## Google Cloud

The production-facing system is deeply integrated with Google Cloud.

Primary services:

- Pub/Sub for claim-check events between ingestion, segmentation,
  normalization, transcription, rules evaluation, and notification stages.
- Cloud Storage for staged, canonical, playback, transcription, ASR manifest,
  SFT run, and inference-output artifacts.
- AlloyDB/Postgres for feeds, feed audit events, rules, transcripts, audio
  segments, and annotations.
- Memorystore/Redis for notification deduplication and rule-service cache
  behavior.
- Cloud Run or Cloud Functions style deployment for HTTP services and
  CloudEvent handlers.
- Vertex AI Gemini for model inference, batch inference, supervised
  fine-tuning, continuous tuning, and tuned endpoint evaluation.
- Google auth, service account impersonation, OIDC verification, and ADC for
  local operator workflows.

Local development emulates Pub/Sub, GCS, Postgres, and Redis through Docker
Compose where possible.

## Audio Sources

Ingestion supports multiple source families through collector modules and a
typed routing registry:

- Broadcastify feed streams via the Icecast collector.
- Broadcastify Calls item polling.
- Fire Notifications collector/client paths.
- OpenMHz websocket collector.
- Echo collector paths, including GCS/object notification style tests.

`backend/pipeline/ingestion/router.py` maps `SourceType` values to collector
functions. Adding a new VM-claimable source is intentionally multi-step:
update the source enum/seed data, source runtime spec, collector registry, and
tests. `backend/pipeline/ingestion/main.py` validates registry drift at
startup.

## Pipeline Messaging

The main audio pipeline uses protobuf payloads and Pub/Sub claim-checks:

1. Ingestion captures source audio and publishes continuous or segmented audio
   events.
2. Segmentation reads continuous Pub/Sub streams, restores order, stitches
   transmissions, uploads raw segments to GCS, and publishes `SegmentedAudio`.
3. Normalization downloads raw segment audio from GCS, transcodes canonical
   FLAC and playback M4A derivatives, persists audio segment metadata, and
   publishes `NormalizedAudio`.
4. Transcription reads `NormalizedAudio`, invokes the active transcriber, writes
   transcript annotations, and publishes `TranscribedAudio`.
5. Rules evaluation reads `TranscribedAudio`, evaluates rules, writes
   evaluation annotations, and publishes alert payloads when needed.
6. Notification consumes alert payloads and sends/deduplicates notifications.

Protobuf schemas live in `protos/` and generated Python code is written under
`backend/pipeline/schema_types/` via `mise run generate:protos`.

## Storage And APIs

The persistent domain APIs are FastAPI services backed by AlloyDB:

- `backend/services/feeds`: feed lifecycle/configuration API.
- `backend/services/audio_segments`: audio segment listing and annotation API.
- `backend/services/rules`: rule CRUD API.

The frontend proxy at `frontend/api` is the browser-facing BFF. It handles auth,
CORS, generated TSOA routes, and forwards to the private backend services.

## Transcription Engines

The transcription service is pluggable through `TRANSCRIBER_TYPE` and
`TRANSCRIBER_CONFIG`.

Available transcribers:

- `GOOGLE_CHIRP_V3`
- `MOCK`
- `LOCAL_WHISPER`
- `GEMINI`

The local development default is the mock transcriber. `mise run dev:whisper`
starts the optional local Whisper service.

## Gemini SFT And Evaluation

Gemini SFT is a packaged model workflow, not just notebooks.

Entry point:

- `gemini-sft prepare --config <run.toml>`
- `gemini-sft tune --config <run.toml> --confirm`
- `gemini-sft eval --config <run.toml>`

Important integration points:

- The SFT config is a local operator TOML input.
- The durable state is copied to GCS under
  `gs://<bucket>/sft/runs/<round-id>/`.
- `config.json` in that GCS run prefix is authoritative for resume and eval.
- `prepare` downloads canonical train/validation/eval manifests and writes
  Gemini model-input JSONL for train and validation.
- `tune` submits or resumes Vertex tuning jobs through `google-genai`.
- `eval` uses Gemini batch inference and writes scorer-ready inference
  manifests.
- Checkpoint scoring currently uses online `models.generate_content` because
  Vertex batch inference accepts publisher models but not tuned endpoint
  resources.

Prompt, request, safety, generation, batch parsing, and context helpers live in
`model/src/common/gemini` to keep notebooks and CLI workflows aligned.

## External Operator Requirements

Common cloud-facing workflows require:

- Google Cloud SDK for local auth and service account impersonation.
- ADC configured before running ASR containers that access GCS/Vertex.
- Correct project selection for ASR evaluation VMs and target GCS buckets.
- Service account token creator access for hybrid frontend development.

# Architecture

## System Shape

The repository has four major subsystems:

- Backend ingestion and processing pipeline.
- Backend domain APIs and storage adapters.
- Frontend proxy and React UI.
- Model research, evaluation, and Gemini SFT tooling.

`CONTEXT.md` is the best glossary for domain terms. Use it before naming new
pipeline states, failure modes, manifest contracts, or SFT artifacts.

## Audio Processing Pipeline

The runtime pipeline is event-driven and claim-check based. Large audio payloads
are stored in GCS; Pub/Sub messages carry metadata and GCS URIs.

High-level flow:

```text
source audio
  -> ingestion collectors
  -> Pub/Sub continuous/segmented events
  -> Beam segmentation
  -> GCS raw segment upload
  -> Pub/Sub SegmentedAudio
  -> normalization/transcoding
  -> AlloyDB audio segment row + GCS canonical/playback/transcription audio
  -> Pub/Sub NormalizedAudio
  -> transcription service
  -> transcript annotation + Pub/Sub TranscribedAudio
  -> rules evaluation
  -> evaluation annotation + alert Pub/Sub
  -> notification
```

Important entry points:

- `backend/pipeline/ingestion/main.py`
- `backend/pipeline/ingestion/router.py`
- `backend/pipeline/segmentation/orchestration.py`
- `backend/pipeline/normalization/main.py`
- `backend/pipeline/normalization/processor.py`
- `backend/pipeline/transcription/main.py`
- `backend/pipeline/transcription/processor.py`
- `backend/pipeline/evaluation/main.py`
- `backend/pipeline/evaluation/processor.py`
- `backend/pipeline/notification/send_notification.py`

## Ingestion Design

Ingestion uses a shared `CollectorRuntime` to claim feeds, run the correct
collector, upload captured chunks, publish downstream events, and record feed
lifecycle/failure state.

The source routing boundary is explicit:

- `SourceType` declares source families.
- `source_runtime_specs.py` declares runtime behavior.
- `router.py` maps source types to collector functions.
- `main.py` checks the registry and runtime-spec sets match before running.

Failure semantics are deliberately domain-specific. `CONTEXT.md` defines the
distinction between object-scoped, external, pipeline-owned, operator-actionable,
quarantine-budgeted, and non-budgeted failures.

## Segmentation Design

Segmentation is an Apache Beam streaming topology.

Key responsibilities:

- Parse Pub/Sub messages into keyed stream elements.
- Restore order for continuous source streams.
- Stitch audio into bounded transmissions with VAD and timeout rules.
- Upload staged raw segment audio to GCS.
- Publish `SegmentedAudio` claim-check messages.
- Route malformed or failed elements to DLQ Pub/Sub output.

The canonical DAG is in `backend/pipeline/segmentation/orchestration.py`.

## Normalization Design

Normalization is a CloudEvent processor that turns staged raw audio into model
and playback artifacts.

Responsibilities:

- Download raw audio bytes from GCS.
- Transcode to lossless FLAC and playback M4A.
- Generate mono FLAC for speech segments to feed transcription.
- Persist audio segment metadata via the audio segments API.
- Attach waveform annotations best-effort.
- Publish `NormalizedAudio` for downstream transcription.
- Emit DLQ messages for permanent failures or exhausted retries.

## Transcription Design

The transcription service is an ASGI/FastAPI app designed for Pub/Sub push.

Key decisions:

- Warm-started client/container cache in `TranscriptionServiceContainer`.
- Transcriber selection by `TRANSCRIBER_TYPE`.
- Ordered Pub/Sub publish by `feed_id`.
- Transcript annotation write happens in a `finally` block when a `segment_id`
  exists.
- Empty transcription from the active engine is converted to
  `[UNINTELLIGIBLE]` for production Chirp-style handling.

Model research scoring distinguishes raw empty responses from
`[UNINTELLIGIBLE]`; do not assume the production fallback semantics and eval
metrics are identical.

## Domain APIs And Storage

The domain APIs are thin FastAPI layers over service classes and storage stores:

- `FeedService` over `FeedStore`
- `AudioSegmentService` over `AudioSegmentStore`
- `AlloyRulesService` over `RulesStore`

Storage adapters live under `backend/pipeline/storage`. They centralize SQL,
pagination, sync/async connection handling, feed lifecycle, audit events, and
domain-specific query behavior.

Feeds admin mutations require trusted actor context from the frontend BFF via
`X-WD-Actor-Id`, because the service-to-service token identifies the BFF
service account rather than the human admin.

## Frontend Architecture

The frontend has a BFF plus UI split:

- `frontend/api` is an Express Functions Framework target. It registers
  generated TSOA routes, handles CORS/cookies, authenticates requests, and
  centralizes HTTP error responses.
- `frontend/common` shares client/common code between the BFF and UI.
- `frontend/transcription-ui` is a React/Vite/MUI application. It gates admin
  routes, handles login/session expiry, displays feeds/transcripts/rules, and
  embeds API docs.

## Model And SFT Architecture

Model code has a separate packaging boundary under `model/`.

Shared model helpers:

- `common.manifest`: canonical manifest parsing and validation.
- `common.scoring`: WER/CER, keyword metrics, empty/unintelligible rate,
  duration buckets, bootstrap comparison.
- `common.inference_manifest`: normalized inference manifest output.
- `common.gemini.prompts`: canonical Gemini prompt and keyword set.
- `common.gemini.context`: prior-context construction.
- `common.gemini.vertex`: request construction, safety/generation config,
  tuning, polling, batch inference, batch-output parsing.
- `common.gemini.tuning_data`: Gemini audio-SFT JSONL builder/validator.

`gemini_sft` owns the operator workflow:

- `config.py`: external TOML validation and derived artifact paths.
- `prepare.py`: copy canonical manifests, build SFT JSONL, preflight.
- `tune.py`: cost confirmation, submit/resume Vertex tuning.
- `evaluate.py`: base/tuned batch inference and scoring.
- `records.py`: summaries and ledger records.

Durable run state is GCS-authoritative; local `results/` is a mirror/cache.

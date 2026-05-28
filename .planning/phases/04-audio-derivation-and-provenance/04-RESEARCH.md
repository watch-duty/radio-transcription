# Phase 4: Audio Derivation And Provenance - Research

**Researched:** 2026-05-28
**Domain:** Offline SFT audio staging, FFmpeg clip derivation, GCS binary publication, and canonical provenance
**Confidence:** HIGH

<user_constraints>
## User Constraints (from CONTEXT.md)

Source: `.planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md` [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md]

### Locked Decisions

### Reuse, Copy, Derive, And Transcode Semantics
- **D-01:** `duration <= 0` is a hard failure. Phase 4 must not guess a labeled span from whole-file duration.
- **D-02:** Probe actual source duration before deciding whether a row is standalone or points into longer audio.
- **D-03:** Reuse only when `offset` is effectively zero, source duration matches row duration within `max(0.5 seconds, row_duration * 0.02)`, the source URI is already `gs://`, and the source format is accepted by model writers.
- **D-04:** Derive a clipped span when `offset > 0` or source duration is longer than row duration beyond the tolerance. This applies to all dataset families; `bcfy_feeds` is expected to derive often, but derivation is not family-limited.
- **D-05:** If `offset + duration` exceeds probed source duration beyond tolerance, fail fast with row context.
- **D-06:** The action vocabulary is `reused`, `copied`, `derived`, and `transcoded`.
- **D-07:** `reused` means no new audio object and no transformation: a supported standalone `gs://` source clip becomes the model-ready URI.
- **D-08:** `copied` means a supported standalone non-GCS source clip is copied byte-for-byte into the dataset-version GCS `audio/copied/` area. This is not an audio transformation.
- **D-09:** `derived` means a new clipped audio object was cut from a longer source file using row `offset` and `duration`.
- **D-10:** `transcoded` means a standalone source file was converted to a model-supported format without clipping.

### Model-Ready URI Contract
- **D-11:** After Phase 4, every published SFT/example row must have `model_ready_audio_uri`.
- **D-12:** `model_ready_audio_uri` must always be a `gs://` URI. HTTPS, public S3 HTTPS, and other external source URLs may appear in source/provenance fields but must not be model-ready output.
- **D-13:** Model writers after the Phase 4 boundary must require `model_ready_audio_uri` and hard fail if it is missing. They must not silently fall back to `audio_uri`.
- **D-14:** Canonical manifests keep both the original/source URI fields and the model-ready URI so generated audio remains auditable.

### Audio Format And Transformation Policy
- **D-15:** Use narrow `ffprobe`/`ffmpeg` subprocess helpers for SFT audio decisions and clipping. Do not reuse production normalization audio processing because it is designed for VAD/normalization workflows and may resample or filter.
- **D-16:** Do not pad and do not resample by default.
- **D-17:** Mix multichannel input to mono when deriving or transcoding. `reused` and byte-for-byte `copied` actions must not alter channels.
- **D-18:** Default generated output format is FLAC because it is lossless, compact compared with WAV, and already accepted by the existing Gemini writer path.
- **D-19:** WAV remains a fallback only after the repo's Gemini SFT MIME validator and model writer support `audio/wav`. Current local code accepts `audio/flac` and `audio/mpeg`.
- **D-20:** Target-specific performance warnings, such as Whisper examples over 30 seconds, remain writer warnings unless a verified consumer rejects them.

### Audio Artifact Layout
- **D-21:** Generated or copied audio lives under action-based folders, not split-based folders:

```text
gs://wd-transcription-data/sft/{dataset_version_id}/audio/
  copied/
  derived/
  transcoded/
```

- **D-22:** Split membership remains in manifests and provenance, not in the physical audio path, because derived clips can also be reused by normal eval runs.

### Provenance Schema
- **D-23:** `model_ready_audio_uri` is always populated after Phase 4.
- **D-24:** `derived_audio_uri` is populated only for actual clipped spans. It is null for `reused`, `copied`, and `transcoded` rows.
- **D-25:** `transformation_metadata.action` must be one of `reused`, `copied`, `derived`, or `transcoded`.
- **D-26:** Minimum transformation metadata includes `original_audio_uri`, `source_audio_uri`, `offset`, `duration`, `source_duration`, `output_duration`, `source_format`, `output_format`, `source_channels`, `output_channels`, `mixed_to_mono`, `resampled`, `padded`, `split`, and `source_group`.
- **D-27:** Include enough ffmpeg/ffprobe command/version summary for debugging and audit without storing noisy full subprocess output in every row.

### Failure And Publication Semantics
- **D-28:** Fail fast on any row that cannot be downloaded, probed, clipped, transcoded, copied, or validated.
- **D-29:** Plan and validate audio decisions before writing final manifests/model inputs so downstream artifacts do not reference missing model-ready audio.
- **D-30:** Preserve the Phase 3 simplicity boundary: no force mode, no partial resume, and no cleanup workflow in Phase 4.

### the agent's Discretion
The planner may choose exact helper/module names, generated audio filename strategy, ffmpeg command flags, local staging directory structure, and whether tests mock subprocesses or use tiny generated audio fixtures, as long as the public action semantics, GCS URI contract, transformation policy, and provenance fields above remain stable.

### Deferred Ideas (OUT OF SCOPE)

- Actual SFT job submission and normal eval execution remain outside Phase 4.
- Force/resume/cleanup support remains out of scope for this phase.
- Broad audio-content duplicate detection and fuzzy URI alias detection remain out of scope.
- WAV output for generated clips can be added later if the repo's Gemini MIME validation and target writer support `audio/wav`.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| AUD-01 | The planner reuses an existing standalone supported clip when a row already points to one utterance clip. [VERIFIED: .planning/REQUIREMENTS.md] | Use `plan_audio_actions()` to probe duration and select `reused` only for standalone supported `gs://` clips within tolerance. [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md] |
| AUD-02 | The planner derives a clip only when a labeled row points into a longer source audio file by offset/duration. [VERIFIED: .planning/REQUIREMENTS.md] | Use probed source duration plus `offset`/`duration` bounds checks before selecting `derived`. [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md] |
| AUD-03 | Derived clips preserve the least-transforming reliable audio format accepted by target writers, with WAV fallback when exact source-format slicing is unreliable. [VERIFIED: .planning/REQUIREMENTS.md] | Default generated output should be FLAC, because current local Gemini validation accepts `audio/flac` and `audio/mpeg`; WAV remains deferred until local writer support changes. [VERIFIED: model/colabs/common/sft.py] [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md] |
| AUD-04 | Multichannel input is mixed to mono when deriving clips. [VERIFIED: .planning/REQUIREMENTS.md] | Use `ffmpeg ... -ac 1 ...` only for `derived` and `transcoded` actions; local FFmpeg probe confirmed output channels become 1. [VERIFIED: local ffmpeg 7.0.2 command run] |
| AUD-05 | The generator does not add padding and does not resample by default unless a target-specific writer requires it. [VERIFIED: .planning/REQUIREMENTS.md] | Do not pass `-ar`, `apad`, `adelay`, or padding filters; record source/output sample rate and output duration after execution. [VERIFIED: local ffmpeg 7.0.2 command run] |
| AUD-06 | Every SFT example records provenance for original audio URI, offset, duration, source group, split, reuse/derived decision, and transformation metadata. [VERIFIED: .planning/REQUIREMENTS.md] | `LabeledSegment` and canonical rows already include `model_ready_audio_uri`, `derived_audio_uri`, and `transformation_metadata`; Phase 4 should populate those before publisher/model writers run. [VERIFIED: model/scripts/sft/dataset_split/types.py] [VERIFIED: model/scripts/sft/dataset_split/canonical.py] |
</phase_requirements>

## Summary

Phase 4 should add a narrow audio-preparation boundary before final manifest/model-input publication. The boundary should take split-assigned `LabeledSegment` rows, probe/download/stage source audio, plan one action per row, materialize any copied/derived/transcoded objects under `DatasetArtifactLayout.audio_prefix_uri`, and return new frozen `LabeledSegment` values with `model_ready_audio_uri`, `derived_audio_uri`, and `transformation_metadata` populated. [VERIFIED: model/scripts/sft/dataset_split/types.py] [VERIFIED: model/scripts/sft/dataset_split/artifacts.py] [VERIFIED: model/scripts/sft/dataset_split/publisher.py]

The important planning constraint is publication ordering. Existing `publish_dataset_version_artifacts()` calls `ensure_dataset_version_absent()` and then uploads text artifacts. If audio is uploaded before this existing function is called, the audio object itself makes the dataset-version prefix exist and the existing prefix guard will fail. The planner should refactor publication into one prechecked flow: compute layout, ensure root absent once, plan/probe all audio, upload required audio create-only, validate enriched rows, then generate and upload final manifests/model inputs/reports. [VERIFIED: model/scripts/sft/dataset_split/publisher.py] [VERIFIED: model/scripts/sft/dataset_split/artifacts.py]

**Primary recommendation:** Add `dataset_split/audio.py` for audio planning/execution and refactor `publisher.py` into a single prechecked publish flow that uses enriched segments everywhere after Phase 4. [VERIFIED: model/scripts/sft/dataset_split/publisher.py] [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md]

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|--------------|----------------|-----------|
| Audio source probing | Offline model tooling (`model/scripts/sft/dataset_split`) | Local `ffprobe` binary | SFT dataset rows are transformed offline before artifact publication. [VERIFIED: .planning/ROADMAP.md] |
| Reuse/copy/derive/transcode decision planning | Offline model tooling (`dataset_split/audio.py`) | Existing model writer MIME helpers | The decision uses row metadata, probed audio metadata, and writer-supported formats. [VERIFIED: model/scripts/sft/dataset_split/model_writers.py] |
| Local staging/download | Offline model tooling | GCS client and `requests` | Existing GCS helpers already download `gs://` objects to scratch; external HTTPS sources need streamed local staging. [VERIFIED: model/colabs/common/gcs_utils.py] [CITED: https://github.com/psf/requests/blob/main/docs/user/quickstart.md] |
| Clip derivation/transcoding | Local `ffmpeg` subprocess | Offline model tooling | Locked decisions require narrow FFmpeg helpers and reject production normalization reuse. [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md] |
| Binary audio upload | GCS client | Offline model tooling | Dataset artifacts and derived audio live under GCS dataset-version root with create-only semantics. [VERIFIED: model/scripts/sft/dataset_split/artifacts.py] [CITED: https://github.com/googleapis/python-storage/blob/main/docs/storage/blob.md] |
| Canonical/model manifest emission | Offline model tooling (`publisher.py`, `canonical.py`, `model_writers.py`) | GCS text upload helpers | Existing publisher owns artifact upload and model writer invocation. [VERIFIED: model/scripts/sft/dataset_split/publisher.py] |
| Leakage validation after audio population | Offline model tooling (`leakage.py`) | Model writers | Existing leakage checks include `model_ready_audio_uri`, but empty values are ignored, so Phase 4 must separately require populated values before publication. [VERIFIED: model/scripts/sft/dataset_split/leakage.py] |

## Project Constraints (from AGENTS.md)

- Use Context7 CLI for current library, cloud, SDK, API, and CLI documentation lookups. [VERIFIED: AGENTS.md]
- Resolve docs with `npx ctx7@latest library <name> "<query>"` before fetching docs with `npx ctx7@latest docs <libraryId> "<query>"`. [VERIFIED: AGENTS.md]
- Run Context7 CLI requests outside Codex's default sandbox, and rerun outside the sandbox after DNS/network failures. [VERIFIED: AGENTS.md]
- Keep Phase 4 code under `model/scripts/sft/dataset_split` and focused tests under `model/scripts/sft/tests`. [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md]
- Preserve existing Phase 3 artifact publisher and model writer contracts while moving the model-input boundary from `audio_uri` to `model_ready_audio_uri`. [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md]
- Python formatting is Ruff-managed with line length 80 and Python target `py313`; `model/scripts/**.py` has a relaxed Ruff profile. [VERIFIED: pyproject.toml]
- Tests under `model/scripts/sft/tests` currently use `unittest` with local `sys.path` setup. [VERIFIED: model/scripts/sft/tests/test_model_writers.py]

## Standard Stack

### Core

| Library / Tool | Version | Purpose | Why Standard |
|----------------|---------|---------|--------------|
| Python dataclasses and `dataclasses.replace` | Python stdlib | Return enriched immutable `LabeledSegment` values without mutating prior phase rows. | Existing split assignment uses frozen dataclasses plus `replace()`. [VERIFIED: model/scripts/sft/dataset_split/types.py] [VERIFIED: model/scripts/sft/dataset_split/split.py] |
| `google-cloud-storage` | 2.19.0, locked in `uv.lock`; upload time 2024-12-05 | Download GCS source objects and create-only upload text/binary artifacts. | Existing GCS helpers and artifact code already use this client. [VERIFIED: uv.lock] [VERIFIED: model/colabs/common/gcs_utils.py] |
| `requests` | 2.33.1, locked in `uv.lock`; upload time 2026-03-30 | Stream non-GCS external audio URLs to local scratch before probing/copying/transcoding. | Requests docs support `stream=True` and `iter_content()` for large downloads. [VERIFIED: uv.lock] [CITED: https://github.com/psf/requests/blob/main/docs/user/quickstart.md] |
| `ffprobe` | 7.0.2-static locally installed | Probe duration, codec, channels, and sample rate before action planning. | Existing ingestion code already uses `ffprobe` for duration; Phase 4 needs richer JSON metadata. [VERIFIED: backend/pipeline/common/audio.py] [VERIFIED: local ffprobe 7.0.2 command run] |
| `ffmpeg` | 7.0.2-static locally installed | Cut and transcode derived audio to FLAC with optional mono downmix. | Locked decisions require narrow `ffmpeg` helpers and not production normalization. [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md] |

### Supporting

| Library / Tool | Version | Purpose | When to Use |
|----------------|---------|---------|-------------|
| `pytest` | 9.0.3, locked in `uv.lock`; upload time 2026-04-07 | Existing root test dependency. | Use only if adding pytest-specific fixtures is worth it; current SFT tests are `unittest`. [VERIFIED: uv.lock] [VERIFIED: model/scripts/sft/tests/test_dataset_reports.py] |
| `unittest` | Python stdlib | Main style for current `model/scripts/sft/tests`. | Use for focused Phase 4 unit tests to match existing suite. [VERIFIED: model/scripts/sft/tests/test_model_writers.py] |
| `tempfile` / `Path` | Python stdlib | Unique scratch files for downloaded and generated audio. | Existing `download_to_scratch()` uses `tempfile.mkstemp()` to avoid basename collisions. [VERIFIED: model/colabs/common/gcs_utils.py] |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Narrow FFmpeg subprocess helpers | Production `AudioProcessor` | Do not use production normalization because it performs VAD, bandpass filtering, and resampling/downmixing for normalization workflows. [VERIFIED: backend/pipeline/normalization/audio/audio_processor.py] |
| Direct GCS signed/HTTP streaming into ffmpeg | Local scratch staging first | Local staging gives repeatable probing, create-only upload, and testable failure boundaries; existing helper already downloads GCS to scratch. [VERIFIED: model/colabs/common/gcs_utils.py] |
| Reusing `audio_uri` in writers | Required `model_ready_audio_uri` | Locked Phase 4 contract says writers must hard fail if `model_ready_audio_uri` is missing. [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md] |

**Installation:**

```bash
# No new Python package is required for the recommended implementation.
# Runtime needs local ffmpeg/ffprobe binaries plus existing project deps.
uv sync
```

**Version verification:** `uv.lock` pins `google-cloud-storage==2.19.0`, `requests==2.33.1`, and `pytest==9.0.3`; local commands reported `ffmpeg version 7.0.2-static` and `ffprobe version 7.0.2-static`. [VERIFIED: uv.lock] [VERIFIED: local ffmpeg 7.0.2 command run] [VERIFIED: local ffprobe 7.0.2 command run]

## Architecture Patterns

### System Architecture Diagram

```text
Split-assigned LabeledSegment rows
        |
        v
Build DatasetArtifactLayout and ensure root prefix absent once
        |
        v
Probe/download/stage each source audio
        |
        v
Decision: standalone supported gs:// clip?
        | yes
        v
      reused ------------------------------+
        |                                  |
        no                                 |
        v                                  |
Decision: standalone supported non-GCS?    |
        | yes                              |
        v                                  |
      copied -> upload to audio/copied/ ---+
        |                                  |
        no                                 |
        v                                  |
Decision: longer source span?              |
        | yes                              |
        v                                  |
      derived -> ffmpeg clip -> upload ----+
        |                                  |
        no                                 |
        v                                  |
      transcoded -> ffmpeg full file -> upload
                                           |
                                           v
Enriched LabeledSegment rows with model_ready_audio_uri/provenance
        |
        v
validate_split_integrity + require populated model_ready_audio_uri
        |
        v
canonical manifests + model writers + reports
        |
        v
create-only text artifact uploads
```

The diagram keeps audio execution before final text artifact generation so downstream manifests never reference missing audio. [VERIFIED: model/scripts/sft/dataset_split/publisher.py] [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md]

### Recommended Project Structure

```text
model/scripts/sft/dataset_split/
├── audio.py            # probe, plan, stage, ffmpeg execution, enrich segments
├── artifacts.py        # add binary create-only upload + audio object URI helpers
├── publisher.py        # refactor into one prechecked audio+text publication flow
├── model_writers.py    # require model_ready_audio_uri for all writer outputs
└── reports.py          # add audio action/provenance summary

model/scripts/sft/tests/
├── test_audio_derivation.py
├── test_dataset_artifacts.py
├── test_dataset_publisher.py
├── test_model_writers.py
└── test_dataset_reports.py
```

This structure matches the existing dataset split module/test ownership. [VERIFIED: model/scripts/sft/dataset_split] [VERIFIED: model/scripts/sft/tests]

### Pattern 1: Audio Planning Dataclasses

**What:** Add frozen dataclasses such as `AudioProbe`, `AudioActionPlan`, and `AudioPreparationResult` in `dataset_split/audio.py`. [VERIFIED: model/scripts/sft/dataset_split/types.py]

**When to use:** Use during Plan 04-01 so decisions can be unit-tested without running FFmpeg uploads. [VERIFIED: .planning/ROADMAP.md]

**Recommended seam:**

```python
# Source: existing frozen dataclass pattern in dataset_split/types.py
@dataclass(frozen=True)
class AudioProbe:
    duration: float
    codec_name: str
    channels: int
    sample_rate: int
    format_name: str | None = None


@dataclass(frozen=True)
class AudioActionPlan:
    segment: LabeledSegment
    action: str
    source_uri: str
    destination_uri: str | None
    probe: AudioProbe
```

Every plan should include row context (`dataset_name`, `row_index`, `source_group`, `split`) in errors because current SFT code raises clean `ValueError` subclasses for malformed rows/artifacts. [VERIFIED: model/scripts/sft/dataset_split/types.py] [VERIFIED: model/scripts/sft/dataset_split/artifacts.py]

### Pattern 2: Single Prechecked Publication Flow

**What:** Refactor `publisher.py` so one orchestration path checks prefix absence once, performs audio preparation, and then uploads final text artifacts. [VERIFIED: model/scripts/sft/dataset_split/publisher.py]

**When to use:** Use in Plan 04-02/04-03 because uploaded audio creates objects under the same root prefix that the existing publisher currently checks for absence. [VERIFIED: model/scripts/sft/dataset_split/artifacts.py]

**Recommended seam:**

```python
# Source: existing publisher/artifact layout pattern
def publish_dataset_version_artifacts(...):
    layout = DatasetArtifactLayout.for_dataset_version(...)
    ensure_dataset_version_absent(storage_client, layout.root_uri)
    audio = prepare_audio_for_publication(
        storage_client,
        layout=layout,
        segments=tuple(segments),
    )
    validate_model_ready_segments(audio.segments)
    return upload_dataset_version_text_artifacts(
        storage_client,
        layout=layout,
        segments=audio.segments,
        audio_summary=audio.summary,
        ...
    )
```

Do not call the old prefix guard after uploading audio, because it will see the newly uploaded audio object. [VERIFIED: model/scripts/sft/dataset_split/artifacts.py]

### Pattern 3: GCS Binary Create-Only Upload

**What:** Mirror `upload_text_create_only()` with `upload_file_create_only()` using `Blob.upload_from_filename(..., if_generation_match=0)`. [VERIFIED: model/scripts/sft/dataset_split/artifacts.py] [CITED: https://github.com/googleapis/python-storage/blob/main/docs/storage/blob.md]

**When to use:** Use for copied, derived, and transcoded audio objects. [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md]

**Example:**

```python
# Source: Context7 /googleapis/python-storage docs for upload_from_filename
def upload_file_create_only(storage_client, uri, local_path, *, content_type):
    bucket_name, blob_path = parse_gcs_uri(uri)
    blob = storage_client.bucket(bucket_name).blob(blob_path)
    blob.upload_from_filename(
        str(local_path),
        content_type=content_type,
        if_generation_match=0,
    )
    return uri
```

Use the same `_is_precondition_failure()` behavior as text uploads so collisions produce `DatasetVersionExistsError`. [VERIFIED: model/scripts/sft/dataset_split/artifacts.py]

### Pattern 4: FFmpeg Probe And Execution Helpers

**What:** Use subprocess argument lists, not shell strings, for `ffprobe` and `ffmpeg`. [VERIFIED: backend/pipeline/common/audio.py]

**Probe command shape:**

```bash
ffprobe -v error -select_streams a:0 \
  -show_entries format=duration:stream=codec_name,channels,sample_rate \
  -of json INPUT
```

Local verification produced JSON with `format.duration`, `streams[0].codec_name`, `streams[0].sample_rate`, and `streams[0].channels`. [VERIFIED: local ffprobe 7.0.2 command run]

**Derived clip command shape:**

```bash
ffmpeg -hide_banner -y -ss OFFSET -t DURATION -i INPUT \
  -map 0:a:0 -vn -ac 1 -c:a flac OUTPUT.flac
```

FFmpeg documentation says input-side `-ss` seeks before the requested position and, when transcoding with default accurate seek enabled, decodes and discards the extra segment between the seek point and requested position. [CITED: https://ffmpeg.org/ffmpeg-all.html]

Omit `-ar` so Phase 4 does not explicitly resample by default; local verification on a 44.1 kHz stereo WAV produced a 44.1 kHz mono FLAC clip. [VERIFIED: local ffmpeg 7.0.2 command run]

### Anti-Patterns to Avoid

- **Uploading audio and then calling the current publisher unchanged:** The current publisher will re-check prefix absence and fail after audio has created the prefix. [VERIFIED: model/scripts/sft/dataset_split/publisher.py]
- **Letting writers fall back to `audio_uri`:** This violates D-13 and can emit long-source URIs instead of clipped model-ready audio. [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md]
- **Reusing production normalization helpers:** `AudioProcessor` performs VAD, bandpass filtering, and resampling/downmixing for normalization, which conflicts with Phase 4 minimum transformation. [VERIFIED: backend/pipeline/normalization/audio/audio_processor.py]
- **Embedding raw `source_group` in object filenames:** Source groups can contain separators such as Echo `area/name`; use a sanitized row identity plus a short hash instead. [VERIFIED: model/scripts/sft/dataset_split/source_keys.py]
- **Treating `model_ready_audio_uri=None` as safe:** The existing leakage gate ignores missing/blank model-ready URIs, so Phase 4 needs an explicit populated-value validator. [VERIFIED: model/scripts/sft/dataset_split/leakage.py]

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Audio duration/codec/channel parsing | Custom binary/audio parser | `ffprobe -of json` | Existing code already shells to ffprobe for duration, and FFmpeg handles many container/codec edge cases. [VERIFIED: backend/pipeline/common/audio.py] |
| Audio cutting/transcoding | Python sample slicing or production DSP path | Narrow `ffmpeg` subprocess helpers | Locked decisions require FFmpeg and no production VAD/filter path. [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md] |
| GCS object create-only semantics | Existence check followed by normal upload | `if_generation_match=0` | GCS client upload methods support generation preconditions; existing artifact tests already enforce this for text uploads. [CITED: https://github.com/googleapis/python-storage/blob/main/docs/storage/blob.md] [VERIFIED: model/scripts/sft/tests/test_dataset_artifacts.py] |
| External URL streaming | `Response.content` full-memory download | `requests.get(..., stream=True)` plus `iter_content()` | Requests docs recommend streaming iteration for large downloads. [CITED: https://github.com/psf/requests/blob/main/docs/user/quickstart.md] |
| Model writer supported-format drift | Duplicate extension/MIME checks in audio planner | Reuse or factor `infer_audio_mime_type()` | Current Gemini and model writer code only accepts FLAC and MP3. [VERIFIED: model/scripts/sft/dataset_split/model_writers.py] [VERIFIED: model/colabs/common/sft.py] |

**Key insight:** The complex part is not just audio cutting; it is keeping artifact publication immutable while avoiding final manifests that point at missing or wrong audio. [VERIFIED: model/scripts/sft/dataset_split/publisher.py] [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md]

## Common Pitfalls

### Pitfall 1: Prefix Guard Trips On Newly Uploaded Audio

**What goes wrong:** Audio uploads under `sft/{dataset_version_id}/audio/` make the dataset-version prefix exist, then unchanged `publish_dataset_version_artifacts()` fails its prefix absence check. [VERIFIED: model/scripts/sft/dataset_split/artifacts.py]

**Why it happens:** Existing Phase 3 publisher was text-only and checks root absence immediately before uploading text artifacts. [VERIFIED: model/scripts/sft/dataset_split/publisher.py]

**How to avoid:** Check root absence once before audio upload, then continue within the same publication flow without rechecking prefix absence. [VERIFIED: model/scripts/sft/dataset_split/publisher.py]

**Warning signs:** Tests need a fake client that asserts `list_blobs()` is called once before any audio/text upload. [VERIFIED: model/scripts/sft/tests/test_dataset_publisher.py]

### Pitfall 2: Writers Still Emit Original `audio_uri`

**What goes wrong:** NeMo/Whisper/Gemini can keep referencing long source files or external URLs after Phase 4 if they read `segment.audio_uri`. [VERIFIED: model/scripts/sft/dataset_split/model_writers.py]

**Why it happens:** Current writer rows use `segment.audio_uri` for `audio_filepath`, `audio_uri`, and Gemini `fileData.fileUri`. [VERIFIED: model/scripts/sft/dataset_split/model_writers.py]

**How to avoid:** Add `_require_model_ready_audio_uri(segment)` and use it in all writer outputs and MIME inference. [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md]

**Warning signs:** Writer tests still passing with `model_ready_audio_uri=None` means the boundary is not enforced. [VERIFIED: model/scripts/sft/tests/test_model_writers.py]

### Pitfall 3: Partial Audio Uploads Without Final Artifacts

**What goes wrong:** A failure after some audio uploads can leave a dataset-version prefix with no final manifests. [VERIFIED: model/scripts/sft/dataset_split/artifacts.py]

**Why it happens:** Phase 4 explicitly excludes resume/cleanup, and GCS uploads are per-object operations. [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md]

**How to avoid:** Probe, download, plan, and local-generate everything possible before first upload; upload with create-only preconditions; upload final manifests/model inputs only after every required audio object exists. [CITED: https://github.com/googleapis/python-storage/blob/main/docs/storage/blob.md]

**Warning signs:** A rerun with the same `dataset_version_id` fails root absence because partial audio exists. [VERIFIED: model/scripts/sft/dataset_split/artifacts.py]

### Pitfall 4: Minimum Transformation Violated By Hidden Resampling

**What goes wrong:** Audio gets resampled or filtered even when no target writer requires it. [VERIFIED: backend/pipeline/normalization/audio/audio_processor.py]

**Why it happens:** Production normalization resamples for VAD/SED and applies filters. [VERIFIED: backend/pipeline/normalization/audio/audio_processor.py]

**How to avoid:** Do not call `AudioProcessor`; do not pass `-ar` to FFmpeg by default; record source and output sample rates in metadata. [VERIFIED: local ffmpeg 7.0.2 command run]

**Warning signs:** Transformation metadata has `resampled=true` in a default Phase 4 run. [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md]

### Pitfall 5: Leakage Gate Runs Before Model-Ready URIs Exist

**What goes wrong:** Cross-split duplicate model-ready audio is missed if validation runs only before Phase 4 enrichment. [VERIFIED: model/scripts/sft/dataset_split/leakage.py]

**Why it happens:** `validate_split_leakage()` ignores `None` or blank `model_ready_audio_uri` values. [VERIFIED: model/scripts/sft/dataset_split/leakage.py]

**How to avoid:** Validate split integrity after audio enrichment and add a hard validator that every segment has a non-empty `gs://` `model_ready_audio_uri`. [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md]

**Warning signs:** Canonical manifests contain null `model_ready_audio_uri` after Phase 4. [VERIFIED: model/scripts/sft/dataset_split/canonical.py]

## Code Examples

Verified patterns from official and local sources:

### Probe Audio Metadata

```python
# Source: local ffprobe 7.0.2 verification and existing subprocess pattern.
def probe_audio(local_path: Path, runner=subprocess.run) -> AudioProbe:
    result = runner(
        [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "a:0",
            "-show_entries",
            "format=duration:stream=codec_name,channels,sample_rate",
            "-of",
            "json",
            str(local_path),
        ],
        capture_output=True,
        check=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    stream = payload["streams"][0]
    return AudioProbe(
        duration=float(payload["format"]["duration"]),
        codec_name=str(stream["codec_name"]),
        channels=int(stream["channels"]),
        sample_rate=int(stream["sample_rate"]),
    )
```

`backend/pipeline/common/audio.py` already uses `subprocess.run(..., capture_output=True, check=True)` for FFprobe duration. [VERIFIED: backend/pipeline/common/audio.py]

### Derive A Mono FLAC Clip Without Resampling

```python
# Source: Context7 FFmpeg docs for -ss and local ffmpeg 7.0.2 verification.
def derive_flac_clip(
    input_path: Path,
    output_path: Path,
    *,
    offset: float,
    duration: float,
    runner=subprocess.run,
) -> None:
    runner(
        [
            "ffmpeg",
            "-hide_banner",
            "-y",
            "-ss",
            f"{offset:.6f}",
            "-t",
            f"{duration:.6f}",
            "-i",
            str(input_path),
            "-map",
            "0:a:0",
            "-vn",
            "-ac",
            "1",
            "-c:a",
            "flac",
            str(output_path),
        ],
        capture_output=True,
        check=True,
        text=True,
    )
```

The absence of `-ar` is intentional because Phase 4 must not resample by default. [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md]

### Stream External URL To Scratch

```python
# Source: Context7 /psf/requests docs for stream=True and iter_content().
def download_external_url_to_scratch(url: str, local_path: Path) -> None:
    with requests.get(url, stream=True, timeout=(10, 120)) as response:
        response.raise_for_status()
        with local_path.open("wb") as output:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    output.write(chunk)
```

The implementation should treat custom authentication for external URLs as out of scope unless a manifest source already provides directly downloadable URLs. [ASSUMED]

### Require Model-Ready Audio In Writers

```python
# Source: locked D-13 plus current writer helper style.
def _require_model_ready_audio_uri(segment: LabeledSegment) -> str:
    uri = (segment.model_ready_audio_uri or "").strip()
    if not uri.startswith("gs://"):
        raise ModelWriterError(
            f"row_index={segment.row_index} missing model_ready_audio_uri"
        )
    return uri
```

Use this helper in `_nemo_row()`, `_whisper_row()`, and `build_gemini_inputs()` before MIME inference. [VERIFIED: model/scripts/sft/dataset_split/model_writers.py] [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md]

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Model writers read `segment.audio_uri`. [VERIFIED: model/scripts/sft/dataset_split/model_writers.py] | Phase 4 must make writers require `segment.model_ready_audio_uri`. [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md] | Phase 4 boundary, 2026-05-28 context. [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md] | Plans must update writer tests and report/canonical expectations. [VERIFIED: model/scripts/sft/tests/test_model_writers.py] |
| Publisher uploads only text artifacts after prefix absence check. [VERIFIED: model/scripts/sft/dataset_split/publisher.py] | Phase 4 publication must include binary audio before final text manifests. [VERIFIED: .planning/ROADMAP.md] | Phase 4. [VERIFIED: .planning/ROADMAP.md] | Plans need a single prechecked flow to avoid prefix guard self-conflict. [VERIFIED: model/scripts/sft/dataset_split/artifacts.py] |
| Canonical rows include null future provenance fields. [VERIFIED: model/scripts/sft/tests/test_dataset_split_normalize.py] | Canonical rows after Phase 4 must contain populated model-ready/provenance fields. [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md] | Phase 4. [VERIFIED: .planning/ROADMAP.md] | Reports and leakage validation must run on enriched rows. [VERIFIED: model/scripts/sft/dataset_split/reports.py] [VERIFIED: model/scripts/sft/dataset_split/leakage.py] |

**Deprecated/outdated:**

- Writer preprocessing recommendation `"preserve_original_uri_with_offset_duration"` should be revised after Phase 4 because Whisper should receive model-ready audio, while provenance keeps original URI and offsets. [VERIFIED: model/scripts/sft/dataset_split/model_writers.py]

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | External HTTPS source audio can be downloaded with plain `requests.get()` and no custom credentials. | Code Examples | Some source rows would fail download and need a credential/session hook. |

## Open Questions (RESOLVED)

1. **RESOLVED: Do any non-GCS source URLs require authentication?**
   - What we know: Phase 4 must fail fast on any row that cannot be downloaded. [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md]
   - What's unclear: The local code does not define a credential adapter for external audio URLs. [VERIFIED: model/scripts/sft/dataset_split]
   - Accepted assumption: Implement plain HTTPS download with timeout and clear row-context errors; authenticated download adapters are out of scope until real manifests require them. [RESOLVED]

2. **RESOLVED: Should copied external objects preserve source content type exactly?**
   - What we know: `copied` is byte-for-byte and must be a supported standalone format. [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md]
   - What's unclear: External sources may omit or misstate `Content-Type`. [ASSUMED]
   - Accepted assumption: Infer model MIME from the destination URI extension using the writer helper, and store observed HTTP content type separately in provenance. [RESOLVED]

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|-------------|-----------|---------|----------|
| Python | Unit tests and SFT tooling | YES | `python3 --version` returned 3.12.13; project root requires Python >=3.13,<3.14. [VERIFIED: local command] [VERIFIED: pyproject.toml] | Use `uv run` environment, which resolved project deps during local metadata check. [VERIFIED: local command] |
| `uv` | Dependency/test execution | YES | 0.11.2 [VERIFIED: local command] | None needed. |
| Node/npm | Context7 CLI docs lookup | YES | Node v22.22.2, npm 10.9.7 [VERIFIED: local command] | None needed. |
| `ffmpeg` | Clip derivation/transcoding | YES | 7.0.2-static [VERIFIED: local command] | Missing binary should skip subprocess fixture tests and block real generation. |
| `ffprobe` | Source/output probing | YES | 7.0.2-static [VERIFIED: local command] | Missing binary should skip subprocess fixture tests and block real generation. |
| Google Cloud SDK | Manual GCS/debug tooling | YES | gcloud 565.0.0 [VERIFIED: local command] | Not needed for unit tests because code uses `google-cloud-storage`. |
| `google-cloud-storage` | GCS helper/runtime | YES | 2.19.0 [VERIFIED: local command] | None needed; already installed in uv environment. |

**Missing dependencies with no fallback:**
- None for research and unit-test planning. [VERIFIED: local command]

**Missing dependencies with fallback:**
- Python command on PATH is 3.12.13 while root project declares 3.13; use `uv run`/project environment for implementation verification rather than bare `python3`. [VERIFIED: local command] [VERIFIED: pyproject.toml]

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | `unittest` for `model/scripts/sft/tests`; `pytest` 9.0.3 is also installed. [VERIFIED: model/scripts/sft/tests/test_model_writers.py] [VERIFIED: uv.lock] |
| Config file | Root `pyproject.toml` has pytest addopts; model common has separate `model/pyproject.toml`; SFT script tests currently self-manage import paths. [VERIFIED: pyproject.toml] [VERIFIED: model/pyproject.toml] |
| Quick run command | `uv run python -m unittest model.scripts.sft.tests.test_audio_derivation` [VERIFIED: model/scripts/sft/tests pattern] |
| Full suite command | `uv run python -m unittest discover model/scripts/sft/tests` [VERIFIED: model/scripts/sft/tests pattern] |

### Phase Requirements -> Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|--------------|
| AUD-01 | Supported standalone `gs://` clip is reused with no destination upload. [VERIFIED: .planning/REQUIREMENTS.md] | unit | `uv run python -m unittest model.scripts.sft.tests.test_audio_derivation` | NO, Wave 0 |
| AUD-02 | Longer source or positive offset derives a clipped span. [VERIFIED: .planning/REQUIREMENTS.md] | unit + subprocess fixture | `uv run python -m unittest model.scripts.sft.tests.test_audio_derivation` | NO, Wave 0 |
| AUD-03 | Generated output defaults to FLAC and supported MIME. [VERIFIED: .planning/REQUIREMENTS.md] | unit + writer integration | `uv run python -m unittest model.scripts.sft.tests.test_model_writers` | EXISTS, update in Wave 0 |
| AUD-04 | Derived/transcoded multichannel input becomes mono. [VERIFIED: .planning/REQUIREMENTS.md] | subprocess fixture | `uv run python -m unittest model.scripts.sft.tests.test_audio_derivation` | NO, Wave 0 |
| AUD-05 | Default command does not pad or pass `-ar`; output duration is checked. [VERIFIED: .planning/REQUIREMENTS.md] | subprocess fixture + mocked runner command assertion | `uv run python -m unittest model.scripts.sft.tests.test_audio_derivation` | NO, Wave 0 |
| AUD-06 | Canonical/report rows include provenance and action summary. [VERIFIED: .planning/REQUIREMENTS.md] | unit | `uv run python -m unittest model.scripts.sft.tests.test_dataset_reports model.scripts.sft.tests.test_dataset_canonical` | EXISTS, update in Wave 0 |

### Sampling Rate

- **Per task commit:** `uv run python -m unittest model.scripts.sft.tests.test_audio_derivation` plus the specific touched existing test module. [VERIFIED: model/scripts/sft/tests]
- **Per wave merge:** `uv run python -m unittest discover model/scripts/sft/tests`. [VERIFIED: model/scripts/sft/tests]
- **Phase gate:** Full SFT script test suite green before `$gsd-verify-work`. [VERIFIED: .planning/config.json]

### Wave 0 Gaps

- [ ] `model/scripts/sft/tests/test_audio_derivation.py` covers AUD-01 through AUD-05. [VERIFIED: .planning/REQUIREMENTS.md]
- [ ] `model/scripts/sft/tests/test_dataset_publisher.py` covers one prefix absence check before audio+text upload and no second prefix check after audio upload. [VERIFIED: model/scripts/sft/tests/test_dataset_publisher.py]
- [ ] `model/scripts/sft/tests/test_model_writers.py` updates writers to require `model_ready_audio_uri` and use it in output rows. [VERIFIED: model/scripts/sft/tests/test_model_writers.py]
- [ ] `model/scripts/sft/tests/test_dataset_reports.py` adds audio transformation summary coverage. [VERIFIED: model/scripts/sft/tests/test_dataset_reports.py]

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|------------------|
| V2 Authentication | No | Phase 4 does not introduce user authentication. [VERIFIED: .planning/ROADMAP.md] |
| V3 Session Management | No | Phase 4 is offline artifact tooling, not a session-bearing web workflow. [VERIFIED: .planning/ROADMAP.md] |
| V4 Access Control | Yes | Use Google ADC / existing GCS IAM through `google-cloud-storage`; do not add custom ACL logic. [VERIFIED: model/colabs/common/gcs_utils.py] |
| V5 Input Validation | Yes | Validate URI scheme, positive duration, offset bounds, supported output action, safe path parts, and non-empty `model_ready_audio_uri`. [VERIFIED: model/scripts/sft/dataset_split/config.py] [VERIFIED: model/scripts/sft/dataset_split/artifacts.py] |
| V6 Cryptography | No | Phase 4 does not create encryption or signing logic; rely on GCS client transport and IAM. [VERIFIED: model/colabs/common/gcs_utils.py] |

### Known Threat Patterns for This Stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Shell injection through source URI or filename | Tampering / Elevation | Use `subprocess.run()` argument lists and never `shell=True`. [VERIFIED: backend/pipeline/common/audio.py] |
| Artifact overwrite/race | Tampering | Use `if_generation_match=0` for binary and text object uploads. [CITED: https://github.com/googleapis/python-storage/blob/main/docs/storage/blob.md] |
| Path traversal in generated object names | Tampering | Reuse safe path validation or generate names from row IDs and hashes instead of raw source fields. [VERIFIED: model/scripts/sft/dataset_split/artifacts.py] |
| External URL hanging or memory exhaustion | Denial of Service | Use streamed downloads, bounded chunk sizes, and explicit timeouts. [CITED: https://github.com/psf/requests/blob/main/docs/user/quickstart.md] |
| Data leakage through model-ready duplicates | Information Disclosure | Re-run `validate_split_integrity()` after `model_ready_audio_uri` is populated. [VERIFIED: model/scripts/sft/dataset_split/leakage.py] |

## Sources

### Primary (HIGH confidence)

- `.planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md` - locked Phase 4 decisions, action semantics, provenance schema, and failure semantics.
- `.planning/REQUIREMENTS.md` - AUD-01 through AUD-06 and v1 requirement traceability.
- `.planning/ROADMAP.md` - Phase 4 plan list and success criteria.
- `AGENTS.md` - project instructions, Context7 requirement, stack/convention constraints.
- `model/scripts/sft/dataset_split/types.py` - `LabeledSegment` provenance fields.
- `model/scripts/sft/dataset_split/artifacts.py` - immutable dataset layout, audio prefix, create-only text upload.
- `model/scripts/sft/dataset_split/publisher.py` - current publication flow and prefix guard.
- `model/scripts/sft/dataset_split/model_writers.py` - current writer dependency on `audio_uri` and supported MIME inference.
- `model/scripts/sft/dataset_split/reports.py` - current dataset report structure.
- `model/scripts/sft/dataset_split/leakage.py` - current source/original/model-ready leakage gates.
- `model/colabs/common/gcs_utils.py` - existing GCS parse/download/upload helpers.
- `backend/pipeline/common/audio.py` - existing FFprobe subprocess pattern.
- `backend/pipeline/normalization/audio/audio_processor.py` - production normalization path to avoid.
- `model/colabs/common/sft.py` - Gemini SFT MIME validator.
- Context7 `/googleapis/python-storage` - `upload_from_filename`, `upload_from_string`, download, and conditional retry docs.
- Context7 `/websites/ffmpeg_ffmpeg-all` - `-ss` accurate seek behavior and stream mapping docs.
- Context7 `/psf/requests` - streaming response docs.
- Local commands - `ffmpeg`/`ffprobe` version and tiny WAV/FLAC probe verification.

### Secondary (MEDIUM confidence)

- `uv.lock` - installed package versions and package upload timestamps.
- `pyproject.toml` and `model/pyproject.toml` - Python versions, dependency constraints, Ruff/Pytest configuration.

### Tertiary (LOW confidence)

- Plain unauthenticated external URL download assumption for all non-GCS source rows. Marked as A1 in the Assumptions Log. [ASSUMED]

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - existing repo dependencies, local lockfile, local binary checks, and Context7 official docs agree. [VERIFIED: uv.lock] [CITED: https://github.com/googleapis/python-storage/blob/main/docs/storage/blob.md]
- Architecture: HIGH - current code clearly shows publisher/model writer/report seams and the prefix guard interaction. [VERIFIED: model/scripts/sft/dataset_split/publisher.py] [VERIFIED: model/scripts/sft/dataset_split/artifacts.py]
- Pitfalls: HIGH - each risk maps to an existing code path or locked Phase 4 decision. [VERIFIED: .planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md]

**Research date:** 2026-05-28
**Valid until:** 2026-06-27 for local architecture; re-check GCS/FFmpeg docs before implementation if dependencies move.

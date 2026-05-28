# Phase 4: Audio Derivation And Provenance - Context

**Gathered:** 2026-05-28
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 4 converts leak-safe, split-assigned `LabeledSegment` rows into model-ready audio examples. It decides whether each row can reuse an existing clip, needs to be copied into the dataset-version GCS tree, needs to be clipped from a longer source file, or needs whole-file transcoding for model compatibility. It writes the audio provenance needed for audit and updates the model-input boundary so NeMo, Whisper, Gemini, and normal eval consumers read `model_ready_audio_uri`.

It does not change Source Group split assignment, re-run balance optimization, submit SFT jobs, run normal eval jobs, or introduce force/resume cleanup behavior.

</domain>

<decisions>
## Implementation Decisions

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

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project And Prior Phase Context
- `.planning/PROJECT.md` — Milestone objective, glossary, and dataset-version terminology.
- `.planning/REQUIREMENTS.md` — Phase 4 requirements `AUD-01` through `AUD-06`.
- `.planning/ROADMAP.md` — Phase 4 scope, success criteria, and plan list.
- `.planning/STATE.md` — Current milestone state and active phase.
- `.planning/phases/01-manifest-and-source-identity/01-CONTEXT.md` — Source identity decisions and Echo ambiguity rules.
- `.planning/phases/02-split-engine-and-leakage-gates/02-CONTEXT.md` — Split-before-derivation rule, leakage gates, and balance scope.
- `.planning/phases/03-gcs-artifacts-and-model-writers/03-CONTEXT.md` — Dataset-version GCS layout, model writer contracts, and reserved `audio/` prefix.

### Dataset Split And Artifact Code
- `model/scripts/sft/dataset_split/types.py` — `LabeledSegment` fields including `model_ready_audio_uri`, `derived_audio_uri`, and `transformation_metadata`.
- `model/scripts/sft/dataset_split/normalize.py` — Current source row normalization and `audio_uri`/`original_audio_uri` behavior.
- `model/scripts/sft/dataset_split/source_keys.py` — Source Group extraction and source URI patterns, including `gs://`, HTTPS Broadcastify archive, Echo public S3 HTTPS, and Fire Notification URLs.
- `model/scripts/sft/dataset_split/leakage.py` — Existing hard gates over Source Group, original audio URI, model-ready URI, and duplicate labeled spans.
- `model/scripts/sft/dataset_split/canonical.py` — Canonical manifest fields that already preserve model-ready and transformation metadata.
- `model/scripts/sft/dataset_split/artifacts.py` — Dataset artifact layout and reserved `audio_prefix_uri`.
- `model/scripts/sft/dataset_split/publisher.py` — Current immutable publish path and model writer invocation point.
- `model/scripts/sft/dataset_split/model_writers.py` — NeMo, Whisper, and Gemini writers that must switch to requiring model-ready audio after Phase 4.

### Existing Audio And GCS Utilities
- `model/colabs/common/gcs_utils.py` — GCS URI parsing, download helper, upload helper, and blob existence behavior.
- `backend/pipeline/common/audio.py` — Existing `ffprobe` duration helper pattern.
- `backend/pipeline/normalization/audio/audio_processor.py` — Production normalization/VAD audio path; useful as a warning for what Phase 4 should not reuse directly.
- `model/colabs/common/sft.py` — Gemini audio SFT builder and MIME validator; currently supports `audio/flac` and `audio/mpeg`.
- `model/colabs/common/audio_utils.py` — Existing torchaudio preprocessing helper that resamples/downmixes for ASR inference, not a Phase 4 minimum-transformation fit.

### Tests And Contracts
- `model/scripts/sft/tests/test_dataset_canonical.py` — Canonical row expectations for model-ready/provenance fields.
- `model/scripts/sft/tests/test_model_writers.py` — Current writer output shapes and Gemini MIME expectations.
- `model/scripts/sft/tests/test_dataset_publisher.py` — Immutable artifact publication expectations.
- `model/scripts/sft/tests/test_dataset_split_leakage.py` — Existing model-ready URI leakage gate tests.

### External Documentation Checked During Discussion
- `https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini-use-supervised-tuning` — User-provided current Gemini Enterprise Agent Platform docs that list Gemini 3.1 Flash-Lite supervised tuning support.
- Context7 `/websites/cloud_google_vertex-ai` supervised tuning docs — `training_dataset_uri` is required and `validation_dataset_uri` is optional for supervised tuning.
- Context7 `/websites/cloud_google_vertex-ai` Gemini fileData docs — Gemini file data requires MIME type and supports GCS file URIs; snippets list `audio/mpeg`, `audio/mp3`, and `audio/wav` in generic fileData contexts.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `LabeledSegment`: Use as the single internal row type. Phase 4 should return new `LabeledSegment` values with model-ready/provenance fields populated.
- `DatasetArtifactLayout.audio_prefix_uri`: Use as the root for action-based audio folders.
- `common.gcs_utils.parse_gcs_uri` and `download_to_scratch`: Reuse for GCS download/path handling; extend or add create-only binary upload behavior as needed.
- `validate_split_integrity`: Re-run after model-ready fields are populated so `model_ready_audio_uri` leakage gates remain active.

### Established Patterns
- Model/SFT tooling lives under `model/scripts/sft/dataset_split/` with focused tests under `model/scripts/sft/tests/`.
- Input configs/manifests are GCS-first, but source audio URIs may be external URLs from existing dataset manifests.
- Existing generated artifacts are immutable and create-only; Phase 4 should preserve that behavior.
- Existing eval/SFT behavior favors fail-fast structural errors and report-only performance risks.

### Integration Points
- Add audio planning/derivation between split assignment and `publish_dataset_version_artifacts`.
- Update model writers to consume `model_ready_audio_uri` after the Phase 4 boundary.
- Extend dataset-version reports with audio action counts and transformation provenance summary.
- Ensure normal eval can consume derived clips by keeping split out of physical audio paths.

</code_context>

<specifics>
## Specific Ideas

- Duration match tolerance: `max(0.5 seconds, row_duration * 0.02)`.
- Source duration is measured with `ffprobe`; row duration comes from the normalized manifest row.
- Example action meanings:
  - `reused`: `model_ready_audio_uri == audio_uri`, no generated audio.
  - `copied`: standalone non-GCS supported source copied to `audio/copied/`.
  - `derived`: clipped span written to `audio/derived/`, and `derived_audio_uri == model_ready_audio_uri`.
  - `transcoded`: standalone unsupported source converted to FLAC in `audio/transcoded/`.
- Generated audio paths should be action-based instead of train/eval-based so eval tooling can reuse the same clips.

</specifics>

<deferred>
## Deferred Ideas

- Actual SFT job submission and normal eval execution remain outside Phase 4.
- Force/resume/cleanup support remains out of scope for this phase.
- Broad audio-content duplicate detection and fuzzy URI alias detection remain out of scope.
- WAV output for generated clips can be added later if the repo's Gemini MIME validation and target writer support `audio/wav`.

</deferred>

---

*Phase: 4-Audio Derivation And Provenance*
*Context gathered: 2026-05-28*

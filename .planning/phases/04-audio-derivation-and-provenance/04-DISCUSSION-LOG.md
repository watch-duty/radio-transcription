# Phase 4: Audio Derivation And Provenance - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-05-28
**Phase:** 04-Audio Derivation And Provenance
**Areas discussed:** reuse vs derive, unsupported standalone formats, provenance shape, model-ready URI boundary, non-GCS source materialization, audio path layout

---

## Reuse Vs Derive

| Option | Description | Selected |
|--------|-------------|----------|
| Family-specific derivation | Treat `bcfy_feeds` as the only source that requires segmentation and reuse all other families. | |
| Data-driven derivation | Probe source duration and derive whenever row offset/duration indicates a span inside longer audio, regardless of family. | ✓ |
| Defensive derive everything | Always generate one clip per row even when the source already appears standalone. | |

**User's choice:** Data-driven derivation.
**Notes:** The user first noted that only `bcfy_feeds` is expected to require segmentation, then clarified that if a row has the information needed for segmentation, it is fine to derive regardless of source. The locked rule is to hard fail when `duration <= 0`, probe source duration, reuse only when the source appears standalone within tolerance, and derive when `offset > 0` or source duration is meaningfully longer than row duration. The agreed tolerance is `max(0.5 seconds, row_duration * 0.02)`.

---

## Unsupported Standalone Formats

| Option | Description | Selected |
|--------|-------------|----------|
| Fail | Reject standalone files whose format is not accepted by model writers. | |
| Transcode full file | Convert unsupported standalone files to model-ready FLAC without clipping. | ✓ |
| Call it derived | Use the `derived` action for both clipped spans and full-file compatibility conversion. | |

**User's choice:** Transcode full file.
**Notes:** The action name is `transcoded`. It should not be treated as a clipped span, and `derived_audio_uri` remains null.

---

## Provenance Shape

| Option | Description | Selected |
|--------|-------------|----------|
| Use derived URI for any new audio | Populate `derived_audio_uri` for clipped, copied, and transcoded outputs. | |
| Use derived URI only for clipped spans | Keep `derived_audio_uri` specific to actual time-span derivation. | ✓ |

**User's choice:** Use `derived_audio_uri` only for clipped spans.
**Notes:** `model_ready_audio_uri` is always populated after Phase 4. `derived_audio_uri` is populated only for `derived` rows. `transformation_metadata.action` records `reused`, `copied`, `derived`, or `transcoded`.

---

## Model-Ready URI Boundary

| Option | Description | Selected |
|--------|-------------|----------|
| Fallback in writers | Writers use `model_ready_audio_uri` when present and otherwise fall back to `audio_uri`. | |
| Require after Phase 4 | Phase 4 is the boundary; model writers require `model_ready_audio_uri` after it. | ✓ |

**User's choice:** Require after Phase 4.
**Notes:** The user asked why `model_ready_audio_uri` could be missing. The clarification was that it may be missing before Phase 4 because Phase 1-3 do not derive/transcode audio, but it must not be missing after Phase 4.

---

## Non-GCS Source Materialization

| Option | Description | Selected |
|--------|-------------|----------|
| Allow external model-ready URIs | Let HTTPS/public S3 source URLs appear directly in model inputs when standalone. | |
| Require GCS model-ready URIs | Copy or generate every non-GCS source into the dataset-version GCS audio tree. | ✓ |

**User's choice:** Require GCS model-ready URIs.
**Notes:** The new action `copied` means a supported standalone non-GCS source was copied byte-for-byte into GCS without audio transformation.

---

## Audio Path Layout

| Option | Description | Selected |
|--------|-------------|----------|
| Split-based folders | Write generated audio under `audio/{action}/{train,eval}/...`. | |
| Action-based folders | Write generated audio under `audio/copied/`, `audio/derived/`, and `audio/transcoded/`, with split recorded in manifests/provenance. | ✓ |

**User's choice:** Action-based folders.
**Notes:** The user noted that derived clips can also be used for normal eval runs, so split should not be embedded in the primary audio path.

---

## the agent's Discretion

- Choose exact helper/module names.
- Choose deterministic generated audio filename strategy.
- Choose ffmpeg command flags consistent with no padding/no resampling by default.
- Choose the exact report grouping for audio action counts and transformation summaries.

## Deferred Ideas

- WAV output fallback is deferred until local Gemini MIME validation/model writer support `audio/wav`.
- Force/resume/cleanup behavior remains deferred.
- Actual SFT and normal eval execution remains outside this phase.

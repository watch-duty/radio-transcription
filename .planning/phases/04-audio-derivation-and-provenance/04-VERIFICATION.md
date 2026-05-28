---
phase: 04-audio-derivation-and-provenance
verified: 2026-05-28T05:18:43Z
status: passed
score: "25/25 must-haves verified"
overrides_applied: 0
---

# Phase 4: Audio Derivation And Provenance Verification Report

**Phase Goal:** Users can reuse standalone clips or derive clips from longer labeled audio while preserving minimal transformation and auditable provenance.
**Verified:** 2026-05-28T05:18:43Z
**Status:** passed
**Re-verification:** No - initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Rows already pointing to standalone clips are reused without unnecessary audio transformation. | VERIFIED | `plan_audio_actions()` stages/probes first, then selects `reused` for supported standalone `gs://` audio with `destination_uri=None`; reused materialization sets `model_ready_audio_uri` to source URI and creates no upload task (`audio.py:312-347`, `audio.py:465-470`). Tests cover `test_supported_standalone_gcs_clip_is_reused` and no upload for reused rows. |
| 2 | Rows pointing into longer files produce derived clips from offset/duration. | VERIFIED | Positive offset or non-standalone source selects `derived`; derived command uses `-ss` and `-t` with row offset/duration and writes `.flac` under `audio/derived/` (`audio.py:362-376`, `audio.py:201-226`). |
| 3 | Derived clips are mono when needed, unpadded, and not resampled by default. | VERIFIED | Derived/transcoded ffmpeg argv includes `-ac 1 -c:a flac`, no `-ar`, no padding filters; metadata records `mixed_to_mono`, `resampled`, and `padded=False` (`audio.py:201-247`, `audio.py:626-632`). Tests assert no `-ar`, `apad`, or `adelay`. |
| 4 | Every example records original audio, offset, duration, source group, split, and transformation provenance. | VERIFIED | `prepare_audio_for_publication()` enriches frozen `LabeledSegment` values via `replace()` with `model_ready_audio_uri`, optional `derived_audio_uri`, and transformation metadata containing original/source URI, offset, duration, source/output format/channels, split, source_group, and command/version summaries (`audio.py:520-525`, `audio.py:606-639`). |
| 5 | Audio action planning probes source audio and rejects duration <= 0 before selecting an action. | VERIFIED | `probe_audio()` parses ffprobe JSON and rejects non-finite or <=0 source duration; `_plan_audio_action()` validates row span, stages source, probes, then selects action (`audio.py:103-142`, `audio.py:320-331`). |
| 6 | Supported standalone `gs://` clips are reused without upload or transformation. | VERIFIED | Reused plans have no destination, local materialization uses source URI, and upload tasks are only created for local results with `upload_uri` (`audio.py:333-347`, `audio.py:427-441`, `audio.py:465-470`). |
| 7 | Longer source spans are clipped only after offset+duration is within probed source duration tolerance. | VERIFIED | `_validate_source_bounds()` checks `offset + duration <= source_duration + max(0.5, duration * 0.02)` before derived action materialization (`audio.py:328`, `audio.py:703-715`). |
| 8 | Actions are exactly reused, copied, derived, or transcoded; copied is reserved for supported standalone non-GCS sources. | VERIFIED | `AUDIO_ACTIONS` is the exact four-action tuple; copied branch requires supported standalone source that did not match reusable `gs://`; unsupported standalone sources transcode (`audio.py:31`, `audio.py:330-390`). |
| 9 | Generated audio uses narrow ffprobe/ffmpeg helpers, FLAC output, mono for derived/transcoded, no padding, and no default resampling. | VERIFIED | `probe_audio()`, `derive_audio_clip()`, and `transcode_audio_file()` use argv lists with timeouts; generated suffix is `.flac`; no `shell=True` appears in Phase 04 source; no production normalization/VAD imports are present (`audio.py:31-41`, `audio.py:103-142`, `audio.py:201-247`). |
| 10 | Copied, derived, and transcoded audio paths live under action folders below `layout.audio_prefix_uri`, never split folders. | VERIFIED | `audio_object_uri()` accepts only copied/derived/transcoded actions and joins `layout.audio_prefix_uri/{action}/{safe-name}`; object names use dataset/row/hash and do not include split (`artifacts.py:166-176`, `artifacts.py:240-264`). |
| 11 | Enriched segments populate `model_ready_audio_uri`, `derived_audio_uri` only for derived clips, and auditable transformation metadata. | VERIFIED | `_materialize_plan_locally()` sets `derived_audio_uri = model_ready_uri` only for `action == "derived"` and otherwise `None`; metadata includes action and all D-26 fields plus command/version summaries (`audio.py:507-525`, `audio.py:606-639`). |
| 12 | Failed probe, download, clip, transcode, copy, upload, or validation aborts before final manifest publication. | VERIFIED | Audio failures raise `AudioDerivationError`; publisher calls audio preparation with deferred upload, builds/serializes all final text payloads, then uploads audio before text artifacts. Tests verify generated duration failure and text planning failure do not upload audio/text (`audio.py:134-141`, `audio.py:567-597`, `publisher.py:131-215`). |
| 13 | NeMo, Whisper, and Gemini writers require non-empty `gs://` `model_ready_audio_uri` and never fall back to `audio_uri`. | VERIFIED | `_require_model_ready_audio_uri()` hard fails unless URI starts with `gs://`; NeMo, Whisper, Gemini all call it for emitted audio URI and MIME inference. `rg` found no `segment.audio_uri` references in `model_writers.py` (`model_writers.py:223-232`, `model_writers.py:256-283`, `model_writers.py:331-337`). |
| 14 | Canonical/model publication preserves original/source URI fields and model-ready URI fields for audit. | VERIFIED | Canonical rows include `audio_uri`, `original_audio_uri`, `model_ready_audio_uri`, `derived_audio_uri`, and `transformation_metadata`; publisher builds canonical, model, metadata, and reports from `audio_result.segments` (`canonical.py:23-40`, `publisher.py:138-189`). |
| 15 | Whisper duration issues remain structured warnings, not hard failures. | VERIFIED | Whisper rows are emitted, and durations over 30 seconds append `WriterWarning` with `severity="warning"` while returning normal `ModelWriterResult` (`model_writers.py:117-157`). |
| 16 | Publisher prepares and validates audio before generating canonical, model, and report text artifacts. | VERIFIED | Publisher checks root once, calls `audio_preparer(... upload=False)`, then builds canonical/per-dataset/model/report payloads from enriched segments (`publisher.py:129-201`). |
| 17 | Publisher keeps the Phase 3 no force, no partial resume, and no cleanup boundary. | VERIFIED | `publish_dataset_version_artifacts()` signature exposes no force/overwrite/resume/cleanup/delete options; tests assert forbidden keyword arguments raise before list/upload side effects (`publisher.py:80-99`, `test_dataset_publisher.py:511-540`). |
| 18 | Publication uses enriched segments from the audio boundary for every downstream artifact. | VERIFIED | `enriched_segments = tuple(audio_result.segments)` feeds canonical manifests, per-dataset manifests, NeMo, Whisper, Gemini, metadata, report, and markdown (`publisher.py:138-201`). |
| 19 | Canonical publication requires every SFT example to have non-empty `gs://` model-ready audio. | VERIFIED | `validate_model_ready_audio()` requires `model_ready_audio_uri` to be non-empty `gs://`; canonical rows/manifests call it after split integrity (`leakage.py:86-96`, `canonical.py:44-76`). |
| 20 | Canonical rows keep both original/source URI fields and model-ready URI fields. | VERIFIED | `canonical_row()` serializes `audio_uri`, `original_audio_uri`, `model_ready_audio_uri`, and `derived_audio_uri` (`canonical.py:23-40`). |
| 21 | `derived_audio_uri` is populated only for actual derived clipped spans. | VERIFIED | Preparation and validation enforce derived-only `derived_audio_uri`; non-derived actions must be blank (`audio.py:511-524`, `leakage.py:159-181`). |
| 22 | `transformation_metadata.action` is one of reused, copied, derived, or transcoded. | VERIFIED | `validate_model_ready_audio()` requires action and checks membership in `AUDIO_ACTIONS`; reports also reject unknown actions (`leakage.py:7-8`, `leakage.py:107-121`, `reports.py:412-424`). |
| 23 | `transformation_metadata` contains every required provenance field before canonical JSONL serialization. | VERIFIED | `validate_model_ready_audio()` checks all D-26 keys before canonical builders serialize rows (`leakage.py:9-27`, `leakage.py:132-157`, `canonical.py:48-76`). |
| 24 | Split integrity and model-ready validation run after audio enrichment so model-ready URI leakage is checked. | VERIFIED | Publisher feeds `enriched_segments` into canonical builders and model writers; canonical builders call `validate_split_integrity()` then `validate_model_ready_audio()` (`publisher.py:138-150`, `canonical.py:48-76`). |
| 25 | Reports expose model-ready audio, action counts, metadata coverage, command/version summary coverage, and fail on missing report provenance before upload. | VERIFIED | Report builder computes `audio_transformation_summary`, validates model-ready URI, metadata mapping, action vocabulary, and D-26 keys, includes Markdown `Audio Transformations`, and publisher renders reports before uploading audio/text (`reports.py:107-178`, `reports.py:253-308`, `publisher.py:180-215`). |

**Score:** 25/25 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `model/scripts/sft/dataset_split/audio.py` | Audio probe, action planning, materialization, upload orchestration, segment enrichment | VERIFIED | Exists and substantive; implements public audio contracts, safety checks, action planning, deferred upload support, metadata enrichment. |
| `model/scripts/sft/dataset_split/artifacts.py` | Create-only binary upload and action-based audio URI helpers | VERIFIED | `upload_file_create_only()` uses `upload_from_filename(... if_generation_match=0)` and maps precondition failures; `audio_object_uri()` uses safe action folders. |
| `model/scripts/sft/dataset_split/model_writers.py` | Model writer hard gate for model-ready audio | VERIFIED | Writers require `model_ready_audio_uri`; no writer fallback to `segment.audio_uri`. |
| `model/scripts/sft/dataset_split/publisher.py` | Single prechecked audio plus text publication flow | VERIFIED | One root absence check; audio prepared before payload generation; audio upload before text publication; no force/resume/cleanup controls. |
| `model/scripts/sft/dataset_split/leakage.py` | Post-audio model-ready audio and provenance validator | VERIFIED | Validates model-ready URI, action vocabulary, D-24 derived URI semantics, D-26 keys, metadata matches, and cross-split model-ready leakage. |
| `model/scripts/sft/dataset_split/canonical.py` | Canonical JSONL hard gate for enriched rows | VERIFIED | Preserves original/source/model-ready/provenance fields and validates before serialization. |
| `model/scripts/sft/dataset_split/reports.py` | JSON/Markdown audio transformation summary | VERIFIED | Includes action counts, URI counts, metadata coverage, and command summary coverage. |
| Phase 04 test files | Focused regression coverage | VERIFIED | Audio, artifact, publisher, writer, leakage, canonical, and report tests exist and passed in focused and full suite runs. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `audio.py` | `artifacts.py` | `audio_object_uri()` and `upload_file_create_only()` | VERIFIED | Imported at `audio.py:18-21`; used for destination URI planning and uploads (`audio.py:353-382`, `audio.py:590-595`). The SDK regex missed this because calls are on separate lines. |
| `audio.py` | `types.py` | `dataclasses.replace()` enrichment of `LabeledSegment` | VERIFIED | Imports `LabeledSegment` and uses `replace(segment, model_ready_audio_uri=..., derived_audio_uri=..., transformation_metadata=...)` (`audio.py:3`, `audio.py:24`, `audio.py:520-525`). |
| `audio.py` | `model_writers.py` | `infer_audio_mime_type()` supported-format contract | VERIFIED | Imported and used for source support and upload content types (`audio.py:23`, `audio.py:439`, `audio.py:793-798`). |
| `publisher.py` | `audio.py` | `prepare_audio_for_publication()` before artifact generation | VERIFIED | Default injection and call with `upload=False` precede canonical/model/report generation (`publisher.py:97`, `publisher.py:131-201`). |
| `publisher.py` | `artifacts.py` | One `ensure_dataset_version_absent()` call before writes | VERIFIED | Exactly one call site, before audio preparation (`publisher.py:129`). |
| `model_writers.py` | `types.py` | `model_ready_audio_uri` on `LabeledSegment` | VERIFIED | Writers call `_require_model_ready_audio_uri(segment)` before emitting model input rows. |
| `canonical.py` | `leakage.py` | `validate_split_integrity()` followed by `validate_model_ready_audio()` | VERIFIED | Import and call order verified in canonical rows/manifests/per-dataset paths (`canonical.py:6-9`, `canonical.py:48-76`). The SDK regex missed this because calls are on adjacent lines. |
| `reports.py` | `types.py` | `LabeledSegment.transformation_metadata` | VERIFIED | Report summary reads and validates `transformation_metadata` for every segment (`reports.py:253-308`, `reports.py:400-424`). |
| `publisher.py` | `reports.py` | `build_dataset_version_report()` with enriched segments | VERIFIED | Publisher passes `enriched_segments` into report generation before upload (`publisher.py:180-189`). |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|--------------------|--------|
| `audio.py` | `AudioPreparationResult.segments` | `plan_audio_actions()` plus `_materialize_plan_locally()` | Yes - staged/probed source audio drives action selection, FFmpeg/copy/reuse behavior, and `replace()` enrichment. | VERIFIED |
| `publisher.py` | `enriched_segments` | `audio_result.segments` from injected/default audio preparer | Yes - same tuple feeds canonical, per-dataset, NeMo, Whisper, Gemini, metadata, reports, and markdown. | VERIFIED |
| `canonical.py` | canonical rows/manifests | `LabeledSegment` values after `validate_model_ready_audio()` | Yes - source/model-ready/provenance fields are serialized from enriched rows. | VERIFIED |
| `reports.py` | `audio_transformation_summary` | `transformation_metadata` and URI fields on enriched rows | Yes - action counts, URI counts, metadata coverage, and command summary counts are computed from segment fields. | VERIFIED |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Phase 04 source compiles | `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model python -m py_compile model/scripts/sft/dataset_split/audio.py model/scripts/sft/dataset_split/publisher.py model/scripts/sft/dataset_split/leakage.py model/scripts/sft/dataset_split/artifacts.py model/scripts/sft/dataset_split/canonical.py model/scripts/sft/dataset_split/model_writers.py model/scripts/sft/dataset_split/reports.py` | Exit 0 | PASS |
| Latest code-review fixes remain covered | `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model python -m unittest model.scripts.sft.tests.test_audio_derivation model.scripts.sft.tests.test_dataset_publisher model.scripts.sft.tests.test_dataset_split_leakage` | Ran 55 tests, OK | PASS |
| Writers, canonical validation, reports, and artifacts pass focused tests | `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model python -m unittest model.scripts.sft.tests.test_model_writers model.scripts.sft.tests.test_dataset_canonical model.scripts.sft.tests.test_dataset_reports model.scripts.sft.tests.test_dataset_artifacts` | Ran 37 tests, OK | PASS |
| Full SFT script regression suite | `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model python -m unittest discover model/scripts/sft/tests` | Ran 176 tests, OK | PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-------------|-------------|--------|----------|
| AUD-01 | 04-01, 04-02 | Reuse existing standalone supported clip when row points to one utterance clip. | SATISFIED | `reused` action for supported standalone `gs://`; no destination/upload/ffmpeg. |
| AUD-02 | 04-01 | Derive a clip only when row points into longer source audio by offset/duration. | SATISFIED | Non-standalone spans select `derived`; source bounds checked before clipping. |
| AUD-03 | 04-01 | Preserve least-transforming reliable accepted format. | SATISFIED | Generated outputs are FLAC, target-writer-supported, with no default resampling or padding. |
| AUD-04 | 04-01 | Multichannel input is mixed to mono when deriving clips. | SATISFIED | Derived/transcoded ffmpeg argv includes `-ac 1`; metadata captures channel changes. |
| AUD-05 | 04-01 | Generator does not add padding and does not resample by default. | SATISFIED | No padding flags or `-ar`; metadata sets `padded=False` and records `resampled` from probes. |
| AUD-06 | 04-01 through 04-04 | Every SFT example records provenance for original audio URI, offset, duration, source group, split, decision, and transformation metadata. | SATISFIED | Audio enrichment, canonical validation, model writer hard gates, and reports all consume enriched provenance. |

No orphaned Phase 4 requirements were found in `.planning/REQUIREMENTS.md`; Phase 4 maps to AUD-01 through AUD-06.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| None | - | - | - | No TODO/FIXME/placeholders, empty implementations, console logging, `shell=True`, production normalization/VAD imports, or raw subprocess stdout/stderr report fields were found in Phase 04 source. Optional `None` fields and empty accumulators are legitimate dataclass/default collection patterns. |

### Human Verification Required

None. This phase is a code-level dataset publication boundary and was verified with source inspection plus focused and full automated tests. Live CLI/GCS end-to-end verification is explicitly Phase 5 roadmap scope.

### Gaps Summary

No blocking gaps found. The Phase 04 goal is achieved in the current codebase: audio is planned from real probes, model-ready audio is enriched with provenance, downstream artifacts require enriched rows, reports summarize transformations, and the latest code-review fixes are present and tested.

---

_Verified: 2026-05-28T05:18:43Z_
_Verifier: the agent (gsd-verifier)_

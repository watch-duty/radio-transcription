---
phase: 04-audio-derivation-and-provenance
plan: 01
subsystem: model-artifacts
tags: [python, audio, ffmpeg, gcs, dataset-versioning, tdd]

# Dependency graph
requires:
  - phase: 03-gcs-artifacts-and-model-writers
    provides: Immutable dataset-version layout, model writer MIME support, and create-only artifact publication
provides:
  - Phase 4 audio action planning for reused, copied, derived, and transcoded sources
  - Binary create-only GCS upload helper and safe action-based audio object URI helper
  - Audio materialization that enriches LabeledSegment rows with model-ready GCS URIs and provenance
affects: [04-audio-derivation-and-provenance, 05-cli-reports-docs-and-verification]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - Narrow ffprobe/ffmpeg subprocess helpers with argv lists
    - Frozen LabeledSegment enrichment through dataclasses.replace
    - Action-based audio object paths below DatasetArtifactLayout.audio_prefix_uri
    - Streamed external source downloads with timeout, chunk, and byte controls

key-files:
  created:
    - model/scripts/sft/dataset_split/audio.py
    - model/scripts/sft/tests/test_audio_derivation.py
  modified:
    - model/scripts/sft/dataset_split/artifacts.py
    - model/scripts/sft/tests/test_dataset_artifacts.py

key-decisions:
  - "Generated and copied audio objects are addressed by action folder, stable row identity, and short hash; raw source URI, raw source_group, and split are not embedded in object paths."
  - "Derived and transcoded outputs use FLAC with -ac 1 and no explicit resampling or padding flags."
  - "prepare_audio_for_publication() returns new frozen LabeledSegment values and never mutates source rows."

patterns-established:
  - "Audio preparation is a pure offline boundary that stages/probes first, then materializes and uploads only after action planning succeeds."
  - "Transformation metadata records source/output probe fields, action, split, source_group, and concise command/version summaries."

requirements-completed: [AUD-01, AUD-02, AUD-03, AUD-04, AUD-05, AUD-06]

# Metrics
duration: 13min
completed: 2026-05-28
---

# Phase 04 Plan 01: Audio Preparation Boundary Summary

**FFmpeg-backed audio planning and provenance enrichment for reusable, copied, derived, and transcoded SFT clips**

## Performance

- **Duration:** 13 min
- **Started:** 2026-05-28T03:44:24Z
- **Completed:** 2026-05-28T03:56:59Z
- **Tasks:** 3
- **Files modified:** 4

## Accomplishments

- Added `dataset_split.audio` with source staging, ffprobe metadata parsing, action planning, FFmpeg clip/transcode helpers, copied-file handling, create-only uploads, and provenance enrichment.
- Extended artifact helpers with `audio_object_uri()` and `upload_file_create_only()`.
- Added no-network Wave 0 tests for action selection, command shape, streaming download controls, binary upload preconditions, action paths, and transformation metadata.
- Verified the full SFT script suite after implementation.

## TDD Gate Compliance

- **RED:** `8446b30` added audio derivation and binary upload tests. The focused unittest run failed as expected with missing `dataset_split.audio`, `audio_object_uri()`, and `upload_file_create_only()` imports.
- **GREEN 1:** `f7c1cc8` implemented binary artifact helpers plus staging/probing/action planning. Focused unittest passed with `23 tests`, `2 skipped` for Task 3 materialization functions.
- **GREEN 2:** `184bb4f` implemented materialization and provenance enrichment. Focused unittest passed with `26 tests`, `0 skipped`.
- **REFACTOR:** No separate refactor commit was needed.

## Task Commits

Each task was committed atomically:

1. **Task 1: Write Wave 0 audio and binary-upload tests** - `8446b30` (test)
2. **Task 2: Implement artifact binary helpers and audio action planning** - `f7c1cc8` (feat)
3. **Task 3: Materialize audio and enrich segments with provenance** - `184bb4f` (feat)

**Plan metadata:** committed separately after this summary.

## Files Created/Modified

- `model/scripts/sft/dataset_split/audio.py` - Audio probe, staging, action planning, FFmpeg materialization, create-only upload orchestration, and enriched segment results.
- `model/scripts/sft/dataset_split/artifacts.py` - Binary create-only upload helper and action-based audio object URI generation.
- `model/scripts/sft/tests/test_audio_derivation.py` - Unit tests for action planning, command argv safety, external staging controls, materialization, and provenance.
- `model/scripts/sft/tests/test_dataset_artifacts.py` - Unit tests for binary upload preconditions and audio object paths.

## Decisions Made

- Used existing writer MIME inference to decide whether source clips are supported for reuse/copy.
- Kept `copied` as a byte-copy path with no FFmpeg invocation and no channel changes.
- Recorded command summaries and first-line program versions rather than subprocess stdout/stderr.

## Verification

- `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model python -m py_compile model/scripts/sft/tests/test_audio_derivation.py model/scripts/sft/tests/test_dataset_artifacts.py` - passed.
- `rg -n "class TestAudioActionPlanning" model/scripts/sft/tests/test_audio_derivation.py` - passed.
- `rg -n "test_upload_file_create_only_uses_generation_precondition" model/scripts/sft/tests/test_dataset_artifacts.py` - passed.
- RED focused unittest before implementation - failed as expected with missing audio/helper contracts.
- `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model python -m unittest model.scripts.sft.tests.test_audio_derivation model.scripts.sft.tests.test_dataset_artifacts` - passed, `26 tests`.
- `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model python -m unittest discover model/scripts/sft/tests` - passed, `142 tests`.
- Acceptance scans for public contracts, metadata keys, create-only upload, no `shell=True`, no FFmpeg `"-ar"` construction, and no production normalization/VAD imports all passed.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Added negative-offset validation**
- **Found during:** Task 2 (Implement artifact binary helpers and audio action planning)
- **Issue:** The plan required positive duration and offset+duration bounds checks but did not explicitly reject negative offsets, which would make FFmpeg clipping semantics unsafe.
- **Fix:** `plan_audio_actions()` now fails before staging/probing when `segment.offset < 0`, with row context.
- **Files modified:** `model/scripts/sft/dataset_split/audio.py`
- **Verification:** Focused unittest and full SFT script suite passed.
- **Committed in:** `f7c1cc8`

---

**Total deviations:** 1 auto-fixed (Rule 2).
**Impact on plan:** The change is a narrow correctness guard and does not alter the planned action vocabulary or artifact layout.

## Issues Encountered

- The GSD commit helper could not stage this ignored `.planning/` summary file because the repository ignores `.planning/`. The final metadata commit used explicit `git add -f` for this summary plus normal staging for tracked planning state files.

## Known Stubs

None. Stub scan matches were limited to intentional test fake collections, optional `None` fields for dataclass contracts, and runtime metadata values such as `ffmpeg_summary = None` for reused/copied actions.

## Threat Flags

None. New subprocess, external-download, object-path, and GCS-upload surfaces were already covered by the plan threat model T-04-01 through T-04-06.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Plan 04-02 can wire `prepare_audio_for_publication()` into the publisher before model writer generation, and can switch model writers to require `model_ready_audio_uri`.

## Self-Check: PASSED

- Found `model/scripts/sft/dataset_split/audio.py`.
- Found `model/scripts/sft/tests/test_audio_derivation.py`.
- Found `.planning/phases/04-audio-derivation-and-provenance/04-01-SUMMARY.md`.
- Found task commit `8446b30`.
- Found task commit `f7c1cc8`.
- Found task commit `184bb4f`.

---
*Phase: 04-audio-derivation-and-provenance*
*Completed: 2026-05-28*

---
phase: 03-gcs-artifacts-and-model-writers
plan: 03
subsystem: model-artifacts
tags: [python, dataset-versioning, nemo, whisper, tdd]

# Dependency graph
requires:
  - phase: 02-split-engine-and-leakage-gates
    provides: Leak-safe split-populated LabeledSegment rows
  - phase: 03-gcs-artifacts-and-model-writers
    provides: Canonical/report patterns and dataset-version artifact layout
provides:
  - Shared model-writer result and warning contracts
  - NeMo JSONL row builder and config fragment
  - Whisper JSONL row builder and structured duration warnings
affects: [03-gcs-artifacts-and-model-writers, 04-audio-derivation-and-provenance]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - Pure LabeledSegment to model-input row transforms
    - Split-integrity validation before model-input serialization
    - Structured writer warnings for dataset-version reports

key-files:
  created:
    - model/scripts/sft/dataset_split/model_writers.py
    - model/scripts/sft/tests/test_model_writers.py
  modified: []

key-decisions:
  - "ModelWriterResult centralizes JSONL serialization and grouped warning output for NeMo and Whisper writers."
  - "NeMo output is a minimal JSONL/config data fragment and never imports NeMo, submits jobs, downloads audio, or uploads artifacts."
  - "Whisper examples longer than 30 seconds remain in output and are reported as structured warnings."
  - "Model writers enumerate allowed fields and never copy raw_row or arbitrary source payload keys."

patterns-established:
  - "build_nemo_inputs() and build_whisper_inputs() call validate_split_integrity() before generating rows."
  - "WriterWarning.to_dict() is the report-facing warning serialization contract."
  - "Model writers preserve original audio URI plus offset/duration for Phase 4 clip derivation."

requirements-completed: [MODL-01, MODL-02, MODL-03, MODL-04, MODL-08, TEST-05]

# Metrics
duration: 9min
completed: 2026-05-28
---

# Phase 03 Plan 03: NeMo And Whisper Model Writer Summary

**Validated NeMo and Whisper dataset-version input builders that preserve original audio spans and report Whisper duration risks**

## Performance

- **Duration:** 9 min
- **Started:** 2026-05-28T00:54:00Z
- **Completed:** 2026-05-28T01:03:00Z
- **Tasks:** 3
- **Files modified:** 2

## Accomplishments

- Added `ModelWriterResult` and `WriterWarning` contracts with deterministic JSONL serialization and grouped warning serialization.
- Added NeMo row generation with `audio_filepath`, `text`, `duration`, `offset`, `example_id`, `segment_id`, plus a train/validation manifest config fragment.
- Added Whisper row generation preserving source/split metadata and preprocessing guidance while keeping over-30-second examples as reportable warnings.
- Added focused no-network tests for NeMo, Whisper, duration warnings, and benchmark/eval manifest isolation.

## TDD Gate Compliance

- **RED:** `f36a98e` added `test_model_writers.py`; focused pytest failed during collection with `ModuleNotFoundError: No module named 'dataset_split.model_writers'`, while `py_compile` passed.
- **GREEN 1:** `233a8e0` added shared writer contracts and NeMo generation; `pytest model/scripts/sft/tests/test_model_writers.py::TestNemoWriter -q` passed with `2 passed`.
- **GREEN 2:** `0d9919e` added Whisper generation and warnings; `pytest model/scripts/sft/tests/test_model_writers.py -q` passed with `5 passed`.
- **REFACTOR:** Ruff formatting was applied before the Task 3 commit; no separate refactor commit was needed.

## Task Commits

Each task was committed atomically:

1. **Task 1: Wave 0 scaffold NeMo and Whisper writer tests** - `f36a98e` (test)
2. **Task 2: Implement shared writer contracts and NeMo writer** - `233a8e0` (feat)
3. **Task 3: Implement Whisper writer and warning reporting** - `0d9919e` (feat)

**Plan metadata:** committed separately after this summary.

## Files Created/Modified

- `model/scripts/sft/dataset_split/model_writers.py` - Shared writer errors/results/warnings, NeMo row/config builder, Whisper row builder, duration warning reporting, and JSONL serialization.
- `model/scripts/sft/tests/test_model_writers.py` - In-memory contract tests for NeMo output shape/config, Whisper metadata/preprocessing, over-30-second warnings, and historical manifest path isolation.

## Decisions Made

- Kept writers as pure in-memory builders; GCS layout and create-only publication remain owned by other Phase 3 modules.
- Used explicit allow-list row construction for both writers instead of dataclass serialization, preventing raw source payload leakage.
- Returned Whisper duration warnings as structured data rather than logs so Plan 03-02 reports can include them.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Kept Task 2 verification scoped to NeMo**
- **Found during:** Task 2
- **Issue:** The initial RED test grouping placed the cross-writer benchmark/eval safety test under `TestNemoWriter`, which would have forced Whisper behavior before the plan's Task 3.
- **Fix:** Moved the cross-writer safety test to `TestWriterSafety` and kept the shared module importable during the NeMo-only slice; Task 3 then implemented the full Whisper behavior.
- **Files modified:** `model/scripts/sft/tests/test_model_writers.py`, `model/scripts/sft/dataset_split/model_writers.py`
- **Verification:** `pytest model/scripts/sft/tests/test_model_writers.py::TestNemoWriter -q` passed after Task 2; full `test_model_writers.py` passed after Task 3.
- **Committed in:** `233a8e0`, completed by `0d9919e`

---

**Total deviations:** 1 auto-fixed (Rule 3).
**Impact on plan:** No behavior scope change; the adjustment preserved the planned task order and final test coverage.

## Issues Encountered

- The patch tool initially applied the RED test file relative to the session's original worktree rather than the requested worktree. The uncommitted file was removed immediately and the same patch was applied by absolute path under `/home/shuojing/watch-duty-repo/.worktrees/sft-dataset-versioning/radio-transcription`; no committed output was affected.

## Known Stubs

None. Stub scan found only `config=None` in the Whisper result path, which is intentional because the Whisper writer has no config fragment in this plan.

## Threat Flags

None. The new split-row-to-model-input surface was already covered by the plan threat model, and `model_writers.py` introduces no GCS, filesystem, environment, download, upload, or job-submission surface.

## Verification

- `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model python -m py_compile model/scripts/sft/tests/test_model_writers.py` - passed.
- `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model --extra dev --extra scoring --extra vertex pytest model/scripts/sft/tests/test_model_writers.py::TestNemoWriter -q` - passed, `2 passed in 0.01s`.
- `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model --extra dev --extra scoring --extra vertex pytest model/scripts/sft/tests/test_model_writers.py -q` - passed, `5 passed in 0.01s`.
- `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model --extra dev --extra scoring --extra vertex --extra optimizer pytest model/scripts/sft/tests model/colabs/common/tests -q` - passed, `204 passed in 2.46s`.
- Acceptance scan `rg -n "Trainer|submit|download_to_scratch|upload_file_to_blob" model/scripts/sft/dataset_split/model_writers.py` returned no matches.
- Acceptance scan `rg -n "raw_row|download_to_scratch|upload_file_to_blob|submit" model/scripts/sft/dataset_split/model_writers.py` returned no matches.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Plan 03-04 can add Gemini writer behavior alongside the shared `ModelWriterResult`/`WriterWarning` contracts, and later publisher work can consume `jsonl_by_split()` plus `warnings_by_writer()` without invoking model runtimes.

## Self-Check: PASSED

- Found `model/scripts/sft/dataset_split/model_writers.py`.
- Found `model/scripts/sft/tests/test_model_writers.py`.
- Found `.planning/phases/03-gcs-artifacts-and-model-writers/03-03-SUMMARY.md`.
- Found task commit `f36a98e`.
- Found task commit `233a8e0`.
- Found task commit `0d9919e`.

---
*Phase: 03-gcs-artifacts-and-model-writers*
*Completed: 2026-05-28*

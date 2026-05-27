---
phase: 01-manifest-and-source-identity
plan: 03
subsystem: dataset-versioning
tags: [sft, validation, cli, gcs, manifests]
requires:
  - phase: 01-01
    provides: Dataset-version config and strict GCS manifest loading
  - phase: 01-02
    provides: Source identity extraction and row normalization
provides:
  - Phase 1 validation orchestrator
  - Minimal dataset-version validation CLI
  - Full offline Phase 1 validation suite
affects: [sft-dataset-versioning, split-generation, phase-02]
tech-stack:
  added: []
  patterns:
    - CLI entrypoints validate user GCS input before creating production readers.
    - Expected validation failures return code 1 with short contextual messages, not tracebacks.
key-files:
  created:
    - model/scripts/sft/dataset_split/validate.py
    - model/scripts/sft/validate_dataset.py
    - model/scripts/sft/tests/test_dataset_split_validate.py
  modified: []
key-decisions:
  - "Phase 1 validation reports loaded, valid, and excluded_empty_text counts only."
  - "Zero valid examples after empty-text exclusions is a hard validation failure."
patterns-established:
  - "Validation orchestration wraps source identity and row validation failures with dataset, manifest, and source_strategy context."
  - "CLI tests patch reader creation so no test instantiates real GCS clients."
requirements-completed: [INPT-01, INPT-02, INPT-03, INPT-04, SRC-01, SRC-02, SRC-03, SRC-04, SRC-05, SRC-06, TEST-01]
duration: 2 min
completed: 2026-05-27
---

# Phase 01 Plan 03: Validation Command Summary

**Offline Phase 1 dataset-version validation command with contextual errors and per-dataset count summaries**

## Performance

- **Duration:** 2 min
- **Started:** 2026-05-27T21:12:45Z
- **Completed:** 2026-05-27T21:13:59Z
- **Tasks:** 3
- **Files modified:** 3

## Accomplishments

- Added `validate_dataset` orchestration across config, GCS input loading, source maps, source extraction, and row normalization.
- Added `validate_dataset.py` CLI with `--config-uri`, clean expected-error handling, and loaded/valid/excluded summaries.
- Ran the full Phase 1 no-network validation suite successfully.

## Task Commits

Each code task was committed atomically:

1. **Task 1: Add validation orchestrator** - `eaa57a6`
2. **Task 2: Add minimal Phase 1 validation CLI** - `0010ece`
3. **Task 3: Run full Phase 1 validation suite** - verification-only task, no code commit

**Plan metadata:** committed with this summary.

## Files Created/Modified

- `model/scripts/sft/dataset_split/validate.py` - Validation summaries, source-map loading, and dataset validation orchestration.
- `model/scripts/sft/validate_dataset.py` - Minimal CLI for validating a GCS dataset-version config.
- `model/scripts/sft/tests/test_dataset_split_validate.py` - Orchestrator and CLI tests with fake readers.

## Decisions Made

- None beyond the approved plan and Phase 1 context decisions.

## Deviations from Plan

None - plan executed exactly as written.

---

**Total deviations:** 0 auto-fixed.
**Impact on plan:** No scope change.

## Issues Encountered

One test fixture source-map JSON string initially had an extra closing brace. The fixture was corrected before implementation was considered green.

## Self-Check: PASSED

- `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_split_validate.py -q` passed.
- `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_split_config.py model/scripts/sft/tests/test_dataset_split_gcs_io.py model/scripts/sft/tests/test_dataset_split_source_keys.py model/scripts/sft/tests/test_dataset_split_normalize.py model/scripts/sft/tests/test_dataset_split_validate.py -q` passed.
- `python3 -m py_compile model/scripts/sft/validate_dataset.py` passed.
- `git status --short --untracked-files=all` showed no generated train/eval/model manifests or audio files after the test run.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Phase 2 can consume validated `LabeledSegment` rows and source groups to implement the leak-safe 80:20 split.

---
*Phase: 01-manifest-and-source-identity*
*Completed: 2026-05-27*

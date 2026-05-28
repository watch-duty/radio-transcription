---
phase: 05-cli-reports-docs-and-verification
plan: 01
subsystem: cli
tags: [sft, dataset-split, dry-run, gcs]

requires:
  - phase: 04-audio-derivation-and-provenance
    provides: model-ready audio publication and provenance contracts
provides:
  - split_dataset.py dry-run and generate commands
  - validation result preserving valid and excluded rows
  - local dry-run preview artifact writer
affects: [phase-05, sft-runbook, dataset-versioning]

tech-stack:
  added: []
  patterns:
    - argparse runbook CLI with direct main(argv) tests
    - plan-only dry-run enrichment separated from audio materialization

key-files:
  created:
    - model/scripts/sft/split_dataset.py
    - model/scripts/sft/dataset_split/dry_run.py
    - model/scripts/sft/tests/test_split_dataset_cli.py
  modified:
    - model/scripts/sft/dataset_split/validate.py
    - model/scripts/sft/dataset_split/reports.py
    - model/scripts/sft/tests/test_dataset_split_validate.py
    - model/scripts/sft/validate_dataset.py

key-decisions:
  - "Dry-run uses preview model-ready gs:// URIs and audio_materialized=false, but never invokes audio materialization."
  - "The public CLI surface is split_dataset.py with dry-run and generate only."

patterns-established:
  - "Validation returns DatasetValidationResult for orchestration while validate_dataset() remains a summary-only library wrapper."
  - "Expected CLI failures print str(exc) and return 1 without failure artifacts."

requirements-completed: [CLI-01, CLI-02, CLI-03]

duration: 8 min
completed: 2026-05-28
---

# Phase 05 Plan 01: Dry-run and Generation CLI Commands Summary

**Runbook CLI for dry-run previews and GCS generation backed by shared dataset loading, splitting, and leakage checks**

## Performance

- **Duration:** 8 min
- **Started:** 2026-05-28T14:06:00Z
- **Completed:** 2026-05-28T14:14:05Z
- **Tasks:** 3
- **Files modified:** 7

## Accomplishments

- Added `split_dataset.py` with `dry-run` and `generate` subcommands.
- Added `DatasetValidationResult` and `load_and_validate_datasets()` so valid rows and non-fatal exclusions flow into later artifacts.
- Added dry-run preview artifact generation for canonical manifests, model inputs, and reports without audio probing, download, upload, or ffmpeg work.

## Task Commits

Each task was committed atomically:

1. **Task 1: Write Wave 0 CLI contract tests** - `2ca683a` (test)
2. **Task 2: Add validation result and dry-run artifact helpers** - `c6ef6fa` (feat)
3. **Task 3: Implement split_dataset.py and retire public validate CLI** - `a2a90ba` (feat)

**Plan metadata:** this summary commit

## Files Created/Modified

- `model/scripts/sft/split_dataset.py` - Public runbook CLI for `dry-run` and `generate`.
- `model/scripts/sft/dataset_split/dry_run.py` - Local preview bundle writer with non-materialized model-ready URI planning.
- `model/scripts/sft/dataset_split/validate.py` - Loader result preserving segments, exclusions, and summaries.
- `model/scripts/sft/dataset_split/reports.py` - Adds the `audio_materialized` report flag needed by dry-run.
- `model/scripts/sft/tests/test_split_dataset_cli.py` - CLI contract tests.
- `model/scripts/sft/tests/test_dataset_split_validate.py` - Library-only validation tests after retiring the public validate script.
- `model/scripts/sft/validate_dataset.py` - Removed the old public validation CLI.

## Decisions Made

- Kept dry-run audio planning in `dataset_split.dry_run` so audio materialization remains isolated to the publisher path.
- Used the existing action vocabulary (`reused`, `copied`, `derived`, `transcoded`) for preview transformation metadata.
- Deleted `validate_dataset.py` instead of keeping a compatibility CLI because validation is now an internal step.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Added `audio_materialized` support in reports.py during Plan 05-01**
- **Found during:** Task 2 (dry-run artifact helper)
- **Issue:** The dry-run helper must serialize `audio_materialized=false`, but report support was scheduled for 05-02.
- **Fix:** Added a backward-compatible `audio_materialized: bool = True` parameter and report field.
- **Files modified:** `model/scripts/sft/dataset_split/reports.py`
- **Verification:** `python3 -m pytest model/scripts/sft/tests/test_dataset_split_validate.py model/scripts/sft/tests/test_dataset_reports.py -q`
- **Committed in:** `c6ef6fa`

---

**Total deviations:** 1 auto-fixed (1 missing critical).
**Impact on plan:** Required for dry-run correctness; 05-02 will extend the same report surface with excluded-row sidecars.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Ready for Plan 05-02 to add the excluded-row sidecar and complete report/failure UX coverage.

## Self-Check: PASSED

- `python3 -m py_compile model/scripts/sft/split_dataset.py`
- `python3 -m pytest model/scripts/sft/tests/test_split_dataset_cli.py model/scripts/sft/tests/test_dataset_split_validate.py -q`

---
*Phase: 05-cli-reports-docs-and-verification*
*Completed: 2026-05-28*

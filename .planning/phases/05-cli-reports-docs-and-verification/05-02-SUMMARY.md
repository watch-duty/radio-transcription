---
phase: 05-cli-reports-docs-and-verification
plan: 02
subsystem: reporting
tags: [sft, dataset-report, excluded-rows, publisher]

requires:
  - phase: 05-cli-reports-docs-and-verification
    provides: split_dataset.py and dry-run artifact generation
provides:
  - reports/excluded_rows.jsonl sidecar for dry-run and generate
  - excluded-row summary fields in dataset_version_report.json
  - compact Markdown report section for non-fatal exclusions
affects: [phase-05, sft-runbook, dataset-reporting]

tech-stack:
  added: []
  patterns:
    - row-level sidecars only for non-fatal exclusions
    - Markdown reports summarize counts and paths, not row contents

key-files:
  created: []
  modified:
    - model/scripts/sft/dataset_split/reports.py
    - model/scripts/sft/dataset_split/publisher.py
    - model/scripts/sft/dataset_split/dry_run.py
    - model/scripts/sft/split_dataset.py
    - model/scripts/sft/tests/test_dataset_reports.py
    - model/scripts/sft/tests/test_dataset_publisher.py
    - model/scripts/sft/tests/test_split_dataset_cli.py

key-decisions:
  - "Successful runs emit reports/excluded_rows.jsonl; hard failures still only print short CLI errors."
  - "Transformation row details remain in canonical manifests, not a second sidecar."

patterns-established:
  - "serialize_excluded_rows() writes one minimal JSON object per non-fatal exclusion."
  - "Publisher artifact inventory owns the GCS sidecar URI; dry-run mirrors that text artifact locally."

requirements-completed: [CLI-03, CLI-04]

duration: 6 min
completed: 2026-05-28
---

# Phase 05 Plan 02: Report Bundle and Failure UX Summary

**Excluded-row sidecar reporting for successful dry-run and generate bundles without adding failure artifacts**

## Performance

- **Duration:** 6 min
- **Started:** 2026-05-28T14:14:30Z
- **Completed:** 2026-05-28T14:18:31Z
- **Tasks:** 3
- **Files modified:** 7

## Accomplishments

- Added `serialize_excluded_rows()` and report summary fields for excluded rows, source-key failures, and audio materialization state.
- Wired `reports/excluded_rows.jsonl` into publisher inventory/uploads and dry-run local output.
- Extended CLI tests to assert hard failures do not create files containing `failure`.

## Task Commits

Each task was committed atomically:

1. **Task 1: Add report and publisher tests for excluded-row sidecar** - `9f233f7` (test)
2. **Task 2: Implement excluded-row sidecar serialization and report fields** - `c841979` (feat)
3. **Task 3: Publish and dry-run the excluded-row sidecar** - `d5c0c30` (feat)

**Plan metadata:** this summary commit

## Files Created/Modified

- `model/scripts/sft/dataset_split/reports.py` - Excluded-row JSONL serialization and summary report fields.
- `model/scripts/sft/dataset_split/publisher.py` - GCS sidecar URI in artifact inventory and planned uploads.
- `model/scripts/sft/dataset_split/dry_run.py` - Local sidecar output for dry-run bundles.
- `model/scripts/sft/split_dataset.py` - Passes validation exclusions into `generate`.
- `model/scripts/sft/tests/test_dataset_reports.py` - Report field and Markdown regression coverage.
- `model/scripts/sft/tests/test_dataset_publisher.py` - Publisher inventory/content-type/sidecar content coverage.
- `model/scripts/sft/tests/test_split_dataset_cli.py` - Dry-run sidecar and no-failure-artifact coverage.

## Decisions Made

- Kept source-key failures as fail-fast CLI errors; successful reports set `source_key_failures=0`.
- Kept Markdown concise: counts, reasons, and sidecar path only.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Ready for Plan 05-03 to update the existing README and run final targeted verification.

## Self-Check: PASSED

- `python3 -m py_compile model/scripts/sft/dataset_split/publisher.py model/scripts/sft/dataset_split/dry_run.py model/scripts/sft/split_dataset.py`
- `python3 -m pytest model/scripts/sft/tests/test_dataset_reports.py model/scripts/sft/tests/test_dataset_publisher.py model/scripts/sft/tests/test_split_dataset_cli.py -q`

---
*Phase: 05-cli-reports-docs-and-verification*
*Completed: 2026-05-28*

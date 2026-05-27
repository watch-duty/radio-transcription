---
phase: 02-split-engine-and-leakage-gates
plan: 02
subsystem: data
tags: [sft, dataset-split, leakage, validation]
requires:
  - phase: 02-split-engine-and-leakage-gates
    provides: split-populated LabeledSegment rows
provides:
  - exact cross-split leakage validation
  - same-split duplicate audio span validation
  - split integrity validation entrypoint
affects: [phase-03-artifact-layout, phase-04-audio-derivation]
tech-stack:
  added: []
  patterns: [exact URI normalization, short fail-fast validation errors]
key-files:
  created:
    - model/scripts/sft/dataset_split/leakage.py
    - model/scripts/sft/tests/test_dataset_split_leakage.py
  modified: []
key-decisions:
  - "Cross-split leakage is checked by exact sets over Source Group, original audio URI, and non-empty model-ready URI."
  - "Duplicate labeled spans use original_audio_uri, offset, and duration only; transcript text is intentionally ignored."
patterns-established:
  - "Hard split validators raise SplitLeakageError with one field-specific conflict."
  - "URI equality strips surrounding whitespace only; no alias or content matching is attempted."
requirements-completed: [SPLT-03, SPLT-04, SPLT-05, TEST-03]
duration: 3 min
completed: 2026-05-27
---

# Phase 02 Plan 02: Leakage Gates Summary

**Exact split leakage and duplicate-span validators for split-populated SFT rows.**

## Performance

- **Duration:** 3 min
- **Started:** 2026-05-27T22:20:43Z
- **Completed:** 2026-05-27T22:23:45Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments

- Added `validate_split_leakage()` for cross-split Source Group, original-audio URI, and model-ready URI conflicts.
- Added `validate_no_duplicate_audio_spans()` and `validate_split_integrity()` for same-split exact audio span duplicates.
- Added focused no-network tests for leakage fields, whitespace URI normalization, duplicate keys, and missing split failures.

## Task Commits

1. **Task 1: Add cross-split leakage validators** - `6544de5`
2. **Task 2: Add duplicate audio span validation** - `6544de5`

## Files Created/Modified

- `model/scripts/sft/dataset_split/leakage.py` - split leakage, duplicate span, and integrity validators.
- `model/scripts/sft/tests/test_dataset_split_leakage.py` - exact overlap and duplicate-span tests.

## Decisions Made

None - followed Phase 2 context and plan behavior.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## Verification

- `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_split_leakage.py -q` passed: 9 tests.
- `python3 -m py_compile model/scripts/sft/dataset_split/leakage.py` passed.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Ready for Plan 02-03 balance reports. The split engine now has post-assignment hard gates for exact leakage and duplicate labeled audio spans.

---
*Phase: 02-split-engine-and-leakage-gates*
*Completed: 2026-05-27*

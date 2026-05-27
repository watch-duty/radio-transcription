---
phase: 02-split-engine-and-leakage-gates
plan: 01
subsystem: data
tags: [sft, dataset-split, ortools, cp-sat]
requires:
  - phase: 01-manifest-and-source-identity
    provides: LabeledSegment rows with leak-safe Source Groups
provides:
  - OR-Tools CP-SAT Source Group train/eval assignment
  - seed-free dataset split configuration
  - split assignment metadata for auditability
affects: [phase-03-artifact-layout, phase-04-audio-derivation]
tech-stack:
  added: [ortools]
  patterns: [global Source Group assignment, weighted CP-SAT balance objective]
key-files:
  created:
    - model/scripts/sft/dataset_split/split.py
    - model/scripts/sft/tests/test_dataset_split_split.py
  modified:
    - model/pyproject.toml
    - model/scripts/sft/requirements.txt
    - model/scripts/sft/dataset_split/config.py
    - model/scripts/sft/tests/test_dataset_split_config.py
    - model/scripts/sft/tests/test_dataset_split_validate.py
key-decisions:
  - "The split algorithm uses OR-Tools CP-SAT with one Boolean variable per Source Group."
  - "The reproducibility contract is assignment plus metadata, not random_seed."
  - "Source Groups are keyed globally to avoid cross-dataset source leakage."
patterns-established:
  - "Split assignment returns replaced LabeledSegment rows with split populated."
  - "Solver metadata records algorithm, status, objective value, time limit, wall time, and weights."
requirements-completed: [SPLT-01, SPLT-02, SPLT-06, SPLT-07, TEST-02]
duration: 6 min
completed: 2026-05-27
---

# Phase 02 Plan 01: Split Engine Summary

**OR-Tools CP-SAT Source Group assignment with seed-free config and split metadata.**

## Performance

- **Duration:** 6 min
- **Started:** 2026-05-27T22:14:25Z
- **Completed:** 2026-05-27T22:20:43Z
- **Tasks:** 3
- **Files modified:** 7

## Accomplishments

- Added the `optimizer` extra with `ortools>=9.15,<10` and updated SFT requirements to install it.
- Removed `random_seed` from the required TOML config contract while tolerating legacy extra keys.
- Added CP-SAT train/eval assignment with per-dataset train/eval Source Group coverage and audit metadata.

## Task Commits

1. **Task 1: Update optimizer dependency and remove unused seed requirement** - `6f9c55e`
2. **Task 2: Implement CP-SAT Source Group assignment** - `a6abb9d`
3. **Task 3: Cover split assignment behavior** - `a6abb9d`

## Files Created/Modified

- `model/scripts/sft/dataset_split/split.py` - CP-SAT assignment, bucket constants, split result dataclasses.
- `model/scripts/sft/tests/test_dataset_split_split.py` - no-network tests for Source Group assignment and metadata.
- `model/scripts/sft/dataset_split/config.py` - seed-free dataset version config parsing.
- `model/pyproject.toml` and `model/scripts/sft/requirements.txt` - OR-Tools dependency wiring.

## Decisions Made

The implementation keys solver variables by global `source_group` string. This preserves per-dataset hard constraints while preventing the same physical Source Group from being assigned differently across configured datasets.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Critical correctness] Use global Source Group variables**
- **Found during:** Task 2
- **Issue:** The plan text said to group by `(dataset_name, source_group)`, which could allow the same Source Group string to land in train for one dataset and eval for another.
- **Fix:** Group solver variables by global `source_group` and build per-dataset constraints from dataset membership.
- **Files modified:** `model/scripts/sft/dataset_split/split.py`, `model/scripts/sft/tests/test_dataset_split_split.py`
- **Verification:** `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_split_split.py -q`
- **Committed in:** `a6abb9d`

**Total deviations:** 1 auto-fixed correctness issue.
**Impact on plan:** Strengthens the leakage guarantee without changing the Phase 2 boundary.

## Issues Encountered

OR-Tools was not installed locally, so `python3 -m pip install "ortools>=9.15,<10"` was run before solver tests.

## Verification

- `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_split_config.py -q` passed: 8 tests.
- `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_split_split.py -q` passed: 6 tests.
- `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_split_config.py model/scripts/sft/tests/test_dataset_split_split.py -q` passed: 14 tests.
- `python3 -m py_compile model/scripts/sft/dataset_split/split.py` passed.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Ready for Plan 02-02 leakage gates. Split assignment now produces split-populated `LabeledSegment` rows that validators can inspect.

---
*Phase: 02-split-engine-and-leakage-gates*
*Completed: 2026-05-27*

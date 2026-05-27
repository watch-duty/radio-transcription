---
phase: 02-split-engine-and-leakage-gates
plan: 03
subsystem: data
tags: [sft, dataset-split, balance-report, pytest]
requires:
  - phase: 02-split-engine-and-leakage-gates
    provides: split assignment and leakage gates
provides:
  - fixed balance bucket helpers
  - weighted split balance report
  - JSON-safe balance report on SplitResult
affects: [phase-03-artifact-layout, phase-04-audio-derivation]
tech-stack:
  added: []
  patterns: [report-only balance quality, JSON-safe report dataclasses]
key-files:
  created:
    - model/scripts/sft/dataset_split/balance.py
    - model/scripts/sft/tests/test_dataset_split_balance.py
  modified:
    - model/scripts/sft/dataset_split/split.py
    - model/scripts/sft/tests/test_dataset_split_split.py
key-decisions:
  - "Balance quality is surfaced as weighted score and component deltas, not a hard gate."
  - "Time/month/hour distribution is report-only and ignored by split assignment."
patterns-established:
  - "Duration and transcript-length buckets are fixed built-ins shared by split scoring and reports."
  - "BalanceReport.to_dict() returns JSON-safe data for later GCS artifacts."
requirements-completed: [SPLT-07, SPLT-08, TEST-02, TEST-03, TEST-04]
duration: 4 min
completed: 2026-05-27
---

# Phase 02 Plan 03: Balance Reports Summary

**Weighted balance reports with component deltas attached to split results.**

## Performance

- **Duration:** 4 min
- **Started:** 2026-05-27T22:23:45Z
- **Completed:** 2026-05-27T22:27:55Z
- **Tasks:** 3
- **Files modified:** 4

## Accomplishments

- Added fixed duration and transcript word-count buckets with boundary tests.
- Added `build_balance_report()` with weighted score, component deltas, bucket summaries, and report-only time distribution.
- Attached a JSON-safe balance report to `SplitResult` and verified the full local SFT test suite.

## Task Commits

1. **Task 1: Add fixed balance buckets and component deltas** - `5324d1a`
2. **Task 2: Generate weighted balance reports** - `0065e8a`
3. **Task 3: Run full Phase 2 SFT suite** - verification-only, no code commit

## Files Created/Modified

- `model/scripts/sft/dataset_split/balance.py` - bucket helpers, report dataclasses, component delta builder.
- `model/scripts/sft/tests/test_dataset_split_balance.py` - bucket, report, and report-only time tests.
- `model/scripts/sft/dataset_split/split.py` - imports shared bucket helpers and attaches balance report dicts.
- `model/scripts/sft/tests/test_dataset_split_split.py` - asserts split results expose balance report output.

## Decisions Made

None - followed Phase 2 context and plan behavior.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## Verification

- `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_split_balance.py -q` passed: 5 tests.
- `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_split_balance.py model/scripts/sft/tests/test_dataset_split_split.py -q` passed: 11 tests.
- `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests -q` passed: 93 tests.
- `python3 -m py_compile model/scripts/sft/dataset_split/split.py model/scripts/sft/dataset_split/leakage.py model/scripts/sft/dataset_split/balance.py` passed.
- No split, leakage, or balance tests import network/audio clients; `gs://` appears only as synthetic fixture strings.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Phase 2 is ready for verification. Phase 3 can consume split-populated rows, leakage validators, and balance report dicts when writing GCS artifacts.

---
*Phase: 02-split-engine-and-leakage-gates*
*Completed: 2026-05-27*

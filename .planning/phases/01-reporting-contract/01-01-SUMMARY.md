---
phase: 01-reporting-contract
plan: "01"
subsystem: gemini-sft-reporting
tags: [gemini-sft, reporting, scoring, tests]

requires: []
provides:
  - Shared exact empty response metric for transcript hypotheses
  - Shared SFT eval report schema with canonical public metric names
  - JSON-compatible, Markdown, and console renderers backed by one column contract
affects: [gemini-sft, checkpoint-scoring, eval-reporting]

tech-stack:
  added: []
  patterns:
    - Frozen dataclass report contract for SFT eval outputs
    - One REPORT_COLUMNS sequence shared by JSON, Markdown, and console renderers

key-files:
  created:
    - model/src/gemini_sft/reporting.py
    - model/tests/gemini_sft/test_reporting.py
  modified:
    - model/src/common/scoring.py
    - model/tests/common/tests/test_scoring.py

key-decisions:
  - "Expose empty_or_unintelligible_rate for the historical empty-or-[UNINTELLIGIBLE] metric and empty_response_rate for exact blank outputs."
  - "Expose raw insertions, deletions, substitutions, and total_reference_words instead of derived-only edit-count summaries."
  - "Keep missing_prediction_count separate from model-produced empty responses."

patterns-established:
  - "EvalReport owns target rows and report metadata; renderers consume the same target column sequence."
  - "ReportArtifacts stay attached to the target row they describe to avoid metric/artifact mix-ups."

requirements-completed: [RPT-01, RPT-02, RPT-03, RPT-04, RPT-05]

duration: not tracked
completed: 2026-06-28
---

# Phase 1 Plan 01: Reporting Foundation Summary

**Shared SFT eval reporting contract with exact empty-response metrics, canonical public names, raw edit counts, and parity across JSON, Markdown, and console renderers.**

## Performance

- **Duration:** not tracked after context handoff
- **Started:** not tracked
- **Completed:** 2026-06-28T16:34:53Z
- **Tasks:** 3
- **Files modified:** 4

## Accomplishments

- Added `empty_response_rate` to shared scoring code without changing the historical empty-or-`[UNINTELLIGIBLE]` helper.
- Created `gemini_sft.reporting` with frozen report dataclasses, a single target-column contract, and JSON-compatible, Markdown, and console renderers.
- Covered the report contract with tests for canonical metric names, edit-count semantics, and renderer header parity.

## Task Commits

Each task was committed atomically:

1. **Task 1: Add exact empty response metric to shared scoring** - `2f1bdd87` (feat)
2. **Task 2: Create shared report schema and renderers** - `1005bf0f` (feat)
3. **Task 3: Test report contract parity and canonical names** - `1a2f0578` (test)

## Files Created/Modified

- `model/src/common/scoring.py` - Adds `empty_response_rate` while preserving `hallucination_rate` semantics.
- `model/tests/common/tests/test_scoring.py` - Covers exact empty responses and historical empty-or-unintelligible behavior.
- `model/src/gemini_sft/reporting.py` - Defines `ReportArtifacts`, `TargetMetrics`, `EvalReport`, metric builders, and renderers.
- `model/tests/gemini_sft/test_reporting.py` - Verifies canonical public keys, total word count semantics, and renderer parity.

## Decisions Made

- Public reports use `empty_or_unintelligible_rate` instead of `hallucination_rate` so the historical metric name describes the actual behavior.
- Public reports use `empty_response_rate` only for stripped-empty model outputs, keeping it distinct from `[UNINTELLIGIBLE]` output handling.
- `total_reference_words` is computed from WER operation counts as `hits + substitutions + deletions`, matching the WER denominator.
- Artifact URIs are modeled as part of each target row so provenance cannot drift away from the metrics it describes.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Verification

- `python3 -m py_compile model/src/gemini_sft/reporting.py` passed.
- `safe-run -- bash -lc 'cd model && PYTHONPATH=src:tests python3 -m pytest tests/common/tests/test_scoring.py tests/gemini_sft/test_reporting.py -q'` passed with `52 passed in 2.32s`.
- `rg -n '"(empty_rate|hallucination_rate)"' model/src/gemini_sft/reporting.py` returned no matches.

## Next Phase Readiness

The shared report contract is ready for batch eval integration in Plan 02 and checkpoint scorer integration in Plan 03.

---
*Phase: 01-reporting-contract*
*Completed: 2026-06-28*

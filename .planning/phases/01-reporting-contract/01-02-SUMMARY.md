---
phase: 01-reporting-contract
plan: "02"
subsystem: gemini-sft-reporting
tags: [gemini-sft, eval, reporting, batch-inference, tests]

requires:
  - phase: 01-reporting-contract
    provides: Shared EvalReport schema and renderers from plan 01-01
provides:
  - Batch eval report rows built from the shared EvalReport schema
  - Batch eval JSON and Markdown summaries rendered from the shared report contract
  - Console output for batch eval using the same target table columns
affects: [gemini-sft, checkpoint-scoring, eval-reporting]

tech-stack:
  added: []
  patterns:
    - Batch eval keeps flat metrics for config and ledger compatibility while written summaries use EvalReport
    - Provider omissions are counted as missing_prediction_count and still scored as empty hypotheses

key-files:
  created: []
  modified:
    - model/src/gemini_sft/evaluate.py
    - model/src/gemini_sft/records.py
    - model/tests/gemini_sft/test_workflow.py

key-decisions:
  - "Keep existing flat metric assembly for config.json and ledger compatibility, but write wer_summary.json and wer_summary.md from EvalReport."
  - "Attach raw Vertex output URI and normalized inference manifest URI to each target row through ReportArtifacts."
  - "Leave missing provider rows in the hypothesis list as empty strings and count them separately with missing_prediction_count."

patterns-established:
  - "Batch eval report targets are assembled immediately after normalized inference manifests are uploaded so artifact URIs are known."
  - "write_wer_summary accepts EvalReport for the canonical path and dict for backward-compatible callers."

requirements-completed: [RPT-01, RPT-02, RPT-03, RPT-04, RPT-05]

duration: not tracked
completed: 2026-06-28
---

# Phase 1 Plan 02: Batch Eval Report Integration Summary

**Config-driven batch eval now writes and prints shared target-oriented report rows with missing-prediction counts and per-target artifact provenance.**

## Performance

- **Duration:** not tracked
- **Started:** not tracked
- **Completed:** 2026-06-28T16:39:27Z
- **Tasks:** 3
- **Files modified:** 3

## Accomplishments

- Updated `evaluate_run` to build `TargetMetrics` for base and tuned targets through `build_target_metrics`.
- Updated `write_wer_summary` so `EvalReport` writes canonical JSON and Markdown while flat dict callers remain supported.
- Added workflow assertions for `targets`, `missing_prediction_count`, exact empty response rate, empty-or-unintelligible rate, raw edit counts, total reference words, and artifact URIs.

## Task Commits

1. **Tasks 1-2: Build and render batch EvalReport rows** - `b48c590b` (feat)
2. **Task 3: Add batch eval report regression tests** - `44006f7c` (test)

## Files Created/Modified

- `model/src/gemini_sft/evaluate.py` - Builds `EvalReport` target rows, records missing predictions, attaches artifacts, writes summaries, and logs the shared console table.
- `model/src/gemini_sft/records.py` - Writes `EvalReport` summaries through shared JSON and Markdown renderers while preserving dict compatibility.
- `model/tests/gemini_sft/test_workflow.py` - Verifies the batch eval report contract and missing prediction behavior.

## Decisions Made

- Flat `metrics` remain in memory for `config.json` updates and `ledger.md`; the persisted WER summary is the new report schema.
- Missing provider rows continue to be represented as empty hypotheses for scoring, but are also exposed as `missing_prediction_count`.
- The missing-prediction regression fixture uses one `[UNINTELLIGIBLE]` prediction plus one missing row so exact empty responses and empty-or-unintelligible outputs are visibly distinct.

## Deviations from Plan

The three planned tasks were delivered in two commits because the batch report construction and summary writer changes are coupled: `evaluate_run` cannot safely write an `EvalReport` until `records.write_wer_summary` accepts it.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Verification

- `python3 -m py_compile model/src/gemini_sft/evaluate.py model/src/gemini_sft/records.py model/tests/gemini_sft/test_workflow.py` passed.
- `safe-run -- bash -lc 'cd model && PYTHONPATH=src:tests python3 -m pytest tests/gemini_sft/test_workflow.py -q'` passed with `33 passed, 6 subtests passed in 0.96s`.
- Workflow tests assert `wer_summary.json["targets"][0]["missing_prediction_count"] == 1` in the missing-provider-row case.

## Next Phase Readiness

Batch eval now uses the shared report contract. Plan 03 can apply the same report schema to checkpoint scoring and remove local metric-name drift.

---
*Phase: 01-reporting-contract*
*Completed: 2026-06-28*

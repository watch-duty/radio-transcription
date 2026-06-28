---
phase: 04-durable-eval
plan: "03"
subsystem: durable-eval-reporting
tags: [gemini-sft, eval, reporting, gcs-artifacts]
requires:
  - phase: 04-01
    provides: singular durable eval_model config contract
  - phase: 04-02
    provides: exact batch prediction metadata reuse
provides:
  - one durable eval target execution path
  - stable run-level eval summary GCS artifacts
  - summary artifact URIs in target reports
affects: [gemini-sft-eval, reporting, run-ledger]
tech-stack:
  added: []
  patterns:
    - one eval run evaluates exactly one durable target
    - local report mirrors are uploaded to stable run-level GCS paths
key-files:
  modified:
    - model/src/gemini_sft/evaluate.py
    - model/src/gemini_sft/records.py
    - model/tests/gemini_sft/test_workflow.py
    - model/tests/gemini_sft/test_reporting.py
key-decisions:
  - "Eval now reads one config.json eval_model and builds one TargetMetrics row."
  - "wer_summary.json and wer_summary.md are uploaded to evals/ after successful report construction."
patterns-established:
  - "ReportArtifacts summary_json_uri and summary_markdown_uri point to stable run-level GCS summaries."
  - "Plural durable eval_models is rejected before manifest download or inference."
requirements-completed: [DATA-01, DATA-02, DATA-05, DATA-06]
duration: 13 min
completed: 2026-06-28
---

# Phase 04 Plan 03: Durable Eval Summary Summary

**`gemini-sft eval` now executes one durable model target and publishes stable summary artifacts**

## Performance

- **Duration:** 13 min
- **Started:** 2026-06-28T21:56:00Z
- **Completed:** 2026-06-28T22:09:00Z
- **Tasks:** 3 completed
- **Files modified:** 4

## Accomplishments

- Collapsed `evaluate_run` from a target tuple/loop to one `eval_model` target loaded from durable `config.json`.
- Removed old base/tuned target lookup from the eval orchestration.
- Added `wer_summary_gcs_uris(...)` and made `write_wer_summary(...)` return local summary paths.
- Uploaded `evals/wer_summary.json` and `evals/wer_summary.md` to the run GCS prefix after report construction.
- Added summary artifact URIs to the single target's `ReportArtifacts`.
- Added tests for exactly one target row, durable summary uploads, summary artifact fields, total reference words, ledger target label, endpoint online eval, and plural durable config rejection before manifest download.

## Task Commits

1. **Tasks 1-3: Single-target eval flow and stable summary uploads** - `4f25e95c`

## Files Modified

- `model/src/gemini_sft/evaluate.py` - Single durable target dispatch, stable summary artifact URIs, GCS uploads.
- `model/src/gemini_sft/records.py` - Stable summary GCS URI helper and local summary path return values.
- `model/tests/gemini_sft/test_workflow.py` - Workflow coverage for one target, summary uploads, and plural durable rejection.
- `model/tests/gemini_sft/test_reporting.py` - Report artifact field coverage for summary URIs.

## Decisions Made

- Summary uploads are run-level artifacts at `evals/wer_summary.{json,md}` rather than target-scoped artifacts.
- A single-target eval still writes label-scoped normalized inference manifests, so checkpoint names remain ordinary model labels.

## Deviations from Plan

None.

## Issues Encountered

None.

## Verification

- `python3 -m py_compile model/src/gemini_sft/evaluate.py model/src/gemini_sft/records.py model/src/gemini_sft/reporting.py`
- `rg -n "require_config_eval_model|evals/wer_summary|summary_json_uri|summary_markdown_uri|for target in eval_model_targets|require_config_eval_models|targets=\\[target_metrics\\]|len\\(metrics\\[\\\"targets\\\"\\]\\)|total_reference_words" model/src/gemini_sft model/tests/gemini_sft`
- `safe-run -- bash -lc 'cd model && PYTHONPATH=src:tests python3 -m pytest tests/gemini_sft/test_workflow.py tests/gemini_sft/test_config.py tests/gemini_sft/test_reporting.py -q'`

## Self-Check: PASSED

- Eval uses `require_config_eval_model`, not a plural durable target loader.
- Eval code contains no `for target in eval_model_targets` loop.
- Report JSON contains exactly one target row.
- Target rows include `total_reference_words`.
- Stable summary JSON and Markdown artifacts are uploaded to GCS and linked from report artifacts.
- Plural durable `eval_models` is rejected before manifest download and inference.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Wave 4 can update examples/docs and run scorer/drift cleanup against the singular eval config and stable summary artifact contract.

---
*Phase: 04-durable-eval*
*Completed: 2026-06-28*

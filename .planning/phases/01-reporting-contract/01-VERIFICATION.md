---
phase: 01-reporting-contract
verified: 2026-06-29T04:08:10Z
status: passed
score: "8/8 must-haves verified"
overrides_applied: 0
---

# Phase 1: Reporting Contract Verification Report

**Phase Goal:** Operators and maintainers can trust one shared SFT eval metric
and report contract across comparable batch and checkpoint paths.
**Verified:** 2026-06-29T04:08:10Z
**Status:** passed
**Re-verification:** Milestone-close inline verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|---|---|---|
| 1 | The canonical report columns include WER, CER, keyword accuracy, empty-or-unintelligible rate, exact empty response rate, S/I/D, total reference words, missing predictions, and artifacts. | VERIFIED | `model/src/gemini_sft/reporting.py` defines `REPORT_COLUMNS` with these fields. |
| 2 | JSON, Markdown, and console reports use one shared structured report schema. | VERIFIED | `EvalReport`, `TargetMetrics`, `report_to_dict`, `render_markdown_report`, and `render_console_report` all live in `model/src/gemini_sft/reporting.py` and render from the same target rows. |
| 3 | Exact empty responses are reported separately from the historical empty-or-unintelligible metric. | VERIFIED | `build_target_metrics` calls both `empty_response_rate` and `hallucination_rate` and stores them in distinct fields. |
| 4 | Missing provider predictions remain in the scoring denominator as empty hypotheses. | VERIFIED | `model/src/gemini_sft/evaluate.py` and `model/scripts/sft/score_gemini_sft_checkpoints_online.py` compute `missing_prediction_count` and score absent predictions as `""`. |
| 5 | Total reference word count is derived from WER edit statistics, not from an unrelated token counter. | VERIFIED | `build_target_metrics` sets `total_reference_words` to hits plus substitutions plus deletions. |
| 6 | Batch eval emits the shared report shape and console report. | VERIFIED | `evaluate_run` builds one `TargetMetrics` row through `build_target_metrics`, writes `EvalReport`, and logs `render_console_report(report)`. |
| 7 | Checkpoint scoring uses the shared report shape and console renderer. | VERIFIED | `score_gemini_sft_checkpoints_online.py` imports `TargetMetrics`, `build_target_metrics`, and `render_console_report` from `gemini_sft.reporting`. |
| 8 | Focused report/checkpoint/workflow tests pass. | VERIFIED | Reporting verification slice passed with `50 passed, 6 subtests passed`. |

**Score:** 8/8 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|---|---|---|---|
| `model/src/gemini_sft/reporting.py` | Shared report schema, target metric builder, JSON/Markdown/console renderers | VERIFIED | Contains `REPORT_COLUMNS`, `ReportArtifacts`, `TargetMetrics`, `EvalReport`, and render helpers. |
| `model/src/common/scoring.py` | Exact empty response metric | VERIFIED | Provides `empty_response_rate` separately from the historical hallucination/empty-or-unintelligible helper. |
| `model/src/gemini_sft/evaluate.py` | Packaged batch/online eval report integration | VERIFIED | Uses `build_target_metrics`, `EvalReport`, `write_wer_summary`, and `render_console_report`. |
| `model/scripts/sft/score_gemini_sft_checkpoints_online.py` | Checkpoint scorer report integration | VERIFIED | Delegates metric construction and console rendering to the shared reporting module. |
| `model/tests/gemini_sft/test_reporting.py` | Shared report contract tests | VERIFIED | Covered by the focused reporting verification slice. |
| `model/tests/gemini_sft/test_checkpoint_scorer.py` | Checkpoint report/scoring behavior tests | VERIFIED | Covered by the focused reporting verification slice. |
| `model/tests/gemini_sft/test_workflow.py` | Packaged eval workflow report behavior tests | VERIFIED | Covered by the focused reporting verification slice. |

### Requirements Coverage

| Requirement | Source Plans | Status | Evidence |
|---|---|---|---|
| RPT-01 | 01-01, 01-02, 01-03 | SATISFIED | Console report columns come from `REPORT_COLUMNS` and include all required metrics and artifact URIs. |
| RPT-02 | 01-01, 01-02, 01-03 | SATISFIED | JSON, Markdown, and console output are rendered from `EvalReport` / `TargetMetrics`. |
| RPT-03 | 01-01 | SATISFIED | Exact empty response rate and empty-or-unintelligible rate are distinct target fields. |
| RPT-04 | 01-01, 01-02, 01-03 | SATISFIED | Missing predictions are counted and scored as empty hypotheses in eval and checkpoint scoring paths. |
| RPT-05 | 01-02, 01-03 | SATISFIED | Batch eval and checkpoint scoring both use the shared report builder and renderer. |

No orphaned Phase 1 requirements were found.

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|---|---|---|---|
| Reporting, workflow, and checkpoint report contract tests pass. | `safe-run -- bash -lc 'cd model && PYTHONPATH=src:tests python3 -m pytest tests/gemini_sft/test_reporting.py tests/gemini_sft/test_workflow.py tests/gemini_sft/test_checkpoint_scorer.py -q'` | `50 passed, 6 subtests passed` | PASS |

### Residual Risks

- No paid Vertex batch job or live online endpoint was invoked during this
  verification. The report contract is verified through mocked workflow and
  scorer tests by design.
- Future metric additions must update `REPORT_COLUMNS`, docs, and drift guards
  together to avoid schema/documentation skew.

### Gaps Summary

No blocking gaps found. Phase 1 can be counted as verified for milestone close.

---
_Verified: 2026-06-29T04:08:10Z_
_Verifier: the agent (inline milestone-close verification)_

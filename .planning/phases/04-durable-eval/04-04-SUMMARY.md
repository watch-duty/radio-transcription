---
phase: 04-durable-eval
plan: "04"
subsystem: docs-and-drift
tags: [gemini-sft, docs, examples, drift-guard, checkpoint-scorer]
requires:
  - phase: 04-03
    provides: one-target eval and stable summary artifacts
provides:
  - singular eval model operator documentation
  - stable eval artifact documentation
  - drift guard for singular example config
affects: [gemini-sft-docs, checkpoint-scorer, drift-tests]
tech-stack:
  added: []
  patterns:
    - examples show one model per config
    - docs defer multi-model and dataset-breakdown behavior explicitly
key-files:
  modified:
    - model/scripts/sft/run_config.example.toml
    - model/scripts/sft/README.md
    - model/tests/common/tests/test_drift_guard.py
key-decisions:
  - "Packaged eval docs teach [eval.model] only; plural [[eval.models]] is unsupported."
  - "Checkpoint sweep remains a separate legacy/ranking path, not packaged eval fan-out."
patterns-established:
  - "Drift tests assert the example config cannot reintroduce [[eval.models]]."
  - "README names request-identity metadata and stable summary artifact paths."
requirements-completed: [EXEC-05, DATA-03, DATA-04, DATA-05]
duration: 9 min
completed: 2026-06-28
---

# Phase 04 Plan 04: Docs And Drift Summary

**Operator docs now match the narrowed one-model durable eval contract**

## Performance

- **Duration:** 9 min
- **Started:** 2026-06-28T22:10:00Z
- **Completed:** 2026-06-28T22:19:00Z
- **Tasks:** 3 completed
- **Files modified:** 3

## Accomplishments

- Replaced the example config's plural `[[eval.models]]` with singular `[eval.model]`.
- Documented that packaged `gemini-sft eval` supports one model per config and external wrappers should handle base/tuned/checkpoint comparisons.
- Documented durable `eval_model`, request-identity reuse metadata, `batch_predictions.meta.json`, and stable `evals/wer_summary.{json,md}` artifacts.
- Documented that dataset breakdowns and multiple eval manifests are follow-up work, not Phase 4 behavior.
- Confirmed the checkpoint scorer still uses `load_eval_run_config` and delegates endpoint scoring through `run_online_target_inference`.
- Added a drift guard that fails if the example config reintroduces `[[eval.models]]`.

## Task Commits

1. **Tasks 1-3: Singular docs/example and drift guard** - `b21292ed`

## Files Modified

- `model/scripts/sft/run_config.example.toml` - Singular `[eval.model]` example and external-wrapper guidance.
- `model/scripts/sft/README.md` - Durable eval semantics, artifact layout, reuse behavior, and deferred dataset-breakdown guidance.
- `model/tests/common/tests/test_drift_guard.py` - Example config guard for `[eval.model]` and no `[[eval.models]]`.

## Decisions Made

- No checkpoint scorer behavior change was needed; it already uses the packaged online executor and does not depend on plural eval config.
- Deferred requirements are documented as explicit follow-up work rather than implemented in Phase 4.

## Deviations from Plan

`score_gemini_sft_checkpoints_online.py` and `test_checkpoint_scorer.py` needed no edits after audit because they did not use the removed plural config API.

**Total deviations:** 1 no-op audit result.
**Impact on plan:** None; scorer compatibility was verified by tests.

## Issues Encountered

None.

## Verification

- `python3 -m py_compile model/scripts/sft/score_gemini_sft_checkpoints_online.py`
- `rg -n "\\[eval\\.model\\]|\\[\\[eval\\.models\\]\\]|eval_model|batch_predictions.meta.json|evals/wer_summary.json|dataset breakdowns|external wrapper|run_online_target_inference|max_retries = 3|one model per config" model/scripts/sft model/tests/common/tests/test_drift_guard.py model/tests/gemini_sft/test_checkpoint_scorer.py`
- `safe-run -- bash -lc 'cd model && PYTHONPATH=src:tests:scripts/sft python3 -m pytest tests/gemini_sft/test_checkpoint_scorer.py tests/common/tests/test_drift_guard.py -q'`

## Self-Check: PASSED

- Example config contains `[eval.model]`.
- Example config contains no `[[eval.models]]`.
- README contains `eval_model`, stable summary paths, and request metadata paths.
- README says dataset breakdowns and multiple eval manifests are follow-up work.
- Drift guard asserts the singular example shape.
- Checkpoint scorer tests pass unchanged.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Phase 4 implementation is ready for phase gates: review, regression, drift checks, verification, and phase completion.

---
*Phase: 04-durable-eval*
*Completed: 2026-06-28*

---
phase: 04-durable-eval
plan: "01"
subsystem: model-eval-config
tags: [gemini-sft, eval-config, durable-config, target-routing]
requires:
  - phase: 03-target-execution
    provides: online target backend routing and resumable online inference
provides:
  - singular local eval target config via [eval.model]
  - singular durable eval target state via eval_model
  - loud rejection for legacy plural eval_models configuration
affects: [gemini-sft-eval, batch-eval, online-eval, checkpoint-scoring]
tech-stack:
  added: []
  patterns:
    - fail-closed config parsing before paid eval work
    - unclassified EvalModelTarget for publisher models and endpoints
key-files:
  created: []
  modified:
    - model/src/gemini_sft/config.py
    - model/tests/gemini_sft/test_config.py
key-decisions:
  - "Kept EvalModelTarget as the shared target type while narrowing config shape to one target."
  - "Rejected local [[eval.models]] and durable eval_models without migration compatibility."
patterns-established:
  - "Durable config loaders fail before inference when stale plural target state is present."
requirements-completed: [DATA-01, DATA-02, DATA-06]
duration: 8 min
completed: 2026-06-28
---

# Phase 04 Plan 01: Singular Eval Model Config Summary

**Gemini SFT eval config now uses one explicit eval model target in local TOML and durable GCS state**

## Performance

- **Duration:** 8 min
- **Started:** 2026-06-28T21:25:45Z
- **Completed:** 2026-06-28T21:33:45Z
- **Tasks:** 3 completed
- **Files modified:** 2

## Accomplishments

- Replaced plural `RunConfig.eval_models` with singular `RunConfig.eval_model`.
- Added local `[eval.model]` parsing and durable `eval_model` validation.
- Added fail-fast errors for local `[[eval.models]]` and durable `eval_models`.
- Verified backend routing still treats the model string generically.

## Task Commits

1. **Task 1: Add singular eval model config API** - `62a84f4b`
2. **Task 2: Add durable eval_model validation** - `62a84f4b`
3. **Task 3: Update backend resolver imports for singular target state** - no code change required; resolver tests passed against the narrowed config API

## Files Created/Modified

- `model/src/gemini_sft/config.py` - Singular eval target parsing, serialization, and durable validation.
- `model/tests/gemini_sft/test_config.py` - Contract tests for `[eval.model]`, `eval_model`, and plural rejection.

## Decisions Made

- Kept `EvalModelTarget` to avoid unnecessary type churn in target execution code.
- Treated durable `eval_models` as stale state and rejected it before any eval inference.

## Deviations from Plan

Task 1 and Task 2 were implemented in one commit because the local parser and durable loader share the same config contract in `config.py`. No behavioral scope was added beyond the plan.

**Total deviations:** 1 process deviation, 0 behavioral deviations.
**Impact on plan:** All Wave 1 acceptance criteria passed.

## Issues Encountered

- Initial plural-rejection test regex encoded the wrong word order. The implementation already contained the required terms, so the test assertion was corrected and rerun.

## Verification

- `python3 -m py_compile model/src/gemini_sft/config.py model/src/gemini_sft/target_execution.py`
- `safe-run -- bash -lc 'cd model && PYTHONPATH=src:tests python3 -m pytest tests/gemini_sft/test_config.py tests/gemini_sft/test_target_execution.py -q'`
- `rg -n "eval_model|\\[eval\\.model\\]|require_config_eval_model|eval_models is not supported|\\[\\[eval\\.models\\]\\]" model/src/gemini_sft model/tests/gemini_sft`

## Self-Check: PASSED

- Local eval config uses exactly one `[eval.model]`.
- Durable GCS config uses exactly one `eval_model`.
- Plural local `[[eval.models]]` and durable `eval_models` fail loudly.
- Model strings remain generic enough for publisher, tuned endpoint, and checkpoint endpoint values.
- Backend resolver behavior remains unchanged.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Wave 2 can extract the request identity helpers and reuse the singular target contract for batch metadata validation.

---
*Phase: 04-durable-eval*
*Completed: 2026-06-28*

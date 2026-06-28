---
phase: 04-durable-eval
plan: "02"
subsystem: batch-eval-reuse
tags: [gemini-sft, batch-eval, request-identity, gcs-artifacts]
requires:
  - phase: 04-01
    provides: singular durable eval_model config contract
provides:
  - shared Gemini request identity helpers
  - exact batch prediction metadata sidecar validation
  - fail-closed stale batch output reuse
affects: [gemini-sft-eval, batch-inference, online-inference]
tech-stack:
  added: []
  patterns:
    - request identity sidecars for reusable inference artifacts
    - exact identity validation for batch output prefixes
key-files:
  created:
    - model/src/common/gemini/request_identity.py
  modified:
    - model/src/common/gemini/batch.py
    - model/src/gemini_sft/target_execution.py
    - model/src/gemini_sft/evaluate.py
    - model/tests/gemini_sft/test_target_execution.py
    - model/tests/gemini_sft/test_workflow.py
key-decisions:
  - "Batch output reuse now requires batch_predictions.meta.json with exact request identity."
  - "Online prediction reuse delegates identity hashing and metadata parsing to common.gemini.request_identity while preserving prefix reuse."
patterns-established:
  - "Common Gemini modules own request identity helpers; gemini_sft modules consume them without creating reverse imports."
  - "Existing batch output without matching metadata raises before submit_fn is called."
requirements-completed: [DATA-01, DATA-06]
duration: 20 min
completed: 2026-06-28
---

# Phase 04 Plan 02: Batch Request Identity Summary

**Batch eval output reuse now fails closed unless `batch_predictions.meta.json` proves exact request identity**

## Performance

- **Duration:** 20 min
- **Started:** 2026-06-28T21:33:49Z
- **Completed:** 2026-06-28T21:53:49Z
- **Tasks:** 3 completed
- **Files modified:** 5

## Accomplishments

- Added `common.gemini.request_identity` for deterministic request identity, metadata payloads, hash validation, exact validation, and prefix validation.
- Updated online target execution to use the shared identity helpers without changing public online helper names.
- Added batch metadata path `evals/<label>/batch_predictions.meta.json`.
- Made existing batch output reuse require exact metadata before reading stale output or calling Vertex submit.
- Added workflow tests for missing metadata, mismatched metadata, exact reuse, and metadata upload on fresh submit.

## Task Commits

1. **Task 1: Extract shared request identity helpers** - `fba51186`
2. **Task 2: Add batch metadata paths and exact identity reuse** - `dd7c8101`
3. **Task 3: Cover batch reuse success and failure modes** - `dd7c8101`

## Files Created/Modified

- `model/src/common/gemini/request_identity.py` - Shared identity construction, hashing, metadata loading, and validation.
- `model/src/common/gemini/batch.py` - Batch metadata URI, exact reuse validation, and sidecar upload before submit.
- `model/src/gemini_sft/target_execution.py` - Online identity delegation to shared helpers.
- `model/src/gemini_sft/evaluate.py` - Batch identity parameters passed through to `run_batch_audio_inference`.
- `model/tests/gemini_sft/test_workflow.py` - Batch metadata regression coverage.

## Decisions Made

- Batch uses exact request identity only; smoke-prefix reuse remains online-only.
- Operational controls such as retry count and concurrency remain outside the identity.

## Deviations from Plan

The minimal singular durable loader change in `evaluate.py` moved into Wave 2 because Wave 1 removed `require_config_eval_models`, and `evaluate.py` needed to compile for Wave 2 tests. Wave 3 still owns the cleanup of the remaining one-target eval flow and stable summary uploads.

**Total deviations:** 1 sequencing deviation.
**Impact on plan:** Wave 2 acceptance criteria passed; Wave 3 scope is reduced, not expanded.

## Issues Encountered

- Existing workflow tests that intentionally reused batch output needed matching metadata fixtures.
- One metadata fixture initially referenced an undefined `config` variable; the fixture was corrected and tests were rerun.

## Verification

- `python3 -m py_compile model/src/common/gemini/request_identity.py model/src/common/gemini/batch.py model/src/gemini_sft/target_execution.py model/src/gemini_sft/evaluate.py`
- `safe-run -- bash -lc 'cd model && PYTHONPATH=src:tests python3 -m pytest tests/gemini_sft/test_workflow.py tests/gemini_sft/test_target_execution.py -q'`
- `rg -n "batch_predictions.meta.json|batch prediction metadata missing|batch prediction request identity mismatch|request_identity_hash|validate_prefix_identity" model/src model/tests/gemini_sft`

## Self-Check: PASSED

- Online identity behavior remains unchanged and still supports valid smoke-prefix resume.
- Batch output reuse requires exact request identity metadata.
- Existing batch output with missing or mismatched metadata fails before paid submit.
- Batch identity excludes operational retry, concurrency, log, and sync knobs.
- Batch metadata includes model, label, eval manifest, audio order, prompts, prior context settings, generation config, and safety settings.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Wave 3 can finish the one-target eval flow and add stable run-level GCS WER summaries using the safe batch reuse contract.

---
*Phase: 04-durable-eval*
*Completed: 2026-06-28*

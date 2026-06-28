---
phase: 02-target-config
plan: "01"
subsystem: gemini-sft-config
tags: [gemini-sft, config, eval-targets, inference-manifests, tests]

requires:
  - phase: 01-reporting-contract
    provides: Shared eval target row and artifact reporting semantics
provides:
  - Public artifact-label validator for normalized inference manifests
  - Offline eval target parser for explicit [[eval.models]] config
  - Durable eval_models serialization for resolved run config records
affects: [gemini-sft, eval-config, inference-manifests, target-config]

tech-stack:
  added: []
  patterns:
    - Frozen dataclass for unclassified eval model target strings
    - Shared artifact label validation reused by config parsing and manifest paths
    - Eval target requiredness split between load_run_config and load_eval_run_config

key-files:
  created:
    - .planning/phases/02-target-config/02-01-SUMMARY.md
  modified:
    - model/src/common/inference_manifest.py
    - model/src/gemini_sft/config.py
    - model/tests/common/tests/test_inference_manifest.py
    - model/tests/gemini_sft/test_config.py

key-decisions:
  - "Eval targets are accepted only from explicit [[eval.models]] tables, with no legacy base_model/endpoint synthesis."
  - "Target labels reuse the normalized inference manifest artifact-label validator."
  - "RunConfig stores eval_models in config.json only when targets are configured."

patterns-established:
  - "EvalModelTarget stays intentionally small: label plus unclassified model string only."
  - "load_eval_run_config requires eval targets while load_run_config validates them only when present."
  - "Config validation remains offline-only and does not inspect GCS or Vertex resources."

requirements-completed: [CFG-01, CFG-02, CFG-03, CFG-04, CFG-06]

duration: 5 min
completed: 2026-06-28
---

# Phase 02 Plan 01: Target Config Parser Summary

**Unified eval target config parsing with shared artifact-label safety and durable eval_models serialization.**

## Performance

- **Duration:** 5 min
- **Started:** 2026-06-28T17:55:08Z
- **Completed:** 2026-06-28T18:00:32Z
- **Tasks:** 3
- **Files modified:** 5

## Accomplishments

- Exposed `validate_artifact_label()` and reused it inside normalized inference manifest path construction.
- Added `EvalModelTarget`, `RunConfig.eval_models`, static `[[eval.models]]` parsing, duplicate-label detection, and config.json serialization.
- Covered eval target validation for required eval targets, valid base/checkpoint strings, invalid labels, duplicates, empty/non-string models, non-table entries, and unsupported fields.

## Task Commits

Each task was committed atomically:

1. **Task 1: Expose artifact-label validation** - `2c3f0448` (feat)
2. **Task 2: Parse and store eval model targets** - `dbd23751` (feat)
3. **Task 3: Cover target config validation with unit tests** - `c5f94080` (test)

**Plan metadata:** recorded in the final `docs(02-01)` commit

## Files Created/Modified

- `model/src/common/inference_manifest.py` - Adds public artifact-label validation and routes manifest path labels through it.
- `model/src/gemini_sft/config.py` - Adds eval target representation, parser validation, requiredness split, and `eval_models` serialization.
- `model/tests/common/tests/test_inference_manifest.py` - Covers valid and invalid artifact labels through the public helper.
- `model/tests/gemini_sft/test_config.py` - Covers explicit target config behavior and validation errors.
- `.planning/phases/02-target-config/02-01-SUMMARY.md` - Records plan execution, verification, and commits.

## Decisions Made

- Followed the Phase 2 decision that `CFG-04` migration compatibility is superseded: eval targets are not synthesized from `[sft].base_model` or durable `endpoint`.
- Kept `model` as an unclassified non-empty string so publisher models, tuned endpoints, and checkpoint endpoints remain backend-agnostic until Phase 3.
- Reused the manifest artifact-label validator for target labels so label safety and future manifest paths cannot drift.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Verification

- `python3 -m py_compile model/src/common/inference_manifest.py model/src/gemini_sft/config.py` passed.
- `safe-run -- bash -lc 'cd model && PYTHONPATH=src:tests python3 -m pytest tests/common/tests/test_inference_manifest.py tests/gemini_sft/test_config.py -q'` passed with `42 passed, 41 subtests passed in 0.16s`.
- `rg -n "base_model/endpoint|\\[\\[eval\\.models\\]\\]|eval_models" model/src/gemini_sft/config.py model/tests/gemini_sft/test_config.py` returned matches in both implementation and tests.

## Next Phase Readiness

Ready for `02-02`: durable `config.json` eval target guard and early eval failure before paid Vertex work.

## Self-Check: PASSED

- Summary file exists at `.planning/phases/02-target-config/02-01-SUMMARY.md`.
- Task commits found in git history: `2c3f0448`, `dbd23751`, `c5f94080`.
- No tracked file deletions were introduced by task commits.

---
*Phase: 02-target-config*
*Completed: 2026-06-28*

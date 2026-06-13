---
phase: 01-contract-manifest-helpers
plan: "01"
subsystem: model-manifest-helpers
tags: [python, canonical-manifest, validation, testing]

requires: []
provides:
  - Strict Canonical Manifest validation helper returning structured issues
  - Fail-loud Canonical Manifest validation wrapper
  - Public logical row identity helper for example_id and segment_id
  - rows_from_manifest required-field failures for audio_filepath and text
affects:
  - phase-02-packaged-consumers
  - gemini-sft-manifest-validation
  - prediction-join-identity

tech-stack:
  added: []
  patterns:
    - Frozen dataclass for structured validation issues
    - Centralized strict validation separate from lenient manifest loading
    - TDD RED/GREEN commits for helper behavior

key-files:
  created:
    - .planning/phases/01-contract-manifest-helpers/01-01-SUMMARY.md
  modified:
    - model/src/common/manifest.py
    - model/tests/common/tests/test_manifest.py

key-decisions:
  - "validate_canonical_manifest(...) is the only strict Canonical Manifest semantic validator."
  - "require_canonical_manifest(...) remains a thin aggregation wrapper over validate_canonical_manifest(...)."
  - "load_manifest() stays lenient; rows_from_manifest() only fails loudly for missing or blank audio_filepath/text."

patterns-established:
  - "CanonicalManifestIssue carries code, message, row_index, and field for future packaged consumers."
  - "canonical_row_identity(...) is the shared public identity helper for raw rows and CanonicalRow instances."
  - "Unknown row fields, unknown metadata keys, and pred_text_* fields are tolerated by strict validation."

requirements-completed:
  - HELP-01
  - HELP-02
  - HELP-03
  - HELP-05

duration: 6 min
completed: 2026-06-13
---

# Phase 01 Plan 01: Strict Helper APIs and Focused Tests Summary

**Canonical Manifest validation helpers with structured issues, identity lookup, and fail-loud compatibility row conversion.**

## Performance

- **Duration:** 6 min
- **Started:** 2026-06-13T19:32:56Z
- **Completed:** 2026-06-13T19:39:09Z
- **Tasks:** 2 completed
- **Files modified:** 2 source/test files

## Accomplishments

- Added `CanonicalManifestIssue`, `validate_canonical_manifest(...)`, `require_canonical_manifest(...)`, and `canonical_row_identity(...)` to `common.manifest`.
- Covered strict row validation for required fields, GCS FLAC audio, numeric offset, positive duration, duplicate identity, duplicate audio URI, split mismatch, shallow metadata typing, and unknown-field tolerance.
- Tightened `rows_from_manifest()` to raise `ValueError` for missing or blank `audio_filepath` or `text`, while keeping `load_manifest()` lenient and preserving offset/duration/default identity compatibility.

## Task Commits

Each TDD gate was committed atomically:

1. **Task 1 RED: strict helper tests** - `ed1c1a29` (test)
2. **Task 1 GREEN: strict helper implementation** - `ccacfafb` (feat)
3. **Task 2 RED: row conversion strictness tests** - `e6aa90b7` (test)
4. **Task 2 GREEN: row conversion implementation** - `591aca2c` (feat)

## Files Created/Modified

- `model/src/common/manifest.py` - Added strict canonical validation helpers, shallow metadata checks, fail-loud wrapper formatting, and required-field row conversion errors.
- `model/tests/common/tests/test_manifest.py` - Added focused unittest coverage for strict validation, public issue shape, identity helper behavior, lenient parsing boundaries, and `rows_from_manifest()` failures.
- `.planning/phases/01-contract-manifest-helpers/01-01-SUMMARY.md` - Captures plan execution outcome.

## Decisions Made

- Followed the plan decision to keep strict validation centralized in `validate_canonical_manifest(...)`.
- Kept `require_canonical_manifest(...)` as a wrapper that only formats validator output.
- Kept `load_manifest()` and full strict validation separate; `rows_from_manifest()` only tightened missing/blank `audio_filepath` and `text`.

## Deviations from Plan

None - plan executed exactly as written.

**Total deviations:** 0 auto-fixed.
**Impact on plan:** No scope changes.

## Issues Encountered

None. TDD RED failures were expected and committed before GREEN implementation.

## Verification

- `safe-run -- uv run --project model --extra dev pytest model/tests/common/tests/test_manifest.py -q` -> `29 passed in 0.02s`
- `git diff --check -- model/src/common/manifest.py model/tests/common/tests/test_manifest.py` -> passed with no output
- `rg -n "validate_canonical_manifest|require_canonical_manifest|canonical_row_identity|CanonicalManifestIssue" model/src/common/manifest.py model/tests/common/tests/test_manifest.py` -> found the new public API in source and tests

## Known Stubs

None. Stub-pattern scan only found internal accumulators and explicit negative-test values.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

The shared manifest helper surface is ready for Phase 2 packaged consumer wiring and identity-aware prediction join work. In worktree mode, shared `STATE.md`, `ROADMAP.md`, and `REQUIREMENTS.md` updates were intentionally left to the orchestrator.

## Self-Check: PASSED

- Found `.planning/phases/01-contract-manifest-helpers/01-01-SUMMARY.md`.
- Found commits `ed1c1a29`, `ccacfafb`, `e6aa90b7`, and `591aca2c`.

---
*Phase: 01-contract-manifest-helpers*
*Completed: 2026-06-13*

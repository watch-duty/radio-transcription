---
phase: 04-audio-derivation-and-provenance
plan: 03
subsystem: model-artifacts
tags: [python, audio, dataset-versioning, validation, canonical, tdd]

# Dependency graph
requires:
  - phase: 04-audio-derivation-and-provenance
    provides: Phase 4 audio enrichment and model-ready publication boundary
provides:
  - Post-audio validation for populated gs:// model-ready audio URIs
  - Transformation action and D-26 provenance metadata hard gate
  - Canonical JSONL validation before train/eval manifest serialization
affects: [04-audio-derivation-and-provenance, 05-cli-reports-docs-and-verification]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - Model-ready audio validation remains separate from split integrity
    - Canonical builders validate split safety and audio provenance before JSONL serialization
    - Tests encode RED/GREEN provenance boundary behavior

key-files:
  created: []
  modified:
    - model/scripts/sft/dataset_split/leakage.py
    - model/scripts/sft/dataset_split/canonical.py
    - model/scripts/sft/tests/test_dataset_split_leakage.py
    - model/scripts/sft/tests/test_dataset_canonical.py

key-decisions:
  - "validate_model_ready_audio() is a hard post-audio gate layered after validate_split_integrity(), not a replacement for leakage validation."
  - "Canonical rows preserve original/source audio fields and model-ready/derived audio provenance, but canonical manifest builders refuse to serialize incomplete Phase 4 rows."

patterns-established:
  - "Phase 4 provenance validation reports row_index, action when known, and the invalid field."
  - "D-24 is enforced directly: derived_audio_uri is required only for derived rows and forbidden for reused, copied, and transcoded rows."

requirements-completed: [AUD-06]

# Metrics
duration: 6 min
completed: 2026-05-28
---

# Phase 04 Plan 03: Model-Ready Provenance Validation Summary

**Canonical JSONL hard gate for gs:// model-ready audio and complete Phase 4 transformation provenance**

## Performance

- **Duration:** 6 min
- **Started:** 2026-05-28T04:17:45Z
- **Completed:** 2026-05-28T04:23:12Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments

- Added failing tests for required model-ready GCS audio, action vocabulary, D-26 metadata keys, D-24 derived URI semantics, and post-enrichment model-ready overlap leakage.
- Added canonical tests proving manifests reject missing model-ready audio and preserve original audio, model-ready audio, derived audio, and transformation metadata.
- Implemented `validate_model_ready_audio()` with row-context errors and wired it into `canonical_rows()`, `canonical_manifests()`, and `per_dataset_manifests()` after split integrity validation.

## TDD Gate Compliance

- **RED:** `c50f48f` added post-audio validation and canonical tests. Focused unittest failed as expected because `validate_model_ready_audio()` did not exist and canonical builders did not reject missing model-ready audio.
- **GREEN:** `7cddf16` implemented model-ready validation and canonical integration. Focused unittest passed with `23 tests`.
- **REFACTOR:** No separate refactor commit was needed.

## Task Commits

Each task was committed atomically:

1. **Task 1: Add post-audio validation and canonical tests** - `c50f48f` (test)
2. **Task 2: Implement model-ready audio validation and canonical integration** - `7cddf16` (feat)

**Plan metadata:** committed separately after this summary.

## Files Created/Modified

- `model/scripts/sft/dataset_split/leakage.py` - Adds `AUDIO_ACTIONS`, `TRANSFORMATION_METADATA_KEYS`, and `validate_model_ready_audio()`.
- `model/scripts/sft/dataset_split/canonical.py` - Calls model-ready validation before canonical JSONL row/manifests serialization.
- `model/scripts/sft/tests/test_dataset_split_leakage.py` - Covers model-ready URI, provenance metadata, action vocabulary, derived URI, and overlap validation.
- `model/scripts/sft/tests/test_dataset_canonical.py` - Covers canonical hard gate and preservation of original plus model-ready audio provenance.

## Decisions Made

- Kept empty `model_ready_audio_uri` ignored by the older `validate_split_leakage()` pre-enrichment check, and added the new hard post-enrichment validator instead.
- Required `transformation_metadata["split"]` and `transformation_metadata["source_group"]` to match the segment row before serialization.
- Enforced D-24 based on `transformation_metadata.action` rather than inferring from URI shape alone.

## Verification

- `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model python -m py_compile model/scripts/sft/tests/test_dataset_split_leakage.py model/scripts/sft/tests/test_dataset_canonical.py` - passed.
- `rg -n "test_model_ready_audio_uri_is_required" model/scripts/sft/tests/test_dataset_split_leakage.py` - passed.
- `rg -n "test_canonical_manifests_require_model_ready_audio" model/scripts/sft/tests/test_dataset_canonical.py` - passed.
- RED focused unittest before implementation - failed as expected with missing `validate_model_ready_audio()` and no canonical missing-audio exception.
- `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model python -m unittest model.scripts.sft.tests.test_dataset_split_leakage model.scripts.sft.tests.test_dataset_canonical` - passed, `23 tests`.
- `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model python -m unittest discover model/scripts/sft/tests` - passed, `159 tests`.
- Acceptance scans for `AUDIO_ACTIONS`, `TRANSFORMATION_METADATA_KEYS`, `startswith("gs://")`, `derived_audio_uri`, `validate_model_ready_audio`, required test names, and all D-26 metadata key strings passed.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## Known Stubs

None. Stub scan matches were limited to intentional optional test helper parameters and negative tests for blank or missing `model_ready_audio_uri`.

## Threat Flags

None. This plan added no subprocess, network, file-access, auth, endpoint, or schema surfaces; it only validates in-memory enriched segment rows before serialization.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Plan 04-04 can rely on canonical artifacts only being generated after model-ready audio and transformation provenance have been validated.

## Self-Check: PASSED

- Found `.planning/phases/04-audio-derivation-and-provenance/04-03-SUMMARY.md`.
- Found `model/scripts/sft/dataset_split/leakage.py`.
- Found `model/scripts/sft/dataset_split/canonical.py`.
- Found task commit `c50f48f`.
- Found task commit `7cddf16`.

---
*Phase: 04-audio-derivation-and-provenance*
*Completed: 2026-05-28*

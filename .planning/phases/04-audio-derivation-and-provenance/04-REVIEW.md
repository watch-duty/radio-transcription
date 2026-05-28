---
phase: 04-audio-derivation-and-provenance
reviewed: 2026-05-28T05:12:30Z
depth: standard
files_reviewed: 8
files_reviewed_list:
  - model/scripts/sft/dataset_split/audio.py
  - model/scripts/sft/dataset_split/publisher.py
  - model/scripts/sft/dataset_split/leakage.py
  - model/scripts/sft/dataset_split/artifacts.py
  - model/scripts/sft/dataset_split/canonical.py
  - model/scripts/sft/tests/test_audio_derivation.py
  - model/scripts/sft/tests/test_dataset_publisher.py
  - model/scripts/sft/tests/test_dataset_split_leakage.py
findings:
  blocker: 0
  critical: 0
  warning: 0
  info: 0
  total: 0
status: clean
---

# Phase 04: Code Review Report

**Reviewed:** 2026-05-28T05:12:30Z
**Depth:** standard focused re-review
**Files Reviewed:** 8
**Status:** clean

## Summary

Re-reviewed Phase 04 after the latest fixes, focused on the prior findings in this report: publication ordering, external URL SSRF boundary, generated output duration tolerance, reused provenance semantics, and empty version output.

All prior blocker and warning findings are resolved in the current source. No new blocker or warning findings were found in the focused scope.

## Prior Finding Status

### Publication ordering

**Status:** Resolved.

`publish_dataset_version_artifacts()` now calls the audio preparer with `upload=False` before artifact generation, preserving local prepared audio tasks without final-prefix audio uploads (`model/scripts/sft/dataset_split/publisher.py:131`). It builds canonical manifests, per-dataset manifests, model inputs, metadata, reports, Markdown, and JSON serializations before calling the audio uploader (`model/scripts/sft/dataset_split/publisher.py:143`, `model/scripts/sft/dataset_split/publisher.py:190`, `model/scripts/sft/dataset_split/publisher.py:203`). The regression test covers a serialization/planning failure and confirms audio upload is not reached (`model/scripts/sft/tests/test_dataset_publisher.py:592`).

### External URL SSRF boundary

**Status:** Resolved.

External source staging now requires HTTPS on the default port, restricts hosts to `ALLOWED_EXTERNAL_AUDIO_HOSTS`, rejects any resolved address that is not global, disables redirects, streams with a timeout, and enforces a maximum byte count (`model/scripts/sft/dataset_split/audio.py:43`, `model/scripts/sft/dataset_split/audio.py:531`, `model/scripts/sft/dataset_split/audio.py:731`). The focused tests cover plain HTTP rejection, unknown host rejection, private address rejection, streaming controls, and byte limits (`model/scripts/sft/tests/test_audio_derivation.py:856`, `model/scripts/sft/tests/test_audio_derivation.py:900`, `model/scripts/sft/tests/test_audio_derivation.py:909`, `model/scripts/sft/tests/test_audio_derivation.py:924`).

### Generated output duration tolerance

**Status:** Resolved.

Generated FLAC outputs now use a separate generated-output tolerance instead of the source-bounds tolerance (`model/scripts/sft/dataset_split/audio.py:35`, `model/scripts/sft/dataset_split/audio.py:718`, `model/scripts/sft/dataset_split/audio.py:786`). The regression test verifies a 0.4 second generated-output mismatch fails before upload (`model/scripts/sft/tests/test_audio_derivation.py:720`).

### Reused provenance semantics

**Status:** Resolved.

`validate_model_ready_audio()` now rejects `action="reused"` unless `model_ready_audio_uri` equals the segment source `audio_uri` (`model/scripts/sft/dataset_split/leakage.py:122`). Publisher test fixtures now model reused rows with the source URI as the model-ready URI, and leakage tests cover both accepted and stale reused URI cases (`model/scripts/sft/tests/test_dataset_publisher.py:299`, `model/scripts/sft/tests/test_dataset_split_leakage.py:304`).

### Empty version output

**Status:** Resolved.

`_program_version()` now treats empty stdout as `"unavailable"` instead of indexing an empty splitlines list (`model/scripts/sft/dataset_split/audio.py:685`). The regression test covers successful command execution with empty stdout (`model/scripts/sft/tests/test_audio_derivation.py:750`).

## Verification

- `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model python -m unittest model.scripts.sft.tests.test_audio_derivation model.scripts.sft.tests.test_dataset_publisher model.scripts.sft.tests.test_dataset_split_leakage` - passed, `Ran 55 tests`, `OK`.
- `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model python -m py_compile model/scripts/sft/dataset_split/audio.py model/scripts/sft/dataset_split/publisher.py model/scripts/sft/dataset_split/leakage.py model/scripts/sft/tests/test_audio_derivation.py model/scripts/sft/tests/test_dataset_publisher.py model/scripts/sft/tests/test_dataset_split_leakage.py` - passed.

---

_Reviewed: 2026-05-28T05:12:30Z_
_Reviewer: the agent (gsd-code-reviewer)_
_Depth: standard focused re-review_

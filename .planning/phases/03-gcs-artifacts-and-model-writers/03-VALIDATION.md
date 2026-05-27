---
phase: 03
slug: gcs-artifacts-and-model-writers
status: draft
nyquist_compliant: true
wave_0_complete: false
created: 2026-05-27
---

# Phase 03 - Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

## Test Infrastructure

| Property | Value |
|----------|-------|
| Framework | `pytest` for model/SFT tests |
| Config file | `model/pyproject.toml` |
| Quick run command | `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model --extra dev --extra scoring --extra vertex pytest model/scripts/sft/tests/test_dataset_artifacts.py model/scripts/sft/tests/test_model_writers.py -q` |
| Full suite command | `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model --extra dev --extra scoring --extra vertex --extra optimizer pytest model/scripts/sft/tests model/colabs/common/tests -q` |
| Estimated runtime | Unknown until Phase 3 tests are added; executor must report observed runtime |

## Sampling Rate

- After every task commit: run the narrowest new test file covering the touched artifact or writer.
- After every plan wave: run `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model --extra dev --extra scoring --extra vertex --extra optimizer pytest model/scripts/sft/tests model/colabs/common/tests -q`.
- Before `$gsd-verify-work`: full model/SFT suite must be green.
- Max feedback latency: no more than one task may land without an automated test command or an explicit production-surfaced error path documented in that task.

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 03-01-01 | 03-01 | 0 | ARTF-01 | T-03-01 | Dataset-version root resolves to `gs://wd-transcription-data/sft/{dataset_version_id}/` | unit | `pytest model/scripts/sft/tests/test_dataset_artifacts.py::test_layout_uses_dataset_version_root -q` | No, W0 | pending |
| 03-01-02 | 03-01 | 0 | ARTF-02, TEST-06 | T-03-01 | Existing prefix and create-only precondition failures abort generation | unit | `pytest model/scripts/sft/tests/test_dataset_artifacts.py::test_existing_prefix_fails -q` | No, W0 | pending |
| 03-02-01 | 03-02 | 0 | ARTF-03 | T-03-02 | Canonical train/eval manifests include enriched fields and omit `raw_row` | unit | `pytest model/scripts/sft/tests/test_dataset_canonical.py -q` | No, W0 | pending |
| 03-02-02 | 03-02 | 0 | ARTF-04 | T-03-02 | Per-dataset train/eval slices group by `dataset_name` and `split` without recomputing split | unit | `pytest model/scripts/sft/tests/test_dataset_canonical.py::test_per_dataset_slices -q` | No, W0 | pending |
| 03-02-03 | 03-02 | 0 | ARTF-05 | T-03-03 | Dataset-version reports include config, leakage, balance, warnings, and artifact inventory only | unit | `pytest model/scripts/sft/tests/test_dataset_reports.py -q` | No, W0 | pending |
| 03-02-04 | 03-02 | 0 | ARTF-06, MODL-08 | T-03-04 | Generation targets only new dataset-version artifacts and does not modify benchmark/eval manifests | regression | `pytest model/scripts/sft/tests/test_dataset_artifacts.py::test_generation_targets_only_new_artifacts -q` | No, W0 | pending |
| 03-03-01 | 03-03 | 0 | MODL-01, MODL-02 | T-03-02 | NeMo rows emit `audio_filepath`, `text`, `duration`, and `offset`; config points at train/eval manifests | unit | `pytest model/scripts/sft/tests/test_model_writers.py::test_nemo_writer_shape -q` | No, W0 | pending |
| 03-03-02 | 03-03 | 0 | MODL-03, MODL-04 | T-03-02 | Whisper rows preserve metadata and report examples over 30 seconds as warnings | unit | `pytest model/scripts/sft/tests/test_model_writers.py::test_whisper_writer_shape_and_warnings -q` | No, W0 | pending |
| 03-04-01 | 03-04 | 0 | MODL-05, MODL-06, MODL-07 | T-03-02 | Gemini JSONL uses nested SFT shape, truthful MIME type, and configurable base model/tuning config | unit | `pytest model/scripts/sft/tests/test_model_writers.py::test_gemini_writer_shape_and_config -q` | No, W0 | pending |

## Wave 0 Requirements

- [ ] `model/scripts/sft/tests/test_dataset_artifacts.py` covers ARTF-01, ARTF-02, ARTF-06, MODL-08, and TEST-06.
- [ ] `model/scripts/sft/tests/test_dataset_canonical.py` covers ARTF-03 and ARTF-04.
- [ ] `model/scripts/sft/tests/test_dataset_reports.py` covers ARTF-05 and D-18/D-19 report boundaries.
- [ ] `model/scripts/sft/tests/test_model_writers.py` covers MODL-01 through MODL-07 and TEST-05.
- [ ] Existing `model/colabs/common/tests/test_sft.py` is updated if Gemini MIME support changes `common.sft`.

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Live GCS write to `gs://wd-transcription-data/sft/{dataset_version_id}/` | ARTF-01, ARTF-02 | Live bucket credentials are not required for unit execution | Optional only: run a dry generation against a disposable dataset-version ID after ADC is configured, then verify objects appear under one prefix and a second run fails fast |

## Validation Sign-Off

- [x] All planned behaviors have an automated test target or a documented optional live-GCS manual check.
- [x] Sampling continuity: no 3 consecutive tasks without automated verification.
- [x] Wave 0 covers missing test files before implementation tasks rely on them.
- [x] No watch-mode flags.
- [x] Feedback latency is bounded by per-task pytest commands.
- [x] `nyquist_compliant: true` set in frontmatter.

**Approval:** pending

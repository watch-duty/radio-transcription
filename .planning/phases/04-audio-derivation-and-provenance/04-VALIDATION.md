---
phase: 04
slug: audio-derivation-and-provenance
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-05-28
---

# Phase 04 - Validation Strategy

Per-phase validation contract for feedback sampling during execution.

## Test Infrastructure

| Property | Value |
|----------|-------|
| Framework | `unittest`; `pytest` is available but current SFT script tests use `unittest` |
| Config file | Root `pyproject.toml`; SFT tests self-manage import paths |
| Quick run command | `uv run python -m unittest model.scripts.sft.tests.test_audio_derivation` |
| Full suite command | `uv run python -m unittest discover model/scripts/sft/tests` |
| Estimated runtime | Under 60 seconds for focused tests; full suite runtime to be measured during execution |

## Sampling Rate

- After every task commit: run the focused test for the changed module.
- After every plan wave: run `uv run python -m unittest discover model/scripts/sft/tests`.
- Before `$gsd-verify-work`: full SFT script test suite must be green.
- Max feedback latency target: under 60 seconds for focused tests.

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 04-01-01 | 01 | 0 | AUD-01 | T-04-01 | Standalone supported `gs://` clips are reused without upload or transformation | unit | `uv run python -m unittest model.scripts.sft.tests.test_audio_derivation` | no, W0 | pending |
| 04-01-02 | 01 | 0 | AUD-02 | T-04-02 | Longer sources and positive offsets derive bounded clips only after source-duration validation | unit | `uv run python -m unittest model.scripts.sft.tests.test_audio_derivation` | no, W0 | pending |
| 04-01-03 | 01 | 0 | AUD-03 | T-04-03 | Generated model-ready audio defaults to FLAC and remains writer-supported | unit | `uv run python -m unittest model.scripts.sft.tests.test_audio_derivation model.scripts.sft.tests.test_model_writers` | partial, W0 | pending |
| 04-01-04 | 01 | 0 | AUD-04 | T-04-04 | Derived/transcoded multichannel input is mixed to mono | subprocess fixture | `uv run python -m unittest model.scripts.sft.tests.test_audio_derivation` | no, W0 | pending |
| 04-01-05 | 01 | 0 | AUD-05 | T-04-04 | Default FFmpeg command does not pad and does not pass `-ar` | unit + subprocess fixture | `uv run python -m unittest model.scripts.sft.tests.test_audio_derivation` | no, W0 | pending |
| 04-02-01 | 02 | 1 | AUD-06 | T-04-05 | Every published segment has non-empty `gs://` `model_ready_audio_uri` and transformation metadata | unit | `uv run python -m unittest model.scripts.sft.tests.test_dataset_canonical model.scripts.sft.tests.test_dataset_split_leakage` | partial | pending |
| 04-02-02 | 02 | 1 | AUD-06 | T-04-03 | Binary audio uploads use create-only GCS preconditions | unit | `uv run python -m unittest model.scripts.sft.tests.test_dataset_artifacts` | exists, update | pending |
| 04-02-03 | 02 | 1 | AUD-06 | T-04-03 | Publisher checks dataset-version prefix absence once before audio and text uploads | unit | `uv run python -m unittest model.scripts.sft.tests.test_dataset_publisher` | exists, update | pending |
| 04-03-01 | 03 | 2 | AUD-06 | T-04-05 | NeMo, Whisper, and Gemini writers require `model_ready_audio_uri` and never fall back to `audio_uri` | unit | `uv run python -m unittest model.scripts.sft.tests.test_model_writers` | exists, update | pending |
| 04-03-02 | 03 | 2 | AUD-06 | T-04-06 | Reports summarize audio actions and transformation provenance | unit | `uv run python -m unittest model.scripts.sft.tests.test_dataset_reports` | exists, update | pending |

## Wave 0 Requirements

- [ ] `model/scripts/sft/tests/test_audio_derivation.py` covers AUD-01 through AUD-05.
- [ ] `model/scripts/sft/tests/test_model_writers.py` covers required `model_ready_audio_uri` writer behavior.
- [ ] `model/scripts/sft/tests/test_dataset_publisher.py` covers one prefix absence check before audio and text publication.
- [ ] `model/scripts/sft/tests/test_dataset_reports.py` covers audio action/provenance summaries.

## Manual-Only Verifications

All Phase 4 behaviors have automated verification planned. Real GCS and real external-source publication remain outside the required Phase 4 test gate; use fake clients and local audio fixtures.

## Validation Sign-Off

- [ ] All tasks have automated verify commands or Wave 0 dependencies.
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify.
- [ ] Wave 0 covers missing test files and changed test contracts.
- [ ] No watch-mode flags.
- [ ] Feedback latency under target for focused tests.
- [ ] `nyquist_compliant: true` set in frontmatter when execution verifies coverage.

**Approval:** pending

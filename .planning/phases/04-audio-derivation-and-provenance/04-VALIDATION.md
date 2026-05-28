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
| 04-01-01 | 01 | 0 | AUD-01..AUD-06 | T-04-01..T-04-06 | Test contracts cover action planning, safe subprocess argv, bounded external download, binary create-only upload, action paths, and provenance enrichment before implementation | test scaffold | `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model python -m py_compile model/scripts/sft/tests/test_audio_derivation.py model/scripts/sft/tests/test_dataset_artifacts.py`; `rg -n "class TestAudioActionPlanning" model/scripts/sft/tests/test_audio_derivation.py`; `rg -n "test_upload_file_create_only_uses_generation_precondition" model/scripts/sft/tests/test_dataset_artifacts.py` | new/update | pending |
| 04-01-02 | 01 | 0 | AUD-01..AUD-05 | T-04-01..T-04-04 | Audio actions are planned only after safe staging/probing, and create-only binary upload/path helpers are available | unit | `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model python -m unittest model.scripts.sft.tests.test_audio_derivation model.scripts.sft.tests.test_dataset_artifacts` | new/update | pending |
| 04-01-03 | 01 | 0 | AUD-01..AUD-06 | T-04-01..T-04-06 | Audio is materialized with no shell execution, no default resampling/padding, and enriched model-ready provenance | unit | `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model python -m unittest model.scripts.sft.tests.test_audio_derivation model.scripts.sft.tests.test_dataset_artifacts` | new/update | pending |
| 04-02-01 | 02 | 1 | AUD-03, AUD-06 | T-04-03, T-04-05, T-04-06 | Test contracts require model writers to use model-ready GCS audio and publisher to prepare audio before final text artifacts | test scaffold | `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model python -m py_compile model/scripts/sft/tests/test_model_writers.py model/scripts/sft/tests/test_dataset_publisher.py`; `rg -n "test_nemo_requires_model_ready_audio_uri" model/scripts/sft/tests/test_model_writers.py`; `rg -n "test_publish_prepares_audio_before_text_artifacts" model/scripts/sft/tests/test_dataset_publisher.py` | update | pending |
| 04-02-02 | 02 | 1 | AUD-03, AUD-06 | T-04-05 | NeMo, Whisper, and Gemini writers require `model_ready_audio_uri` and never fall back to `audio_uri` | unit | `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model python -m unittest model.scripts.sft.tests.test_model_writers` | update | pending |
| 04-02-03 | 02 | 1 | AUD-06 | T-04-03, T-04-06 | Publisher checks dataset-version prefix absence exactly once before audio and text uploads, then publishes enriched segments | unit | `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model python -m unittest model.scripts.sft.tests.test_dataset_publisher model.scripts.sft.tests.test_model_writers` | update | pending |
| 04-03-01 | 03 | 2 | AUD-06 | T-04-05, T-04-06 | Test contracts require populated model-ready audio, action vocabulary, derived URI rules, and canonical provenance preservation | test scaffold | `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model python -m py_compile model/scripts/sft/tests/test_dataset_split_leakage.py model/scripts/sft/tests/test_dataset_canonical.py`; `rg -n "test_model_ready_audio_uri_is_required" model/scripts/sft/tests/test_dataset_split_leakage.py`; `rg -n "test_canonical_manifests_require_model_ready_audio" model/scripts/sft/tests/test_dataset_canonical.py` | update | pending |
| 04-03-02 | 03 | 2 | AUD-06 | T-04-05, T-04-06 | Canonical JSONL generation hard-fails unless rows are split-safe, model-ready, and auditable after audio enrichment | unit | `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model python -m unittest model.scripts.sft.tests.test_dataset_split_leakage model.scripts.sft.tests.test_dataset_canonical` | update | pending |
| 04-04-01 | 04 | 2 | AUD-06 | T-04-05, T-04-06 | Test contracts require JSON and Markdown reports to expose audio action counts and provenance completeness | test scaffold | `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model python -m py_compile model/scripts/sft/tests/test_dataset_reports.py`; `rg -n "test_report_includes_audio_transformation_summary" model/scripts/sft/tests/test_dataset_reports.py`; `rg -n "test_report_rejects_missing_audio_transformation_metadata" model/scripts/sft/tests/test_dataset_reports.py`; `rg -n "test_markdown_includes_audio_transformation_summary" model/scripts/sft/tests/test_dataset_reports.py` | update | pending |
| 04-04-02 | 04 | 2 | AUD-06 | T-04-05, T-04-06 | Dataset reports summarize model-ready audio actions and provenance without raw subprocess output | unit | `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model python -m unittest model.scripts.sft.tests.test_dataset_reports` | update | pending |

## Test-First Requirements

- [ ] Plan 04-01 Task 1 creates/updates audio derivation and artifact tests for AUD-01 through AUD-06 before implementation tasks run full unit tests.
- [ ] Plan 04-02 Task 1 creates/updates writer and publisher boundary tests before implementation tasks run full unit tests.
- [ ] Plan 04-03 Task 1 creates/updates post-audio validation and canonical tests before implementation runs full unit tests.
- [ ] Plan 04-04 Task 1 creates/updates report provenance tests before implementation runs full unit tests.

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

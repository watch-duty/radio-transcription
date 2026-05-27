---
phase: 01
slug: manifest-and-source-identity
status: draft
nyquist_compliant: true
wave_0_complete: true
created: 2026-05-27
---

# Phase 1 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest via Python 3.13 |
| **Config file** | `pyproject.toml` |
| **Quick run command** | `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_version_config.py model/scripts/sft/tests/test_dataset_version_gcs_io.py model/scripts/sft/tests/test_dataset_version_source_keys.py model/scripts/sft/tests/test_dataset_version_normalize.py model/scripts/sft/tests/test_dataset_version_validate.py -q` |
| **Full suite command** | `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_version_config.py model/scripts/sft/tests/test_dataset_version_gcs_io.py model/scripts/sft/tests/test_dataset_version_source_keys.py model/scripts/sft/tests/test_dataset_version_normalize.py model/scripts/sft/tests/test_dataset_version_validate.py -q` |
| **Estimated runtime** | ~5 seconds |

---

## Sampling Rate

- **After every task commit:** Run the plan-specific pytest command from that task.
- **After every plan wave:** Run the full Phase 1 suite.
- **Before `$gsd-verify-work`:** Full Phase 1 suite must be green.
- **Max feedback latency:** 10 seconds.

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 01-01-01 | 01 | 1 | INPT-01 | T-01-01 | Config rejects unsafe/non-GCS input paths | unit | `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_version_config.py -q` | ✅ W0 | ⬜ pending |
| 01-01-02 | 01 | 1 | INPT-02, INPT-03 | T-01-02 | GCS loader fails fast on missing/malformed configured inputs | unit | `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_version_gcs_io.py -q` | ✅ W0 | ⬜ pending |
| 01-02-01 | 02 | 2 | SRC-01, SRC-02, SRC-03, SRC-04, SRC-05 | T-01-03 | Ambiguous source identity fails rather than guessing | unit | `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_version_source_keys.py -q` | ✅ W0 | ⬜ pending |
| 01-02-02 | 02 | 2 | INPT-04 | T-01-04 | Empty labels are excluded, not sent to SFT writers | unit | `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_version_normalize.py -q` | ✅ W0 | ⬜ pending |
| 01-03-01 | 03 | 3 | SRC-06, TEST-01 | T-01-05 | All extractor families have valid/missing/ambiguous test coverage | unit | `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_version_validate.py -q` | ✅ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Existing pytest infrastructure covers all Phase 1 requirements. No framework installation or manual fixture setup is required.

---

## Manual-Only Verifications

All Phase 1 behaviors have automated verification. Production GCS access is intentionally not required by tests; tests use fake readers to cover success and failure classes.

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies.
- [x] Sampling continuity: no 3 consecutive tasks without automated verify.
- [x] Wave 0 covers all MISSING references.
- [x] No watch-mode flags.
- [x] Feedback latency < 10 seconds.
- [x] `nyquist_compliant: true` set in frontmatter.

**Approval:** approved 2026-05-27

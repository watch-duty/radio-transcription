---
phase: 02
slug: split-engine-and-leakage-gates
status: draft
nyquist_compliant: true
wave_0_complete: true
created: 2026-05-27
---

# Phase 02 - Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest |
| **Config file** | root `pyproject.toml`; model package `model/pyproject.toml` |
| **Quick run command** | `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_split_split.py model/scripts/sft/tests/test_dataset_split_leakage.py model/scripts/sft/tests/test_dataset_split_balance.py -q` |
| **Full suite command** | `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests -q` |
| **Estimated runtime** | ~1 second |

---

## Sampling Rate

- **After every task commit:** Run the plan-specific pytest command.
- **After every plan wave:** Run `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests -q`.
- **Before `$gsd-verify-work`:** Full SFT test suite must be green.
- **Max feedback latency:** 10 seconds.

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 02-01-01 | 01 | 1 | SPLT-02, TEST-02 | T-02-01 | Config does not retain unused seed contract | unit | `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_split_config.py -q` | yes | pending |
| 02-01-02 | 01 | 1 | SPLT-01, SPLT-06, SPLT-07, TEST-02 | T-02-02 | Source Groups are assigned wholly to one split with per-dataset coverage | unit | `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_split_split.py -q` | no | pending |
| 02-02-01 | 02 | 2 | SPLT-03, SPLT-04, SPLT-05, TEST-03 | T-02-03 | Cross-split exact overlaps fail | unit | `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_split_leakage.py -q` | no | pending |
| 02-02-02 | 02 | 2 | TEST-03 | T-02-04 | Duplicate labeled audio spans fail within a split | unit | `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_split_leakage.py -q` | no | pending |
| 02-03-01 | 03 | 3 | SPLT-07, SPLT-08, TEST-04 | T-02-05 | Balance report exposes weighted score and component deltas | unit | `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_split_balance.py -q` | no | pending |
| 02-03-02 | 03 | 3 | TEST-02, TEST-03, TEST-04 | - | Full Phase 2 suite passes without network or audio downloads | regression | `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests -q` | yes | pending |

---

## Wave 0 Requirements

Existing infrastructure covers all phase requirements.

---

## Manual-Only Verifications

All phase behaviors have automated verification.

---

## Validation Sign-Off

- [x] All tasks have automated verify commands.
- [x] Sampling continuity: no 3 consecutive tasks without automated verify.
- [x] Wave 0 covers all missing references.
- [x] No watch-mode flags.
- [x] Feedback latency < 10 seconds.
- [x] `nyquist_compliant: true` set in frontmatter.

**Approval:** approved 2026-05-27

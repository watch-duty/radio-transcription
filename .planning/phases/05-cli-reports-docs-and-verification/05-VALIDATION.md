---
phase: 05
slug: cli-reports-docs-and-verification
status: draft
nyquist_compliant: true
wave_0_complete: false
created: 2026-05-28
---

# Phase 05 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest/unittest under `model/scripts/sft/tests` |
| **Config file** | `model/pyproject.toml` for model tests; direct `python3 -m pytest` is acceptable for script tests |
| **Quick run command** | `python3 -m py_compile model/scripts/sft/split_dataset.py` |
| **Full suite command** | `python3 -m pytest model/scripts/sft/tests -q` |
| **Estimated runtime** | ~20-60 seconds |

---

## Sampling Rate

- **After every task commit:** Run the focused command listed in the task.
- **After every plan wave:** Run `python3 -m pytest model/scripts/sft/tests -q` when feasible.
- **Before `$gsd-verify-work`:** Full targeted SFT script suite must be green.
- **Max feedback latency:** 60 seconds for focused CLI/report tests.

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 05-01-01 | 01 | 0 | CLI-01/CLI-02/CLI-03 | T05-01 | Dry-run has no audio side effects and generate errors surface nonzero | CLI/unit | `python3 -m pytest model/scripts/sft/tests/test_split_dataset_cli.py -q` | ❌ W0 | ⬜ pending |
| 05-02-01 | 02 | 1 | CLI-04 | T05-02 | Reports include excluded rows without duplicating canonical transformation data | unit | `python3 -m pytest model/scripts/sft/tests/test_dataset_reports.py model/scripts/sft/tests/test_dataset_publisher.py -q` | ✅ | ⬜ pending |
| 05-03-01 | 03 | 2 | CLI-05 | — | Existing docs/help explain only non-obvious user-facing behavior | docs/test | `python3 -m pytest model/scripts/sft/tests -q` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `model/scripts/sft/tests/test_split_dataset_cli.py` — CLI dispatch, dry-run bundle, short error, and generate smoke tests.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| CLI help wording | CLI-05 | User-facing help quality is easier to inspect than over-specify | Run `python model/scripts/sft/split_dataset.py --help`, `dry-run --help`, and `generate --help`; confirm terms are clear and no stale `validate` command appears. |

---

## Validation Sign-Off

- [x] All tasks have automated verification or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all missing references
- [x] No watch-mode flags
- [x] Feedback latency < 60s for focused tests
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** pending


---
phase: 02-split-engine-and-leakage-gates
verified: 2026-05-28T15:16:04Z
status: passed
score: "11/11 must-haves verified"
overrides_applied: 0
requirements_verified:
  - id: SPLT-01
    status: verified
    evidence: "Split assignment keeps each Source Group wholly in one split."
  - id: SPLT-02
    status: verified
    evidence: "Split output includes source assignment and algorithm/report metadata through SplitResult."
  - id: SPLT-03
    status: verified
    evidence: "Leakage validation fails cross-split Source Group overlap."
  - id: SPLT-04
    status: verified
    evidence: "Leakage validation fails cross-split original-audio URI overlap."
  - id: SPLT-05
    status: verified
    evidence: "Leakage validation fails cross-split model-ready audio URI overlap."
  - id: SPLT-06
    status: verified
    evidence: "Split generation fails when a configured dataset has no valid SFT examples."
  - id: SPLT-07
    status: verified
    evidence: "Balance scoring considers dataset family/name, source count, row count, duration, duration buckets, and transcript-length buckets; time fields remain report-only."
  - id: SPLT-08
    status: verified
    evidence: "Balance reports expose requested and actual ratios plus component deltas."
  - id: TEST-02
    status: verified
    evidence: "Focused split tests cover source-group assignment, per-dataset coverage, and seed-free config behavior."
  - id: TEST-03
    status: verified
    evidence: "Focused leakage tests cover source, original-audio, and model-ready URI overlap failures."
  - id: TEST-04
    status: verified
    evidence: "Focused balance tests cover bucket helpers and report contents."
human_verification: []
---

# Phase 2: Split Engine And Leakage Gates Verification Report

**Phase Goal:** Users can produce a balance-first 80:20 train/SFT Eval Split that satisfies hard no-leak gates and reports balance quality.
**Verified:** 2026-05-28T15:16:04Z
**Status:** passed

## Goal Achievement

Phase 2 is verified from the Phase 2 plan summaries, the validation strategy, and the current SFT script regression suite. The split layer assigns whole Source Groups, validates the three hard leakage dimensions, and reports balance quality across the correlated factors selected during Phase 2 planning.

## Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|---|---|---|---|---|
| SPLT-01 | 02-01 | Assign whole Source Groups to one split | SATISFIED | Split tests assert no Source Group is split across train/eval. |
| SPLT-02 | 02-01 | Emit assignment and algorithm metadata | SATISFIED | `SplitResult` carries assignment and report metadata for later artifacts. |
| SPLT-03 | 02-02 | Fail Source Group overlap | SATISFIED | Leakage tests cover cross-split Source Group overlap. |
| SPLT-04 | 02-02 | Fail original-audio overlap | SATISFIED | Leakage tests cover cross-split original audio URI overlap. |
| SPLT-05 | 02-02 | Fail model-ready URI overlap | SATISFIED | Leakage tests cover cross-split model-ready audio URI overlap. |
| SPLT-06 | 02-01 | Fail zero valid examples | SATISFIED | Split tests cover empty/invalid dataset behavior. |
| SPLT-07 | 02-01, 02-03 | Score correlated split factors | SATISFIED | Balance tests cover duration and transcript buckets plus dataset/source/row/duration inputs. |
| SPLT-08 | 02-03 | Report requested and actual ratios/deltas | SATISFIED | Balance report tests assert component deltas and JSON-safe report output. |
| TEST-02 | 02-01, 02-03 | Split assignment tests | SATISFIED | Phase 2 summaries record focused and full test runs. |
| TEST-03 | 02-02, 02-03 | Leakage gate tests | SATISFIED | Phase 2 summaries record leakage and full suite runs. |
| TEST-04 | 02-03 | Balance report tests | SATISFIED | Phase 2 summary records balance and full suite runs. |

## Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|---|---|---|---|
| Full SFT script suite | `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests -q` | `182 passed, 59 subtests passed in 0.43s` | PASS |
| CLI module compile smoke | `python3 -m py_compile model/scripts/sft/split_dataset.py` | Exit 0 | PASS |

## Human Verification

None required. Phase 2 has no external-service dependency.

---

_Verified: 2026-05-28T15:16:04Z_

---
phase: 05-cli-reports-docs-and-verification
verified: 2026-05-28T14:21:12Z
status: passed
score: "25/25 must-haves verified"
overrides_applied: 0
---

# Phase 05: CLI, Reports, Docs, And Verification Report

**Phase Goal:** Users can run dry-run and generation commands, inspect reports, and hand generated artifacts to SFT workflows with clear terminology.
**Verified:** 2026-05-28T14:21:12Z
**Status:** passed

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Dry-run writes a local plan bundle with canonical and model-input previews using planned model-ready `gs://` URIs. | VERIFIED | `build_dry_run_artifacts()` enriches split rows, builds canonical/per-dataset/NeMo/Whisper/Gemini artifacts, and writes a local tree; CLI test asserts expected files exist. |
| 2 | Dry-run does not materialize audio and marks `audio_materialized=false`. | VERIFIED | Dry-run uses `dataset_split.dry_run`, which does not import the audio preparer; report tests and CLI tests assert `audio_materialized` is false. |
| 3 | Dry-run fails when `--output-dir` already exists. | VERIFIED | `build_dry_run_artifacts()` raises `DatasetArtifactError("output_dir already exists...")`; CLI test covers return code 1 and short output. |
| 4 | `split_dataset.py` is the public script and exposes only `dry-run` and `generate`. | VERIFIED | `split_dataset.py` has only two subparsers; `validate_dataset.py` was removed; help tests assert no `validate`. |
| 5 | Commands are config-driven and require the agreed arguments. | VERIFIED | `dry-run` requires `--config-uri` and `--output-dir`; `generate` requires `--config-uri`; subcommand help tests assert options. |
| 6 | `generate` exposes no scratch-dir/force/resume/cleanup flags. | VERIFIED | `rg -n "scratch-dir|force|resume|cleanup" model/scripts/sft/split_dataset.py` returned no matches. |
| 7 | Expected hard failures print short errors and write no failure files. | VERIFIED | CLI catches domain errors, prints `str(exc)`, returns 1, and tests assert no `*failure*` artifact appears. |
| 8 | Generate dispatches to the existing publisher. | VERIFIED | `_generate()` calls `publish_dataset_version_artifacts()` with config-derived dataset version, ratios, prompts, root prefix, split result, and excluded rows. |
| 9 | Validation preserves valid rows, excluded rows, and summaries. | VERIFIED | `DatasetValidationResult` contains `segments`, `excluded`, and `summaries`; `validate_dataset()` remains a summary wrapper. |
| 10 | Successful dry-run and generate bundles include `reports/excluded_rows.jsonl`. | VERIFIED | Publisher inventory/planned artifacts include the sidecar; dry-run writes the same sidecar locally; tests cover both. |
| 11 | Report JSON includes excluded rows, source-key failures, audio materialization, leakage, balance, and transformations. | VERIFIED | `DatasetVersionReport.to_dict()` includes these fields and existing split/audio/report tests pass. |
| 12 | Markdown report shows excluded-row counts and sidecar path only. | VERIFIED | `render_dataset_version_markdown()` adds `## Excluded Rows`; tests assert it excludes row text/audio examples. |
| 13 | No transformations sidecar was added. | VERIFIED | Transformation details remain in canonical manifests; only `reports/excluded_rows.jsonl` was added. |
| 14 | Existing README, not a new doc, explains runbook commands and terms. | VERIFIED | `model/scripts/sft/README.md` contains `## Dataset Split Runbook` and the five required terms. |
| 15 | Tests stay local and do not add live GCS checks. | VERIFIED | All new tests use fake readers/storage clients or text assertions. |
| 16 | Final targeted verification passes. | VERIFIED | `python3 -m py_compile model/scripts/sft/split_dataset.py` and `python3 -m pytest model/scripts/sft/tests -q` passed. |
| 17 | Publisher sidecar content uses minimal row fields. | VERIFIED | `serialize_excluded_rows()` writes `dataset_name`, `row_index`, `audio_uri`, and `reason`; publisher test parses sidecar content. |
| 18 | Source-key failures remain fail-fast short errors. | VERIFIED | Successful reports set `source_key_failures=0`; no failure diagnostic files were added. |
| 19 | Dry-run report explicitly says audio is not materialized. | VERIFIED | Dry-run passes `audio_materialized=False` into report construction and tests assert the JSON field. |
| 20 | Generate report says audio is materialized. | VERIFIED | Publisher passes `audio_materialized=True` into report construction. |
| 21 | CLI prints useful success locations. | VERIFIED | Dry-run prints output directory and report path; generate prints root URI plus report/model-input artifact URIs. |
| 22 | Existing validation CLI tests were moved off the deleted public script. | VERIFIED | `test_dataset_split_validate.py` no longer imports `validate_dataset as cli`. |
| 23 | Report sidecar upload is create-only with the text artifact set. | VERIFIED | Publisher includes the sidecar in planned text artifacts with `application/x-ndjson`, uploaded through `upload_text_create_only()`. |
| 24 | README avoids a drift-prone artifact tree. | VERIFIED | README states exact generated paths are printed by the CLI and recorded in `dataset_version_report.json`. |
| 25 | Help output guards absent options. | VERIFIED | Tests assert no `--scratch-dir` and no `validate` in dry-run/generate help. |

**Score:** 25/25 truths verified

### Required Artifacts

| Artifact | Expected | Status |
|----------|----------|--------|
| `model/scripts/sft/split_dataset.py` | Public runbook CLI | VERIFIED |
| `model/scripts/sft/dataset_split/validate.py` | Shared loading result with excluded rows | VERIFIED |
| `model/scripts/sft/dataset_split/dry_run.py` | Plan-only dry-run bundle writer | VERIFIED |
| `model/scripts/sft/dataset_split/reports.py` | Summary report fields and excluded-row serialization | VERIFIED |
| `model/scripts/sft/dataset_split/publisher.py` | GCS sidecar publication | VERIFIED |
| `model/scripts/sft/README.md` | Existing docs updated with runbook terms | VERIFIED |

### Requirements Coverage

| Requirement | Status | Evidence |
|-------------|--------|----------|
| CLI-01 | SATISFIED | Dry-run validates config/manifests, computes split, and writes local reports without audio upload. |
| CLI-02 | SATISFIED | Generate calls the publisher to write canonical/model inputs and reports to GCS. |
| CLI-03 | SATISFIED | Expected failures return 1 with short messages; successful commands print report paths. |
| CLI-04 | SATISFIED | Report bundle includes leakage, balance, source-key failure count, excluded rows, and transformation summaries. |
| CLI-05 | SATISFIED | README defines Source Group, Labeled Segment, SFT Example, SFT Eval Split, and Dataset Version. |

### Behavioral Spot-Checks

| Behavior | Command | Result |
|----------|---------|--------|
| Compile CLI | `python3 -m py_compile model/scripts/sft/split_dataset.py` | PASS |
| Full targeted SFT tests | `python3 -m pytest model/scripts/sft/tests -q` | 182 passed, 59 subtests passed |
| No validate subcommand | `rg -n "add_parser\\(\"validate\"" model/scripts/sft` | No matches |
| No out-of-scope flags | `rg -n "scratch-dir|force|resume|cleanup" model/scripts/sft/split_dataset.py` | No matches |

### Human Verification Required

None. Live GCS dry-run/generate execution is intentionally outside automated tests for this runbook script.

### Gaps Summary

No blocking gaps found. Phase 05 meets the CLI, reports, docs, and verification goal.


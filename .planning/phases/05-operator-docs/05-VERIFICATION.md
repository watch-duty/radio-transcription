---
phase: 05-operator-docs
verified: 2026-06-29T03:27:21Z
status: passed
score: "10/10 must-haves verified"
overrides_applied: 0
---

# Phase 5: Operator Docs Verification Report

**Phase Goal:** A new operator can follow documented commands and placeholder
configs from manifests through reports while keeping local experiment artifacts
out of source control.
**Verified:** 2026-06-29T03:27:21Z
**Status:** passed
**Re-verification:** No - initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | README routes a new operator to the canonical docs and does not duplicate the full runbook. | VERIFIED | `model/scripts/sft/README.md` is 42 lines, links the runbook and companion docs at lines 19-23, includes only runtime, command summary, and verification boundary sections. |
| 2 | Human-facing SFT docs use OKF-compatible Markdown in a small docs bundle. | VERIFIED | `docs/index.md`, `runbook.md`, `configs.md`, `metrics.md`, `artifacts.md`, and `hygiene.md` all have `type`, `title`, `description`, and `tags` frontmatter. No doc build tool was added. |
| 3 | Operator can follow prepare, tune, eval, report inspection, checkpoint scoring, masked eval, unmasked eval, and hygiene from the runbook. | VERIFIED | `docs/runbook.md` contains the required ordered sections, CLI commands, report paths, checkpoint scoring command, masked/unmasked config guidance, and final hygiene section at lines 47-220. |
| 4 | Paid or potentially paid Vertex operations are clearly marked before use with expected GCS prefixes. | VERIFIED | `runbook.md` marks `tune`, `eval`, and checkpoint scoring as paid or potentially paid and shows `gs://BUCKET/sft/runs/ROUND_ID/`, `evals/LABEL/`, and `evals/wer_summary.{json,md}` before the commands. |
| 5 | Docs teach one `[eval.model]` per config/run and separate configs or external wrappers for comparisons. | VERIFIED | `runbook.md` lines 17-20 and 66-68, `configs.md` lines 27-29 and 73-86, and `run_config.example.toml` lines 35-49 all state the singular target contract. |
| 6 | Placeholder examples cover base-only, tuned, checkpoint, masked, and unmasked eval runs without real credentials or run artifacts. | VERIFIED | `run_config.example.toml` uses placeholder project, bucket, manifests, and model values; `configs.md` includes base, tuned, checkpoint snippets and one masked variant. Secret scan found only placeholders such as `PROJECT`, endpoint IDs, `your-*`, and `YYYY-MM-DD-*`. |
| 7 | Metric docs define every report metric and match canonical report columns. | VERIFIED | `metrics.md` lines 13-30 define every `gemini_sft.reporting.REPORT_COLUMNS` value; `test_sft_operator_metric_docs_track_report_columns` asserts exact ordered equality against the imported constant. |
| 8 | Docs distinguish exact empty response rate from empty-or-unintelligible rate and explain missing predictions separately. | VERIFIED | `metrics.md` lines 32-52 define `empty_or_unintelligible_rate`, `empty_response_rate`, and `missing_prediction_count` with the required separate semantics. |
| 9 | Docs distinguish durable GCS state from local cache or mirror output and identify files that must not be committed. | VERIFIED | `artifacts.md` lists durable GCS run state, eval artifacts, normalized manifests, and local cache boundaries; `hygiene.md` lists `.local.toml`, root `results/`, SFT JSONL outputs, inference manifests, online predictions, batch predictions, and generated eval outputs. |
| 10 | Maintainer can run or follow a final artifact hygiene check that catches local `.local.toml`, raw prediction JSONL, inference manifests, and `results/` artifacts. | VERIFIED | `runbook.md` and `hygiene.md` contain the staged-file `rg` check; `.gitignore` covers `/results/`, `*.local.toml`, `model/data/inference_manifests/*.jsonl(.gz)`, and SFT result JSONL; drift guards and `git check-ignore --no-index` spot-checks confirm behavior. |

**Score:** 10/10 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `.gitignore` | Narrow local SFT/operator artifact ignore coverage | VERIFIED | Lines 92-99 ignore SFT JSONL, root `/results/`, `*.local.toml`, and generated local inference manifests without ignoring `run_config.example.toml`. |
| `model/scripts/sft/README.md` | Thin entrypoint to OKF docs | VERIFIED | 42 lines with runtime, doc links, command summary, and no-paid-Vertex test boundary. |
| `model/scripts/sft/run_config.example.toml` | Canonical full placeholder TOML | VERIFIED | 63 lines, singular `[eval.model]`, placeholder-only values, inline prompt comment, and execution controls. |
| `model/scripts/sft/docs/index.md` | OKF navigation index | VERIFIED | Links `runbook.md`, `configs.md`, `metrics.md`, `artifacts.md`, and `hygiene.md`. |
| `model/scripts/sft/docs/runbook.md` | Canonical operator runbook | VERIFIED | 220 lines with required workflow, paid warnings, GCS paths, report inspection, masked/unmasked guidance, and hygiene check. |
| `model/scripts/sft/docs/configs.md` | Placeholder config reference and snippets | VERIFIED | Documents one full placeholder config plus base, tuned, checkpoint, and masked snippets; unsupported shapes are called out. |
| `model/scripts/sft/docs/metrics.md` | Canonical metric glossary | VERIFIED | Defines all report columns, artifact URI fields, row count metadata, empty-output semantics, and missing-prediction handling. |
| `model/scripts/sft/docs/artifacts.md` | Durable GCS and local cache reference | VERIFIED | Lists run-level state, eval target artifacts, normalized manifests, report summaries, and local cache boundary. |
| `model/scripts/sft/docs/hygiene.md` | Operator pre-commit hygiene checklist | VERIFIED | Contains never-commit list, `git status --short --ignored`, and staged-file `rg` command. |
| `model/tests/common/tests/test_drift_guard.py` | Text-only docs/config drift guard | VERIFIED | Imports `gemini_sft.reporting`, validates metrics docs against `REPORT_COLUMNS`, validates hygiene docs and `.gitignore`, and performs `git check-ignore` checks without external services. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `README.md` | `docs/runbook.md` | Relative Markdown link | VERIFIED | README line 19 links `[Operator runbook](docs/runbook.md)`. |
| `README.md` | Companion docs | Relative Markdown links | VERIFIED | README lines 20-23 link configs, metrics, artifacts, and hygiene docs. |
| `docs/index.md` | Docs bundle | Relative Markdown links | VERIFIED | Index lines 10-15 link all five docs. |
| `docs/runbook.md` | `docs/hygiene.md` | Final hygiene section | VERIFIED | Runbook line 220 points operators to `docs/hygiene.md`; drift guard also asserts the runbook and hygiene `rg` patterns match. |
| `docs/metrics.md` | `gemini_sft.reporting.REPORT_COLUMNS` | Imported constant guard | VERIFIED | `test_drift_guard.py` lines 184-202 imports `reporting.REPORT_COLUMNS` and checks exact ordered documentation equality. |
| `docs/artifacts.md` | Stable eval summary paths | Evaluate/records path vocabulary | VERIFIED | `artifacts.md` lists `evals/wer_summary.json` and `.md`; `records.py` exposes `wer_summary_gcs_uris` for those paths and `evaluate.py` wires the summary URI fields into report artifacts. |
| `test_drift_guard.py` | `docs/hygiene.md` and `.gitignore` | Text and `git check-ignore` guard | VERIFIED | Lines 213-328 check hygiene terms, ignore rules, blocked paths, and allowed durable per-run records. |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|--------------------|--------|
| `docs/metrics.md` | Documented report columns | `gemini_sft.reporting.REPORT_COLUMNS` imported by `test_drift_guard.py` | Yes - imported runtime constant, not copied fixture data | VERIFIED |
| `docs/hygiene.md` and `runbook.md` | Staged artifact regex | Regex extracted from both docs by `test_drift_guard.py` | Yes - compiled and tested against blocked and allowed sample paths | VERIFIED |
| `.gitignore` | Ignore rules | `git check-ignore --no-index` in drift guard and manual spot-check | Yes - git evaluates actual ignore behavior | VERIFIED |
| Static docs bundle | Markdown links and content | Static Markdown files | N/A - docs do not render dynamic data | VERIFIED |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Drift guards pass | `safe-run -- env PYTHONPATH=src:tests python3 -m pytest tests/common/tests/test_drift_guard.py -q` | Orchestrator reported `10 passed, 34 subtests passed` | PASS |
| SFT config/report/workflow tests pass | `safe-run -- env PYTHONPATH=src:tests python3 -m pytest tests/gemini_sft/test_config.py tests/gemini_sft/test_reporting.py tests/gemini_sft/test_checkpoint_scorer.py tests/gemini_sft/test_workflow.py -q` | Orchestrator reported `91 passed, 48 subtests passed` | PASS |
| Reviewed file whitespace is clean | `safe-run -- git diff --check -- ...reviewed files...` | Exit 0, no output | PASS |
| Local artifact ignore rules work | `git check-ignore --no-index results/run/output.jsonl model/scripts/sft/results/run/output.jsonl model/scripts/sft/results/run/output.jsonl.gz model/data/inference_manifests/base.jsonl model/data/inference_manifests/base.jsonl.gz scratch.local.toml` | Exit 0, all paths printed | PASS |
| Durable per-run records remain trackable | `git check-ignore --no-index model/scripts/sft/results/run/config.json model/scripts/sft/results/run/status.json model/scripts/sft/results/run/wer_summary.md model/scripts/sft/run_config.example.toml` | Exit 1, no output | PASS |
| Advisory review gate | `.planning/phases/05-operator-docs/05-REVIEW.md` | Clean review, 0 findings | PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-------------|-------------|--------|----------|
| DOC-01 | 05-01 | README explains the config-driven operator path for prepare, tune, eval, checkpoint scoring, masked eval, and unmasked eval. | SATISFIED | README links the runbook and command summary; runbook documents the complete path from config through reports and hygiene. |
| DOC-02 | 05-02 | Repo contains placeholder example configs for common base-only, tuned, checkpoint, masked, and unmasked eval runs without real local credentials or run artifacts. | SATISFIED | One full placeholder TOML plus short target and masked snippets in `configs.md`; secret scan found placeholders only. |
| DOC-03 | 05-02, 05-03 | Documentation explains every report metric, including exact empty response rate versus empty-or-unintelligible rate. | SATISFIED | `metrics.md` covers all `REPORT_COLUMNS`; drift guard enforces exact ordered match and rejects legacy names. |
| DOC-04 | 05-01, 05-02, 05-03 | Documentation identifies durable GCS state, local cache/mirror outputs, and files that must not be committed. | SATISFIED | `artifacts.md`, `hygiene.md`, `runbook.md`, `.gitignore`, and drift guard cover durable/local boundaries and local artifact classes. |
| DOC-05 | 05-01, 05-02, 05-03 | Tests or docs include a final artifact hygiene check preventing accidental commits of local `.local.toml`, raw prediction JSONL, inference outputs, or `results/` files. | SATISFIED | Runbook and hygiene docs include final staged-file check; `.gitignore` and `test_drift_guard.py` verify the relevant classes. |

No orphaned Phase 5 requirements were found. `DOC-01` through `DOC-05` are all
mapped to Phase 5 and claimed by at least one Phase 5 plan.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `model/scripts/sft/docs/configs.md` and `run_config.example.toml` | Multiple | `placeholder` language | INFO | Intentional and required by DOC-02; examples use placeholder-only values. |
| `model/tests/common/tests/test_drift_guard.py` | 206-209 | Legacy metric names | INFO | Negative test data only; the test asserts those names are not documented. |

No TODO/FIXME/HACK markers, placeholder stubs, empty implementations, or
console-only handlers were found in the delivered docs/test scope.

### Human Verification Required

None for this phase's automated goal verification. The phase is documentation
and lightweight hygiene only, and the context explicitly excludes paid Vertex,
notebook, Docker, and end-to-end eval validation.

### Residual Risks

- The verification did not submit Vertex tuning, run Vertex batch inference,
  call online endpoints, execute notebooks, or run a full eval. That is
  consistent with the Phase 5 boundary and repo test-safety guidance.
- A real first-time operator walkthrough could still improve wording and
  sequencing, but no required content or wiring gap was found.
- `ROADMAP.md` and `STATE.md` contain stale progress counters for Phase 5, but
  `gsd-sdk query roadmap.analyze --raw` sees all Phase 5 plans and summaries on
  disk. This is planning-state drift, not a delivered-file blocker.
- Future report schema or artifact path changes must keep the drift guards in
  sync so docs do not become stale.

### Follow-Up Suggestions

- Run a non-paid dry-run walkthrough with a new operator using placeholder
  values to catch wording issues.
- When a future phase changes report columns or artifact paths, update
  `test_drift_guard.py` and the operator docs in the same change.
- Refresh roadmap/state progress metadata through the orchestrator so planning
  files no longer show stale Phase 5 counters.

### Gaps Summary

No blocking gaps found. All Phase 5 roadmap success criteria and
`DOC-01` through `DOC-05` are satisfied by substantive, wired docs, placeholder
config coverage, ignore rules, and text-only drift guards.

---

_Verified: 2026-06-29T03:27:21Z_
_Verifier: the agent (gsd-verifier)_

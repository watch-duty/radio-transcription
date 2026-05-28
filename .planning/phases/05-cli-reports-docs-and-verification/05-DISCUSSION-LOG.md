# Phase 5: CLI, Reports, Docs, And Verification - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-05-28
**Phase:** 5-CLI, Reports, Docs, And Verification
**Areas discussed:** Dry-Run Artifact Contract, Generate Command Shape, Failure Report Location, Report Bundle Contents And Granularity, End-To-End Verification Scope

---

## Dry-Run Artifact Contract

| Option | Description | Selected |
|--------|-------------|----------|
| Reports only | Write validation/split/leakage/balance reports, but no local canonical or model-input files. | |
| Plan bundle | Write reports plus local previews of canonical manifests and NeMo/Whisper/Gemini model inputs, but no audio upload. | ✓ |
| Full local mirror | Mirror the whole dataset-version tree locally, including planned audio paths and all text artifacts. | |

**User's choice:** Plan bundle.
**Notes:** Dry-run should be useful for inspecting downstream SFT inputs before publication.

| Option | Description | Selected |
|--------|-------------|----------|
| Local materialization | Download/probe sources and run copy/derive/transcode into scratch space, but upload nothing. | |
| Plan-only | Compute splits and planned paths without downloading/probing/running ffmpeg. | ✓ |
| Two modes | Default fast plan-only plus optional full materialization. | |

**User's choice:** Plan-only.
**Notes:** Dry-run should not exercise audio materialization.

| Option | Description | Selected |
|--------|-------------|----------|
| Planned final `gs://` URIs | Preview model inputs with the destination URIs generate will publish and mark audio as not materialized. | ✓ |
| Original source URIs | Easier to inspect, but previews differ from generate for segmented or unsupported audio. | |
| Omit rows needing derived/transcoded audio | Avoids pretending files exist, but makes previews incomplete. | |

**User's choice:** Planned final `gs://` URIs.
**Notes:** Reports should state `audio_materialized=false`.

| Option | Description | Selected |
|--------|-------------|----------|
| Overwrite local dry-run bundle | Local dry-run is iterative; overwrite reports/previews in the chosen local directory. | |
| Fail if directory exists | Mirrors immutability for the local bundle and avoids accidental replacement. | ✓ |
| Timestamp every dry-run | Preserves history, but adds directory churn. | |

**User's choice:** Fail if directory exists.
**Notes:** Local dry-run output should be explicit and non-overwriting.

---

## Generate Command Shape

| Option | Description | Selected |
|--------|-------------|----------|
| Extend `validate_dataset.py` | Add subcommands to the existing validation script. | |
| Create `generate_dataset.py` | Dedicated generation script. | |
| Fold into `pipeline.py` | Put dataset generation next to Gemini tune/eval. | |
| Use `split_dataset.py` | Name the script after the real task. | ✓ |

**User's choice:** `split_dataset.py`.
**Notes:** The existing dataset-version naming overemphasized versioning; the main task is train/eval splitting.

| Option | Description | Selected |
|--------|-------------|----------|
| Keep validate | Expose `validate` as a separate public command. | |
| Remove validate | Validation is internal to dry-run and generate. | ✓ |

**User's choice:** Remove separate validate.
**Notes:** User asked whether a separate validate entry is still needed; decision was no.

| Option | Description | Selected |
|--------|-------------|----------|
| Config URI only | CLI reads manifests and source maps from TOML referenced by `--config-uri`. | ✓ |
| Config URI plus override flags | Allow ratios, prefix, model settings, scratch dir overrides. | |
| Separate manifest flags | Pass manifests directly on the CLI. | |

**User's choice:** Config-driven CLI.
**Notes:** Dry-run requires `--config-uri` and `--output-dir`; generate requires `--config-uri`.

| Option | Description | Selected |
|--------|-------------|----------|
| Temporary scratch by default | Use existing temporary directory behavior. | ✓ |
| Require `--scratch-dir` | Make disk usage explicit. | |
| Optional `--scratch-dir` | Useful for debugging large runs. | |

**User's choice:** Temporary scratch by default.
**Notes:** No public scratch-dir flag in Phase 5.

---

## Failure Report Location

| Option | Description | Selected |
|--------|-------------|----------|
| Local failure report only | Print short error and write a local failure report. | |
| Config-adjacent GCS failure prefix | Write failures near the input config. | |
| No failure report file | Print only the short error. | ✓ |

**User's choice:** No failure report file.
**Notes:** Avoid additional artifact locations for this runbook script.

| Option | Description | Selected |
|--------|-------------|----------|
| No failure report file | Match generate; exit nonzero with short error. | ✓ |
| Failure report in output dir | More diagnosable, but creates partial dry-run directory. | |
| Failure report beside output dir | Adds another path convention. | |

**User's choice:** No failure report file for dry-run.
**Notes:** Avoid partial dry-run bundles.

| Option | Description | Selected |
|--------|-------------|----------|
| First failing dataset plus cause | Include dataset/context and cause in the error. | ✓ |
| One-line generic error only | Very clean, but less actionable. | |
| First N errors inline | More actionable, but noisy. | |

**User's choice:** First failing dataset plus cause.
**Notes:** Short errors should be enough for debugging.

| Option | Description | Selected |
|--------|-------------|----------|
| Fail and leave uploaded audio | No cleanup/resume/force; user can choose a new dataset version. | ✓ |
| Attempt cleanup | Contradicts prior no-cleanup decision. | |
| Write a partial-state report | Adds another artifact path. | |

**User's choice:** Fail and leave uploaded audio.
**Notes:** Keep Phase 5 simple and aligned with earlier no-cleanup decisions.

---

## Report Bundle Contents And Granularity

| Option | Description | Selected |
|--------|-------------|----------|
| Summary-only report | Counts/summaries only. | |
| Summary report plus row-level sidecars | Add JSONL sidecars for row-level details where useful. | ✓ |
| Everything embedded in one JSON | One large report file. | |

**User's choice:** Summary report plus row-level sidecars.
**Notes:** Avoid bloating the summary report.

| Option | Description | Selected |
|--------|-------------|----------|
| Only excluded rows | Add `excluded_rows.jsonl`; transformations stay in canonical manifests. | ✓ |
| Excluded rows + transformations index | More convenient, duplicates canonical fields. | |
| Excluded rows + source-key diagnostics + transformations | Most complete, conflicts with fail-fast source-key handling. | |

**User's choice:** Only excluded rows.
**Notes:** The sidecar covers non-fatal skipped rows without duplicating canonical manifests.

| Option | Description | Selected |
|--------|-------------|----------|
| Short error only | Source-key failures fail fast; successful reports can say zero. | ✓ |
| Best-effort diagnostics before failing | Continue scanning to collect multiple failures. | |
| Always write source-key sidecar | Requires failure artifacts. | |

**User's choice:** Short error only.
**Notes:** Source-key failures are hard validation failures.

| Option | Description | Selected |
|--------|-------------|----------|
| Only non-fatal exclusions | Rows intentionally skipped, currently empty transcript/text. | ✓ |
| All attempted invalid rows | Requires continuing after validation failures. | |
| Only aggregate counts | Not a sidecar. | |

**User's choice:** Only non-fatal exclusions.
**Notes:** Hard failures stop the command.

| Option | Description | Selected |
|--------|-------------|----------|
| Summaries and paths only | Include counts and sidecar paths, no row-level tables. | ✓ |
| Top N row examples | Helps inspection, but exposes row details. | |
| Full row tables | Not appropriate for large/proprietary manifests. | |

**User's choice:** Summaries and paths only.
**Notes:** Markdown should stay readable and low-drift.

---

## End-To-End Verification Scope

| Option | Description | Selected |
|--------|-------------|----------|
| Fake-GCS end-to-end CLI path | Full dry-run/generate path with fake storage. | |
| Minimal tests | Cover CLI contract and error surfacing only. | ✓ |
| Live GCS automated test | Highest fidelity, but brittle and requires credentials. | |

**User's choice:** Minimal tests.
**Notes:** User clarified this script is a runbook, not production code.

| Option | Description | Selected |
|--------|-------------|----------|
| Smoke dispatch only | Test generate reaches generation path and converts raised errors to nonzero short messages. | ✓ |
| Fake publication test | Assert expected fake uploaded objects. | |
| No generate tests | Rely on existing publisher tests and manual runbook usage. | |

**User's choice:** Smoke dispatch only.
**Notes:** Avoid full fake publication if it adds complexity.

| Option | Description | Selected |
|--------|-------------|----------|
| Concrete runbook only | Add usage examples and behavior docs. | |
| Runbook plus terminology glossary | Add definitions and examples. | |
| No new docs | Update existing docs/comments only if needed. | ✓ |

**User's choice:** No new docs.
**Notes:** Add only information the code itself does not reveal; avoid future drift.

| Option | Description | Selected |
|--------|-------------|----------|
| Targeted SFT script tests only | Run relevant SFT script tests plus compile new script. | ✓ |
| All model tests | Broader, includes unrelated common eval tests. | |
| Skip automated verification | Manual inspection only. | |

**User's choice:** Targeted SFT script tests only.
**Notes:** Verification should match the changed area.

## the agent's Discretion

- Choose the simplest implementation structure that shares validation and splitting between commands.
- Keep CLI output short and actionable.
- Avoid adding docs or flags that duplicate code behavior and will drift.

## Deferred Ideas

None.

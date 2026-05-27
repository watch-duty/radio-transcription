# Phase 3: GCS Artifacts And Model Writers - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md - this log preserves the alternatives considered.

**Date:** 2026-05-27
**Phase:** 03-GCS Artifacts And Model Writers
**Areas discussed:** Artifact Layout And Overwrite Safety, Canonical Manifest Schema, Model Writer Outputs, Report And Metadata Contents

---

## Artifact Layout And Overwrite Safety

| Option | Description | Selected |
|--------|-------------|----------|
| 1 | Version-root tree under `gs://wd-transcription-data/sft/{dataset_version_id}/` with structured subfolders. | yes |
| 2 | Flat files under the dataset-version root. | |
| 3 | Model-first tree. | |

**User's choice:** 1
**Notes:** The tree should include `config/`, `metadata/`, `manifests/`, `model_inputs/`, `reports/`, and a reserved `audio/` area.

| Option | Description | Selected |
|--------|-------------|----------|
| 1 | Mention SFT run path only; do not implement it. | yes |
| 2 | Reserve a concrete run path name. | |
| 3 | Include run folder under the dataset-version tree. | |

**User's choice:** 1
**Notes:** SFT run reports are run-specific, not dataset-version artifacts.

| Option | Description | Selected |
|--------|-------------|----------|
| 1 | Any object under the prefix means the dataset version exists. | yes |
| 2 | Only a marker file counts. | |
| 3 | Protect individual files only. | |

**User's choice:** 1
**Notes:** Any existing object blocks generation for that dataset version.

| Option | Description | Selected |
|--------|-------------|----------|
| 1 | Allow full replacement with audit record. | |
| 2 | Require manual cleanup first / no force in Phase 3. | yes |
| 3 | Force only missing files. | |

**User's choice:** Skip force for now.
**Notes:** Phase 3 should fail if the prefix exists. No overwrite, partial resume, or force flag.

---

## Canonical Manifest Schema

| Option | Description | Selected |
|--------|-------------|----------|
| 1 | Enriched canonical rows with source, split, provenance, IDs, audio URI, transcript, offset, and duration. | yes |
| 2 | Minimal model-facing rows only. | |
| 3 | Keep raw input rows. | |

**User's choice:** 1
**Notes:** `raw_row` should not be included in generated canonical manifests.

| Option | Description | Selected |
|--------|-------------|----------|
| 1 | Require model-ready URI before writing model inputs. | |
| 2 | Always point to original source audio and preserve offset/duration. | yes |
| 3 | Emit both but prefer model-ready URI. | |

**User's choice:** 2
**Notes:** Phase 4 owns audio derivation or later URI adjustment.

| Option | Description | Selected |
|--------|-------------|----------|
| 1 | Mark rows as requiring audio derivation. | |
| 2 | Mark artifacts as draft/pre-derivation. | |
| 3 | No special label. | yes |

**User's choice:** 3
**Notes:** Phase 3 uses original audio URI directly; Phase 4 can adjust if needed.

---

## Model Writer Outputs

| Option | Description | Selected |
|--------|-------------|----------|
| 1 | NeMo manifest plus config fragment. | yes |
| 2 | Manifest only. | |
| 3 | Tarred dataset layout. | |

**User's choice:** 1
**Notes:** Emit train/eval JSONL with NeMo-compatible fields and a config fragment.

| Option | Description | Selected |
|--------|-------------|----------|
| 1 | Loader-friendly Whisper JSONL plus recommendations. | yes |
| 2 | Hugging Face dataset directory. | |
| 3 | Manifest URL list only. | |

**User's choice:** 1
**Notes:** Examples over 30 seconds should be reported rather than failed unless a verified consumer rejects them.

| Option | Description | Selected |
|--------|-------------|----------|
| 1 | Gemini 3.1 Flash-Lite default with configurable tuning parameters. | yes |
| 2 | Keep existing Gemini 2.5 Flash default. | |
| 3 | No default model. | |

**User's choice:** 1
**Notes:** User pointed to current Gemini Enterprise Agent Platform docs showing Gemini 3.1 Flash-Lite supervised tuning support. Keep the base model configurable.

---

## Report And Metadata Contents

| Option | Description | Selected |
|--------|-------------|----------|
| 1 | Full artifact inventory plus validation summary. | yes |
| 2 | Minimal manifest-only metadata. | |
| 3 | Split/report-only metadata. | |

**User's choice:** 1
**Notes:** Reports should include config, counts/durations, leakage, balance, artifact inventory, and writer warnings.

| Option | Description | Selected |
|--------|-------------|----------|
| 1 | Hard fail schema/data-shape errors, report model-specific risk warnings. | yes |
| 2 | Hard fail every model-specific risk. | initially selected, then revised after code review |
| 3 | Report all issues only. | |

**User's choice:** Initially 2, then accepted the evaluated hybrid policy.
**Notes:** Existing eval/SFT behavior is hybrid: malformed join keys and Gemini SFT preflight failures are blockers, while runtime/performance risks are surfaced. Use that model for Phase 3.

## The Agent's Discretion

- Choose exact helper/module names, local staging paths, and report field grouping.
- Prefer existing project patterns over new framework-level interfaces.

## Deferred Ideas

- SFT run reports and model-run comparison reports belong outside the immutable dataset-version artifact tree.
- Audio derivation and transformation provenance belong to Phase 4.
- Force/overwrite and partial resume are out of Phase 3.

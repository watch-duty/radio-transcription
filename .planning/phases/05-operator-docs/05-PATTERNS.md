# Phase 5: Operator Docs - Pattern Map

**Mapped:** 2026-06-28
**Files analyzed:** 10
**Analogs found:** 10 / 10

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `model/scripts/sft/README.md` | documentation entrypoint | transform | `model/scripts/sft/README.md` | exact |
| `model/scripts/sft/run_config.example.toml` | config example | file-I/O | `model/scripts/sft/run_config.example.toml` | exact |
| `model/scripts/sft/docs/index.md` | documentation index | request-response | `model/data/inference_manifests/README.md` + OKF spec | role-match |
| `model/scripts/sft/docs/runbook.md` | operator runbook | batch | `model/research/gemini_prior_context_sft/protocol.md` | role-match |
| `model/scripts/sft/docs/configs.md` | config reference | file-I/O | `model/scripts/sft/run_config.example.toml` + `model/src/gemini_sft/config.py` | exact |
| `model/scripts/sft/docs/metrics.md` | metric glossary | transform | `model/src/gemini_sft/reporting.py` + `model/src/common/scoring.py` | exact |
| `model/scripts/sft/docs/artifacts.md` | artifact reference | file-I/O | `model/scripts/sft/README.md` + `model/src/gemini_sft/evaluate.py` | exact |
| `model/scripts/sft/docs/hygiene.md` | artifact hygiene docs | file-I/O | `.gitignore` + `model/scripts/sft/README.md` | role-match |
| `.gitignore` | config | file-I/O | existing `.gitignore` SFT output rules | role-match |
| `model/tests/common/tests/test_drift_guard.py` | test guard | file-I/O | existing SFT config drift guard in same file | exact |

## Pattern Assignments

### `model/scripts/sft/README.md` (documentation entrypoint, transform)

**Analog:** `model/scripts/sft/README.md`

**Entrypoint pattern** (lines 1-4):
```markdown
# Watch Duty Gemini SFT CLI

Gemini supervised fine-tuning is exposed as the packaged `gemini-sft` command
from the `radio-transcription-model` distribution under `model/`.
```

**Runtime pattern to keep concise** (lines 6-23):
```markdown
## Runtime

The recommended operator runtime is the lightweight ASR Docker service.
...
gemini-sft --help
```

**Command path to link into runbook** (lines 25-35):
```markdown
gemini-sft prepare --config /path/to/run.toml
gemini-sft tune --config /path/to/run.toml --confirm
gemini-sft eval --config /path/to/run.toml
```

**Boundary pattern** (lines 224-228):
```markdown
## Verification

Unit tests mock GCS and Vertex boundaries. They must not submit paid Vertex
tuning jobs, run Vertex batch inference, execute notebooks, or run end-to-end
evals.
```

**Apply:** Replace the long README with a thin entrypoint: runtime, command
summary, and links to `docs/runbook.md`, `docs/configs.md`, `docs/metrics.md`,
`docs/artifacts.md`, and `docs/hygiene.md`. Do not duplicate the full runbook.

---

### `model/scripts/sft/run_config.example.toml` (config example, file-I/O)

**Analog:** `model/scripts/sft/run_config.example.toml`

**Placeholder identity and manifests** (lines 1-17):
```toml
round_id = "YYYY-MM-DD-short-description"
dataset = "dataset-version-name"
inference_dataset_slug = "echo/eval"
train_manifest_uri = "gs://your-bucket/path/manifests/canonical/train.jsonl"
validation_manifest_uri = "gs://your-bucket/path/manifests/canonical/validation.jsonl"
eval_manifest_uri = "gs://your-bucket/path/manifests/canonical/eval.jsonl"
```

**Singular eval target pattern** (lines 31-44):
```toml
[eval.model]
# `gemini-sft eval` supports one model per config.
label = "base"
model = "gemini-3.1-flash-lite"

# For a checkpoint or tuned endpoint, use the same table shape:
# label = "checkpoint_6"
# model = "projects/PROJECT/locations/us-central1/endpoints/ENDPOINT_ID"
```

**Inline prompt pattern** (lines 55-59):
```toml
[prompts]
# Optional inline overrides only. Prompt files are intentionally unsupported so
# the resolved prompt text can be stored in config.json for resume/eval.
# system = "..."
# user = "..."
```

**Parser contract backing the example** (`model/src/gemini_sft/config.py` lines 419-441):
```python
if "models" in eval_table:
    msg = (
        "eval has no plural eval target support: use [eval.model], "
        "not [[eval.models]]"
    )
    raise RunConfigError(msg)

unsupported_eval_fields = sorted(set(eval_table) - {"execution", "model"})
```

**Apply:** Keep one full placeholder config only. Put masked-eval as a compact
snippet in `docs/configs.md` unless planning decides a second placeholder file is
worth the extra surface.

---

### `model/scripts/sft/docs/index.md` (documentation index, request-response)

**Analogs:** `model/data/inference_manifests/README.md`, `.planning/phases/04-durable-eval/04-04-SUMMARY.md`, OKF spec.

**Local source-doc organization pattern** (`model/data/inference_manifests/README.md` lines 6-31):
```markdown
## Artifact Types

A source/canonical manifest is the row-per-segment input dataset.
...
SFT run state is the durable control-plane record under `sft/runs/<round-id>/`.
```

**Local frontmatter style pattern** (`.planning/phases/04-durable-eval/04-04-SUMMARY.md` lines 1-13):
```yaml
---
phase: 04-durable-eval
plan: "04"
subsystem: docs-and-drift
tags: [gemini-sft, docs, examples, drift-guard, checkpoint-scorer]
...
---
```

**External OKF pattern:** The OKF spec defines each concept as a Markdown file
with YAML frontmatter and a free-form Markdown body; `type` is required and
`title`, `description`, `resource`, `tags`, and `timestamp` are recommended.
Use the Phase 5 context's lightweight frontmatter fields and do not add a build
tool.

**Apply:** `index.md` should be a short navigation file with OKF frontmatter and
links to the five focused docs. Keep it useful as a directory index, not a
second runbook.

---

### `model/scripts/sft/docs/runbook.md` (operator runbook, batch)

**Analog:** `model/research/gemini_prior_context_sft/protocol.md`

**Runbook section sequence pattern** (lines 1-23, 97-120):
```markdown
# Gemini Prior-Context SFT Protocol

## Objective
...
## SFT Settings
...
## Execution
```

**Command-oriented execution pattern** (lines 107-118):
```markdown
1. Build combined canonical train, validation, and eval manifests under ...
2. Run `gemini-sft prepare` ...
3. Inspect prepared Gemini JSONL ...
4. Run `gemini-sft tune --confirm`.
5. Run `gemini-sft eval` ...
```

**Acceptance pattern** (lines 120-127):
```markdown
## Acceptance Checks

- Prepare preflight passes.
- Vertex tuning job succeeds and writes a tuned endpoint to `config.json`.
- Batch scorer completes and writes WER/CER summaries.
```

**Use with caution:** The protocol file contains real historical GCS paths and
run IDs. Copy the section structure and command sequencing, not the concrete
production artifact values.

---

### `model/scripts/sft/docs/configs.md` (config reference, file-I/O)

**Analogs:** `model/scripts/sft/run_config.example.toml`, `model/src/gemini_sft/config.py`

**Config field contract** (`model/src/gemini_sft/config.py` lines 174-181):
```python
def load_run_config(path: str | Path) -> RunConfig:
    """Load, validate, and resolve an external TOML run config."""
    return _load_run_config(path, require_training_manifests=True)

def load_eval_run_config(path: str | Path) -> RunConfig:
    """Load an eval TOML config without requiring train/validation manifests."""
    return _load_run_config(path, require_training_manifests=False)
```

**Eval target validation contract** (`model/src/gemini_sft/config.py` lines 467-485):
```python
expected_keys = {"label", "model"}
keys = set(raw_target)
if keys != expected_keys:
    ...
    raise RunConfigError(msg)

label = _required_artifact_label(raw_target, "eval.model.label")
model = _required_str(raw_target, "eval.model.model")
return EvalModelTarget(label=label, model=model)
```

**Execution controls contract** (`model/src/gemini_sft/config.py` lines 500-529):
```python
unsupported_fields = sorted(
    set(raw_execution) - {"backend", "concurrency", "limit", "max_retries"}
)
...
return EvalExecutionConfig(
    backend=_optional_eval_execution_backend(...),
    limit=_optional_positive_int(...),
    concurrency=_optional_positive_int(..., default=16),
    max_retries=_optional_positive_int(..., default=3),
)
```

**Apply:** Document fields by table, then show short snippets for base, tuned
endpoint/checkpoint endpoint, and masked eval. The base/tuned/checkpoint snippets
must all use the same `[eval.model]` table shape.

---

### `model/scripts/sft/docs/metrics.md` (metric glossary, transform)

**Analogs:** `model/src/gemini_sft/reporting.py`, `model/src/common/scoring.py`, `model/tests/gemini_sft/test_reporting.py`

**Canonical public columns** (`model/src/gemini_sft/reporting.py` lines 18-32):
```python
REPORT_COLUMNS = (
    "target_label",
    "model",
    "wer",
    "cer",
    "keyword_accuracy",
    "empty_or_unintelligible_rate",
    "empty_response_rate",
    "insertions",
    "deletions",
    "substitutions",
    "total_reference_words",
    "missing_prediction_count",
    "artifacts",
)
```

**Metric construction pattern** (`model/src/gemini_sft/reporting.py` lines 104-128):
```python
wer_result = compute_wer(refs, hyps, normalizer=normalizer)
cer_result = compute_cer(refs, hyps, normalizer=normalizer)
keyword_rows = keyword_metrics(refs, hyps, keywords)
...
empty_or_unintelligible_rate=hallucination_rate(hyps),
empty_response_rate=empty_response_rate(hyps),
insertions=int(wer_result["insertions"]),
deletions=int(wer_result["deletions"]),
substitutions=int(wer_result["substitutions"]),
total_reference_words=total_reference_words,
missing_prediction_count=missing_prediction_count,
```

**Empty metric definitions** (`model/src/common/scoring.py` lines 245-276):
```python
def hallucination_rate(hypotheses: list[str]) -> float:
    """Percentage of hypotheses that are empty or the [UNINTELLIGIBLE] token."""
    ...
    flagged = sum(
        1
        for h in hypotheses
        if not h.strip() or h.strip() == "[UNINTELLIGIBLE]"
    )

def empty_response_rate(hypotheses: list[str]) -> float:
    """Percentage of hypotheses whose stripped text is exactly empty."""
```

**Legacy-name guard** (`model/tests/gemini_sft/test_reporting.py` lines 39-54):
```python
for key in (
    "empty_or_unintelligible_rate",
    "empty_response_rate",
    ...
):
    self.assertIn(key, row)
self.assertNotIn("empty_rate", row)
self.assertNotIn("hallucination_rate", row)
```

**Apply:** Define every current report column from `REPORT_COLUMNS`. Do not make
legacy names primary. Explain that missing provider rows are scored as empty
hypotheses but remain operationally separate as `missing_prediction_count`.

---

### `model/scripts/sft/docs/artifacts.md` (artifact reference, file-I/O)

**Analogs:** `model/scripts/sft/README.md`, `model/src/gemini_sft/evaluate.py`, `model/src/gemini_sft/records.py`

**GCS run prefix layout** (`model/scripts/sft/README.md` lines 137-169):
```text
gs://<bucket>/sft/runs/<round-id>/
  run_config.toml
  config.json
  status.json
  manifests/canonical/train.jsonl
  manifests/canonical/validation.jsonl
  manifests/canonical/eval.jsonl
  model_inputs/gemini/train.jsonl
  model_inputs/gemini/validation.jsonl
  preflight/report.json
  tuning/status.json
  evals/README.txt
...
evals/wer_summary.json
evals/wer_summary.md
```

**Normalized manifest path pattern** (`model/scripts/sft/README.md` lines 171-179):
```text
gs://<bucket>/inference_manifests/<inference_dataset_slug>/<model_family_slug>/<round_id>/base.jsonl
gs://<bucket>/inference_manifests/<inference_dataset_slug>/<model_family_slug>/<round_id>/<target-label>.jsonl
```

**Durable summary URI helper** (`model/src/gemini_sft/records.py` lines 87-93):
```python
def wer_summary_gcs_uris(run_gcs_prefix: str) -> tuple[str, str]:
    """Return stable run-level WER summary GCS artifact URIs."""
    prefix = run_gcs_prefix.rstrip("/")
    return (
        f"{prefix}/evals/wer_summary.json",
        f"{prefix}/evals/wer_summary.md",
    )
```

**Eval upload pattern** (`model/src/gemini_sft/evaluate.py` lines 221-230, 273-280):
```python
summary_json_uri, summary_markdown_uri = wer_summary_gcs_uris(run_gcs_prefix)
artifacts = ReportArtifacts(
    raw_output_uri=raw_output_uri,
    online_predictions_uri=online_predictions_uri,
    normalized_manifest_uri=inference_manifest_uri,
    summary_json_uri=summary_json_uri,
    summary_markdown_uri=summary_markdown_uri,
)
...
upload_local_file(storage_client, summary_json_path, summary_json_uri)
upload_local_file(storage_client, summary_markdown_path, summary_markdown_uri)
```

**Apply:** Make GCS the source of truth. Local `results/<round-id>/` is a
cache/mirror only, matching README lines 181-188.

---

### `model/scripts/sft/docs/hygiene.md` (artifact hygiene docs, file-I/O)

**Analogs:** `.gitignore`, `model/scripts/sft/README.md`

**Existing ignore coverage** (`.gitignore` lines 23-24, 89-93):
```gitignore
mise.local.toml
mise.*.local.toml

# SFT pipeline build outputs ...
model/scripts/sft/results/**/*.jsonl
model/scripts/sft/results/**/*.jsonl.gz
```

**Local mirror warning** (`model/scripts/sft/README.md` lines 181-188):
```markdown
Local `results/<round-id>/` files are a mirror/cache only. `config.json` in GCS
is the durable state machine...
The stable `evals/wer_summary.json` and `evals/wer_summary.md` files are
overwritten after each successful eval rerun for the same `round_id`.
```

**Apply:** End the runbook with a concrete pre-commit checklist. Include commands
that inspect `git status --short` for accidental `.local.toml`, root `results/`,
`model/data/inference_manifests/*.jsonl`, raw prediction JSONL, and local eval
outputs. If `.gitignore` is updated, keep rules narrow and avoid hiding intended
placeholder docs/configs.

---

### `.gitignore` (config, file-I/O)

**Analog:** existing `.gitignore`

**Pattern to extend only if needed** (lines 89-93):
```gitignore
# SFT pipeline build outputs ...
model/scripts/sft/results/**/*.jsonl
model/scripts/sft/results/**/*.jsonl.gz
```

**Gap to evaluate in planning:** Current rules do not ignore root `results/`,
local `*.local.toml`, or local `model/data/inference_manifests/*.jsonl` artifacts
shown in the current dirty worktree. Planning should either add narrow ignore
rules or make the docs/test hygiene guard catch those paths explicitly.

---

### `model/tests/common/tests/test_drift_guard.py` (test guard, file-I/O)

**Analog:** `model/tests/common/tests/test_drift_guard.py`

**Imports/helper pattern** (lines 1-20):
```python
from __future__ import annotations

import ast
import json
import tempfile
import unittest
from pathlib import Path
...
_MODEL_DIR = Path(__file__).resolve().parents[3]
_SCRIPTS_DIR = _MODEL_DIR / "scripts"
```

**Committed text guard pattern** (lines 169-178):
```python
def test_sft_example_config_uses_singular_eval_model(self) -> None:
    text = (_SCRIPTS_DIR / "sft" / "run_config.example.toml").read_text(
        encoding="utf-8"
    )

    self.assertIn("[eval.model]", text)
    self.assertIn("one model per config", text)
    self.assertIn("[eval.execution]", text)
    self.assertIn("max_retries = 3", text)
    self.assertNotIn("[[eval.models]]", text)
```

**Apply:** If adding a lightweight artifact hygiene guard, add another text-only
test here that reads `.gitignore`, `model/scripts/sft/docs/hygiene.md`, and/or
the README to assert the prohibited local artifact classes are named. Keep it
free of GCS, Vertex, Docker, and broad filesystem traversal.

## Shared Patterns

### OKF Markdown

**Source:** Phase 5 context plus OKF v0.1 spec/blog.
**Apply to:** `model/scripts/sft/docs/*.md`

Use lightweight YAML frontmatter:
```yaml
---
type: runbook
title: Gemini SFT Operator Runbook
description: End-to-end prepare, tune, eval, and report workflow.
tags: [gemini-sft, operator-docs]
---
```

External references:
- OKF spec: https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md
- Google Cloud OKF blog: https://cloud.google.com/blog/products/data-analytics/how-the-open-knowledge-format-can-improve-data-sharing

### Paid Vertex Boundary

**Source:** `model/scripts/sft/README.md` lines 224-228.
**Apply to:** README, runbook, configs, hygiene docs.

Docs should mark `gemini-sft tune --confirm`, `gemini-sft eval`, and checkpoint
online scoring as paid or potentially paid Vertex operations. State the intended
GCS output prefix before each paid command.

### Metrics Contract

**Source:** `model/src/gemini_sft/reporting.py` lines 18-32 and 104-128.
**Apply to:** `docs/metrics.md`, `docs/runbook.md`, README summary.

Metric docs must match `REPORT_COLUMNS`; do not introduce `empty_rate`,
`hallucination_rate`, `hits`, or `correct_words` as primary operator columns.

### Durable Artifact Authority

**Source:** `model/src/gemini_sft/config.py` lines 806-828,
`model/src/gemini_sft/evaluate.py` lines 211-230 and 273-280.
**Apply to:** runbook, artifacts, hygiene docs.

GCS `config.json`, canonical manifests, model inputs, eval target outputs,
normalized inference manifests, and `evals/wer_summary.{json,md}` are durable
inspection points. Local `results/` is cache/mirror only.

### Lightweight Verification

**Source:** `model/data/manifests/README.md` lines 64-65 and
`model/tests/common/tests/test_drift_guard.py` lines 169-178.
**Apply to:** docs-only changes and optional hygiene guard.

For docs-only edits, prefer `git diff --check`. If code/test is added, keep it
targeted and text-only unless the planner deliberately chooses a narrow model
test.

## No Analog Found

| File/Concern | Role | Data Flow | Reason |
|--------------|------|-----------|--------|
| Local OKF doc generator/build tool | config/tooling | batch | No local OKF build or validation tool exists; Phase 5 context explicitly says not to add a required documentation build tool unless already used locally. |
| Source-doc OKF frontmatter template | documentation | request-response | Source docs use plain Markdown headings; only `.planning` artifacts have YAML-style frontmatter. Use context-provided fields and OKF spec guidance. |

## Metadata

**Analog search scope:** `.planning/`, `.gitignore`, `model/scripts/sft/`,
`model/src/gemini_sft/`, `model/src/common/scoring.py`, `model/tests/`,
`model/data/`, `model/research/`, `documentation/`

**Files scanned:** 40+ candidate docs/code/test files via `rg`/`find`; 18 files
read directly.

**Pattern extraction date:** 2026-06-28

---
type: reference
title: Gemini SFT Artifact Reference
description: Durable GCS state and local cache locations for Gemini SFT runs.
tags: [gemini-sft, artifacts]
---

# Gemini SFT Artifact Reference

## Durable GCS State

GCS is the durable source of truth for config-driven Gemini SFT runs. The run
prefix is:

```text
gs://BUCKET/sft/runs/ROUND_ID/
```

Every prepared round includes:

- `gs://BUCKET/sft/runs/ROUND_ID/run_config.toml`
- `gs://BUCKET/sft/runs/ROUND_ID/config.json`
- `gs://BUCKET/sft/runs/ROUND_ID/manifests/canonical/eval.jsonl`

Training rounds additionally include `status.json`, canonical train and
validation manifests, `model_inputs/gemini/{train,validation}.jsonl`,
`preflight/report.json`, `tuning/status.json`, and `evals/README.txt`. Eval-only
rounds set `config.json` status to `eval_prepared` and intentionally omit those
training-only artifacts, including the root `status.json`.

Successful evaluations add:

- `gs://BUCKET/sft/runs/ROUND_ID/evals/wer_summary.json`
- `gs://BUCKET/sft/runs/ROUND_ID/evals/wer_summary.md`

`config.json` is the durable state machine for resume and eval. Local copies are
mirrors only.

## Eval Model Artifacts

Each `[eval.model].label` gets a stable eval directory under the run prefix.
Zero-context batch and online backends write these provider-output artifacts:

- `gs://BUCKET/sft/runs/ROUND_ID/evals/LABEL/input.jsonl`
- `gs://BUCKET/sft/runs/ROUND_ID/evals/LABEL/output/`
- `gs://BUCKET/sft/runs/ROUND_ID/evals/LABEL/batch_job.meta.json`
- `gs://BUCKET/sft/runs/ROUND_ID/evals/LABEL/batch_predictions.meta.json`
- `gs://BUCKET/sft/runs/ROUND_ID/evals/LABEL/online_predictions.jsonl`
- `gs://BUCKET/sft/runs/ROUND_ID/evals/LABEL/online_predictions.meta.json`

Nonzero-context rolling evaluation instead writes stable target-level audit
artifacts plus digest-versioned per-wave attempt artifacts:

- `gs://BUCKET/sft/runs/ROUND_ID/evals/LABEL/rolling_history_index.json`
- `gs://BUCKET/sft/runs/ROUND_ID/evals/LABEL/rolling_history_audit.jsonl`
- `gs://BUCKET/sft/runs/ROUND_ID/evals/LABEL-rolling-wave-NNNN-DIGEST/online_predictions.jsonl`
- `gs://BUCKET/sft/runs/ROUND_ID/evals/LABEL-rolling-wave-NNNN-DIGEST/online_predictions.meta.json`

The online predictions JSONL is an attempt cache. It can include successful
prediction rows and the latest errored attempt rows. Successful rows are reused
on resume; errored rows are retried.

The rolling-history files are present for nonzero prior context. The index maps
causal waves to their durable online attempt artifacts. If a recovered upstream
prediction changes a downstream request, that wave's digest and artifact path
also change; the stale artifact remains available for audit but is not reused.
The JSONL audit records row identity, eligible and supplied dependency counts,
omission categories, and dependency hashes without transcript text. It is
evidence that history came from the same target's finalized earlier
predictions. Errors, missing or blank outputs, and case-insensitive
`[UNINTELLIGIBLE]` outputs are omitted after the last K candidate rows are
selected; older candidates do not refill those slots.

For batch eval, `batch_job.meta.json` records the submitted Vertex job name and
request identity before polling so an interrupted process can resume the same
job. It is recovery state, not a distributed lock. The
`batch_predictions.meta.json` completion sidecar is published only after usable
output has been loaded and validated for reuse.

The stable report inspection points are:

- `gs://BUCKET/sft/runs/ROUND_ID/evals/wer_summary.json`
- `gs://BUCKET/sft/runs/ROUND_ID/evals/wer_summary.md`

Those summary URIs are overwritten by the latest successful eval for the same
`ROUND_ID`.

## Normalized Inference Manifests

Eval also uploads a scorer-ready normalized inference manifest outside the SFT
run prefix:

```text
gs://BUCKET/inference_manifests/INFERENCE_DATASET_SLUG/MODEL_FAMILY_SLUG/ROUND_ID/LABEL.jsonl
```

The normalized manifest is assembled only after provider inference has
finalized. At that point, evaluation references are joined back for scoring. It
preserves the eval source rows and adds prediction fields for rows that received
successful provider predictions. Missing provider outputs and unresolved online
errors omit prediction fields and are scored as empty hypotheses in eval
reports. The normalized manifest is a durable artifact, not a local experiment
output.

## Local Cache Or Mirror

Local `results/ROUND_ID/` and downloaded or generated local mirrors are cache
only. They are useful for inspection while a command runs, but they are not
evidence that durable eval reuse will succeed.

Use GCS `config.json`, request-identity metadata, target output metadata,
normalized inference manifests, and `evals/wer_summary.{json,md}` to inspect or
reuse a run. If local cache contents disagree with GCS, treat GCS as
authoritative.

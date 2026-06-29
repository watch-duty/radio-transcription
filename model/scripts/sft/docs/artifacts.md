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

Durable run-level files include:

- `gs://BUCKET/sft/runs/ROUND_ID/run_config.toml`
- `gs://BUCKET/sft/runs/ROUND_ID/config.json`
- `gs://BUCKET/sft/runs/ROUND_ID/status.json`
- `gs://BUCKET/sft/runs/ROUND_ID/manifests/canonical/{train,validation,eval}.jsonl`
- `gs://BUCKET/sft/runs/ROUND_ID/model_inputs/gemini/{train,validation}.jsonl`
- `gs://BUCKET/sft/runs/ROUND_ID/evals/wer_summary.json`
- `gs://BUCKET/sft/runs/ROUND_ID/evals/wer_summary.md`

`config.json` is the durable state machine for resume and eval. Local copies are
mirrors only.

## Eval Target Artifacts

Each `[eval.model].label` gets its own eval target directory under the run
prefix. Batch and online targets write different provider-output artifacts:

- `gs://BUCKET/sft/runs/ROUND_ID/evals/LABEL/input.jsonl`
- `gs://BUCKET/sft/runs/ROUND_ID/evals/LABEL/output/`
- `gs://BUCKET/sft/runs/ROUND_ID/evals/LABEL/batch_predictions.meta.json`
- `gs://BUCKET/sft/runs/ROUND_ID/evals/LABEL/online_predictions.jsonl`
- `gs://BUCKET/sft/runs/ROUND_ID/evals/LABEL/online_predictions.meta.json`

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

The normalized manifest preserves the eval source rows and adds prediction
fields for rows that received provider predictions. It is a durable artifact,
not a local experiment output.

## Local Cache Or Mirror

Local `results/ROUND_ID/` and downloaded or generated local mirrors are cache
only. They are useful for inspection while a command runs, but they are not
evidence that durable eval reuse will succeed.

Use GCS `config.json`, request-identity metadata, target output metadata,
normalized inference manifests, and `evals/wer_summary.{json,md}` to inspect or
reuse a run. If local cache contents disagree with GCS, treat GCS as
authoritative.

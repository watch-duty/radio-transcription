---
type: reference
title: Gemini SFT Artifact Ownership
description: Durable state, cache boundaries, and comparison isolation for Gemini SFT.
tags: [gemini-sft, artifacts]
sources:
  - resource: ../../../src/gemini_sft/artifacts.py
    title: Artifact path construction and durable state helpers
  - resource: ../../../src/gemini_sft/target_execution.py
    title: Provider-output persistence and reuse
---

# Gemini SFT Artifact Ownership

[`artifacts.py`](../../../src/gemini_sft/artifacts.py) owns artifact names and
paths. This document records only the ownership rules operators need.

## Durable Versus Local

GCS is authoritative for prepared configuration, tuning state, provider
outputs, reports, and normalized inference manifests. Local
`results/ROUND_ID/` is a cache or mirror.

If local and durable state disagree, use the durable state and the current
implementation's validation rules.

## Stable Namespaces

Run state is rooted under:

```text
gs://BUCKET/sft/runs/ROUND_ID/
```

Training rounds additionally persist canonical training and validation inputs,
preflight evidence, and tuning state under this prefix. Eval-only rounds omit
training-only state. Exact filenames and status requirements remain code-owned.

Normalized inference manifests are published under the shared
`inference_manifests/` namespace. The exact layout is code-owned and may evolve;
obtain current URIs from durable configuration, metadata, or report artifacts
rather than reconstructing paths from documentation.

## Recovery And Reuse

Provider outputs are reusable only when current code accepts their request
identity and completion metadata. A file's existence alone is insufficient.

When recovering:

1. inspect durable configuration and provider metadata;
2. confirm the target, manifest, prompts, context, and generation identity;
3. let current code decide whether to resume, retry, or reject cached output;
4. retain stale or failed artifacts for diagnosis without treating them as the
   active result.

For zero-context batch evaluation, `batch_job.meta.json` records the submitted
Vertex job and its request identity before polling. Current code validates that
sidecar when resuming; its existence alone is not a distributed lock or proof
of completed, reusable predictions.

Detailed snapshot, retry, batch-job, and rolling-history behavior belongs to
[`target_execution.py`](../../../src/gemini_sft/target_execution.py).

## Comparison Isolation

Use a distinct `round_id` for every model target and evaluation geometry.
Separate run prefixes prevent one comparison from overwriting or reusing
another comparison's state.

Reports and row-level predictions must be reviewed together. The report gives
aggregate quality and execution signals; the normalized manifest provides the
row-level evidence used for derived analysis.

## Handoff Checklist

For a durable handoff, record:

- the run identity;
- the frozen manifest identity;
- the model or endpoint resource;
- the tuning job when applicable;
- the report URI;
- the normalized row-level prediction URI;
- unresolved provider errors or missing predictions.

Do not hand off a local cache path as the durable result.

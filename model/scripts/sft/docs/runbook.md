---
type: runbook
title: Gemini SFT Operator Runbook
description: Stable prepare, tune, and evaluation workflow for Gemini SFT.
tags: [gemini-sft, operator-docs]
sources:
  - resource: ../README.md
    title: CLI entry point and code ownership map
  - resource: https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/start/gcp-auth
    title: Google Cloud local authentication
---

# Gemini SFT Operator Runbook

This runbook describes operator decisions and command order. The
[implementation ownership map](../README.md#code-ownership) is authoritative
for fields, defaults, validation, state transitions, retries, and artifact
names.

## Before You Start

Use a reviewed repository commit containing the code you intend to run.
Authenticate a local workstation with Application Default Credentials:

```bash
gcloud auth application-default login
```

The identity must be able to read the manifests and audio, write the chosen GCS
run prefix, and use Vertex tuning or inference as applicable.

Open the repository's lightweight ASR container:

```bash
docker compose -f asr-eval-docker-compose.yml run --rm notebooks-cpu bash
```

Inside the container, inspect current command help:

```bash
gemini-sft --help
```

Use one operator or process for preparation and tuning. Choose a unique
`round_id` whose durable run prefix is empty.

## 1. Prepare A Configuration

Copy [`run_config.example.toml`](../run_config.example.toml) outside version
control. Replace its placeholders and review every active value. The example is
a schema reference, not a recommended experiment.

Use separate configs and run identities for different models, checkpoints,
datasets, masking variants, prompts, or prior-context geometries. See
[configuration guidance](configs.md).

## 2. Prepare And Preflight

```bash
gemini-sft prepare --config /path/to/run.toml
```

Preparation validates and freezes the run configuration and inputs. It does not
submit tuning or inference.

Do not continue after a preparation failure. Correct the underlying issue and
follow the behavior enforced by current code; do not work around validation by
editing durable artifacts. Once prepared, treat the run identity and resolved
configuration as immutable.

## 3. Submit Or Resume Tuning

Skip this step for eval-only runs.

```bash
gemini-sft tune --config /path/to/run.toml --confirm
```

This can create paid Vertex work. Before running it, verify the base model,
datasets, hyperparameters, project, region, and run identity.

If the local process stops, inspect the durable run state and Vertex before
rerunning. Current recovery and submission behavior lives in
[`tune.py`](../../../src/gemini_sft/tune.py); it is not a distributed lock.
Never run concurrent tuning commands for the same round.

## 4. Inventory Checkpoints

After tuning finishes, record every checkpoint's ID, epoch, step, and exact
endpoint resource from Vertex. Do not derive an endpoint location from the
tuning region; use the returned resource.

Create a new eval-only config and `round_id` for each checkpoint and evaluation
lane. Tuning does not retarget its prepared training configuration.

## 5. Prepare And Run Evaluation

Prepare every eval-only config before evaluation:

```bash
gemini-sft prepare --config /path/to/eval-run.toml
gemini-sft eval --config /path/to/eval-run.toml
```

Evaluation can create paid batch or online inference. Independent prepared runs
may execute in parallel when quota permits.

For zero-context batch evaluation, `batch_job.meta.json` records the submitted
job identity before polling. On recovery, let current code validate this
sidecar and decide whether to resume the existing job; do not infer reusability
from the file's existence alone.

For prior-context evaluation, references must never enter provider requests.
History comes from the evaluated target's earlier predictions according to the
current implementation. See
[evaluation methodology](evaluation-methodology.md#predicted-prior-context).

## 6. Accept A Result

Do not treat the existence of a report as proof of complete inference. Confirm:

- target and manifest identity;
- expected evaluation row count;
- provider error and missing-prediction status;
- every required checkpoint and lane;
- durable report and row-level prediction artifacts.

Interpret execution gaps separately from transcription quality. See
[metric interpretation](metrics.md) and
[artifact ownership](artifacts.md).

## Recovery Principles

- GCS state is authoritative; local `results/` is a cache.
- Reuse only when current code accepts the durable request identity.
- Inspect Vertex before rerunning when submission may have succeeded but durable
  job identity is absent.
- A terminal failed or cancelled tuning job requires diagnosis and a new run
  identity for another attempt.
- Provider retry and snapshot behavior belongs to
  [`target_execution.py`](../../../src/gemini_sft/target_execution.py), not
  copied runbook prose.

## Before Committing

Run the checks in [artifact hygiene](hygiene.md), including the shared staged
artifact guard:

```bash
git status --short --ignored
git diff --cached --name-only | rg '(^results/|^model/data/inference_manifests/|\.local\.toml$|^model/scripts/sft/results/.*\.jsonl(\.gz)?$|online_predictions\.jsonl$|batch_predictions.*\.jsonl$)'
```

Do not commit real run configs, predictions, downloaded manifests,
credentials, or local result caches unless explicitly requested.

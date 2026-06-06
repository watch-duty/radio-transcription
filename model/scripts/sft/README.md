# Watch Duty Gemini SFT CLI

Gemini supervised fine-tuning is exposed as the packaged `gemini-sft` command
from the `radio-transcription-model` distribution under `model/`.

## Runtime

The recommended operator runtime is the lightweight ASR Docker service. It
mounts the repo at `/workspace` and installs `/workspace/model[scoring,vertex]`
in editable mode on container startup, so notebooks and CLI workflows see live
package changes.

```bash
docker compose -f asr-eval-docker-compose.yml run --rm notebooks-cpu \
  bash -lc 'gemini-sft --help'
```

Local fallback from the repo root:

```bash
python3 -m pip install -e "model[scoring,vertex]"
gemini-sft --help
```

## Commands

```bash
gemini-sft prepare --config /path/to/run.toml
gemini-sft tune --config /path/to/run.toml --confirm
gemini-sft eval --config /path/to/run.toml
```

`prepare` builds Gemini model-input JSONL, copies canonical manifests into the
run prefix, and runs preflight checks. `tune` submits or resumes the paid Vertex
SFT job. `eval` runs batch inference and writes WER/CER/keyword summaries.

There is no compatibility wrapper for the previous script entrypoint.

## Run Config

Real run configs are external inputs and should not be committed. Commit only
placeholder examples.

```toml
round_id = "YYYY-MM-DD-short-description"
dataset = "dataset-version-name"
train_manifest_uri = "gs://your-bucket/path/manifests/canonical/train.jsonl"
validation_manifest_uri = "gs://your-bucket/path/manifests/canonical/validation.jsonl"
eval_manifest_uri = "gs://your-bucket/path/manifests/canonical/eval.jsonl"

[gcp]
project = "your-gcp-project"
bucket = "your-gcs-bucket"
location = "us-central1"

[sft]
base_model = "gemini-3.1-flash-lite"
epoch_count = 6
adapter_size = "SIXTEEN"
learning_rate_multiplier = 1.0

[prompts]
# Optional inline overrides only.
# system = "..."
# user = "..."
```

Supported adapter sizes are `ONE`, `TWO`, `FOUR`, `EIGHT`, and `SIXTEEN`.

## Records

Authoritative records are written to:

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
```

Local `results/<round-id>/` files are a mirror/cache only. Evaluation summaries
include GCS batch-output paths so WER can be recalculated from raw inference
results.

## Prompt Parity

Gemini prompts live in `common.gemini.prompts`. Gemini request construction,
Vertex tuning, batch inference, and batch-output parsing live in
`common.gemini.vertex`. Drift-guard tests enforce that the SFT workflow and
maintained Gemini eval notebook import the same helpers.

## Verification

Unit tests mock GCS and Vertex boundaries. They must not submit paid Vertex
tuning jobs, run Vertex batch inference, execute notebooks, or run end-to-end
evals.

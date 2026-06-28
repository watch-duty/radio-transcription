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

`round_id` names the GCS run prefix `gs://<bucket>/sft/runs/<round_id>/`.
Use a new `round_id` for each experiment; the CLI treats an existing prefix as
owned by that run and will not overwrite it as a fresh run. `round_id` must be
a single portable path component: letters, numbers, `.`, `_`, and `-` only.

```toml
round_id = "YYYY-MM-DD-short-description"
dataset = "dataset-version-name"
inference_dataset_slug = "echo/eval"
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

[eval]
[[eval.models]]
label = "base"
model = "gemini-3.1-flash-lite"

[eval.execution]
# Optional. Omit backend for default routing.
# backend = "online"
# limit = 100
concurrency = 16
max_retries = 3

[prompts]
# Optional inline overrides only.
# system = "..."
# user = "..."
```

Supported adapter sizes are `ONE`, `TWO`, `FOUR`, `EIGHT`, and `SIXTEEN`.
Prompt overrides are inline-only. Local prompt files are intentionally rejected
because the resolved prompt text is copied into `config.json` for reproducible
resume/eval runs.

`dataset` identifies the SFT training recipe/version. `inference_dataset_slug`
identifies the evaluated corpus/split used for normalized inference-manifest
output placement, such as `echo/eval`. Older local `run.toml` files must add
this field before they can be used with the current CLI.

Eval requires explicit `[[eval.models]]` targets. Each target contains only a
`label` and `model`; `model` is a publisher model ID or Vertex
endpoint/checkpoint resource string by operator intent. Eval does not
synthesize targets from `[sft].base_model` or the tuned `endpoint` stored in
GCS `config.json`.

`[eval.execution]` is optional. When `backend` is omitted, publisher/model IDs
default to batch inference and full endpoint resources default to online
generate-content. Set `backend = "batch"` or `backend = "online"` only when the
whole run should force one backend; use separate config files/runs to compare
forced backends. `limit = 100` is an optional smoke row cap applied to the
evaluated prefix before prior-context histories are built. `concurrency` and
`max_retries` control online target execution.

The legacy `score_gemini_sft_checkpoints_online.py` script still discovers
checkpoint endpoints from a tuning job for sweep/ranking workflows. Packaged
`gemini-sft eval` does not discover checkpoints; list each checkpoint endpoint
explicitly in `[[eval.models]]` when using the packaged eval path.

Masked and unmasked evals are separate config files/runs with distinct
`round_id`, `eval_manifest_uri`, and `inference_dataset_slug` values. There is
no `eval_label`, `masked` field, or eval-sibling abstraction.

## Data Split Contract

The input manifests are canonical row-per-segment JSONL, not Gemini SFT JSONL.
`prepare` stores those canonical manifests under the run prefix, then derives
Gemini model-input JSONL only for train and validation.

`validation_manifest_uri` is wired into the Vertex tuning job as the validation
dataset. `eval_manifest_uri` is held out for reporting and is converted to
batch-inference requests during `eval`, not during `prepare`.

`prepare` rejects train/validation and train/eval audio URI overlap. Validation
and eval may intentionally point at the same manifest for runs where the Vertex
validation set is also the final reporting set; only training audio must stay
out of both. `prepare` also runs preflight checks against both train and
validation Gemini JSONL because malformed validation rows can fail the paid
Vertex job just like malformed training rows.

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

After `eval`, the same prefix also contains batch inference inputs and outputs.
Each target label gets its own artifact directory. Batch targets contain
`input.jsonl` and `output/`; online targets contain
`online_predictions.jsonl` and `online_predictions.meta.json`:

```text
evals/base/input.jsonl
evals/base/output/
evals/checkpoint_6/online_predictions.jsonl
evals/checkpoint_6/online_predictions.meta.json
```

`gemini-sft eval` also writes normalized inference manifests under the shared
`inference_manifests/` tree. These JSONL files preserve the eval source rows and
add a prediction field named for the model family on rows that received a
prediction record:

```text
gs://<bucket>/inference_manifests/<inference_dataset_slug>/<model_family_slug>/<round_id>/base.jsonl
gs://<bucket>/inference_manifests/<inference_dataset_slug>/<model_family_slug>/<round_id>/<target-label>.jsonl
```

Local `results/<round-id>/` files are a mirror/cache only. `config.json` in GCS
is the durable state machine: if it contains `job_name`, `tune` reattaches to
that Vertex tuning job instead of submitting another one. Evaluation summaries
include raw Vertex batch-output URIs and normalized inference-manifest URIs, so
WER can be recalculated from provider responses or from the scorer-ready JSONL.

## Evaluation Semantics

`eval` runs the durable `eval_models` targets from GCS `config.json`. Missing
provider predictions are scored as empty hypotheses, which makes them count as
full deletions instead of removing those segments from the denominator. The
normalized inference manifests leave `pred_text_*` absent for missing
prediction records; explicit empty model outputs are written as
`pred_text_* = ""`.

Eval manifests must use one unique model-ready `audio_filepath` clip URI per
row. The batch path rejects duplicate audio URIs because one provider prediction
record cannot be assigned to multiple manifest rows.

Batch inference uses `[gcp].location` unless a full resource name carries its
own location. Online endpoint targets use the endpoint resource location when
present.

## Prompt Parity

Gemini prompts live in `common.gemini.prompts`. Gemini request construction,
Vertex tuning, batch inference, and batch-output parsing live in
`common.gemini.vertex`. Drift-guard tests enforce that the SFT workflow and
maintained Gemini eval notebook import the same helpers.

## Verification

Unit tests mock GCS and Vertex boundaries. They must not submit paid Vertex
tuning jobs, run Vertex batch inference, execute notebooks, or run end-to-end
evals.

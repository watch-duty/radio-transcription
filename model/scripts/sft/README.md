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

`prepare` creates either a training round or an eval-only round. Training
rounds copy the canonical manifests, build train/validation Gemini model-input
JSONL, and run preflight checks. Eval-only rounds validate and copy only the
canonical eval manifest. `tune` submits or resumes the paid Vertex SFT job.
`eval` runs exactly the model target recorded for the prepared round and writes
one WER/CER/keyword report.

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

[eval.model]
label = "target-label"
model = "gemini-3.1-flash-lite"

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

Every prepared round that will be evaluated owns exactly one `[eval.model]`
target. The table must contain only `label` and `model`: `label` is the safe
path/report label for the target, and `model` is a publisher model ID or full
Vertex resource name. `[eval.model]` is optional for a training-only prepare,
but it is required by `gemini-sft eval` and by eval-only preparation.

Preparation copies the target into GCS `config.json`, and the target is
immutable for that `round_id`. Evaluation rejects a local `[eval.model]` that
does not match the durable target, and tuning never rewrites it to a newly
created endpoint. To evaluate an endpoint after tuning completes, create a new
eval-only config with a new `round_id`, omit both `train_manifest_uri` and
`validation_manifest_uri`, and set `[eval.model].model` to the endpoint resource
name. Eval-only configs still include `[sft]`; `sft.base_model` identifies the
publisher family for endpoint artifacts.

## Data Split Contract

The input manifests are canonical row-per-segment JSONL, not Gemini SFT JSONL.
`prepare` stores those canonical manifests under the run prefix, then derives
Gemini model-input JSONL only for train and validation.

`validation_manifest_uri` is wired into the Vertex tuning job as the validation
dataset. `eval_manifest_uri` is held out for reporting and is converted to
inference requests during `eval`, not during `prepare`.

`prepare` rejects train/validation and train/eval audio URI overlap. Validation
and eval may intentionally point at the same manifest for runs where the Vertex
validation set is also the final reporting set; only training audio must stay
out of both. `prepare` also runs preflight checks against both train and
validation Gemini JSONL because malformed validation rows can fail the paid
Vertex job just like malformed training rows.

## Records

Every prepared round writes these authoritative records:

```text
gs://<bucket>/sft/runs/<round-id>/
  run_config.toml
  config.json
  manifests/canonical/eval.jsonl
```

Training rounds additionally write `status.json`, the canonical train and
validation manifests, Gemini train/validation model-input JSONL,
`preflight/report.json`, `tuning/status.json`, and `evals/README.txt`. Eval-only
rounds intentionally omit those training-only artifacts.

After `eval`, batch artifacts use the immutable target label:

```text
evals/<target-label>/input.jsonl
evals/<target-label>/output/
evals/<target-label>/batch_job.meta.json
evals/<target-label>/batch_predictions.meta.json
```

`batch_job.meta.json` records the submitted job name and request identity before
polling, so a later invocation can resume that job. The
`batch_predictions.meta.json` completion sidecar is written only after usable
prediction output has been loaded; completed-output reuse requires both that
sidecar and prediction JSONL under `output/`. Online evaluation instead writes:

```text
evals/<target-label>/online_predictions.jsonl
evals/<target-label>/online_predictions.meta.json
```

Each successful eval also publishes the one-target report at
`evals/wer_summary.{json,md}`. The normalized inference manifest uses the same
target label:

```text
gs://<bucket>/inference_manifests/<inference_dataset_slug>/<model_family_slug>/<round_id>/<target-label>.jsonl
```

The normalized JSONL preserves the eval source rows and adds a prediction field
named for the evaluated model family on rows that received a prediction record.
Publisher targets derive that family from `[eval.model].model`; endpoint
targets derive it from `sft.base_model`.

Local `results/<round-id>/` files are a mirror/cache only. GCS `config.json` is
the durable state machine: if it contains a tuning `job_name`, `tune` reattaches
to that Vertex tuning job instead of submitting another one. The eval report
includes the target's raw batch-output or online-prediction URI and normalized
inference-manifest URI, so its metrics can be recalculated.

## Evaluation Semantics

`gemini-sft eval` evaluates exactly the `[eval.model]` target persisted by
`prepare` and writes one report containing one `target` object. Publisher model
IDs default to batch inference, while full Vertex endpoint resources default to
online inference; `[eval.execution].backend` may explicitly select either
backend.

Serialize `gemini-sft eval` invocations for the same `round_id` and evaluation
target. `batch_job.meta.json` lets a later invocation resume a job whose name
was persisted before polling, but this recovery state is not a distributed
submission lock; concurrent invocations can still submit duplicate jobs.

Missing provider predictions are scored as empty hypotheses, which makes them
count as full deletions instead of removing those segments from the denominator.
The normalized inference manifest leaves `pred_text_*` absent for a missing
prediction record; an explicit empty model output is written as
`pred_text_* = ""`.

Eval manifests must use one unique model-ready `audio_filepath` clip URI per
row. Evaluation rejects duplicate audio URIs because one provider prediction
record cannot be assigned safely to multiple manifest rows.

A full Vertex resource target uses the location embedded in its resource name.
Targets without an embedded location use the configured/default location
selected for that publisher model.

## Prompt Parity

Gemini prompts live in `common.gemini.prompts`. Gemini request construction,
Vertex tuning, batch inference, and batch-output parsing live in
`common.gemini.vertex`. Drift-guard tests enforce that the SFT workflow and
maintained Gemini eval notebook import the same helpers.

## Verification

Unit tests mock GCS and Vertex boundaries. They must not submit paid Vertex
tuning jobs, run Vertex batch inference, execute notebooks, or run end-to-end
evals.

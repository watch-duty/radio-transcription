---
type: runbook
title: Gemini SFT Operator Runbook
description: End-to-end prepare, tune, eval, and report workflow.
tags: [gemini-sft, operator-docs]
---

# Gemini SFT Operator Runbook

## Scope

This is the canonical operator path for Gemini SFT training and evaluation.
Training rounds prepare Gemini inputs and may submit or resume Vertex tuning.
Eval-only rounds prepare an existing publisher model, tuned endpoint, or
checkpoint for one evaluation without rebuilding training inputs. Both paths
use one immutable eval target per prepared round.

The packaged eval contract supports one `[eval.model]` per config/run. Base,
tuned endpoint, and checkpoint endpoint comparisons use separate configs or an
external wrapper.

## Before You Start

Use the lightweight ASR runtime from the repo root:

```bash
docker compose -f asr-eval-docker-compose.yml run --rm notebooks-cpu \
  bash -lc 'gemini-sft --help'
```

Create a local run TOML outside version control. Start from
`model/scripts/sft/run_config.example.toml` and replace placeholder values with
your GCS bucket, project, manifests, and run identity. Real run configs,
`.local.toml` files, raw predictions, downloaded inference manifests, and
`results/` output are local operator artifacts unless the user explicitly asks
to commit them.

Durable run state lives under:

```text
gs://BUCKET/sft/runs/ROUND_ID/
```

Local `results/ROUND_ID/` is only a cache or mirror. Use GCS artifacts for
resume, reuse, review, and handoff.

## 1. Prepare A Config

Use one config per run and one eval target per config:

```toml
[context]
prior_turn_count = 8
prior_context_mode = "text_turns"

[eval.model]
label = "base"
model = "gemini-3.1-flash-lite"
```

`text_turns` is the recommended prior-context shape for SFT: prior user turns
contain text only, prior model turns contain prior transcripts, and only the
current user turn contains audio. Use `transcript` for a simple inline prior
transcript block, or `guarded_transcript_block` for an inline block with
explicit instructions not to re-transcribe or continue prior turns.

Transcript provenance differs across training and evaluation. SFT preparation
may use preceding reference transcripts as supervised training data.
Evaluation never uses reference transcripts as context: it supplies only the
evaluated target model's own finalized predictions for eligible earlier rows.
The reference is joined back only after provider inference for scoring.

For a tuned endpoint or checkpoint endpoint, keep the same table shape and
change only the label and model resource:

```toml
[eval.model]
label = "checkpoint_6"
model = "projects/PROJECT/locations/us-central1/endpoints/ENDPOINT_ID"
```

To compare base, tuned, and checkpoint resources, create separate configs or
use an external wrapper that invokes the CLI once per config. The committed
example contains training manifests and a publisher-model eval target. A
training-only config may omit `[eval.model]`, but it cannot be evaluated.

### Prompt Configuration

System and user prompts are separate Gemini request fields: the system prompt
is the system instruction, and the user prompt is placed in each current user
turn. Use inline prompt strings only; prompt file fields are unsupported so the
resolved values remain reproducible in the durable config.

Omit `[prompts].user` to retain the legacy default text instruction. To match
production's audio-only user turn exactly, use:

```toml
[prompts]
# Omit `user` to keep the default text instruction.
# Set an explicit empty string for production's audio-only user turn.
user = ""
```

System prompts cannot be empty, and whitespace-only system or user values are
invalid. The explicit empty user value is allowed only with no prior context;
positive `context.prior_turn_count` requires a non-empty user prompt.

The tuned endpoint does not exist when its training round is prepared, and
tuning never mutates the round's eval target. After tuning completes, create a
new eval-only config with a new `round_id`, omit both `train_manifest_uri` and
`validation_manifest_uri`, and set `[eval.model].model` to the endpoint or
checkpoint resource. Run `prepare` again for that config before eval.

### Build A Validation Manifest

Vertex AI SFT requires the validation set to be smaller than 5000 rows.
This team's convention is to build `validation.jsonl` as a sample of
`eval.jsonl`, with every sampled row's `split` field relabeled to
`"validation"` (eval and validation are intentionally the same
underlying data, viewed through two different splits). Use:

```bash
uv run python model/scripts/sft/build_validation_manifest_from_eval.py \
  --eval gs://BUCKET/path/manifests/canonical/eval.jsonl \
  --out-validation-uri gs://BUCKET/path/manifests/canonical/validation.jsonl
```

Point `validation_manifest_uri` at the script's output. If you instead
hand `prepare` a validation manifest whose rows still carry
`split="eval"`, it fails with `split_mismatch` — that error's message
names this script; it means the manifest was never relabeled after being
copied from eval, not that the validator is wrong.

## 2. Build Gemini SFT Inputs

`prepare` is the non-paid validation and input-build step:

```bash
gemini-sft prepare --config /path/to/run.toml
```

For a training round, it copies canonical manifests into the run prefix,
validates exact-row and physical-recording split overlap, derives Gemini JSONL
for train and validation, writes preflight output, and stores resolved prompts
in durable GCS `config.json`. Every training-round row must provide explicit
physical-source provenance through `original_audio_uri`,
`source_audio.audio_filepath`, or both. When both are present, they are treated
as aliases for the same recording. Physical identity also uses equal optional
source SHA-256 values; it does not infer identity from filenames or model-ready
clip paths. For an eval-only round, `prepare`
validates and publishes only `run_config.toml`, `config.json`, and the canonical
eval manifest. Its durable config status is `eval_prepared`; it intentionally
does not publish the training-only root `status.json`.

Every prepared round has these durable inspection points:

- `config.json`
- `run_config.toml`
- `manifests/canonical/eval.jsonl`

Training rounds additionally have `status.json`, canonical train and validation
manifests, `model_inputs/gemini/*.jsonl`, `preflight/report.json`,
`tuning/status.json`, and `evals/README.txt`.

## 3. Submit Or Resume Vertex Tuning

This is a paid Vertex operation for training rounds. Eval-only rounds skip this
step. Before running it, confirm the expected tuning and run-state output
prefix:

```text
gs://BUCKET/sft/runs/ROUND_ID/
```

Then submit or resume tuning:

```bash
gemini-sft tune --config /path/to/run.toml --confirm
```

If `config.json` already records a tuning job, `tune` resumes that job instead
of submitting a new one. Review `tuning/status.json`, `status.json`, and
`config.json` in GCS to confirm job identity and endpoint state.

## 4. Run Eval

This is a paid or potentially paid Vertex operation because it can submit batch
inference or call online endpoint prediction. Before running it, confirm the
expected target prediction artifact prefix:

```text
gs://BUCKET/sft/runs/ROUND_ID/evals/LABEL/
```

Also confirm the stable report summary paths:

```text
gs://BUCKET/sft/runs/ROUND_ID/evals/wer_summary.{json,md}
```

Run eval for the single `[eval.model]` target in the config:

```bash
gemini-sft eval --config /path/to/run.toml
```

The round must already have been prepared with that target. To evaluate a tuned
or checkpoint endpoint created by another round, use the separately prepared
eval-only config from step 1.

Backend routing depends on the context count:

- With `prior_turn_count = 0`, the existing default routing applies: publisher
  model IDs use batch inference and endpoint resources use online prediction,
  unless a compatible backend is selected explicitly.
- With `prior_turn_count > 0`, an omitted backend automatically selects rolling
  online prediction. Explicit batch selection is rejected before any provider
  call because causal history depends on earlier predictions from the same
  evaluation.

Online model and endpoint resources use the location embedded in their full
resource name. Location-less publisher model IDs use the `us` multi-region
endpoint by default; this includes the short publisher ID
[`gemini-3.1-flash-lite`](https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini/3-1-flash-lite).

For nonzero context, `eval` builds transcript-free provider segments from the
manifest's source and timing metadata. It groups rows by split and source,
requires each history candidate to start strictly before and end no later than
the current clip, and
uses deterministic ordering. The configured last K candidates are selected
before unusable results are filtered. Errors, missing or blank predictions, and
case-insensitive `[UNINTELLIGIBLE]` predictions are omitted without refilling
from older rows. History contains prediction text only; every request contains
exactly one audio object, the current clip. Reference-bearing scoring rows are
retained in a separate collection and never passed to scheduling or request
construction; they are paired with predictions only after provider inference
has finalized.

The local TOML must match the durable GCS `config.json` for eval-affecting
fields, including `[eval.model]`, `[eval.execution].backend`,
`[eval.execution].limit`, prompts, prior-context settings, base model, and eval
manifest. Local `[eval.execution].concurrency` and
`[eval.execution].max_retries` are runtime controls and may be changed to resume
an online eval under different quota conditions. Changing only the local TOML
after `prepare` does not retarget a run; `eval` fails loudly on a mismatch. Use
the matching prepared config, or create a separate prepared `round_id` for a
different model or eval set.

Zero-context batch eval runs write `evals/LABEL/input.jsonl`,
`evals/LABEL/output/`,
`evals/LABEL/batch_job.meta.json`, and
`evals/LABEL/batch_predictions.meta.json`. The job sidecar is written before
polling so an interrupted invocation can resume the same Vertex job; it is not
a distributed submission lock. The prediction sidecar marks output reusable
only after it has been loaded and validated. Online endpoint eval runs write
`evals/LABEL/online_predictions.jsonl` and
`evals/LABEL/online_predictions.meta.json`. Successful online rows are reused
on resume. Errored rows are preserved for diagnosis but retried on the next
run. Rolling online eval additionally writes a transcript-free rolling index,
a per-row history audit, and per-wave online artifacts, so reviewers can verify
causal dependencies and omissions without exposing transcript content.

Eval also writes normalized inference manifests under the shared
`inference_manifests/` GCS tree and uploads `evals/wer_summary.json` and
`evals/wer_summary.md` to the run prefix. Existing batch or online outputs are
reused only when request-identity metadata matches the current target, prompts,
eval manifest, audio order, prior-context settings, generation config, and
safety settings. Cached output is rejected when its request identity differs.

## 5. Read Reports

Start with the console table from `gemini-sft eval`, then inspect the durable
summary files:

```text
gs://BUCKET/sft/runs/ROUND_ID/evals/wer_summary.json
gs://BUCKET/sft/runs/ROUND_ID/evals/wer_summary.md
```

The report row includes the configured target label and model, WER, CER,
keyword accuracy, empty-or-unintelligible rate, insertions, deletions,
substitutions, total reference words, missing prediction count, and artifact
URIs.

Missing provider predictions are scored as empty hypotheses and stay in the
WER/CER denominator. They are reported in `missing_prediction_count` and count
toward `empty_or_unintelligible_rate` because the scored hypothesis is empty.
Online endpoint failures that remain unresolved after retries are also reported
in metadata `online_error_count`.

## 6. Masked And Unmasked Eval Runs

Masked and unmasked evals are separate config files and separate runs. Keep the
same command sequence and change only the run identity and eval manifest
placement fields:

```toml
round_id = "YYYY-MM-DD-short-description-masked"
inference_dataset_slug = "echo/masked_v2/eval"
eval_manifest_uri = "gs://your-bucket/path/manifests/echo/masked_v2/eval.jsonl"
```

Use a second config for the unmasked manifest with its own `round_id`,
`inference_dataset_slug`, and `eval_manifest_uri`.

## 7. Artifact Hygiene Before Commit

Before committing, inspect tracked, untracked, and ignored files:

```bash
git status --short --ignored
```

Then check the staged set for local/generated experiment artifacts:

```bash
git diff --cached --name-only | rg '(^results/|^model/data/inference_manifests/|\.local\.toml$|^model/scripts/sft/results/.*\.jsonl(\.gz)?$|online_predictions\.jsonl$|batch_predictions.*\.jsonl$)'
```

Any match must be unstaged unless the user explicitly asked to commit that
generated artifact. Durable artifacts to inspect in GCS include `config.json`,
`status.json`, canonical manifests, `model_inputs/gemini/*.jsonl`,
`evals/LABEL/input.jsonl`, `evals/LABEL/output/`,
`evals/LABEL/batch_job.meta.json`,
`evals/LABEL/batch_predictions.meta.json`,
`evals/LABEL/online_predictions.jsonl`,
`evals/LABEL/online_predictions.meta.json`, normalized inference manifests, and
`evals/wer_summary.{json,md}`. Local `results/` is cache/mirror only. See
`docs/hygiene.md` for the detailed explanation.

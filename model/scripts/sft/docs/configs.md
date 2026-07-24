---
type: reference
title: Gemini SFT Config Examples
description: Placeholder config shapes for config-driven Gemini SFT runs.
tags: [gemini-sft, configs]
---

# Gemini SFT Config Examples

## Full Placeholder Config

Use [`../run_config.example.toml`](../run_config.example.toml) as the only
committed full placeholder config. Copy it outside the repo for a real run and
replace every placeholder before running `gemini-sft prepare`, `gemini-sft
tune`, or `gemini-sft eval`.

The example uses placeholder values only. In particular:

- `round_id = "YYYY-MM-DD-short-description"` becomes the GCS run prefix under
  `gs://<bucket>/sft/runs/<round_id>/`.
- `[gcp].bucket` is the bucket name only, not a `gs://` URI.
- Prompt overrides are inline strings only; prompt file fields are unsupported.
- `[eval.model]` is singular, with exactly `label` and `model`.

The full example prepares a training round whose configured eval target is the
publisher model. A training-only prepare may omit `[eval.model]`, but that round
cannot be passed to `gemini-sft eval`. After tuning produces an endpoint, prepare
a separate eval-only round for that endpoint as described below.

## Prior Context Contract

Use `[context]` to control the number and representation of prior same-source
transcripts:

```toml
[context]
prior_turn_count = 8
prior_context_mode = "text_turns"
```

The representation is shared, but transcript provenance is deliberately
different:

- During SFT preparation, prior reference transcripts are supervised training
  data.
- During evaluation, references are never provider input. History may contain
  only finalized predictions from the evaluated target model for eligible
  earlier rows. References are joined only after inference for scoring.

Supported context representations:

| Mode | Shape | Use |
| --- | --- | --- |
| `text_turns` | Prior `user(text prompt) -> model(prior transcript)` turns, then the current user turn with audio. | Recommended default for SFT prior context. |
| `transcript` | One current user turn with a simple numbered prior-transcript block plus current audio. | Compact one-turn context. |
| `guarded_transcript_block` | One current user turn with a guarded numbered prior-transcript block plus current audio. | Compact context with explicit "do not re-transcribe or continue prior turns" instructions. |

Training and rolling evaluation use the same transcript-free structural
schedule. Rows are grouped by split and source. Within floating-point boundary
tolerance, a dependency must start strictly before the current segment and
finish no later than the current start. Equal intervals and intervals where one
contains the other are rejected as duplicate contextual segments. Partial
overlap is allowed, but overlapping rows cannot become dependencies of each
other; both may become history for a later row after both have ended.

Contextual rows require one complete source-provenance tuple: either original
source URI plus original offset, or a complete `source_audio`
URI/offset/duration tuple. The configured K is applied to structural
dependencies before unusable references or predictions are omitted, without
refilling older rows. Every evaluation request contains transcript-only
predicted history and exactly one audio input: the current clip.

## Eval Target Snippets

Each eval run has one `[eval.model]` target. Compare base, tuned, and
checkpoint resources with separate config files or an external wrapper that
launches separate configs.

Base model:

```toml
[eval.model]
label = "base"
model = "gemini-3.1-flash-lite"
```

Tuned endpoint:

```toml
[eval.model]
label = "tuned"
model = "projects/PROJECT/locations/us-central1/endpoints/TUNED_ENDPOINT_ID"
```

Checkpoint endpoint:

```toml
[eval.model]
label = "checkpoint_6"
model = "projects/PROJECT/locations/us-central1/endpoints/CHECKPOINT_ENDPOINT_ID"
```

All three examples use the same `[eval.model]` table shape. The operator
chooses the label used for the report row and artifact directory; the `model`
value is the publisher model ID or Vertex endpoint resource that should be
evaluated.

## Eval-Only Target After Tuning

Tuning never rewrites a prepared round's immutable `[eval.model]`. To evaluate
the tuned endpoint or a checkpoint after it exists, copy the full config to a
new file, use a new `round_id`, omit both `train_manifest_uri` and
`validation_manifest_uri`, and set `[eval.model].model` to the endpoint resource.
Keep `eval_manifest_uri`, `[gcp]`, `[sft]`, `[context]`, and `[prompts]` as needed.
In an eval-only config, `sft.base_model` identifies the publisher model family
used to name normalized prediction fields.

Run `gemini-sft prepare` for this new config before `gemini-sft eval`. Eval-only
preparation validates and publishes only the config and canonical eval manifest;
it does not build tuning JSONL or submit a tuning job.

## Masked Eval Variant

Keep masked and unmasked evals as separate configs/runs. A masked eval variant
changes only the run identity and eval manifest placement fields:

```toml
round_id = "YYYY-MM-DD-short-description-masked"
inference_dataset_slug = "echo/masked_v2/eval"
eval_manifest_uri = "gs://your-bucket/path/manifests/echo/masked_v2/eval.jsonl"
```

Unmasked eval uses the normal full placeholder config with its own `round_id`,
`inference_dataset_slug`, and `eval_manifest_uri`.

## Placeholder Safety

Committed examples must stay placeholder-only. Acceptable example tokens include
`PROJECT`, `TUNED_ENDPOINT_ID`, `CHECKPOINT_ENDPOINT_ID`,
`YYYY-MM-DD-short-description`, and `gs://your-bucket/...`.

Before committing config docs or examples, check that any concrete-looking
project, endpoint, local path, run ID, or generated artifact path is either a
placeholder or has been removed.

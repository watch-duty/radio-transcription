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

## Prior Context Modes

Use `[context]` to control prior same-source transcripts:

```toml
[context]
prior_turn_count = 8
prior_context_mode = "text_turns"
```

Supported SFT/eval modes:

| Mode | Shape | Use |
| --- | --- | --- |
| `text_turns` | Prior `user(text prompt) -> model(prior transcript)` turns, then the current user turn with audio. | Recommended default for SFT prior context. |
| `transcript` | One current user turn with a simple numbered prior-transcript block plus current audio. | Compact one-turn context. |
| `guarded_transcript_block` | One current user turn with a guarded numbered prior-transcript block plus current audio. | Compact context with explicit "do not re-transcribe or continue prior turns" instructions. |

`audio` is available only to lower-level inference helpers for notebook-style
prior audio turns. Do not use it in SFT configs because Vertex SFT examples
must contain only the current audio part.

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

All three targets use the same table shape. The operator chooses the label used
for report rows and artifact directories; the `model` value is the publisher
model ID or Vertex endpoint resource that should be evaluated.

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

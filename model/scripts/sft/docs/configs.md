---
type: reference
title: Gemini SFT Configuration Guidance
description: Stable configuration lifecycle and operator decisions for Gemini SFT runs.
tags: [gemini-sft, configuration]
sources:
  - resource: ../../../src/gemini_sft/config.py
    title: Configuration parser and validation
  - resource: ../run_config.example.toml
    title: Complete placeholder configuration
---

# Gemini SFT Configuration Guidance

[`config.py`](../../../src/gemini_sft/config.py) defines the accepted TOML
fields, defaults, validation, and durable representation.
[`run_config.example.toml`](../run_config.example.toml) is the complete
placeholder shape. Do not maintain a second schema in Markdown.

## Start A Run

Copy the example outside version control, replace every placeholder, review
every active setting, and choose a new `round_id`. The example demonstrates
syntax; its values are not recommendations for every run.

Use `gemini-sft prepare --config ...` to validate the file. Do not submit
tuning when preparation reports any error.

## Configuration Lifecycle

Preparation publishes a resolved durable configuration under the run prefix.
That state—not a later local edit—is authoritative for tuning, evaluation,
resume, and audit.

- Treat `round_id` as immutable.
- Do not edit a prepared run into a different experiment.
- Use a new config and `round_id` when changing datasets, prompts, context,
  model target, or other request-affecting settings.
- Runtime controls that code explicitly permits during resume remain governed
  by [`config.py`](../../../src/gemini_sft/config.py).

## Training And Eval-Only Runs

A training config supplies training, validation, and evaluation manifests.
An eval-only config omits training inputs and targets an already available
publisher model or endpoint. Exact mode requirements are validated by code.

Tuning does not turn its training config into checkpoint-evaluation configs.
Create a separate prepared eval-only run for each model or checkpoint being
compared, and copy the exact endpoint resource returned by Vertex.

## Prompts

Prompt defaults and overrides are request behavior, not prose documentation.
Their resolution and validation live in
[`config.py`](../../../src/gemini_sft/config.py), while request construction
lives in [`prepare.py`](../../../src/gemini_sft/prepare.py) and
[`target_execution.py`](../../../src/gemini_sft/target_execution.py).

Omit prompt overrides when defaults are intended. Add an inline override only
for a deliberate experiment, and prepare it under a new run identity.

## Prior Context

`prior_turn_count = 0` is stateless. Positive prior context changes request
construction and evaluation scheduling; use only a representation accepted by
the current parser.

Training may use reference transcripts as supervised context. Evaluation must
never expose references to the provider: prior context comes only from the
evaluated target's finalized earlier predictions. See
[evaluation methodology](evaluation-methodology.md#predicted-prior-context).

## Validation Manifest

When validation intentionally reuses a subset of eval data, use
[`build_validation_manifest_from_eval.py`](../build_validation_manifest_from_eval.py)
so selected rows are relabeled for the validation split:

```bash
uv run python model/scripts/sft/build_validation_manifest_from_eval.py --help
```

The script's help and implementation own its current arguments and constraints.

## Comparison Isolation

Use separate configs and run prefixes for:

- different checkpoints or models;
- masked and unmasked manifests;
- different prior-context settings;
- materially different prompts or generation settings.

This keeps durable state and report identities unambiguous without requiring
the documentation to mirror the configuration schema.

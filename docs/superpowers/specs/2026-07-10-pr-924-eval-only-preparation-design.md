# PR 924 Eval-Only Preparation Design

## Goal

Make the PR's documented one-target-per-round evaluation workflow executable
for publisher models, tuned endpoints, and checkpoint endpoints whose resource
names are known only after a separate tuning run completes.

An eval-only round must be preparable through the CLI without training or
validation manifests. Preparation must create the durable state that
`gemini-sft eval` already requires, while preserving the existing training
preparation and tuning workflows.

## Configuration modes

Add `load_prepare_run_config` as the preparation-specific config boundary. It
will accept exactly two modes:

1. Training mode has both `train_manifest_uri` and
   `validation_manifest_uri`. Its `[eval.model]` remains optional.
2. Eval-only mode has neither training manifest and requires `[eval.model]`.

Having only one training manifest is invalid. Having neither training
manifest nor `[eval.model]` is invalid.

The existing loaders retain their contracts:

- `load_run_config` continues to require both training manifests and is used
  by tuning.
- `load_eval_run_config` continues to require `[eval.model]` while allowing
  training manifests to be absent.

The internal config loader will decouple the two requirement flags so these
three public entry points share parsing and validation rather than retrying
parses or matching exception text.

## Preparation dispatch

`gemini-sft prepare` will load through `load_prepare_run_config` and dispatch
from `prepare_run` based on the validated manifest pair.

Training mode keeps the current behavior unchanged: download and validate all
three canonical splits, reject split overlap, build Gemini tuning JSONL,
preflight it, and publish the existing artifact set.

Eval-only mode will use a dedicated `PreparedEvalArtifacts` record and helper.
It will:

1. Write the local `run_config.toml` mirror.
2. Download the configured eval manifest to the existing canonical eval path.
3. Parse and validate it through the strict packaged canonical boundary.
4. Upload `run_config.toml` and canonical `eval.jsonl` to their existing GCS
   URIs.
5. Write and upload durable `config.json` last, with status `eval_prepared`
   and the validated eval row count.

It will not create or upload train/validation canonical manifests, Gemini
tuning JSONL, a preflight report, tuning status, or the eval placeholder
README. Publishing `config.json` last keeps it as the commit marker consumed by
evaluation.

`RunConfig.to_record_dict` will omit train/validation and Gemini tuning
artifact URIs when the training manifest pair is absent. The base model and
other currently required `[sft]` fields remain in the record; the base model
is needed to identify the family of tuned endpoint targets. Relaxing the
existing TOML schema is outside this fix.

The prepare CLI will log eval-only completion separately instead of pretending
that zero training rows passed tuning preflight.

## Evaluation target identity

The durable `[eval.model]` target remains immutable. Tuning will not rewrite it
to the newly created endpoint. To evaluate that endpoint, the operator creates
a new eval-only config and round after the endpoint resource name is known.
This preserves the PR's one-target-per-round reproducibility contract.

Normalized inference output must use the evaluated model family:

- Publisher model targets derive the model-family slug from
  `eval_model.model`.
- Endpoint targets derive it from `sft.base_model`, because the endpoint
  resource name does not encode its publisher model family.

A small evaluation helper will centralize this selection before normalized
manifest path and `pred_text_*` construction.

## Error handling

Preparation fails before durable writes when:

- exactly one training manifest is configured;
- eval-only mode lacks `[eval.model]`;
- the eval manifest cannot be downloaded or parsed;
- strict canonical validation fails; or
- the eval manifest contains zero rows.

Storage upload failures continue to propagate through the existing CLI error
boundary. A partially uploaded eval-only prefix without `config.json` remains
non-reusable under the current fail-closed prefix checks.

`gemini-sft tune` continues to call `load_run_config`, so an eval-only config
cannot accidentally submit a tuning job.

## Testing

Tests will prove:

1. The preparation loader accepts both complete training configs and eval-only
   configs, while rejecting partial training pairs and targetless eval-only
   configs.
2. Eval-only preparation uploads exactly `run_config.toml`, canonical
   `eval.jsonl`, and `config.json`, writes `config.json` last, and never invokes
   tuning-data generation or preflight.
3. Malformed, empty, or canonically invalid eval-only manifests fail before
   durable state is published.
4. Evaluation can consume the state produced by eval-only preparation.
5. Tuning rejects an eval-only config before any provider submission.
6. A publisher target different from `sft.base_model` uses the target's model
   family, while an endpoint target uses the base model family.
7. Existing training preparation, tuning, batch/online evaluation, resume, and
   reporting tests remain green.

## Out of scope

- Automatically mutating a training round's eval target after tuning.
- Bootstrapping durable state implicitly inside `gemini-sft eval`.
- Changing artifact paths for existing training rounds.
- Making `[sft]` optional or redesigning the external TOML schema.
- Preparing multiple evaluation targets in one round.

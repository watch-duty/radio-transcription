# Phase 2 Patterns: Target Config

**Phase:** 02-target-config
**Date:** 2026-06-28
**Status:** Complete

## Existing Patterns To Preserve

### Config Validation

- External TOML errors use `RunConfigError`.
- Error messages include the config field path, for example
  `context.prior_turn_count` or `prompts.system_file`.
- Config loaders perform static validation only; unit tests should not contact
  GCS or Vertex.
- `load_run_config()` and `load_eval_run_config()` share `_load_run_config()`
  with boolean requiredness flags.

### Durable Config

- `RunConfig.to_record_dict()` is the only source for persisted
  `config.json` fields produced by prepare.
- Prompt text is copied into `config.json`; target config should follow the
  same resolved-state pattern.
- GCS config helpers such as `require_config_str()` validate durable state read
  during tune/eval.

### Artifact Labels

- Normalized inference manifests use safe single path components for model
  family, run ID, and artifact label.
- `artifact_label` must not include `.jsonl`.
- Reusing the artifact-label validator keeps target labels and manifest output
  paths aligned.

### Tests

- `model/tests/gemini_sft/test_config.py` uses a `_valid_toml()` helper and
  focused `assertRaisesRegex` checks.
- `model/tests/gemini_sft/test_workflow.py` uses `FakeStorageClient` and
  patches Vertex boundaries so no paid calls happen.
- `model/tests/common/tests/test_inference_manifest.py` owns artifact path
  safety tests.

## New Phase 2 Patterns

### Eval Target Dataclass

Represent target entries with a frozen dataclass near `RunConfig`, for example:

```python
@dataclass(frozen=True)
class EvalModelTarget:
    label: str
    model: str
```

Keep it intentionally small. Backend classification is not part of Phase 2.

### Target Parser

Parse `[[eval.models]]` once in `gemini_sft.config`:

- Missing targets are allowed for `load_run_config()`.
- Missing targets are rejected for `load_eval_run_config()`.
- If present, targets are validated even for prepare/tune configs.
- Each entry must have exactly `label` and `model`.
- `label` reuses artifact-label validation.
- `model` is a stripped non-empty string.
- Duplicate labels raise before paid work.

### Durable Eval Guard

Add a helper for GCS `config.json`, for example
`require_config_eval_models(config)`, that validates the serialized
`eval_models` list with the same rules. `gemini_sft.evaluate.evaluate_run()`
should call this before provider work. Phase 3 can later replace hard-coded
base/tuned execution with a loop over the returned targets.

### Superseded Requirement Handling

Keep `CFG-04` visible in plan frontmatter for traceability, but implement the
newer decision:

- No legacy target synthesis from `[sft].base_model`.
- No legacy target synthesis from GCS `endpoint`.
- Missing `[[eval.models]]` tells the operator to add `label` and `model` and
  states that there is no `base_model`/`endpoint` fallback.

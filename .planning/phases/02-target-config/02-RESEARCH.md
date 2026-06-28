# Phase 2 Research: Target Config

**Phase:** 02-target-config  
**Date:** 2026-06-28  
**Status:** Complete

## Question

What does Phase 2 need to know before planning unified eval target config?

## Findings

### Existing Owner Boundary

`model/src/gemini_sft/config.py` is the right owner for external TOML parsing,
derived paths, prompt resolution, and durable `config.json` serialization.
It already has the exact split Phase 2 needs:

- `load_run_config()` requires train and validation manifests.
- `load_eval_run_config()` does not require train and validation manifests.
- `RunConfig.to_record_dict()` is the durable GCS `config.json` boundary.

Target parsing should live in this module unless it becomes shared by another
package later.

### Target Shape

The planned TOML shape is:

```toml
[[eval.models]]
label = "base"
model = "gemini-3.1-flash-lite"

[[eval.models]]
label = "checkpoint_6"
model = "projects/PROJECT/locations/us-central1/endpoints/ENDPOINT_ID"
```

TOML parses this as `data["eval"]["models"]`, a list of tables. Each table
should contain exactly `label` and `model`. Phase 2 should not classify the
model string as publisher, tuned endpoint, or checkpoint endpoint.

### Safe Label Reuse

`model/src/common/inference_manifest.py` already enforces the path safety rule
needed for target labels through private `_validate_safe_segment()` and
`build_inference_manifest_blob_path(... artifact_label=...)`.

Phase 2 should expose a small public validator, for example
`validate_artifact_label()`, and make target labels reuse that instead of
duplicating regexes in `gemini_sft.config`.

### Durable State

Prepared runs store resolved state in GCS `config.json`. If a training config
contains eval models, `RunConfig.to_record_dict()` should store:

```json
{
  "eval_models": [
    {"label": "base", "model": "gemini-3.1-flash-lite"},
    {"label": "checkpoint_6", "model": "projects/PROJECT/locations/us-central1/endpoints/ENDPOINT_ID"}
  ]
}
```

This mirrors the prompt pattern: resolved runtime values are copied to GCS so
resume/eval does not depend on one local TOML file.

### Eval Fail-Fast Boundary

`model/src/gemini_sft/evaluate.py` currently selects hard-coded `base` and
`tuned` targets from `base_model` and GCS `endpoint`. Phase 2 is config-only,
but it still must prevent paid eval work when the new target config is absent
or malformed. The least risky Phase 2 change is a durable `eval_models` guard
at the start of eval, before manifest download or Vertex batch submission.

Backend selection and executing each configured target belongs to Phase 3.

### Superseded Migration Requirement

`CFG-04` in `.planning/REQUIREMENTS.md` and Phase 2 roadmap text still mention
migration support for old `base_model` plus GCS `endpoint` eval selection.
The later user decision supersedes that: support only new `[[eval.models]]`.
Plans must keep `CFG-04` traceable while explicitly implementing the
superseding decision, not the stale migration behavior.

### Masked And Unmasked Runs

No separate eval-sibling abstraction is needed. Masked and unmasked evals are
ordinary separate config files/runs:

- Each has its own `eval_manifest_uri`.
- Each has its own `inference_dataset_slug`.
- Each uses a distinct `round_id`.
- No `eval_label` or `masked = true` flag is introduced.

## Recommended Plan Shape

1. Add eval target parsing, label validation, duplicate detection, and durable
   `eval_models` serialization in `gemini_sft.config`.
2. Add durable `config.json` validation for `eval_models` and wire eval to fail
   before paid work when the field is absent or malformed.
3. Add lightweight examples and masked/unmasked config tests so operators see
   the separate-config pattern before Phase 3 execution work.

## Out Of Scope

- Vertex/GCS existence checks for configured models or manifests.
- Batch versus online backend routing.
- Multi-target execution or parallelism.
- Full operator documentation for every workflow; that remains Phase 5.


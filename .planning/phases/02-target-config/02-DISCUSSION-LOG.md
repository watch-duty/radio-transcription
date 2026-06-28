# Phase 2: Target Config - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-28
**Phase:** 2-Target Config
**Areas discussed:** Target config shape, Migration compatibility, Target labels and artifact paths, Masked/unmasked eval config, Validation boundary

---

## Target Config Shape

| Option | Description | Selected |
|--------|-------------|----------|
| Label + model | Use `label` plus one `model` string for publisher model IDs, tuned endpoints, or checkpoint endpoints; infer backend later. | ✓ |
| Typed entries | Require `type = base|tuned|checkpoint` with resource-specific fields. | |
| String list | Allow bare model strings and infer labels automatically. | |

**User's choice:** Label + model.
**Notes:** Target entries live under `[[eval.models]]`, contain exactly `label` and `model`, and do not include backend hints or metadata in Phase 2.

---

## Migration Compatibility

| Option | Description | Selected |
|--------|-------------|----------|
| Auto-synthesize legacy targets | Build `base` from `base_model` and `tuned` from GCS `config.json.endpoint` when present. | |
| Require new config for eval | Old eval target formats are not supported. | ✓ |
| Support both, but prefer explicit | Auto-synthesize only with a warning; explicit `eval.models` wins. | |

**User's choice:** Support only the new config; assume old formats never exist.
**Notes:** `[sft].base_model` stays tuning-only. GCS `config.json.endpoint` remains tune metadata and is ignored for eval target selection. Resolved targets are stored as `eval_models`.

---

## Target Labels And Artifact Paths

| Option | Description | Selected |
|--------|-------------|----------|
| Same safe segment rule | Reuse existing artifact-label validation exactly. | ✓ |
| Slug-normalize labels | Accept loose labels and convert them to safe slugs. | |
| Very strict lowercase only | Allow only lowercase letters, numbers, and underscore. | |

**User's choice:** Same safe segment rule.
**Notes:** Duplicate labels are rejected. `base` and `tuned` are allowed but not reserved. The target label maps directly to normalized inference manifest `artifact_label`.

---

## Masked/Unmasked Eval Config

| Option | Description | Selected |
|--------|-------------|----------|
| Separate config files | Each config has its own `eval_manifest_uri` and `inference_dataset_slug`. | ✓ |
| One config with switch | Add a variant switch and derive URIs. | |
| One config with multiple eval sets | Run multiple eval sets from one config. | |

**User's choice:** Separate config files.
**Notes:** No separate `eval_label`; `inference_dataset_slug` is the eval corpus/split/variant path. Masked and unmasked configs should use distinct `round_id` values. Slug validation is syntactic only.

---

## Validation Boundary

| Option | Description | Selected |
|--------|-------------|----------|
| Shape + safe strings only | Offline/static validation of shape, labels, duplicates, and field types. | ✓ |
| Also check GCS manifest existence | Validate cloud object availability. | |
| Also check Vertex resources exist | Validate model/endpoint resources with Vertex. | |

**User's choice:** Shape + safe strings only, with `model` accepting any non-empty string.
**Notes:** `[[eval.models]]` is required only for `load_eval_run_config`. Training configs validate and store eval targets when present. Missing eval targets should produce an actionable error mentioning `[[eval.models]]`, `label`, `model`, and no legacy fallback.

---

## the agent's Discretion

- Exact dataclass/function/module names are left to implementation planning.
- Existing loader constraints unrelated to eval target selection can remain unless they block the locked decisions.

## Deferred Ideas

- CFG-04 migration compatibility is superseded by user decision and should not be implemented for eval targets.
- Backend execution and batch-vs-online target routing remain Phase 3.
- Multi-target parallel execution remains Phase 4.
- Full example config/operator documentation remains Phase 5.

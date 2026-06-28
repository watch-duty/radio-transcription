---
phase: 02-target-config
verified: 2026-06-28T18:47:01Z
status: passed
score: 8/8 must-haves verified
overrides_applied: 1
overrides:
  - must_have: "CFG-04: Existing configs that use base_model and a tuned endpoint in GCS config.json remain evaluable during migration"
    reason: "Superseded by Phase 2 context D-05/D-09 and the current verification request: support only the new [[eval.models]] contract; base_model/endpoint fallback must fail closed."
    accepted_by: "user"
    accepted_at: "2026-06-28T18:47:01Z"
deferred:
  - truth: "Configured checkpoint and arbitrary endpoint targets are executed by backend-specific target routing"
    addressed_in: "Phase 3"
    evidence: "Phase 3 goal: executes each configured target through the correct backend while preserving prompt/request/prior-context behavior."
  - truth: "Multiple configured targets run in parallel and emit one normalized manifest per target"
    addressed_in: "Phase 4"
    evidence: "Phase 4 success criteria cover multi-target eval, durable GCS state, and one normalized inference manifest per target label."
  - truth: "Full operator docs and placeholder configs for base-only, tuned, checkpoint, masked, and unmasked runs"
    addressed_in: "Phase 5"
    evidence: "Phase 5 success criteria cover complete README workflow and placeholder configs for common run types."
---

# Phase 2: Target Config Verification Report

**Phase Goal:** Operators can describe base models, tuned endpoints,
checkpoint endpoints, and masked/unmasked evals through explicit validated
configs before paid Vertex work starts.

**Verified:** 2026-06-28T18:47:01Z
**Status:** passed
**Re-verification:** No - initial verification
**Can mark Phase 2 complete:** Yes

## Verdict

Phase 2 is complete against the accepted contract. The implementation defines
one explicit eval target shape, validates TOML and durable `config.json`
records before paid eval, refuses legacy base_model/endpoint fallback, keeps
masked/unmasked evals as separate configs, and documents the current operator
shape.

`CFG-04` in `.planning/REQUIREMENTS.md` is stale as written. This report
applies the user-accepted override from Phase 2 context and this verification
request: old `base_model` plus `endpoint` config is intentionally rejected.

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|---|---|---|
| 1 | Eval targets use one explicit `[[eval.models]]` shape with exactly `label` and `model`. | VERIFIED | `EvalModelTarget` has only `label` and `model` in `model/src/gemini_sft/config.py:64`; `_eval_model_targets()` rejects unsupported keys at `config.py:449-462`; tests cover unsupported target fields in `model/tests/gemini_sft/test_config.py:427-442`. |
| 2 | `model` is an unclassified non-empty string supporting publisher IDs and endpoint/checkpoint paths. | VERIFIED | TOML parsing uses `_required_str()` without resource classification at `config.py:472`; durable validation strips non-empty strings at `config.py:383-389`; tests serialize `checkpoint_6` with an endpoint path at `test_config.py:182-220` and `test_config.py:466-488`. |
| 3 | Legacy target synthesis from `[sft].base_model` or GCS `endpoint` is not supported. | VERIFIED (override applied for stale CFG-04 wording) | Missing TOML targets raise an error mentioning `[[eval.models]]`, `label`, `model`, and no `base_model/endpoint` fallback at `config.py:25-34`; durable config with only `base_model` and `endpoint` fails before manifest download or submit in `test_workflow.py:1590-1620`. |
| 4 | Target labels reuse artifact-label safety, reject duplicates, and map directly to artifact labels. | VERIFIED | `validate_artifact_label()` is public in `model/src/common/inference_manifest.py:91-106`; manifest path building calls it at `inference_manifest.py:133`; config parsing calls it at `config.py:478-484`; duplicate labels fail at `config.py:468-470` and `config.py:378-380`. |
| 5 | Eval targets are optional for prepare/tune config loading but required for eval loading. | VERIFIED | `_load_run_config()` passes `required=not require_training_manifests` at `config.py:199-202`; `load_run_config()` accepts no eval models in `test_config.py:90-123`; `load_eval_run_config()` rejects missing targets in `test_config.py:167-180`. |
| 6 | Resolved eval targets are persisted into durable `config.json` as `eval_models` when present. | VERIFIED | `RunConfig.to_record_dict()` writes `eval_models` at `config.py:145-148`; tests verify training and eval serialization at `test_config.py:182-220` and `test_config.py:300-332`. |
| 7 | Durable `config.json` eval target validation happens before paid eval work. | VERIFIED | `evaluate_run()` calls `require_config_eval_models(config)` at `evaluate.py:113`, before `download_jsonl_manifest()` at `evaluate.py:147` and `batch_infer()` at `evaluate.py:159`; workflow tests assert manifest download and batch submit are not called for missing/invalid/unsupported durable targets at `test_workflow.py:1590-1689`. |
| 8 | Masked and unmasked evals are separate configs/runs with no eval-sibling abstraction. | VERIFIED | Tests load distinct `round_id`, `eval_manifest_uri`, and `inference_dataset_slug` values and assert no `eval_label` or `masked` record keys at `test_config.py:222-298`; parser rejects `[eval]` sibling fields `masked` and `eval_label` at `test_config.py:444-464`; README documents separate configs at `model/scripts/sft/README.md:101-103`. |

**Score:** 8/8 truths verified

### Deferred Items

Items not yet met but explicitly addressed in later milestone phases.

| # | Item | Addressed In | Evidence |
|---|---|---|---|
| 1 | Full target-driven execution for configured checkpoint/endpoint labels. | Phase 3 | Phase 3 goal covers backend-specific execution. Phase 2 has a fail-closed guard instead: `evaluate.py:136-145`. |
| 2 | Multiple configured targets running in parallel and one normalized manifest per target. | Phase 4 | Phase 4 success criteria explicitly cover multi-target eval and per-target normalized manifests. |
| 3 | Complete operator docs and all placeholder config variants. | Phase 5 | Phase 5 success criteria cover full workflow docs and placeholder configs for base-only, tuned, checkpoint, masked, and unmasked evals. |

### Required Artifacts

| Artifact | Expected | Status | Details |
|---|---|---|---|
| `model/src/common/inference_manifest.py` | Public artifact-label validator shared by target config and manifest paths. | VERIFIED | `validate_artifact_label()` exists and is wired into path building at lines 91-106 and 133. |
| `model/src/gemini_sft/config.py` | Eval target dataclass, TOML parser, durable validator, and serialization. | VERIFIED | `EvalModelTarget`, `eval_models`, `_eval_model_targets()`, and `require_config_eval_models()` all exist and are substantive. |
| `model/src/gemini_sft/evaluate.py` | Fail-fast durable eval target guard before provider work. | VERIFIED | Imports/calls `require_config_eval_models()` and refuses unsupported target sets before manifest download or batch inference. |
| `model/scripts/sft/run_config.example.toml` | Placeholder target config example using only supported fields. | VERIFIED | Contains `[eval]` and one copyable `[[eval.models]]` target with only `label`/`model`; comments explain checkpoint labels are deferred to the later runner. |
| `model/scripts/sft/README.md` | Operator-facing target config and masked/unmasked guidance. | VERIFIED | Documents explicit targets, no base_model/endpoint synthesis, endpoint/checkpoint resource strings, and separate masked/unmasked configs. |
| `model/tests/common/tests/test_inference_manifest.py` | Artifact-label validation coverage. | VERIFIED | Valid and invalid label tests at lines 224-240. |
| `model/tests/gemini_sft/test_config.py` | TOML/durable target config coverage. | VERIFIED | Covers required targets, serialization, invalid labels, duplicates, invalid model values, unsupported fields, durable JSON validation, and masked/unmasked configs. |
| `model/tests/gemini_sft/test_workflow.py` | Paid-boundary guard coverage. | VERIFIED | Tests missing, invalid, and unsupported durable `eval_models` all return `1` before manifest download or batch submission. |

### Key Link Verification

| From | To | Via | Status | Details |
|---|---|---|---|---|
| `model/src/gemini_sft/config.py` | `model/src/common/inference_manifest.py` | `validate_artifact_label` | VERIFIED | Imported at `config.py:15-18`, called by `_required_artifact_label()` at `config.py:478-484`. |
| `model/src/gemini_sft/evaluate.py` | `model/src/gemini_sft/config.py` | durable eval target guard | VERIFIED | Imported at `evaluate.py:38-45`, called at `evaluate.py:113`. |
| `model/scripts/sft/README.md` | `model/scripts/sft/run_config.example.toml` | same `[[eval.models]]` shape and separate-config guidance | VERIFIED | README example at lines 68-71 matches example TOML lines 31-39; both state masked/unmasked use separate configs/runs. Automated key-link query had an escaped-pattern false negative, manually checked. |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|---|---|---|---|---|
| `model/src/gemini_sft/config.py` | `RunConfig.eval_models` | Operator TOML `[eval].models` parsed in `_eval_model_targets()` | Yes | FLOWING - assigned into `RunConfig` at `config.py:281` and serialized to `config.json` at `config.py:145-148`. |
| `model/src/gemini_sft/config.py` | durable `eval_models` | GCS `config.json` dictionary | Yes | FLOWING - `require_config_eval_models()` validates the actual JSON list at `config.py:326-391`. |
| `model/src/gemini_sft/evaluate.py` | `eval_model_targets` | `require_config_eval_models(config)` | Yes | FLOWING - guards target state at `evaluate.py:113`; unsupported data refuses eval at `evaluate.py:136-145`. |
| `model/scripts/sft/README.md` / `run_config.example.toml` | target examples | Static operator docs | N/A | VERIFIED - not dynamic code; examples avoid real credentials and unsupported fields. |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|---|---|---|---|
| Phase 2 parser, guard, docs-adjacent workflow tests pass. | `safe-run -- env PYTHONPATH=src:tests python3 -m pytest tests/common/tests/test_inference_manifest.py tests/gemini_sft/test_config.py tests/gemini_sft/test_workflow.py -q` from `model/` | `86 passed, 55 subtests passed in 1.01s` | PASS |
| Config-only target validation suite passes. | `safe-run -- env PYTHONPATH=src:tests python3 -m pytest tests/gemini_sft/test_config.py -q` from `model/` | `34 passed, 28 subtests passed in 0.16s` | PASS |
| Modified Python modules compile. | `python3 -m py_compile src/common/inference_manifest.py src/gemini_sft/config.py src/gemini_sft/evaluate.py` from `model/` | Exit code 0 | PASS |
| Plan artifact/key-link queries. | `gsd-sdk query verify.artifacts` and `gsd-sdk query verify.key-links` for the three Phase 2 plans | Artifacts passed 9/9; two key links passed automatically; README/example link manually verified after escaped-pattern false negative. | PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|---|---|---|---|---|
| CFG-01 | 02-01, 02-02, 02-03 | Configure one or more eval targets through one unified models-style shape. | SATISFIED | Parser accepts multiple `[[eval.models]]` entries; tests serialize base plus `checkpoint_6`. |
| CFG-02 | 02-01, 02-02, 02-03 | Target can represent publisher/base, tuned endpoint, or checkpoint endpoint without checkpoint-specific CLI options. | SATISFIED | `model` is an unclassified string; README says publisher ID or endpoint/checkpoint resource string; unsupported execution is fail-closed until Phase 3. |
| CFG-03 | 02-01, 02-03 | Labels are safe artifact paths and collision-free report columns. | SATISFIED | `validate_artifact_label()` reused; duplicate labels rejected for TOML and durable JSON. |
| CFG-04 | 02-01, 02-02 | Old base_model plus endpoint configs remain evaluable during migration. | OVERRIDDEN | Superseded by Phase 2 D-05/D-09 and current verification request. Verified replacement behavior: no legacy fallback; stale config fails before paid eval. |
| CFG-05 | 02-03 | Masked and unmasked evals are separate configs/manifests without eval-sibling abstraction. | SATISFIED | Tests and docs cover distinct run/config coordinates and reject `eval_label` / `masked` fields. |
| CFG-06 | 02-01, 02-02, 02-03 | Validation errors identify missing, invalid, or unsupported fields before paid Vertex work. | SATISFIED | TOML and durable JSON validators cover missing targets, invalid labels, duplicate labels, bad model values, non-table entries, and unsupported fields; workflow tests assert no batch submit. |

No orphaned Phase 2 requirements were found beyond the accepted `CFG-04`
supersession.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|---|---|---|---|---|
| `model/scripts/sft/README.md` | 42 | "placeholder examples" | INFO | Intentional documentation statement; not a stub. |

The hardcoded-empty scan found normal initial lists/dicts and test fixtures, not
user-visible stub data. No `TODO`, `FIXME`, `NotImplemented`, or console-only
implementation was found in the reviewed Phase 2 files.

### Human Verification Required

None. Phase 2 is static/config validation plus targeted mocked workflow tests;
paid Vertex execution is intentionally deferred and guarded.

### Residual Risks

- `evaluate_run()` still uses the old base/tuned runner after the target guard.
  This is intentional Phase 2 scope; Phase 3 owns target-driven execution.
- `model/src/gemini_sft/config.py` still has `continuous_tuning.checkpoint_id`
  for tuning metadata. It is not part of `[[eval.models]]` and is not used for
  eval target selection.
- Full operator docs/examples are intentionally incomplete until Phase 5. The
  Phase 2 README/example only expose the new target shape and fail-closed
  boundaries.

### Gaps Summary

No blocking gaps found. Phase 2 can be marked complete.

---

_Verified: 2026-06-28T18:47:01Z_
_Verifier: the agent (gsd-verifier)_

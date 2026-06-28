# Phase 2: Target Config - Context

**Gathered:** 2026-06-28
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 2 defines the offline config contract for eval targets. Operators should
be able to describe base publisher models, tuned endpoints, checkpoint
endpoints, and masked/unmasked eval runs through explicit validated config
fields before paid Vertex work starts.

This phase does not implement backend execution, batch-vs-online routing,
multi-target parallelism, durable dataset breakdowns, or operator docs. Those
belong to later phases. Phase 2 should focus on config parsing, target
validation, GCS `config.json` serialization, and tests that prove invalid config
fails before any paid operation.

</domain>

<decisions>
## Implementation Decisions

### Target Config Shape
- **D-01:** Eval targets live under `[[eval.models]]` in TOML.
- **D-02:** Each target entry has exactly two required fields: `label` and
  `model`.
- **D-03:** `model` is the single target string. It may be a publisher model ID,
  tuned endpoint resource, or checkpoint endpoint resource by operator intent,
  but Phase 2 validation accepts any non-empty string and does not classify it.
- **D-04:** Do not add `type`, `backend`, `description`, or arbitrary metadata
  fields to target entries in Phase 2. Backend selection is Phase 3.

### Migration Compatibility
- **D-05:** Support only the new `[[eval.models]]` target config for eval target
  selection. Assume old eval target formats do not exist.
- **D-06:** `[sft].base_model` remains tuning configuration only. It must not be
  auto-synthesized as an eval target.
- **D-07:** The tuned `endpoint` field in GCS `config.json` remains tuning
  metadata but must not affect eval target selection after Phase 2.
- **D-08:** Resolved eval targets are copied into durable GCS `config.json` as
  `eval_models`.
- **D-09:** CFG-04's earlier migration-compatibility requirement is superseded
  by user decision. Downstream planning should not spend scope preserving
  old `base_model` plus `endpoint` eval behavior.

### Target Labels And Artifact Paths
- **D-10:** Target labels reuse the existing safe artifact segment rule:
  letters, numbers, `.`, `_`, and `-`; not empty; not `.` or `..`; no `/`; no
  `.jsonl` suffix.
- **D-11:** Duplicate target labels are config errors before paid work.
- **D-12:** `base` and `tuned` are allowed as ordinary conventional labels, but
  they are not reserved and carry no automatic behavior.
- **D-13:** The target `label` maps directly to normalized inference manifest
  `artifact_label`, for example `checkpoint_6` writes
  `<round_id>/checkpoint_6.jsonl`.

### Masked And Unmasked Eval Config
- **D-14:** Masked and unmasked evals are separate config files/runs.
- **D-15:** Each config supplies its own `eval_manifest_uri` and
  `inference_dataset_slug`.
- **D-16:** Do not add a separate `eval_label` field. `inference_dataset_slug`
  is the explicit eval corpus/split/variant path for artifacts and display
  context.
- **D-17:** Masked and unmasked configs must use distinct `round_id` values so
  their GCS run prefixes do not mix config or eval artifacts.
- **D-18:** Validate `inference_dataset_slug` syntax only. Do not enforce words
  such as `masked` or add a `masked = true|false` field.

### Validation Boundary
- **D-19:** Phase 2 validation is offline/static only. It validates target
  shape, label safety, duplicate labels, field types, and non-empty model
  strings.
- **D-20:** Do not check GCS manifest existence or Vertex resource existence in
  Phase 2.
- **D-21:** `[[eval.models]]` is required only for `load_eval_run_config`.
  Prepare/tune configs do not require eval targets.
- **D-22:** If `[[eval.models]]` is present in a training config, prepare/tune
  loading should validate it and store resolved `eval_models` in GCS
  `config.json`.
- **D-23:** Missing eval targets in `load_eval_run_config` should produce an
  actionable error that mentions `[[eval.models]]`, required `label` and
  `model`, and the lack of legacy `base_model`/`endpoint` fallback.

### the agent's Discretion
The implementation can choose exact dataclass/function names. Prefer keeping
target parsing in or near `gemini_sft.config` unless planning finds an existing
shared config module is a better fit. Existing loader constraints unrelated to
eval target selection can remain unchanged unless they block the decisions
above.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Planning
- `.planning/PROJECT.md` - Core value, active requirements, out-of-scope
  boundaries, and decision table.
- `.planning/REQUIREMENTS.md` - CFG-01 through CFG-06 plus the now-superseded
  CFG-04 migration assumption.
- `.planning/ROADMAP.md` - Phase 2 goal, success criteria, and dependency order.
- `.planning/phases/01-reporting-contract/01-CONTEXT.md` - Phase 1 report
  contract decisions that target config must feed.
- `.planning/phases/01-reporting-contract/01-01-SUMMARY.md` - Shared report
  schema and renderer artifacts.
- `.planning/phases/01-reporting-contract/01-02-SUMMARY.md` - Batch eval report
  integration and target-row artifact behavior.
- `.planning/phases/01-reporting-contract/01-03-SUMMARY.md` - Checkpoint scorer
  report integration and ranking metadata behavior.

### Codebase Maps
- `.planning/codebase/ARCHITECTURE.md` - Model/SFT architecture and package
  boundaries.
- `.planning/codebase/CONVENTIONS.md` - GCS run state, prompt/config rules,
  safe artifact hygiene, and Gemini request conventions.
- `.planning/codebase/CONCERNS.md` - Prior context semantics, eval gap, cloud
  cost boundaries, and generated artifact risks.

### Config And Artifact Code
- `model/src/gemini_sft/config.py` - Existing TOML loader, `RunConfig`,
  `to_record_dict`, prompt validation, and GCS path derivation.
- `model/tests/gemini_sft/test_config.py` - Existing config validation tests
  and style for expected `RunConfigError` messages.
- `model/src/common/inference_manifest.py` - Existing safe segment validation
  and `artifact_label` path rules that target labels should reuse.
- `model/src/gemini_sft/evaluate.py` - Current `load_eval_run_config` caller,
  GCS `config.json` use, and eval target assumptions to update later.
- `model/scripts/sft/score_gemini_sft_checkpoints_online.py` - Current
  checkpoint endpoint scoring and report target behavior that Phase 3/4 will
  align behind target config.

### Operator-Facing Examples
- `model/scripts/sft/run_config.example.toml` - Existing placeholder config
  that Phase 2 may update with `[[eval.models]]` examples.
- `model/scripts/sft/README.md` - Current config fields, GCS records, eval
  semantics, and local artifact guidance.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `gemini_sft.config.RunConfig` already centralizes external TOML parsing and
  GCS `config.json` serialization.
- `gemini_sft.config.load_run_config` and `load_eval_run_config` already split
  training-required and eval-only manifest requirements. Phase 2 can add
  eval-target requiredness to this same boundary.
- `common.inference_manifest._validate_safe_segment` already enforces the
  desired safe artifact label rule. If it remains private, planning should
  either expose a small public validator or mirror the exact behavior with
  tests tied to artifact-path compatibility.
- `RunConfig.to_record_dict()` is the durable GCS `config.json` serialization
  point and should include `eval_models` when configured.

### Established Patterns
- External config errors raise `RunConfigError` with field names in the message.
- Prompt overrides are inline-only and resolved into GCS `config.json`; target
  config should follow the same "resolved state in GCS" pattern.
- Unit tests should mock cloud boundaries and should not check real GCS or
  Vertex resources for config validation.
- Local `.local.toml`, inference manifests, and `results/` outputs are not
  source artifacts.

### Integration Points
- Add an eval target representation to `model/src/gemini_sft/config.py`.
- Parse optional `[[eval.models]]` for all config loads; require it only in
  `load_eval_run_config`.
- Store resolved targets as `eval_models` in `RunConfig.to_record_dict()`.
- Update config tests for required eval targets, label validation, duplicate
  labels, non-empty model strings, and no legacy target fallback.
- Update placeholder config examples only if Phase 2 plans include example
  config changes; full operator docs remain Phase 5.

</code_context>

<specifics>
## Specific Ideas

Preferred TOML shape:

```toml
[[eval.models]]
label = "base"
model = "gemini-3.1-flash-lite"

[[eval.models]]
label = "checkpoint_6"
model = "projects/PROJECT/locations/us-central1/endpoints/ENDPOINT_ID"
```

Preferred GCS `config.json` shape:

```json
{
  "eval_models": [
    {"label": "base", "model": "gemini-3.1-flash-lite"},
    {"label": "checkpoint_6", "model": "projects/PROJECT/locations/us-central1/endpoints/ENDPOINT_ID"}
  ]
}
```

</specifics>

<deferred>
## Deferred Ideas

- CFG-04 migration compatibility is intentionally not folded into Phase 2.
  User decision supersedes it: old eval target formats are assumed not to exist.
- Backend selection, including batch vs online and checkpoint endpoint handling,
  is Phase 3.
- Multiple configured targets running in parallel is Phase 4.
- Full masked/unmasked example config documentation is Phase 5 unless needed as
  a small placeholder during implementation.

</deferred>

---

*Phase: 2-Target Config*
*Context gathered: 2026-06-28*

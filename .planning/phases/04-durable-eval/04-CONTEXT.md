# Phase 4: Durable Eval - Context

**Gathered:** 2026-06-28T21:00:23Z
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 4 makes `gemini-sft eval` durable and easy to rerun for one configured
model target. It should evaluate from GCS `config.json`, enforce exact artifact
identity before reusing existing predictions, upload normalized inference and
summary artifacts to GCS, and print the shared console report without requiring
local `results/` as source of truth.

The phase deliberately narrows earlier roadmap language: internal multi-target
parallelism is out of scope, and dataset-level breakdowns are deferred. A
caller that wants to compare multiple models/checkpoints should run separate
configs in parallel outside the CLI.

</domain>

<decisions>
## Implementation Decisions

### Single Eval Model Contract
- **D-01:** `gemini-sft eval` should support exactly one model target per run.
  The operator-facing config should use a singular `[eval.model]` table, not
  plural `[[eval.models]]`.
- **D-02:** Durable GCS `config.json` should store the singular model as
  `eval_model`, not `eval_models`.
- **D-03:** The new contract does not need migration compatibility. If a local
  eval config contains plural `[[eval.models]]`, or durable `config.json`
  contains plural `eval_models`, fail loudly with an actionable error.
- **D-04:** The single `eval.model.model` string remains unclassified: it may be
  a publisher model ID, tuned endpoint, or checkpoint endpoint. Checkpoints are
  still represented the same way as any other model resource.
- **D-05:** The eval report has exactly one target row for the configured
  `eval.model.label`; no internal target fan-out, target ordering, or
  multi-target aggregation is needed in this phase.

### Execution And Reuse
- **D-06:** Because there is exactly one model target, Phase 4 must not add
  internal multi-target parallelism. Operators can run separate config files in
  parallel with their own wrappers when comparing base/tuned/checkpoint models.
- **D-07:** Batch output reuse must be guarded by a request-identity sidecar,
  mirroring the online prediction metadata introduced in Phase 3.
- **D-08:** The reusable batch identity must include the same request-defining
  fields as online reuse: target label, model string, eval manifest URI,
  evaluated audio URI order, system prompt, user prompt, prior context count,
  prior context mode, generation config, and safety settings.
- **D-09:** Operational knobs such as concurrency, retry count, and log/sync
  cadence must not invalidate prediction reuse.
- **D-10:** If existing batch output is present without matching identity
  metadata, fail before any paid batch submission. Do not silently reuse it and
  do not silently resubmit over it.
- **D-11:** Existing online identity behavior remains in force: metadata is
  required for reuse, mismatches fail before paid calls, and smoke-prefix reuse
  is allowed only when request identity and audio order prefix rules match.

### Durable Reports
- **D-12:** Summary JSON and Markdown should be uploaded to stable run-level
  paths:

  ```text
  gs://<bucket>/sft/runs/<round-id>/evals/wer_summary.json
  gs://<bucket>/sft/runs/<round-id>/evals/wer_summary.md
  ```

- **D-13:** These stable summary files should be overwritten on each successful
  eval rerun for the same `round_id`.
- **D-14:** Report artifacts should include raw Vertex batch output when
  applicable, online prediction JSONL when applicable, normalized inference
  manifest URI, and the uploaded summary JSON/Markdown URIs.
- **D-15:** Console output remains important and should print the full shared
  metrics table, including WER, CER, keyword accuracy, empty metrics, S/I/D,
  total reference words, missing prediction count, and artifacts.

### Dataset Breakdowns
- **D-16:** Dataset-level breakdowns are deferred. Phase 4 should not add
  multiple eval manifests, row-level dataset attribution requirements, path
  inference, or `unknown` breakdown buckets.
- **D-17:** Keep the current single `eval_manifest_uri` / durable
  `canonical_eval_uri` shape for now. Multiple eval manifests can be designed
  in a follow-up phase if needed.
- **D-18:** The overall one-model report should still include row count and
  total reference word count through the existing target metrics.

### the agent's Discretion
No user decision delegated implementation semantics wholesale. The agent may
choose internal helper names and test organization, but must preserve the
single-model contract and fail-closed reuse behavior above.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Planning
- `.planning/PROJECT.md` - Project value, artifact authority, prompt parity,
  and local artifact hygiene constraints.
- `.planning/REQUIREMENTS.md` - Original Phase 4 requirements. Treat
  `EXEC-05`, `DATA-03`, and `DATA-04` as superseded/deferred by this context.
- `.planning/ROADMAP.md` - Original Phase 4 goal and success criteria. Treat
  internal multi-target parallelism and dataset breakdown criteria as narrowed
  by this context.
- `.planning/phases/01-reporting-contract/01-CONTEXT.md` - Shared metric
  vocabulary, exact empty response semantics, target report schema, and console
  report expectations.
- `.planning/phases/02-target-config/02-CONTEXT.md` - Prior plural target
  config decisions. Phase 4 deliberately replaces this eval target shape with a
  singular `[eval.model]` contract.
- `.planning/phases/03-target-execution/03-CONTEXT.md` - Backend routing,
  online request identity, prompt/prior-context parity, and execution knob
  boundaries to preserve.

### Codebase Maps
- `.planning/codebase/ARCHITECTURE.md` - Model/SFT package boundaries and
  durable GCS run-state architecture.
- `.planning/codebase/CONVENTIONS.md` - GCS `config.json` authority, prompt and
  request helpers, safety settings, empty-output semantics, and git hygiene.
- `.planning/codebase/TESTING.md` - Model package test expectations and
  no-paid-Vertex boundary for unit tests.

### Config And Eval Code
- `model/src/gemini_sft/config.py` - Current `EvalModelTarget`,
  `EvalExecutionConfig`, TOML parsing, and durable config validation. This is
  the main place to change plural `eval.models` / `eval_models` to singular
  `eval.model` / `eval_model`.
- `model/src/gemini_sft/evaluate.py` - Current `gemini-sft eval` flow, GCS
  `config.json` loading, backend dispatch, normalized manifest upload, report
  creation, local summary writing, and config update.
- `model/src/gemini_sft/target_execution.py` - Online backend routing,
  resumable online prediction artifacts, request identity sidecar, and
  generate-content execution.
- `model/src/common/gemini/batch.py` - Batch input/output GCS paths, existing
  path-based output reuse, batch output parsing, and duplicate/extra prediction
  guards.
- `model/src/gemini_sft/reporting.py` - Shared `EvalReport`,
  `TargetMetrics`, artifact fields, metric calculation, and console/Markdown
  rendering.
- `model/src/gemini_sft/records.py` - Local `wer_summary.{json,md}` writer and
  ledger behavior; Phase 4 should add GCS summary upload without making local
  results authoritative.
- `model/src/common/inference_manifest.py` - Normalized inference manifest path
  and row construction rules.

### Tests And Docs
- `model/tests/gemini_sft/test_config.py` - Config parsing tests to update for
  singular `eval.model`, singular durable `eval_model`, and rejection of plural
  forms.
- `model/tests/gemini_sft/test_workflow.py` - Eval workflow tests to update for
  one target, batch identity reuse, summary uploads, and artifact fields.
- `model/tests/gemini_sft/test_target_execution.py` - Online identity tests to
  keep aligned while adding batch identity coverage.
- `model/tests/gemini_sft/test_reporting.py` - Shared report schema tests to
  preserve.
- `model/tests/common/tests/test_drift_guard.py` - Prompt/request/context
  import drift guards that should remain green.
- `model/scripts/sft/README.md` - Operator-facing workflow documentation to
  update for singular eval model, fail-closed reuse, and GCS summary artifacts.
- `model/scripts/sft/run_config.example.toml` - Example config to update from
  plural `[[eval.models]]` to singular `[eval.model]`.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `EvalExecutionConfig` already owns backend, limit, concurrency, and
  `max_retries`; keep these config-wide and do not add per-target overrides.
- `target_execution.build_online_request_identity`,
  `request_identity_hash`, and metadata validation provide the pattern to copy
  for batch output identity.
- `common.gemini.vertex.build_request`,
  `GEMINI_GENERATION_CONFIG`, and `GEMINI_SAFETY_SETTINGS` are the canonical
  request source of truth for both online and batch inference.
- `build_context_histories` remains the dynamic prior-context builder; prior
  transcripts are not static manifest data.
- `ReportArtifacts`, `EvalReport`, and `render_console_report` already provide
  the shared reporting surface.

### Established Patterns
- GCS `config.json` is authoritative for eval state. The local TOML validates
  the requested shape, but eval execution should use the resolved durable GCS
  config.
- Prompt overrides are inline-only and copied into durable config state. Reuse
  identities must include resolved prompt text, not file paths.
- Missing predictions score as empty hypotheses and remain in the denominator.
  Missing rows are operationally separate from exact empty model responses.
- Tests should mock GCS and Vertex boundaries. No Phase 4 unit test should
  submit live batch jobs or online prediction calls.
- Local `results/` is a cache/mirror and should not be the source of truth for
  successful eval reuse or report links.

### Integration Points
- Replace plural target loaders in `config.py` with singular model loaders while
  keeping label validation via `validate_artifact_label`.
- Update `evaluate_run` to resolve and execute a single target instead of
  looping over targets.
- Add batch request identity metadata near `common.gemini.batch` or
  `gemini_sft.target_execution`, reusing the online identity/hash pattern where
  practical.
- Upload `wer_summary.json` and `wer_summary.md` to run-level GCS `evals/`
  paths after a successful eval and include those URIs in the target artifacts.
- Update the checkpoint scorer only if it depends on plural durable eval target
  structures; its endpoint scoring may remain a specialized script if it
  continues to delegate online inference through the package executor.

</code_context>

<specifics>
## Specific Ideas

The desired local config shape should look like:

```toml
[eval.model]
label = "checkpoint_6"
model = "projects/PROJECT/locations/us-central1/endpoints/ENDPOINT_ID"

[eval.execution]
# backend = "online"
# limit = 100
concurrency = 16
max_retries = 3
```

The corresponding durable `config.json` shape should include:

```json
{
  "eval_model": {
    "label": "checkpoint_6",
    "model": "projects/PROJECT/locations/us-central1/endpoints/ENDPOINT_ID"
  }
}
```

Recommended batch metadata path:

```text
gs://<bucket>/sft/runs/<round-id>/evals/<label>/batch_predictions.meta.json
```

Recommended batch metadata contents should mirror online metadata:

```json
{
  "request_identity_hash": "...",
  "request_identity": {
    "schema_version": 1,
    "target_label": "...",
    "model": "...",
    "eval_manifest_uri": "...",
    "audio_uris": ["..."],
    "system_prompt": "...",
    "user_prompt": "...",
    "prior_context_count": 8,
    "prior_context_mode": "text_turns",
    "generation_config": {"temperature": 0.0, "max_output_tokens": 512},
    "safety_settings": [...]
  }
}
```

</specifics>

<deferred>
## Deferred Ideas

- Internal multi-target eval execution is deferred indefinitely for now. Use
  separate config files and an external wrapper when comparing multiple models
  or checkpoints in parallel.
- Dataset-level breakdowns for `bcfy_calls`, `bcfy_feeds`, `echo`, and
  `fire_notifications` are deferred to a follow-up phase.
- Multiple eval manifests inside one run are deferred. They may be useful for
  future dataset breakdowns, but Phase 4 keeps one `eval_manifest_uri`.
- Versioned eval summaries are deferred. Phase 4 overwrites stable
  `evals/wer_summary.{json,md}` on successful rerun.

</deferred>

---

*Phase: 4-Durable Eval*
*Context gathered: 2026-06-28T21:00:23Z*

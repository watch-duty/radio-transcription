# Phase 3: Target Execution - Context

**Gathered:** 2026-06-28
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 3 packages target execution for `gemini-sft eval`. The runner should
execute the explicit `[[eval.models]]` targets created in Phase 2 through the
right Vertex backend while preserving shared Gemini prompt, request,
generation, safety, and prior-context behavior.

This phase replaces the temporary hard-coded base/tuned eval runner with
target-driven execution. It may execute multiple configured targets, but
multi-target parallelism, durable dataset breakdowns, GCS summary authority,
and full stale-output hash validation remain Phase 4. Full operator docs and
example variants remain Phase 5.

</domain>

<decisions>
## Implementation Decisions

### Backend Routing
- **D-01:** Default routing is conservative and does not perform live probe
  jobs. Publisher/base model IDs route to Vertex batch inference. Full Vertex
  endpoint resources route to online `generate_content`.
- **D-02:** Backend selection is resolved in code from the configured target
  `model` string. Keep `[[eval.models]]` entries exactly as Phase 2 defined
  them: `label` plus `model` only.
- **D-03:** `[eval.execution].backend` is optional and config-wide. If omitted,
  default routing applies. If present, it must be either `batch` or `online`
  and forces all targets in that config through that backend.
- **D-04:** Do not add an `auto` value. Omitted `backend` means default routing.
- **D-05:** Do not add per-target backend override tables in Phase 3. If an
  operator needs different forced backends, they can use separate configs/runs.

### Explicit Targets And Checkpoint Metadata
- **D-06:** Phase 3 eval runs only targets listed in durable `eval_models`.
  It must not discover additional checkpoints from a tuning job.
- **D-07:** Phase 3 must not fetch the tuning job only to enrich reports with
  checkpoint epoch/step metadata. Epoch and step remain absent unless a later
  phase adds an explicit metadata source.
- **D-08:** Checkpoint endpoints are just model strings in `eval_models`.
  They should not introduce checkpoint-specific primary CLI branches.

### Online Resume And Reuse
- **D-09:** Online execution resumes existing prediction JSONL by default.
  Existing rows are downloaded from GCS and completed `audio_filepath` rows are
  skipped.
- **D-10:** Resume is allowed only when the existing prediction artifact was
  produced by the same full request identity. If identity does not match, fail
  loudly before making new paid calls.
- **D-11:** Full request identity includes target label, target model string,
  eval manifest URI, evaluated audio URI set and order, system prompt, user
  prompt, prior context count, prior context mode, generation config, and
  safety settings.
- **D-12:** Operational execution settings such as concurrency, retry count,
  local/GCS sync cadence, and log cadence do not invalidate reuse.
- **D-13:** A smoke-limited run can reuse only the matching evaluated prefix.
  A later full run may resume those rows only when the manifest order and full
  request identity match.

### Smoke And Failure Behavior
- **D-14:** Exhausted online rows should be written as empty predictions with
  an `error` field, kept in the scoring denominator, and reflected in the
  report error count.
- **D-15:** The command should continue through row-level online failures and
  still write resumable artifacts and reports. It should not abort the target
  solely because some rows exhausted retries.
- **D-16:** Execution knobs are config-owned, not primarily CLI-owned.
  `[eval.execution]` may expose only:
  `backend`, `limit`, `concurrency`, and `max_retries`.
- **D-17:** `limit` is a smoke-test row cap. If omitted, eval uses the full
  eval manifest. It must preserve the same target routing, prompts, request
  construction, and prior-context behavior as the full run.
- **D-18:** Do not expose `retry_sleep_seconds`, `sync_every`, or `log_every`
  in the Phase 3 config contract. Keep those as internal defaults, using the
  existing checkpoint scorer behavior as the starting point unless research
  finds a safer value.

### Prompt And Prior-Context Parity
- **D-19:** New Phase 3 execution code must call shared `common.gemini`
  helpers for request construction. Do not assemble Gemini request JSON in a
  new workflow layer.
- **D-20:** Use `build_context_histories` as the dynamic prior-context builder
  for batch and online eval. Prior context is not static manifest data.
- **D-21:** Online and batch execution must use the same resolved prompt text,
  generation config, safety settings, and prior-context mode/count from durable
  GCS `config.json`.

### the agent's Discretion
Implementation planning may choose exact module names and type names. The prior
research suggested `gemini_sft.target_execution` or `gemini_sft.inference` for
package-owned target execution. Prefer extracting reusable package code from
`model/scripts/sft/score_gemini_sft_checkpoints_online.py` rather than keeping
that script as the maintained implementation surface.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Planning
- `.planning/PROJECT.md` - Core value, active requirements, constraints, and
  key decisions for the Gemini SFT workflow.
- `.planning/REQUIREMENTS.md` - Phase 3 requirements EXEC-01, EXEC-02,
  EXEC-03, EXEC-04, and EXEC-06 plus Phase 4 boundary for EXEC-05.
- `.planning/ROADMAP.md` - Phase 3 goal, success criteria, and dependency
  order.
- `.planning/phases/01-reporting-contract/01-CONTEXT.md` - Report contract,
  missing-prediction behavior, and checkpoint/batch report parity decisions.
- `.planning/phases/02-target-config/02-CONTEXT.md` - Locked target config
  shape, no legacy fallback, label semantics, masked/unmasked separate-run
  decisions, and Phase 3 deferrals.
- `.planning/phases/02-target-config/02-VERIFICATION.md` - Confirms Phase 2
  fail-closed target guard and explicitly defers target-driven execution to
  Phase 3.
- `.planning/research/SUMMARY.md` - Earlier implementation-plan direction for
  packaged target execution, parity, and backend routing risks.
- `.planning/research/PITFALLS.md` - Prompt drift, prior-context drift,
  backend confusion, online resume/sync, and cost/stale-output pitfalls.

### Codebase Maps
- `.planning/codebase/ARCHITECTURE.md` - Model/SFT package boundaries and
  existing `gemini_sft` workflow ownership.
- `.planning/codebase/INTEGRATIONS.md` - Google Cloud, Vertex AI, batch
  inference, tuning, and checkpoint-scoring integration notes.
- `.planning/codebase/CONVENTIONS.md` - GCS run state, prompt/request source
  of truth, safety settings, empty-output semantics, and artifact hygiene.

### Target Config And Eval Code
- `model/src/gemini_sft/config.py` - `EvalModelTarget`, `eval_models`,
  `require_config_eval_models`, context config, and TOML/GCS config parsing.
- `model/src/gemini_sft/evaluate.py` - Current batch eval path, Phase 2
  fail-closed target guard, context history use, reporting, and normalized
  manifest upload behavior.
- `model/tests/gemini_sft/test_config.py` - Target config and `[eval]`
  validation tests to extend for `[eval.execution]`.
- `model/tests/gemini_sft/test_workflow.py` - Existing eval workflow tests,
  prior-context request tests, and paid-boundary guard tests.

### Shared Gemini Execution Helpers
- `model/src/common/gemini/context.py` - `build_context_histories` and
  transcript/text-turn prior-context helpers.
- `model/src/common/gemini/vertex.py` - `build_request`, shared generation
  config, safety settings, Vertex batch submit/poll behavior, and resource
  location handling.
- `model/src/common/gemini/batch.py` - Reusable batch input/output
  orchestration and missing-prediction behavior.
- `model/src/common/gemini/tuning_data.py` - SFT JSONL request construction
  parity boundary.

### Current Online Checkpoint Scoring
- `model/scripts/sft/score_gemini_sft_checkpoints_online.py` - Existing async
  online checkpoint scorer to package or extract: endpoint location extraction,
  retries, concurrency, row limit, resume, sync, and shared reporting.
- `model/tests/gemini_sft/test_checkpoint_scorer.py` - Checkpoint scorer report
  schema tests that should inform package extraction tests.

### Operator-Facing Docs
- `model/scripts/sft/README.md` - Current CLI behavior, target config notes,
  GCS artifact layout, eval semantics, and Phase 3 placeholder language.
- `model/scripts/sft/run_config.example.toml` - Placeholder config shape to
  extend only if Phase 3 plans include execution config examples.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `common.gemini.vertex.build_request` already builds one canonical request for
  batch and online execution, including prompt text, safety settings,
  generation config, audio part, and selected history mode.
- `common.gemini.batch.run_batch_audio_inference` already writes batch JSONL,
  reuses existing batch output when present, parses predictions, rejects
  duplicate eval audio URIs, and reports missing predictions.
- `score_gemini_sft_checkpoints_online.py` already has the online mechanics:
  async `generate_content`, endpoint location extraction, row limit,
  concurrency, retries, local append, GCS sync, resume from existing GCS
  predictions, and shared report rendering.
- `gemini_sft.reporting` already provides target-oriented report objects and
  renderers from Phase 1.

### Established Patterns
- GCS `config.json` is authoritative for eval state; local `results/` is a
  mirror/cache.
- Paid operations must validate config and artifact state before submission.
- Prompt text is resolved into durable config and prompt files are not allowed.
- Unit tests should mock GCS and Vertex boundaries. No paid Vertex calls in
  tests.
- Phase 2 kept target entries free of `type`, `backend`, and metadata fields;
  Phase 3 execution config must not backslide by adding those fields inside
  `[[eval.models]]`.

### Integration Points
- Extend `gemini_sft.config` with optional `[eval.execution]` parsing and
  durable serialization.
- Replace the exact Phase 2 supported-target comparison in
  `gemini_sft.evaluate.evaluate_run` with target-driven execution.
- Add a target resolver/executor package boundary that chooses batch or online
  backend from each `EvalModelTarget` plus optional config-wide backend.
- Extract online execution helpers from
  `model/scripts/sft/score_gemini_sft_checkpoints_online.py` into package code
  used by `gemini-sft eval`; keep the script as a thin wrapper or defer script
  cleanup to planning.
- Add tests that prove online and batch execution use identical
  `build_request` input for the same row/history/prompt settings.
- Add tests that prove existing online predictions with mismatched request
  identity fail before new paid calls.

</code_context>

<specifics>
## Specific Ideas

Preferred optional execution config:

```toml
[eval.execution]
# Optional. If omitted, use default routing:
# publisher/base model IDs -> batch; full Vertex endpoint resources -> online.
backend = "online"  # batch | online
limit = 100         # optional smoke-test row cap; omitted means full eval
concurrency = 16    # optional online concurrency
max_retries = 3     # optional online retry attempts
```

Do not expose these Phase 3 config fields:

```toml
retry_sleep_seconds = 2.0
sync_every = 100
log_every = 100
```

Those remain internal defaults. `sync_every` means how many newly completed
online prediction rows are appended locally before uploading the prediction
JSONL back to GCS; it is useful for interruption recovery but should not be
part of the operator config surface in Phase 3.

Recommended online prediction artifact path for a target label:

```text
gs://<bucket>/sft/runs/<round-id>/evals/<target-label>/online_predictions.jsonl
```

</specifics>

<deferred>
## Deferred Ideas

- Live probing to determine whether a full Vertex endpoint resource supports
  batch inference is deferred. Phase 3 should not spend probe jobs.
- Per-target backend override maps are deferred. Separate configs/runs are
  sufficient for forced mixed backend experiments in Phase 3.
- Tuning-job checkpoint discovery and epoch/step metadata enrichment are
  deferred. Phase 3 runs explicit targets only.
- Full stale-output hashing across batch and online artifacts is Phase 4,
  though Phase 3 online resume must still validate full request identity.
- Multi-target parallel execution is Phase 4. Phase 3 may loop over multiple
  configured targets but does not need to run targets in parallel by default.
- Dataset-level breakdowns and GCS-authoritative summary artifacts are Phase 4.
- Complete operator docs and example variants are Phase 5.

</deferred>

---

*Phase: 3-Target Execution*
*Context gathered: 2026-06-28*

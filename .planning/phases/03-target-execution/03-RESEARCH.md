# Phase 3 Research: Target Execution

**Phase:** 03-target-execution
**Date:** 2026-06-28
**Status:** Complete
**Confidence:** HIGH for repo-local implementation paths; MEDIUM for live
Vertex endpoint batch compatibility.

## Research Question

What needs to be true to implement Phase 3 without drifting prompt/request,
prior-context, generation, safety, or artifact behavior from the existing
Gemini SFT paths?

## External API Findings

Context7 resolved the current Google Gen AI Python SDK documentation to
`/googleapis/python-genai`.

Relevant SDK surfaces:

- Vertex client initialization uses `genai.Client(vertexai=True, project=..., location=...)`.
- Vertex batch jobs are created through `client.batches.create(model=..., src=..., config=...)`.
- Tuned model or endpoint online calls are supported through
  `client.models.generate_content(model=<endpoint>, contents=..., config=...)`.

Implication: Phase 3 should keep using the existing `common.gemini.vertex`
wrappers for batch and request construction, and should package the current
checkpoint scorer's online `models.generate_content` behavior for endpoint
targets. The public docs confirm both surfaces, but do not remove the repo's
uncertainty about which tuned/checkpoint endpoint resource forms are accepted
by batch prediction. Therefore default routing should remain conservative:
publisher/model IDs to batch, full endpoint resources to online, with an
explicit config-wide backend override for controlled experiments.

## Repo Findings

### Durable Config

`model/src/gemini_sft/config.py` now owns:

- `EvalModelTarget(label, model)`
- TOML parsing for `[[eval.models]]`
- durable `config.json` validation through `require_config_eval_models`
- prompt and prior-context fields copied into `RunConfig.to_record_dict()`

This is the right place to add `[eval.execution]` parsing and durable
serialization. Keep it static: no GCS or Vertex calls during config loading.

### Batch Execution

`model/src/common/gemini/batch.py` already provides
`run_batch_audio_inference`. It:

- builds request JSONL with `common.gemini.vertex.build_request`
- uploads input JSONL to `<run_gcs_prefix>/evals/<label>/input.jsonl`
- reuses existing batch output when present
- submits through `submit_batch_inference`
- parses output with `parse_batch_output`
- rejects duplicate eval audio URIs and prediction rows outside the manifest
- reports missing predictions while leaving them to score as empty hypotheses

Phase 3 should call this helper directly for batch targets rather than
building another batch path.

### Online Execution

`model/scripts/sft/score_gemini_sft_checkpoints_online.py` already has the
online mechanics Phase 3 needs:

- endpoint location extraction from full Vertex resource names
- `genai.Client(...).aio.models.generate_content(...)`
- `types.GenerateContentConfig` using the same generation and safety settings
- async bounded concurrency
- retry loop with backoff
- local append plus periodic GCS sync
- resume by downloading existing online prediction JSONL
- row-level error rows with empty predictions
- report rows built by `gemini_sft.reporting`

The gap is that this lives in a script and lacks request-identity validation.
Phase 3 should extract the reusable pieces into package code, then make the
script call that package code so checkpoint scoring and packaged eval cannot
drift.

### Prompt And Prior Context

`common.gemini.vertex.build_request` is the canonical request builder for:

- system instruction
- current user prompt
- current audio part
- prior context modes: `audio`, `text_turns`, `transcript`,
  `vapo_p3_transcript`
- `GEMINI_GENERATION_CONFIG`
- `GEMINI_SAFETY_SETTINGS`

`common.gemini.context.build_context_histories` dynamically groups same-source
rows, sorts by source offset/order, and excludes empty or `[UNINTELLIGIBLE]`
history rows. Phase 3 must build histories once from canonical eval source
rows and feed the same history list to batch and online target execution.

### Reporting

`gemini_sft.reporting` already provides the shared target-oriented report
contract. It has `TargetMetrics.metadata`, which can carry Phase 3 online
operational fields such as `backend`, `online_error_count`, and
`request_identity_hash` without changing Phase 1 report columns. Phase 4 can
decide whether to promote any of those metadata fields into top-level durable
summary columns.

### Existing Tests To Extend

- `model/tests/gemini_sft/test_config.py`: add `[eval.execution]` validation
  tests.
- `model/tests/gemini_sft/test_workflow.py`: replace hard-coded base/tuned
  eval assumptions with target-driven backend tests.
- `model/tests/gemini_sft/test_checkpoint_scorer.py`: ensure the script uses
  the packaged online path and still emits the shared report schema.
- `model/tests/common/tests/test_drift_guard.py`: add import/use guards for the
  package online executor and scorer script.
- `model/tests/common/tests/test_gemini_batch.py` and
  `model/tests/common/tests/test_gemini_vertex.py`: keep request and batch
  helper coverage where it already belongs.

## Implementation Implications

1. Add a small execution config object, not another workflow language.
   `[eval.execution]` may expose only `backend`, `limit`, `concurrency`, and
   `max_retries`.
2. Backend routing should be deterministic and offline:
   - forced `backend = "batch"` or `"online"` applies to all targets
   - omitted backend routes full endpoint resources to online
   - omitted backend routes all other model strings to batch
3. Online resume needs a sidecar metadata artifact, not just prediction rows.
   Store a deterministic request identity next to
   `online_predictions.jsonl`, for example `online_predictions.meta.json`.
4. Request identity should be split into request core plus evaluated audio URI
   order. Exact reuse requires both to match; smoke-prefix reuse requires the
   prior URI list to be a prefix of the current URI list and the request core
   to match.
5. Online row failures should be written as `{pred_text: "", error: ...}` and
   included in the denominator. Do not abort a whole target solely because
   some rows exhaust retries.
6. `gemini-sft eval` can loop over targets sequentially in Phase 3. Parallel
   target execution is Phase 4.
7. Normalized inference manifests can still be written per evaluated target
   using the existing helper. Dataset breakdowns and GCS-authoritative summary
   artifacts remain Phase 4.

## Risks

- **Endpoint batch ambiguity:** Official SDK docs show batch creation and tuned
  endpoint online generation, but do not prove every endpoint resource supports
  batch. Default endpoint routing should be online unless a later live
  validation phase changes it.
- **Stale online predictions:** Prediction rows alone are insufficient for
  safe reuse. Missing metadata must fail loudly if prediction rows exist.
- **Smoke/full confusion:** `limit` changes the evaluated audio set. Prefix
  reuse must validate row order before mixing smoke rows into a full run.
- **Script/package drift:** Leaving online execution in the script would
  reintroduce the drift Phase 3 is meant to remove.

## Sources

- Context7 `/googleapis/python-genai`: Google Gen AI Python SDK docs for
  Vertex client initialization, batch jobs, and tuned endpoint
  `generate_content`.
- `model/src/common/gemini/vertex.py`
- `model/src/common/gemini/batch.py`
- `model/src/common/gemini/context.py`
- `model/src/gemini_sft/config.py`
- `model/src/gemini_sft/evaluate.py`
- `model/scripts/sft/score_gemini_sft_checkpoints_online.py`
- `model/src/gemini_sft/reporting.py`
- `.planning/phases/03-target-execution/03-CONTEXT.md`

## Research Complete

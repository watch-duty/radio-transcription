# Phase 4: Durable Eval - Research

## Research Complete

### Question

What do we need to know to plan Phase 4 well after the discussion narrowed the
scope to one `[eval.model]` per eval run, strict artifact reuse, stable GCS
summary artifacts, and deferred dataset breakdowns?

### External Documentation Notes

- Google Cloud Vertex AI Gemini batch prediction uses explicit JSONL input and
  JSONL output locations in Cloud Storage. The batch job request contains the
  model path, `inputConfig.gcsSource.uris`, and
  `outputConfig.gcsDestination.outputUriPrefix`. Source:
  https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/capabilities/batch-prediction-from-cloud-storage
- Vertex batch prediction responses include the model, input config, output
  config, state, timestamps, and model version. This reinforces that the repo
  should treat the GCS input/output locations and its own sidecar metadata as
  the durable reproducibility surface, not local `results/`.
- The docs show Cloud Storage as the exchange layer for batch input/output and
  generated artifacts. Stable GCS paths for `evals/wer_summary.{json,md}` match
  that pattern and should be overwritten only after a successful eval.

### Current Code Findings

- `model/src/gemini_sft/config.py` still models eval targets as plural
  `eval_models: tuple[EvalModelTarget, ...]`, parses `[[eval.models]]`, and
  validates durable `config.json` through `require_config_eval_models`.
- `model/src/gemini_sft/evaluate.py` calls `require_config_eval_models`,
  loops over every target, uploads one normalized inference manifest per target
  label, and writes only local `results/<round-id>/wer_summary.{json,md}`.
- `model/src/common/gemini/batch.py` writes `evals/<label>/input.jsonl` and
  reuses `evals/<label>/output/` when JSONL exists, but has no metadata sidecar
  proving the output matches the current model, prompt, manifest, context, or
  safety/generation settings.
- `model/src/gemini_sft/target_execution.py` already has the online identity
  pattern: deterministic request identity hash, metadata sidecar, exact/prefix
  audio URI validation, and fail-before-paid-call semantics for missing or
  mismatched metadata.
- `model/src/gemini_sft/reporting.py` already supports
  `summary_json_uri` and `summary_markdown_uri` in `ReportArtifacts`; Phase 4
  can populate these before rendering the report.
- `model/src/gemini_sft/records.py` writes local summaries but does not return
  their local paths or upload them to GCS.
- `model/scripts/sft/README.md` and `model/scripts/sft/run_config.example.toml`
  still document plural `[[eval.models]]`.

### Recommended Plan Shape

1. Change config parsing and durable state from plural targets to a singular
   `eval_model`, while rejecting plural local and durable forms loudly.
2. Add batch request-identity metadata before changing eval flow so stale
   output safety is independently testable.
3. Update `gemini-sft eval` to execute exactly one durable model target, attach
   summary artifact URIs, upload `wer_summary.{json,md}` to stable run-level
   GCS paths, and keep normalized inference manifest output.
4. Update docs/examples and compatibility tests so plural target and dataset
   breakdown assumptions do not reappear.

### Validation Architecture

Unit tests should stay under `model/tests` with fake GCS and patched Vertex
boundaries. No test should submit live Vertex batch jobs, live online
generate-content calls, or full notebook execution.

Targeted checks:

- `safe-run -- bash -lc 'cd model && PYTHONPATH=src:tests python3 -m pytest tests/gemini_sft/test_config.py tests/gemini_sft/test_target_execution.py tests/gemini_sft/test_workflow.py tests/gemini_sft/test_reporting.py tests/gemini_sft/test_checkpoint_scorer.py tests/common/tests/test_drift_guard.py -q'`
- `python3 -m py_compile model/src/gemini_sft/config.py model/src/gemini_sft/evaluate.py model/src/gemini_sft/target_execution.py model/src/common/gemini/batch.py model/src/gemini_sft/records.py model/scripts/sft/score_gemini_sft_checkpoints_online.py`
- `rg -n "eval_models|\\[\\[eval\\.models\\]\\]|require_config_eval_models" model/src/gemini_sft model/scripts/sft model/tests/gemini_sft`

### Open Risks

- Requirement traceability still lists `EXEC-05`, `DATA-03`, and `DATA-04` for
  Phase 4, but the Phase 4 context intentionally supersedes or defers them.
  Plans must name these IDs and describe the narrowed behavior explicitly so
  coverage checks distinguish deliberate deferral from omission.
- Batch smoke-prefix reuse is not safe by default because Vertex batch output
  writes into an output prefix. Unlike online JSONL appends, Phase 4 should
  require exact batch identity for output reuse and fail on mismatch.
- Moving online identity helpers into `common.gemini` reduces duplication, but
  it changes import boundaries. If implementation keeps online helpers in
  `gemini_sft.target_execution`, batch identity must not import `gemini_sft`
  from `common.gemini.batch`.

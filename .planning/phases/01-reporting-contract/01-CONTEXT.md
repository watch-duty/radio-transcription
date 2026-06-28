# Phase 1: Reporting Contract - Context

**Gathered:** 2026-06-28
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 1 defines and wires a shared SFT eval reporting contract. The work should
make batch eval and checkpoint scoring produce the same operator-facing metric
columns, JSON shape, Markdown shape, and console output for comparable targets.
It should not introduce the later unified target config, masked/unmasked run
orchestration, checkpoint backend selection, multi-target parallel execution, or
dataset breakdown features; those belong to later phases.

</domain>

<decisions>
## Implementation Decisions

### Metric Vocabulary
- **D-01:** Use `empty_or_unintelligible_rate` as the public name for the
  historical metric that flags hypotheses whose stripped text is empty or
  exactly `[UNINTELLIGIBLE]`.
- **D-02:** Use `empty_response_rate` only for exact empty model output, where
  the stripped hypothesis is an empty string.
- **D-03:** Do not show `empty_rate` or `hallucination_rate` as primary
  operator-facing column names in new reports. Compatibility readers may accept
  those legacy keys as inputs, but the shared report should emit the canonical
  names above.
- **D-04:** Standardize insertion, deletion, and substitution fields as raw
  counts in the shared report contract. Existing percentage fields can remain as
  implementation details or derived metrics, but the required report columns are
  counts.
- **D-05:** Include `total_reference_words` in every per-target metrics row. It
  is the WER denominator: `hits + substitutions + deletions`.

### Report Schema
- **D-06:** JSON, Markdown, and console output should come from one structured
  report object rather than three independent formatting paths.
- **D-07:** The per-target report row should include at least:
  `target_label`, `model`, `wer`, `cer`, `keyword_accuracy`,
  `empty_or_unintelligible_rate`, `empty_response_rate`, `insertions`,
  `deletions`, `substitutions`, `total_reference_words`,
  `missing_prediction_count`, and artifact URIs.
- **D-08:** Artifact fields should cover the target's raw provider output when
  available, online prediction JSONL when applicable, normalized inference
  manifest URI when available, and summary artifact URIs when uploaded.
- **D-09:** Console output should print the full report table, not only a terse
  best-checkpoint JSON payload. Markdown should use the same columns and order
  as the console table.

### Missing Predictions
- **D-10:** Missing model predictions remain scored as empty hypotheses so they
  stay in the WER/CER denominator as full deletions.
- **D-11:** Reports must expose `missing_prediction_count` separately from exact
  empty responses. A provider that returns no row and a model that returns an
  explicit empty text are operationally different even though both score as
  empty hypotheses.
- **D-12:** Batch eval should keep rejecting duplicate eval audio URIs and
  prediction rows outside the eval manifest; this phase should not relax those
  safety checks.

### Batch And Checkpoint Parity
- **D-13:** Phase 1 should extract or add shared metric/report helpers that both
  `gemini-sft eval` and the checkpoint scorer can call.
- **D-14:** Phase 1 should align report columns and metric semantics across
  batch and checkpoint paths without changing how each path performs inference.
- **D-15:** Checkpoint scoring can continue to use resumable online predictions
  in this phase. Choosing backends from a unified target config belongs to
  Phase 3 after Phase 2 defines target config.

### the agent's Discretion
The implementation can choose exact module and type names. Prefer a small
SFT-owned helper such as `gemini_sft.reporting` or similar over duplicating
rendering code in scripts. Keep the public contract simple enough for tests to
assert exact columns and exact metric names.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Planning
- `.planning/REQUIREMENTS.md` - Reporting Contract requirements RPT-01 through
  RPT-05 and related out-of-scope boundaries.
- `.planning/ROADMAP.md` - Phase 1 goal, success criteria, and dependency order.
- `.planning/codebase/CONCERNS.md` - Existing Gemini SFT eval gap, empty
  response terminology, prior-context semantics, and artifact hygiene risks.
- `.planning/codebase/CONVENTIONS.md` - SFT run state, prompt/request source of
  truth, safety settings, empty output semantics, and local artifact rules.
- `.planning/codebase/TESTING.md` - Model-package test boundaries and guidance
  to mock GCS/Vertex instead of running paid evals.

### SFT Workflow Docs
- `model/scripts/sft/README.md` - Current `gemini-sft` commands, durable GCS
  records, evaluation semantics, normalized inference manifest placement, and
  prompt parity expectations.

### Reporting And Scoring Code
- `model/src/gemini_sft/evaluate.py` - Config-driven batch eval, current metric
  assembly, missing-prediction empty-hypothesis behavior, and artifact URI
  capture.
- `model/src/gemini_sft/records.py` - Current WER summary and ledger rendering.
- `model/scripts/sft/score_gemini_sft_checkpoints_online.py` - Current online
  checkpoint scoring, exact empty response metric, checkpoint summary JSON/MD,
  and prediction artifact URI handling.
- `model/src/common/scoring.py` - WER/CER implementation, edit counts, keyword
  metrics, duration buckets, and historical empty-or-unintelligible helper.
- `model/src/common/gemini/batch.py` - Batch output loading and missing
  prediction warning behavior.
- `model/tests/gemini_sft/test_workflow.py` - Existing tests for eval reporting,
  missing predictions, normalized inference manifests, and edit breakdown
  denominator behavior.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `common.scoring.compute_wer` returns WER plus raw `insertions`, `deletions`,
  `substitutions`, and `hits`. This is the right source for both edit counts
  and `total_reference_words`.
- `common.scoring.compute_cer`, `keyword_metrics`, and `hallucination_rate`
  already provide the base metrics needed for the shared contract.
- `gemini_sft.evaluate.build_metrics` and `add_tuned_metrics` already assemble
  most batch metrics, but currently use prefixed base/tuned fields and expose
  the historical empty metric as `base_empty_rate` or `tuned_empty_rate`.
- `score_gemini_sft_checkpoints_online.score_predictions` already computes
  exact empty response rate and keyword accuracy for checkpoints, but it keeps
  legacy `empty_rate` and `hallucination_rate` keys.
- `gemini_sft.records.write_wer_summary` is the current batch JSON/Markdown
  writer and can either be replaced by or call into the shared report renderer.

### Established Patterns
- GCS `config.json` under the run prefix is authoritative run state; local
  `results/` files are mirrors only.
- Unit tests for SFT mock GCS and Vertex boundaries. Phase 1 tests should use
  fake predictions and should not submit paid Vertex jobs.
- Batch eval already treats missing predictions as empty strings for scoring so
  they count as deletions. The gap is surfacing `missing_prediction_count` in
  the report object and table.
- Normalized inference manifests omit the prediction field for missing provider
  rows, while explicit empty responses can be represented as `pred_text_* = ""`.
  The report needs to distinguish these cases.

### Integration Points
- Add the shared report schema and renderers in an importable module used by
  both `model/src/gemini_sft/evaluate.py` and
  `model/scripts/sft/score_gemini_sft_checkpoints_online.py`.
- Update batch eval metric assembly to produce per-target rows instead of only
  flat `base_*` and `tuned_*` fields, while preserving migration compatibility
  if existing tests or docs expect the old files during this milestone.
- Update checkpoint summary rendering to use the same column names and order as
  batch eval.
- Add focused model tests under `model/tests/gemini_sft` for the shared report
  object, JSON/Markdown/console parity, empty metric separation, raw edit
  counts, total reference words, and missing prediction counts.

</code_context>

<specifics>
## Specific Ideas

The primary console/Markdown table should be target-oriented, for example:

| target | WER | CER | keyword_accuracy | empty_or_unintelligible_rate | empty_response_rate | S | I | D | total_ref_words | missing_predictions | artifacts |
|--------|-----|-----|------------------|------------------------------|---------------------|---|---|---|-----------------|---------------------|-----------|

JSON should preserve the same information in a structured form, for example a
top-level report with `round_id`, `generated_at`, optional eval metadata, and a
`targets` list. Do not make local `results/` authoritative in this phase; later
durable GCS summary behavior is Phase 4.

</specifics>

<deferred>
## Deferred Ideas

- Unified `models` target config for base, tuned, and checkpoint endpoints is
  Phase 2.
- Backend selection between Vertex batch and online checkpoint inference is
  Phase 3.
- Multi-target parallel execution, masked/unmasked durable runs, and dataset
  breakdown reports are Phase 4.
- Operator docs, example configs, and final artifact hygiene guidance are
  Phase 5.

</deferred>

---

*Phase: 1-Reporting Contract*
*Context gathered: 2026-06-28*

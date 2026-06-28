# Phase 1: Reporting Contract - Research

## RESEARCH COMPLETE

**Phase:** 1 - Reporting Contract
**Question answered:** What needs to be known to plan a shared SFT eval report
contract across batch eval and checkpoint scoring?

## Scope Summary

Phase 1 should standardize metric semantics and report rendering, not change
target configuration or inference backends. The existing code already computes
the needed primitive metrics, but the batch and checkpoint paths assemble and
render them differently:

- `gemini-sft eval` writes flat `base_*` and `tuned_*` metrics through
  `model/src/gemini_sft/evaluate.py` and `model/src/gemini_sft/records.py`.
- `model/scripts/sft/score_gemini_sft_checkpoints_online.py` writes separate
  checkpoint summary JSON/Markdown and prints only the best checkpoint JSON.
- `common.scoring.hallucination_rate` is the historical
  empty-or-unintelligible metric, but the name is confusing for operator
  reports.
- Exact empty response rate exists only in the checkpoint scorer script.
- Batch missing predictions are scored as empty strings, but the count is only a
  warning and is not surfaced in the summary files.

## Technical Findings

### Shared Report Contract

The most useful implementation shape is a small importable module under
`model/src/gemini_sft`, for example `gemini_sft.reporting`. It can define the
target-oriented report contract once and expose renderers for console,
Markdown, and JSON-compatible dictionaries.

The shared per-target row should include:

- `target_label`
- `model`
- `wer`
- `cer`
- `keyword_accuracy`
- `empty_or_unintelligible_rate`
- `empty_response_rate`
- `insertions`
- `deletions`
- `substitutions`
- `total_reference_words`
- `missing_prediction_count`
- `artifacts`

`total_reference_words` should be computed from the WER output as:

```text
hits + substitutions + deletions
```

That matches the denominator used by WER and by the existing percentage edit
breakdown in `gemini_sft.evaluate.add_error_breakdown`.

### Metric Semantics

`common.scoring.hallucination_rate` currently returns the historical
empty-or-unintelligible rate: a hypothesis is flagged when stripped text is
empty or exactly `[UNINTELLIGIBLE]`.

The plan should keep that implementation for compatibility but expose the
operator-facing field as `empty_or_unintelligible_rate`.

Exact empty response rate should move from the checkpoint scorer into shared
code, most likely `common.scoring.empty_response_rate`, with this behavior:

- Return `0.0` for an empty hypothesis list.
- Count only hypotheses where `not hyp.strip()`.
- Return a rounded percentage with the same two-decimal behavior as existing
  scoring helpers.

### Batch Eval Integration

`gemini_sft.evaluate.evaluate_run` already has the correct denominator policy:

```python
base_hyps = [base_preds.get(row.audio_filepath, "") for row in eval_rows]
```

The plan should preserve that behavior and add:

```python
missing_prediction_count = sum(
    1 for row in eval_rows if row.audio_filepath not in predictions
)
```

The batch report should attach:

- raw Vertex batch output URI from `BatchPredictionMap.output_uri`
- normalized inference manifest URI returned by `upload_inference_manifest`

The summary writer in `gemini_sft.records` is the current integration point for
JSON/Markdown output. It can be changed to write the shared report dictionary
and shared Markdown table. Console output can use the same table string.

### Checkpoint Scorer Integration

The checkpoint scorer already has:

- resumable online prediction JSONL
- base batch prediction loading
- WER/CER
- keyword accuracy
- exact empty response rate
- checkpoint summary uploads to GCS

It should stop rendering a separate legacy table and instead build target rows
with the shared report helper. It should print the full table to stdout before
or instead of the current `best_checkpoint` JSON so operators can inspect all
checkpoint rows on the console.

Artifact fields should include:

- base raw batch output URI
- checkpoint online prediction JSONL URI
- summary JSON/Markdown URIs after upload

### Compatibility

Existing local `results/<round-id>/wer_summary.{json,md}` and
`checkpoint_score_summary.{json,md}` filenames can remain. Phase 1 is about the
content contract, not moving durable artifact ownership to GCS. Phase 4 owns
durable multi-target reports.

If old flat keys such as `base_wer` are needed for `append_ledger`, derive them
from the shared report row rather than letting a second metric computation path
remain authoritative.

## Validation Architecture

Use targeted model tests only. Do not submit Vertex jobs or run broad local
resource-heavy tests.

Recommended coverage:

- `model/tests/common/tests/test_scoring.py`
  - `empty_response_rate([]) == 0.0`
  - `empty_response_rate(["", "copy", "  "]) == 66.67`
  - `hallucination_rate(["", "[UNINTELLIGIBLE]", "copy"]) == 66.67`
- `model/tests/gemini_sft/test_reporting.py`
  - `build_target_metrics` emits canonical field names.
  - JSON and Markdown/console renderers use the same target columns.
  - Legacy names `empty_rate` and `hallucination_rate` are not public report
    row keys.
  - `total_reference_words` equals `hits + substitutions + deletions`.
- `model/tests/gemini_sft/test_workflow.py`
  - batch eval summary includes `missing_prediction_count`.
  - batch JSON/Markdown/console report distinguishes missing predictions from
    exact empty responses.
  - normalized inference manifest behavior remains unchanged for missing
    provider rows.
- `model/tests/gemini_sft/test_checkpoint_scorer.py`
  - checkpoint summary table uses canonical columns.
  - checkpoint rows include raw edit counts, total reference words, exact empty
    response rate, empty-or-unintelligible rate, and prediction artifact URI.

Suggested targeted commands:

```bash
safe-run -- bash -lc 'cd model && PYTHONPATH=src:tests python3 -m pytest tests/common/tests/test_scoring.py tests/gemini_sft/test_reporting.py -q'
safe-run -- bash -lc 'cd model && PYTHONPATH=src:tests python3 -m pytest tests/gemini_sft/test_workflow.py tests/gemini_sft/test_checkpoint_scorer.py -q'
```

## Risks And Mitigations

| Risk | Mitigation |
|------|------------|
| Existing consumers expect `base_wer`/`tuned_wer` flat keys. | Keep migration aliases where needed, but make shared target rows authoritative. |
| Console, JSON, and Markdown drift again. | Render all three from the same report object and test the shared column list. |
| Missing predictions and exact empty strings are conflated. | Track `missing_prediction_count` separately and compute `empty_response_rate` from actual hypotheses only. |
| Checkpoint script imports from `model/src` fail in direct script execution. | Follow the existing script import pattern and add tests that import the script exactly as today. |
| Plans accidentally expand into target config or backend execution. | Keep Phase 1 edits limited to metrics, rendering, and integration with existing paths. |

## Research Conclusion

Plan Phase 1 as one foundation plan followed by two integration plans:

1. Shared report schema, metric helpers, and renderer tests.
2. Batch eval report integration.
3. Checkpoint scorer report integration.

Plans 2 and 3 can run in parallel after Plan 1 because their write sets do not
overlap.

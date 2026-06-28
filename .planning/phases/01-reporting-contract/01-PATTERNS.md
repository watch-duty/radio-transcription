# Phase 1: Reporting Contract - Pattern Map

## PATTERN MAPPING COMPLETE

## File Ownership Map

| Planned file | Role | Closest existing analog | Notes |
|--------------|------|-------------------------|-------|
| `model/src/gemini_sft/reporting.py` | Shared report schema and renderers | `model/src/gemini_sft/records.py` | Keep pure, importable, and testable without GCS or Vertex. |
| `model/src/common/scoring.py` | Shared metric primitives | Existing `hallucination_rate`, `compute_wer`, `compute_cer` | Add exact empty response helper next to historical empty-or-unintelligible helper. |
| `model/src/gemini_sft/evaluate.py` | Batch eval integration | Existing `build_metrics`, `add_tuned_metrics`, `add_error_breakdown` | Preserve empty-hypothesis fallback for missing predictions. |
| `model/src/gemini_sft/records.py` | Batch summary writer | Existing `write_wer_summary`, `_render_wer_md` | Convert to shared report rendering instead of bespoke Markdown. |
| `model/scripts/sft/score_gemini_sft_checkpoints_online.py` | Online checkpoint scoring integration | Existing `score_predictions`, `write_summary` | Keep resumable online inference untouched; replace score/report assembly. |
| `model/tests/gemini_sft/test_reporting.py` | Shared report unit tests | `model/tests/gemini_sft/test_workflow.py` | New focused test surface for pure report objects and renderers. |
| `model/tests/gemini_sft/test_workflow.py` | Batch path regression tests | Existing fake GCS/Vertex tests | Add assertions around shared report content and missing prediction count. |
| `model/tests/gemini_sft/test_checkpoint_scorer.py` | Checkpoint path regression tests | Existing checkpoint summary tests | Update legacy expectations to canonical report columns. |
| `model/tests/common/tests/test_scoring.py` | Shared scoring tests | Existing hallucination rate tests | Add exact empty response tests. |

## Data Flow Pattern

Current batch flow:

```text
canonical eval manifest
  -> run_batch_audio_inference
  -> BatchPredictionMap
  -> refs/hyps
  -> compute_wer / compute_cer / keyword_metrics / hallucination_rate
  -> flat metrics dict
  -> records.write_wer_summary
```

Target batch flow:

```text
canonical eval manifest
  -> run_batch_audio_inference
  -> BatchPredictionMap
  -> refs/hyps + missing_prediction_count
  -> reporting.build_target_metrics
  -> reporting.EvalReport
  -> records.write_wer_summary + console table
```

Current checkpoint flow:

```text
canonical eval manifest
  -> base batch prediction output
  -> online checkpoint predictions
  -> score_predictions
  -> checkpoint_score_summary.{json,md}
  -> print best_checkpoint JSON
```

Target checkpoint flow:

```text
canonical eval manifest
  -> base batch prediction output
  -> online checkpoint predictions
  -> reporting.build_target_metrics per target
  -> reporting.EvalReport
  -> checkpoint_score_summary.{json,md}
  -> print full report table
```

## Existing Patterns To Preserve

- The model package keeps core imports light. `gemini_sft.reporting` must not
  import `google-genai`, `google-cloud-storage`, or other Vertex clients.
- `common.scoring` requires the scoring extra only when JiWER-backed functions
  are called. `empty_response_rate` should not require JiWER.
- SFT tests use `model/tests/fake_gcs.py` and monkeypatch Vertex boundaries.
  New tests should not submit paid jobs.
- Existing artifact filenames can remain during Phase 1:
  `wer_summary.json`, `wer_summary.md`, `checkpoint_score_summary.json`, and
  `checkpoint_score_summary.md`.
- GCS run state remains authoritative, but local `results/` still exists as a
  mirror/cache until Phase 4 changes durable reporting.

## Plan Dependency Map

| Plan | Wave | Depends on | Writes |
|------|------|------------|--------|
| `01-01` shared reporting foundation | 1 | none | `common.scoring`, `gemini_sft.reporting`, shared tests |
| `01-02` batch eval integration | 2 | `01-01` | `gemini_sft.evaluate`, `gemini_sft.records`, workflow tests |
| `01-03` checkpoint scorer integration | 2 | `01-01` | checkpoint scorer script and checkpoint tests |

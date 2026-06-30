---
type: reference
title: Gemini SFT Metric Glossary
description: Canonical report columns and metric semantics for Gemini SFT eval reports.
tags: [gemini-sft, metrics, reports]
---

# Gemini SFT Metric Glossary

Gemini SFT eval reports expose one target row per configured `[eval.model]`.
The public row columns are the values in `REPORT_COLUMNS`.

| Column | Meaning |
| --- | --- |
| `target_label` | Artifact-safe label from `[eval.model].label`; this names the report row and target artifact directory. |
| `model` | Publisher model ID, tuned endpoint, or checkpoint endpoint from `[eval.model].model`. |
| `wer` | Word error rate percentage after the shared dispatch normalizer is applied to references and hypotheses. |
| `cer` | Character error rate percentage after the same shared normalization pass. |
| `keyword_accuracy` | Occurrence-weighted percentage of configured dispatch keywords found in the paired hypothesis when they appear in the reference. |
| `empty_or_unintelligible_rate` | Percentage over all eval rows whose explicit provider prediction is empty after stripping whitespace or exactly `[UNINTELLIGIBLE]`; missing provider rows remain in the denominator but not the numerator. |
| `insertions` | Word insertion count from the WER alignment. |
| `deletions` | Word deletion count from the WER alignment. |
| `substitutions` | Word substitution count from the WER alignment. |
| `total_reference_words` | WER denominator for the target row: matched reference words plus substitutions plus deletions. |
| `missing_prediction_count` | Count of eval rows where the provider output did not include a prediction for that audio URI. |
| `artifacts` | Object containing durable artifact URIs for the row. |

Reports expose the evaluated row count as report metadata named
`n_eval_examples`, not as a target-table column.

## Empty Output Metrics

`empty_or_unintelligible_rate` is the metric for explicit provider predictions
that are empty after stripping whitespace or exactly `[UNINTELLIGIBLE]`.

Missing provider rows are scored as empty hypotheses for WER/CER so they remain
in the WER/CER denominator. For empty-output metrics, they remain in the eval-row
denominator but are not counted as empty model responses.

Use this column when deciding whether a target failed to answer or produced the
explicit unusable-audio token.

## Missing Predictions

Provider outputs are matched back to eval rows by audio URI. If a provider row
is missing, `gemini-sft eval` supplies an empty hypothesis for scoring so the
reference row remains in the WER/CER denominator.

`missing_prediction_count` stays operationally separate from exact empty model
responses. A missing provider row means no prediction record was returned for
that audio URI; an exact empty response means the provider returned a prediction
record whose stripped text was empty.

## Artifact URI Fields

The `artifacts` object can include:

| Key | Meaning |
| --- | --- |
| `raw_output_uri` | Durable GCS prefix for raw Vertex batch output when the target used batch inference. |
| `online_predictions_uri` | Durable GCS JSONL for online endpoint predictions when the target used online inference. |
| `normalized_manifest_uri` | Durable normalized inference manifest with source rows and prediction fields. |
| `summary_json_uri` | Stable GCS URI for the target run's JSON report summary. |
| `summary_markdown_uri` | Stable GCS URI for the target run's Markdown report summary. |

These fields point to durable GCS artifacts. Local files under `results/` are a
cache or mirror, not the source of truth for report reuse.

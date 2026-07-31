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
| `empty_or_unintelligible_rate` | Percentage over all eval rows whose scored hypothesis is empty after stripping whitespace or equals `[UNINTELLIGIBLE]` under a case-insensitive comparison. Missing provider rows and unresolved online errors are scored as empty hypotheses and count in the numerator. |
| `insertions` | Word insertion count from the WER alignment. |
| `deletions` | Word deletion count from the WER alignment. |
| `substitutions` | Word substitution count from the WER alignment. |
| `total_reference_words` | WER denominator for the target row: matched reference words plus substitutions plus deletions. |
| `missing_prediction_count` | Count of eval rows where the provider output did not include a successful prediction for that audio URI. |
| `artifacts` | Object containing durable artifact URIs for the row. |

Reports expose the evaluated row count as report metadata named
`n_eval_examples`, not as a target-table column.

## Empty Output Metrics

`empty_or_unintelligible_rate` is the metric for scored hypotheses that are
empty after stripping whitespace or equal `[UNINTELLIGIBLE]` under a
case-insensitive comparison. The scored hypothesis is either successful
provider prediction text or the empty-string fallback used when no successful
prediction exists.

Missing provider rows are scored as empty hypotheses for WER/CER so they remain
in the WER/CER denominator. They also count in
`empty_or_unintelligible_rate`. Use `missing_prediction_count` and
backend-specific metadata such as `online_error_count` to distinguish execution
gaps from successful empty model responses.

Use this column when deciding whether a target failed to answer or produced the
explicit unusable-audio token.

## Missing Predictions

Provider outputs are matched back to eval rows by audio URI. If no successful
provider prediction exists for an individual row, `gemini-sft eval` supplies
an empty hypothesis for scoring so the reference row remains in the WER/CER
denominator. If every online request fails, evaluation exits nonzero without
publishing a scoring report; this distinguishes total provider failure from a
partially successful run.

`missing_prediction_count` stays operationally separate from exact empty model
responses. A missing provider row means no successful prediction record was
returned for that audio URI. An exact empty response means the provider
successfully returned a prediction record whose stripped text was empty.

For online endpoint eval, errored rows are retried on resume. If a partially
successful eval report is generated while errors remain unresolved, those rows
count in both `missing_prediction_count` and metadata `online_error_count`.

## Artifact URI Fields

The `artifacts` object can include:

| Key | Meaning |
| --- | --- |
| `raw_output_uri` | Durable GCS prefix for raw Vertex batch output when the target used batch inference. |
| `online_predictions_uri` | Durable GCS JSONL for online endpoint prediction attempts when the target used online inference. It can include the latest errored attempt rows for diagnosis. |
| `rolling_history_index_uri` | Durable transcript-free index of causal waves and their online prediction artifacts for nonzero-context evaluation. |
| `rolling_history_audit_uri` | Durable transcript-free per-row audit of eligible, supplied, and omitted prior-prediction dependencies. |
| `normalized_manifest_uri` | Durable normalized inference manifest with source rows and successful prediction fields. |
| `summary_json_uri` | Stable GCS URI for the target run's JSON report summary. |
| `summary_markdown_uri` | Stable GCS URI for the target run's Markdown report summary. |

These fields point to durable GCS artifacts. Local files under `results/` are a
cache or mirror, not the source of truth for report reuse.

Rolling-history audits never contain transcript or reference text. Evaluation
references are unavailable to provider requests and are joined only after
inference finalizes for scoring.

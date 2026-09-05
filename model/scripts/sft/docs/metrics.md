---
type: reference
title: Gemini SFT Metric Interpretation
description: Stable interpretation of quality and execution signals in Gemini SFT reports.
tags: [gemini-sft, metrics, reports]
sources:
  - resource: ../../../src/gemini_sft/reporting.py
    title: Report schema and metric calculations
---

# Gemini SFT Metric Interpretation

[`reporting.py`](../../../src/gemini_sft/reporting.py) defines the current
serialized schema, report columns, normalization, and calculations. Generated
JSON is the machine-readable contract; this document explains how to interpret
the main concepts.

## Public Report Columns

This table is machine-checked against `reporting.REPORT_COLUMNS`; the code
remains authoritative for serialization and calculation.

| Column | Meaning |
| --- | --- |
| `target_label` | Operator-facing label for the evaluated target. |
| `model` | Publisher model, tuned endpoint, or checkpoint endpoint. |
| `wer` | Word error rate percentage after shared normalization. |
| `cer` | Character error rate percentage after shared normalization. |
| `keyword_accuracy` | Occurrence-weighted accuracy for configured dispatch keywords. |
| `empty_or_unintelligible_rate` | Percentage of scored hypotheses that are empty or explicitly unintelligible. |
| `insertions` | Word insertions in the WER alignment. |
| `deletions` | Word deletions in the WER alignment. |
| `substitutions` | Word substitutions in the WER alignment. |
| `total_reference_words` | Reference-word denominator used for WER. |
| `missing_prediction_count` | Eval rows without a successful provider prediction. |
| `artifacts` | Durable artifact URIs associated with the result. |

## Quality Metrics

Word error rate is the sum of word insertions, deletions, and substitutions
divided by reference words after the shared normalizer. Character error rate
applies the corresponding character-level comparison.

Keyword accuracy measures configured dispatch-keyword recovery. Read its exact
aggregation in current code before comparing it with a differently produced
metric.

Always compare counts and denominators alongside percentages. A lower rate on a
different row population is not necessarily an improvement.

## Empty Output

The packaged empty-or-unintelligible metric captures scored hypotheses that are
empty after normalization or equal the implementation's explicit
unintelligible token. The token comparison is case-insensitive after surrounding
whitespace is stripped. Missing predictions are represented as empty scored
hypotheses, so they count in this rate.

Keep three cases distinct:

1. a successful provider response with empty text;
2. a successful response containing the explicit unusable-audio token;
3. no successful prediction because execution failed or output was missing.

They may contribute similarly to a quality rate while requiring different
operational action. Use provider-error and missing-prediction evidence to
separate them.

## Execution Completeness

Quality metrics are interpretable only after confirming the report covers the
intended target and row population. Review:

- evaluated row count;
- missing predictions;
- unresolved provider errors;
- target and manifest identity;
- row-level normalized predictions.

The exact metadata fields and failure behavior are code-owned. A summary
artifact alone does not prove complete inference.

## Packaged And Derived Metrics

Only metrics emitted by
[`reporting.py`](../../../src/gemini_sft/reporting.py) are packaged CLI metrics.
Common downstream analyses—such as source, sample-rate, duration, word-count,
or cross-slice breakdowns—are derived from the normalized row-level manifest.

A derived report must:

- name the source manifest and prediction artifact;
- define each slice and missing-metadata policy;
- preserve the parent evaluation membership;
- reconcile row counts and denominators to the parent report;
- avoid changing predictions while slicing.

Total evaluation duration is also derived unless present in the current report
schema. Compute it from the frozen manifest and bind it to that manifest's
identity.

## Comparing Checkpoints

Use identical rows, request geometry, normalization, and slicing rules for every
checkpoint. Report execution gaps next to quality metrics, and do not rank a
checkpoint whose required evaluation lanes are incomplete.

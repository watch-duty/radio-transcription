---
type: methodology
title: Gemini SFT Evaluation Methodology
description: Stable comparison policy for Gemini SFT checkpoints and evaluation datasets.
tags: [gemini-sft, evaluation, checkpoints]
sources:
  - resource: ../../../src/gemini_sft/evaluate.py
    title: Evaluation orchestration
  - resource: ../../../src/gemini_sft/target_execution.py
    title: Provider execution and prior-context scheduling
  - resource: ../../../src/gemini_sft/reporting.py
    title: Report generation
---

# Gemini SFT Evaluation Methodology

This document owns evaluation policy. The implementation owns request shape,
scheduling, retries, report fields, and artifact paths.

## Comparison Unit

Prepare one eval run for one model target, one manifest, and one request
geometry. Use distinct run identities when any of those inputs differ.

For a tuning job:

1. inventory every checkpoint and its exact endpoint resource;
2. prepare the same evaluation lanes for every checkpoint;
3. keep manifests and request-affecting settings identical within a lane;
4. reconcile the expected checkpoint-by-lane matrix before comparing results.

Do not select a checkpoint from an incomplete matrix.

## Evaluation Lanes

Keep conceptually different requests separate:

- unmasked audio without prior context;
- masked audio without prior context;
- unmasked audio with predicted prior context, when supported by the current
  implementation.

Additional experiments get separate lanes and run identities. Do not combine
different geometries into a single aggregate that hides their behavior.

## Dataset Isolation

Evaluation audio must not occur in training. Freeze and review disjointness
before provider inference, using the strongest physical-recording identity
available for the dataset rather than filenames alone.

Record enough immutable dataset identity to reproduce the comparison, such as a
manifest generation or digest and the zero-overlap verification receipt. The
generic evaluator scores the manifest it receives; it is not a substitute for
dataset-construction ownership checks.

Use the same frozen rows and order for every checkpoint in a lane. Any exclusion
or correction creates a new dataset version.

## Predicted Prior Context

Evaluation references are scoring data, never provider input.

For positive prior context:

- history comes only from the evaluated target's finalized earlier
  predictions;
- history is causal and same-source according to the implementation's current
  eligibility rules;
- missing or unusable earlier outputs are handled by code, not replaced with
  reference transcripts;
- the current clip remains the request's current audio.

The exact schedule and omission rules live in
[`target_execution.py`](../../../src/gemini_sft/target_execution.py). Inspect
its durable audit artifacts when validating a prior-context run.

## Execution Completeness

Separate execution completeness from transcription quality. Before interpreting
WER or related metrics, verify that:

- the report belongs to the intended target and frozen manifest;
- the evaluated row count matches the lane;
- provider errors and missing predictions meet the comparison's acceptance
  threshold;
- all expected checkpoint-and-lane results exist.

A summary file alone does not prove complete inference.

## Reporting And Slices

Use the packaged report for metrics calculated by
[`reporting.py`](../../../src/gemini_sft/reporting.py). Dataset-source,
sample-rate, duration, word-count, or other breakdowns are derived analyses
unless current code explicitly publishes them.

Derived slices must:

- use the same row-level predictions as the unsliced report;
- preserve the frozen lane membership;
- state the metadata source and treatment of missing values;
- reconcile their totals to the parent evaluation.

Do not present a derived metric as a packaged CLI contract.

## Acceptance Checklist

An evaluation comparison is ready for review only when:

- training/eval physical-recording disjointness is evidenced;
- every checkpoint has every required lane;
- prior-context lanes use predictions rather than references;
- target, manifest, and request identities match the intended comparison;
- execution gaps are reported separately from successful empty outputs;
- aggregate and derived slice totals reconcile.

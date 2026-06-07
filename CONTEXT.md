# Radio Transcription Model Context

This glossary captures package and workflow terms used by the model subtree.

## Shared ASR/Model Infrastructure

Reusable Python helpers that are not tied to one provider or training workflow.
These live under `model/src/common` and include manifest parsing, scoring, GCS
helpers, audio helpers, auth helpers, and generic inference runners.

## Gemini-Specific Shared Primitives

Reusable Gemini helpers shared by notebooks and SFT. These live under
`model/src/common/gemini` and include Gemini transcription prompts, Vertex
request construction, tuning, batch inference, batch-output parsing, and Gemini
audio-SFT JSONL helpers.

## Gemini SFT Workflow

The repeatable supervised fine-tuning workflow for Gemini radio transcription.
It is exposed by the `gemini-sft` CLI from `model/src/gemini_sft` and owns run
configuration, preparation, preflight, tuning, evaluation, records, and cost
confirmation.

## SFT Run Config

An external TOML file consumed by `gemini-sft`. It provides the dataset
manifests, GCP project/bucket/location, Gemini SFT hyperparameters, and optional
inline prompt overrides. The resolved prompt text and derived paths are copied
to GCS `config.json`, so later stages do not depend on the operator's local
TOML file.

## Operator Runtime

The environment used by researchers/operators to run notebooks and CLI commands.
The recommended runtime is the lightweight ASR Docker service. Docker is an
environment wrapper; it is not the conceptual owner of Gemini SFT.

## Maintained Notebook

A notebook that is expected to keep working against packaged shared helpers.
Maintained notebooks should import from `common` or `common.gemini` through the
editable model package install.

## Canonical Manifest

The row-per-audio-segment JSONL contract used before provider-specific model
input conversion. Rows include fields such as `audio_filepath`, `text`,
`offset`, and `duration`.

## Train, Validation, And Eval Splits

Train rows become Gemini SFT training examples. Validation rows become the
Vertex tuning validation dataset. Eval rows are held out for reporting and are
converted to Vertex batch-inference requests only during `gemini-sft eval`.
`prepare` rejects train/validation and train/eval audio URI overlap.

## Gemini Model-Input JSONL

The provider-specific JSONL format submitted to Vertex Gemini supervised
tuning. It contains `systemInstruction` plus user/model `contents` turns with
audio `fileData` and transcript text.

## Preflight Report

The JSON report written before any paid tune submission. It records schema,
target-text, rough token-cap, train/validation overlap, duplicate train URI, and
GCS reachability failures. A failed report blocks tuning; the workflow expects
operators to fix source data rather than auto-filter examples.

## GCS-Authoritative Run State

The run record under `gs://<bucket>/sft/runs/<round-id>/`. This prefix is the
source of truth for `config.json`, status, manifests, Gemini input JSONL,
preflight reports, tuning status, and evaluation artifacts. Local `results/`
files are a mirror/cache only.

## Vertex Tuning Job Name

The server-side Vertex resource name returned immediately after tune
submission. It is persisted in GCS `config.json` before polling so `gemini-sft
tune` can resume polling the same paid job after a local process exit.

## Eval Artifact

Evaluation outputs that let maintainers inspect or recalculate model quality,
including local `wer_summary.{json,md}`, ledger rows, and GCS paths to raw
Vertex batch inference results.

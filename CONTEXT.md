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

## Gemini Model-Input JSONL

The provider-specific JSONL format submitted to Vertex Gemini supervised
tuning. It contains `systemInstruction` plus user/model `contents` turns with
audio `fileData` and transcript text.

## GCS-Authoritative Run State

The run record under `gs://<bucket>/sft/runs/<round-id>/`. This prefix is the
source of truth for `config.json`, status, manifests, Gemini input JSONL,
preflight reports, tuning status, and evaluation artifacts. Local `results/`
files are a mirror/cache only.

## Eval Artifact

Evaluation outputs that let maintainers inspect or recalculate model quality,
including local `wer_summary.{json,md}`, ledger rows, and GCS paths to raw
Vertex batch inference results.

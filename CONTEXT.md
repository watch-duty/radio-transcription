# Radio Transcription Context

This glossary captures package, workflow, ingestion, failure-handling, and
quarantine terms used across the repository.

## Model Context

### Shared ASR/Model Infrastructure

Reusable Python helpers that are not tied to one provider or training workflow.
These live under `model/src/common` and include manifest parsing, scoring, GCS
helpers, audio helpers, auth helpers, and generic inference runners.

### Gemini-Specific Shared Primitives

Reusable Gemini helpers shared by notebooks and SFT. These live under
`model/src/common/gemini` and include Gemini transcription prompts, Vertex
request construction, tuning, batch inference, batch-output parsing, and Gemini
audio-SFT JSONL helpers.

### Gemini SFT Workflow

The repeatable supervised fine-tuning workflow for Gemini radio transcription.
It is exposed by the `gemini-sft` CLI from `model/src/gemini_sft` and owns run
configuration, preparation, preflight, tuning, evaluation, records, and cost
confirmation.

### SFT Run Config

An external TOML file consumed by `gemini-sft`. It provides the dataset
manifests, GCP project/bucket/location, Gemini SFT hyperparameters, and optional
inline prompt overrides. The resolved prompt text and derived paths are copied
to GCS `config.json`, so later stages do not depend on the operator's local
TOML file.

### Operator Runtime

The environment used by researchers/operators to run notebooks and CLI commands.
The recommended runtime is the lightweight ASR Docker service. Docker is an
environment wrapper; it is not the conceptual owner of Gemini SFT.

### Maintained Notebook

A notebook that is expected to keep working against packaged shared helpers.
Maintained notebooks should import from `common` or `common.gemini` through the
editable model package install.

### Canonical Manifest

The row-per-audio-segment JSONL contract used before provider-specific model
input conversion. Rows include fields such as `audio_filepath`, `text`,
`offset`, and `duration`.

### Train, Validation, And Eval Splits

Train rows become Gemini SFT training examples. Validation rows become the
Vertex tuning validation dataset. Eval rows are held out for reporting and are
converted to Vertex batch-inference requests only during `gemini-sft eval`.
`prepare` rejects train/validation and train/eval audio URI overlap. Validation
and eval may point at the same manifest when the Vertex validation set is also
the reporting set.

### Gemini Model-Input JSONL

The provider-specific JSONL format submitted to Vertex Gemini supervised
tuning. It contains `systemInstruction` plus user/model `contents` turns with
audio `fileData` and transcript text.

### Preflight Report

The JSON report written before any paid tune submission. It records schema,
target-text, rough token-cap, train/validation overlap, duplicate train URI, and
GCS reachability failures. A failed report blocks tuning; the workflow expects
operators to fix source data rather than auto-filter examples.

### GCS-Authoritative Run State

The run record under `gs://<bucket>/sft/runs/<round-id>/`. This prefix is the
source of truth for `config.json`, status, manifests, Gemini input JSONL,
preflight reports, tuning status, and evaluation artifacts. Local `results/`
files are a mirror/cache only.

### Vertex Tuning Job Name

The server-side Vertex resource name returned immediately after tune
submission. It is persisted in GCS `config.json` before polling so `gemini-sft
tune` can resume polling the same paid job after a local process exit.

### Eval Artifact

Evaluation outputs that let maintainers inspect or recalculate model quality,
including local `wer_summary.{json,md}`, ledger rows, and GCS paths to raw
Vertex batch inference results.

## Ingestion Context

### Feed

A configured upstream audio source that the ingestion system may claim, poll,
stream, and process. A feed has one lifecycle status at a time.

### Leased Feed

A feed currently owned by one worker through a fencing token. A leased feed can
carry stale failure state from a previous failed processing episode.

### Captured Chunk

An audio payload emitted by a collector for runtime upload, publish, and
bookmarking. Avoid: call item, file listing, source response.

### Source Observation

A successful non-audio source check emitted by a collector when the source was
reachable but no audio payload should be processed. Avoid: empty chunk,
synthetic chunk.

### Observation Boundary

The source-specific scope used to decide whether item failures are isolated or
feed-level. For polling collectors this is usually one response page or file
listing.

### Collector-Local Failure Streak

An in-memory streak of failed poll, fetch, connection, or source operations
inside one collector task. It resets on successful source contact, even when no
audio is present.

### Feed Failure Episode

A terminal feed-level failure recorded in storage after a collector or runtime
decides the current feed cannot make progress. Consecutive feed failure
episodes drive quarantine.

### Status Reason

The current canonical abnormal-condition label for a feed. It says whether the
likely owner is the source/provider or the ingestion system.

### Quarantine Reason

The detailed, redacted diagnostic message persisted when a feed failure episode
crosses the quarantine threshold. It describes that threshold-crossing episode
for debugging; it is not the lifecycle owner label and does not summarize the
full failure budget history.

### Quarantine

A lifecycle state that makes a feed ineligible for normal claiming after too
many consecutive feed failure episodes.

### Example Dialogue

Developer: "The Calls API returned an empty `calls` list."

Domain expert: "That is a source observation, not a captured chunk. Reset the
collector-local failure streak, and clear feed failure state only if the leased
feed was dirty."

Developer: "A page had three call items and all downloads failed."

Domain expert: "That observation boundary produced a feed failure episode. If
those episodes repeat, the feed can be quarantined."

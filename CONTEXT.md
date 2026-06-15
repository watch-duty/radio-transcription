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

The unified strict train/eval input contract used before provider-specific
model input conversion. Each row is row-per-audio-segment JSONL with required
`audio_filepath`, `text`, `offset`, `duration`, `example_id`, and `segment_id`
fields. Strict `audio_filepath` values are model-ready `gs://...flac` clip
URIs, and `(example_id, segment_id)` is the logical row identity, unique within
one manifest.

Optional shallow metadata may include `split`, `lang`, `dataset`,
`source_audio`, and `audio_processing`. Strict validation through
`validate_canonical_manifest(...)` ignores unknown row fields, unknown metadata
keys, and prediction-enriched fields such as `pred_text_*`. See
`model/data/manifests/README.md` for the detailed contract.

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

### Normalized Inference Manifest

A scorer-ready eval artifact that preserves canonical manifest rows and adds
model prediction fields. It requires reference transcription text on every row;
a single row owns a single inference input; prediction records must belong to
that manifest's rows. A `pred_text_*` field is present only when a prediction
record existed for that row, and an empty string value means the prediction
record existed and contained empty text.

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

A terminal failure recorded after policy decides retry, backoff, or probing is
not expected to restore progress without operator intervention. Consecutive feed
failure episodes drive quarantine, including v1 cases where the repair is a
code, deploy, or internal system fix.

### Non-Budgeted Ingestion Failure

A visible, retryable ingestion failure that does not count toward feed
quarantine because policy expects retry, backoff, or probing to recover without
operator intervention, or because the condition is outside operator control.

### External Source Condition

A source/provider condition such as offline, unreachable, or rate-limited source
access. It remains non-budgeted because retry, backoff, probing, or upstream
recovery is the expected path.

### Operator-Actionable Failure

An ingestion failure that requires a human-initiated correction before normal
claiming should resume. The correction may target one feed, a batch of feeds,
code, deploy configuration, credentials, or another internal system.

### Quarantine-Budgeted Failure

An ingestion failure where retry, backoff, or probing is not expected to restore
progress and an operator can fix the condition. Consecutive quarantine-budgeted
failures drive quarantine.

### Feed Configuration Failure

A feed-row or source-specific feed configuration problem that prevents
ingestion from addressing the intended source. Missing source identifiers and
invalid source-specific feed paths are feed configuration failures.

### Runtime Configuration Failure

A shared deploy, environment, credential-location, transport, or source-class
configuration problem that prevents ingestion from operating correctly. It is
not specific to one feed row, even when first observed while processing one
feed.

### Pipeline-Owned Failure

A post-capture ingestion failure after source capture has succeeded or
partially succeeded. The source feed may be healthy, so v1 keeps these failures
outside the feed quarantine budget while preserving visibility for repair and
replay work.

### Post-Bookmark Publish Failure

A pipeline-owned failure where captured audio was uploaded and the feed cursor
was advanced, but the corresponding publish did not complete. It records
pipeline-gap telemetry and releases the feed through the non-budgeted retry
path in v1.

### Retryable Pipeline Failure

A post-capture ingestion failure that policy expects retry to recover without
operator intervention. It remains visible but does not count toward quarantine.

### Terminal Auth Or Access Refusal

An explicit authentication or authorization refusal that remains after
collector-local retry, token refresh, or reconnect policy has been exhausted.
It remains non-budgeted in v1 because credential/session/provider state can
recover outside feed-row action, and policy can later move the route if a
specific terminal auth family proves deterministic.

### Credential Access Failure

A failure to retrieve or access credentials from an internal credential store.
It is distinct from terminal auth or access refusal because the upstream source
has not necessarily rejected the credential.

### Source Payload Contract Failure

A failure where the source returned apparently successful data, but its shape or
encoding does not match the collector contract. It remains non-budgeted in v1
because malformed provider responses can be transient or source-side. Avoid:
ambiguous collector error, item failure.

### Ambiguous Item Failure

An item-scoped media/download/probe failure that cannot prove the whole feed or
collector contract is broken. It remains non-budgeted unless later evidence
promotes it to a more precise operator-actionable failure.

### Status Reason

The current canonical abnormal-condition label for a feed. It is visible to
operators and can be one input to routing policy, but routing requires policy
evidence and must not use status reason alone.

### Policy Evidence

Structured ownership, scope, and stage facts used with status reason to decide
whether an ingestion failure is quarantine-budgeted, non-budgeted,
pipeline-owned, or unknown.

### Routing Policy

The canonical decision for a status reason plus policy evidence combination. It
decides whether the ingestion failure is quarantine-budgeted or non-budgeted.

### Unexpected System Failure

The residual fallback for untyped bugs or missing classification evidence. It
is non-budgeted until a future change replaces it with a more precise status
reason and policy evidence.

### Quarantine Reason

The detailed diagnostic message persisted when a feed failure episode
crosses the quarantine threshold. It describes that threshold-crossing episode
for debugging; it is not the lifecycle owner label and does not summarize the
full failure budget history. It is not a stable machine-readable code and
should not drive control flow. Ingestion keeps the full useful diagnostic in
memory; storage caps it only at the database persistence boundary.

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

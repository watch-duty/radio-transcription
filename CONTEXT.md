# Reference Transcript Review

This context defines the language used for ranking existing radio transcription
reference transcripts for human review.

## Language

**Reference Transcript**:
The current transcript attached to an audio segment before human review. It may
be correct or incorrect, so avoid calling it a golden or ground-truth transcript.
_Avoid_: Golden transcription, ground truth

**Empty Reference Transcript**:
A reference transcript whose text is empty or becomes empty after ranking
normalization. It can appear in the review dataset and may still receive a
model prediction for same-source context continuity, but it is excluded from WER
calculation and Label Studio packaging, so it is not a review candidate.
_Avoid_: Empty reviewed transcript, WER-ranked row

**Review Candidate**:
A review dataset row eligible to be ranked by WER and packaged for Label Studio
review. Review candidates are the human-review queue, not the complete set of
rows that may receive model predictions for context continuity.
_Avoid_: Review dataset row when referring only to rows sent for review

**Review Batch Size**:
The explicit number of top-ranked unreviewed review candidates an operator asks
to package for Label Studio. It is an operator choice for each batch, not a
hidden default.
_Avoid_: Default package limit

**Review Batch**:
A Label Studio package selected from a complete review ranking after skipping
previously reviewed segments. Multiple batches can come from the same complete
review ranking as more reviewed transcript facts become available.
_Avoid_: New ranking run

**Review Package Summary**:
Operator-facing counts for a Label Studio package: requested review batch size,
packaged task count, and previously reviewed segments skipped while selecting
the highest-ranked unreviewed rows.
_Avoid_: Silent package filtering

**Previously Reviewed Segment**:
An audio segment that already has a reviewed transcript fact, identified by
audio segment ID. Future Label Studio packages skip these segments even if they
remain high in the WER ranking. A Label Studio `Skip` annotation is not a
reviewed transcript fact and does not make the segment previously reviewed.
_Avoid_: Low-priority candidate, skipped annotation

**Review Ranking**:
An ordering of existing audio segments from highest to lowest model-versus-reference
WER, used to choose which reference transcripts humans should review first.
_Avoid_: Dataset generation, model evaluation leaderboard

**Complete Review Ranking**:
A review ranking computed after the full review dataset has been processed for
model predictions. This is the ranking suitable for the human-review handoff.
_Avoid_: Partial ranking, smoke ranking

**Fresh Ranking Output**:
A final ranking artifact path that does not already exist when a ranking command
starts. Resume uses the prediction cache, not pre-existing ranked or excluded
outputs.
_Avoid_: Reused ranked output path

**Review Dataset**:
The combined set of canonical manifest rows used for reference transcript
review. Input manifest labels and source split labels do not create separate
review datasets or context boundaries, and split labels are not part of review
outputs.
_Avoid_: Split-specific review dataset

**Dataset Name**:
Provenance label copied from canonical rows to review outputs to help trace
where a row came from. It does not partition ranking, ADK sessions, cache
selection, or prior context.
_Avoid_: Ranking partition, context group

**Source Window Metadata**:
Provenance and ordering fields such as source window ID, offset, duration, and
row index. They help locate and audit a row. Only source group plus row index
defines same-source prior ordering for context.
_Avoid_: Review partition

**Review Task Provenance**:
Metadata carried with a Label Studio task so operators can trace it back to the
ranked artifact and source audio, without exposing the model prediction text to
the reviewer.
_Avoid_: Reviewer hint

**Source Group**:
A group of audio segments that share the same radio source identity for context
purposes. Prior context for a segment may only come from earlier segments in the
same source group.
_Avoid_: Channel when referring to the persisted ranking key

**Prior Successful Same-Source Segment**:
An earlier segment in the same source group that produced a usable model
prediction. Review ranking context may include up to 30 of these segments,
even when a prior segment is not itself a Label Studio review candidate.
_Avoid_: Last 30 rows, prior reference transcripts

**Failed Prediction**:
A model prediction attempt that remains unusable after bounded retries. Failed
predictions are recorded for audit but are not eligible as prior context.
_Avoid_: Empty context row, skipped source row

**Model Prediction Text**:
Gemini's transcript hypothesis used for WER ranking and same-source model
context. It is not shown to human reviewers in Label Studio tasks, previews,
README files, reviewed exports, or correction overlays, because reviewers should
judge the reference transcript against the audio. It may appear in ranking
audit artifacts before Label Studio packaging.
_Avoid_: Reviewer hint, suggested correction

**Empty Prediction**:
A usable model hypothesis whose transcript text is empty after empty-response
retries are exhausted. It is eligible as prior context and is not the same as
`[UNINTELLIGIBLE]`, which is an explicit model transcript token.
_Avoid_: Failed prediction, unintelligible token

**Empty Reviewed Transcript**:
A Label Studio annotation explicitly marked `Reviewed` whose submitted
transcript text is empty. It is a valid reviewed correction fact and does not
by itself mean the audio should be excluded from future inputs.
_Avoid_: Exclusion decision, skipped review

**Submitted Transcript**:
The transcript text submitted by a reviewer in Label Studio after trimming
leading and trailing whitespace. Internal spacing is preserved. Whitespace-only
submissions become empty reviewed transcripts.
_Avoid_: Raw textarea value

**Unchanged Reviewed Transcript**:
A reviewed transcript whose submitted text matches the original reference
transcript. It is still a reviewed correction fact and remains eligible for the
correction overlay.
_Avoid_: No-op, not reviewed

**Original Reference Transcript**:
The reference transcript value shown to the reviewer before editing. Reviewed
export artifacts preserve it alongside the submitted transcript for before/after
audit.
_Avoid_: Replacement transcript, model prediction

**Missing Transcript Result**:
A malformed reviewed Label Studio annotation that has no transcription control
result in the export. It is not the same as an empty reviewed transcript, which
comes from an explicit empty textarea value.
_Avoid_: Empty reviewed transcript, unchanged transcript

**Latest Reviewed Annotation**:
The most recent completed Label Studio annotation on a task whose review status
is `Reviewed`, considered across all exported tasks for the same audio segment.
It is the annotation used to produce a corrected transcript fact. `Skip`
annotations do not produce corrected transcript facts and do not suppress an
earlier reviewed annotation.
_Avoid_: Latest task annotation, skip-as-correction

**Correction Overlay**:
The latest reviewed transcript facts keyed by audio segment, shaped for
downstream consumers that want to replace existing reference transcripts.
It carries replacement transcript facts, not derived review status, action, or
exclusion policy. Before/after audit belongs in reviewed export artifacts, not
the overlay.
_Avoid_: Exclusion list, overlay action policy

**Malformed Reviewed Row**:
A parsed reviewed transcript row that is missing required identity, annotation,
or transcript fields. A correction overlay is not produced when any malformed
reviewed row is present.
_Avoid_: Partial overlay row

**Malformed Reviewed Export**:
A Label Studio export containing at least one malformed reviewed annotation or
task selected for reviewed output. Parsed reviewed rows are not produced when a
malformed reviewed export is present; operators use structured errors to fix
the export first.
_Avoid_: Partial reviewed export

**Cached Failed Prediction**:
A failed prediction record from an earlier ranking run. During a context-policy
transition it may be retried because the old failure does not represent the new
run conditions.
_Avoid_: Permanent failure

**Prediction Cache**:
The durable history of model predictions and failures used to resume review
ranking and audit how each ranked row was produced. It may contain multiple
records for the same audio segment across runs.
_Avoid_: Session memory

**Complete Prediction Cache Coverage**:
A prediction cache state where every review dataset row has an active compatible
cache record: either a successful model prediction or a recorded failed
prediction. Cache-only ranking requires this coverage because it cannot infer
missing rows.
_Avoid_: Best-effort cache ranking

**Active Prediction Cache Entry**:
The latest compatible successful cache record selected for the exact
model-ready audio object in a specific review ranking run. Compatible failed
records do not hide an earlier compatible success, but they remain part of the
cache history.
_Avoid_: Last cache row, latest compatible failure

**ADK Session**:
Run-local context state used while asking Gemini to transcribe source-group
segments. It is reconstructed from the prediction cache and is not the durable
record of review ranking progress.
_Avoid_: Durable checkpoint

**Ranking Inference Path**:
The canonical way review ranking obtains model predictions for WER scoring.
For this workflow, the ranking inference path uses ADK rather than direct
GenAI calls.
_Avoid_: Parallel direct-run path

**Review Artifact URI**:
A `gs://` URI used for shared review workflow CLI input and output artifacts.
Local paths are not accepted for shared artifacts; scripts may still use
temporary local files internally while reading from or writing to GCS. The raw
Label Studio export JSON is the exception because it is consumed only by the
export parser and is not meant for human review.
_Avoid_: Local shared review artifact path

**Review Preview**:
A human-readable queue inspection artifact, such as `preview.csv`, that helps
operators see which audio segments will be sent to review. It is not the review
surface for playback, editing, or marking review status; Label Studio is the
authoritative review UI.
_Avoid_: Standalone review UI

**Excluded Ranking Row**:
A review dataset row that is not eligible for Label Studio packaging because it
does not have a compatible successful model prediction for WER ranking or has an
empty normalized reference transcript. It is kept for audit/debug only and is
not a review candidate. Presence in `excluded.jsonl` means the row was
intentionally withheld from the Label Studio queue, not necessarily that it was
skipped during model inference.
_Avoid_: Low-priority review row

**Duplicate Exact Audio Segment**:
Two or more review dataset rows whose model-ready audio objects have the same
exact content identity, even if their `gs://` URIs differ. This can happen when
source manifests overlap, a dataset version is assembled from repeated rows, or
two URIs point to identical clipped audio. It is treated as malformed review
input even when the rows look otherwise identical, because the review dataset is
expected to contain zero duplicate exact audio segments.
_Avoid_: Duplicate URI, similar audio, duplicate source window

## Example Dialogue

Developer: "Should this current segment include context from another feed?"

Domain expert: "No. Only prior successful same-source segments are eligible, and
never prior reference transcripts."

Developer: "If a segment fails once, can the next segment use it as context?"

Domain expert: "No. Retry the segment first. If it still fails after bounded
retries, record the failure and continue without using it as prior context."

Developer: "Should an old failed prediction block a new ADK ranking run?"

Domain expert: "No. Retry old failures when the context policy changes. For
steady-state re-runs, retry same-policy failures only when the operator asks."

Developer: "If a run stops halfway through, should we reconnect to the old ADK
session?"

Domain expert: "No. Recreate run-local ADK context from the durable prediction
cache."

Developer: "If the cache contains more than one record for the same audio, do we
always use the last one?"

Domain expert: "No. Use the latest compatible successful prediction for scoring
and context. A later compatible failure does not hide it."

Developer: "Should maintainers choose between direct GenAI and ADK ranking
runners?"

Domain expert: "No. ADK is the ranking inference path; direct GenAI is not a
parallel review-ranking option."

Developer: "Can I write ranked rows locally and upload them later?"

Domain expert: "No. Review workflow CLI artifacts use `gs://` paths so the
handoff stays in one shared location."

Developer: "If a reviewer marks a task Reviewed and leaves the transcript
empty, did they skip it?"

Domain expert: "No. That is an empty reviewed transcript. It is valid reviewed
input, and downstream policy can decide how to use it later."

Developer: "If the reviewer deletes the prefilled transcript and marks the task
Reviewed, what is the corrected transcription?"

Domain expert: "Use the empty string. Explicitly reviewed empty text is a valid
corrected transcription."

Developer: "If the export has a newer Skip after an older Reviewed annotation,
which transcript should be used?"

Domain expert: "Use the latest Reviewed annotation. Skip is not a corrected
transcript fact."

Developer: "If the export includes the same audio segment in two tasks, which
review should the overlay use?"

Domain expert: "Use the latest reviewed annotation for that audio segment
across the exported tasks."

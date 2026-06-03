# Use ADK for Review-Ranking Inference

Review ranking needs same-source prior audio and model predictions to be linked
as conversational context while keeping the prediction cache as the durable
resume and audit record. We will use ADK as the review-ranking inference path
for `rank_gemini.py preflight` and `rank_gemini.py run`, remove the old direct
GenAI review-ranking helper, and keep `model[vertex]`/`common.vertex` only for
non-review batch, tuning, and notebook paths.

This keeps one canonical review-ranking path and avoids maintaining parallel
cache semantics for direct GenAI and ADK. Cache compatibility is ADK-era only:
old direct GenAI cache rows are treated as incompatible rather than migrated.

The review-ranking ADK agent is a root `LlmAgent` run through `Runner`, so it
uses root `mode="chat"` in ADK 2.1. The job still behaves as one transcription
turn per audio segment by using no tools or sub-agents and setting
`RunConfig(max_llm_calls=1)`.

Preflight uses `gemini-3.1-flash-lite` to keep smoke validation cheap. The
authoritative full run uses `gemini-3.5-flash`.

The authoritative `rank_gemini.py run` command has no row limit. Correct review
ranking depends on scoring the full review dataset, so partial inference would
produce an incomplete global ordering. Bounded smoke validation belongs to
`rank_gemini.py preflight --sample-size`, whose artifacts are not the human
review handoff. Preflight still writes ranked/excluded artifacts under a fresh
smoke prefix to validate schema and upload behavior before the full run.

Prediction cache is the resumable progress artifact; ranked JSONL, ranked CSV,
and excluded JSONL are completion artifacts. If a full run fails partway
through, the command flushes prediction-cache progress and fails without writing
partial ranked/excluded outputs.

Any command that writes ranked/excluded artifacts requires fresh output paths
before inference or cache scoring starts. Existing prediction cache is allowed
for resume/cache scoring, but existing `ranked.jsonl`, `ranked.csv`, or
`excluded.jsonl` paths cause an early failure so stale completion artifacts
cannot be mistaken for the current review handoff.

Cache-only ranking requires complete prediction cache coverage for the supplied
review pool. `rank-cache` may treat compatible failed prediction records as
coverage for excluded/audit rows, but missing compatible cache coverage causes
an early failure because the command cannot infer missing rows or produce a
trustworthy complete ranking.

Label Studio packaging takes an explicit review batch size, skips audio segments
that already have reviewed transcript facts, and then chooses the highest-ranked
remaining rows. `Skip` annotations do not count as reviewed transcript facts.
Excluded rows are never used to backfill a package. The package README reports
requested, packaged, and previously-reviewed skipped counts so operators can see
why the task count may be lower than requested.

Label Studio tasks keep provenance needed to trace back to the ranked artifact
and source audio, but they do not expose Gemini prediction text to reviewers.

Reviewed export rows preserve the original reference transcript and submitted
transcript for before/after audit. Correction overlay output remains focused on
replacement facts and carries the latest reviewed submitted transcript as
`replacement_transcript`. Reviewed transcripts are included even when unchanged
from the original reference.

Subsequent review batches reuse the same complete ranking with the latest
reviewed/correction overlay facts to skip completed audio. Rerun Gemini/ranking
only when the review dataset, prompt, model, context policy, or source audio
changes.

Inference eligibility is broader than Label Studio packaging eligibility.
Empty-reference rows are still sent to Gemini during review-ranking prediction
runs so successful predictions can preserve same-source context continuity for
later rows, but they are excluded from WER ranking and are never packaged as
Label Studio tasks. Context eligibility depends on successful prediction status,
same-source identity, and row order, not on Label Studio packaging eligibility.

Review workflow CLI artifacts are GCS-only at the command boundary. The review
pool, ranking, Label Studio package/export parser outputs, and
correction-overlay commands reject local shared-artifact paths and use `gs://`
inputs and outputs so the human handoff and downstream correction artifacts
stay in a shared durable location. The raw Label Studio export JSON input may
remain local because only `parse_label_studio_export.py` consumes it and humans
do not need to browse it before parsing.

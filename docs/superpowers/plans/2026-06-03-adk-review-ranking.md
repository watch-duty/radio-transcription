# ADK Review Ranking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace direct GenAI review-ranking inference with an ADK Gemini runner that uses up to 30 prior successful same-source audio segments and their model predictions as context.

**Architecture:** Keep the existing review-pool, prediction-cache, scoring, and Label Studio packaging flow. Make the existing `rank_gemini.py run` and `rank_gemini.py preflight` commands use an ADK backend that may process multiple source groups concurrently while preserving row order within each source group, using in-memory ADK sessions to replay compatible cached predictions as linked user/model event pairs and call Gemini through ADK for misses.

**Tech Stack:** Python, `google-adk`, ADK `Runner`, `InMemorySessionService`, Vertex-routed Gemini models, existing `common.ranking` cache/scoring utilities.

**Decision Record:** `docs/adr/0001-adk-review-ranking-inference.md`

---

## Validated Facts

- Latest `google-adk` works locally as `2.1.0` and brings a compatible `google-genai` 1.x dependency.
- `google-adk 2.1.0` conflicts with the current `model[vertex]` `google-genai>=2.3,<3` extra. The ADK path must not require the `vertex` extra.
- `model[adk]` is the review-ranking runtime extra for this plan. Keep `model[vertex]` intact for the existing direct GenAI/Vertex batch, tuning, and notebook paths.
- Gemini transcription generation settings should live in a lightweight shared module, not in the direct Vertex helper. Keep the values unchanged and have both ADK review ranking and `common.vertex` import them.
- ADK review ranking should route to Vertex using explicit full model resource names built from CLI `--project`, `--location`, and `--model`. Do not mutate process-global `GOOGLE_GENAI_USE_VERTEXAI`, `GOOGLE_CLOUD_PROJECT`, or `GOOGLE_CLOUD_LOCATION` inside the CLI.
- Use `gemini-3.1-flash-lite` for preflight and `gemini-3.5-flash` for the
  full run. Vertex docs identify Gemini 3.1 Flash-Lite with model ID
  `gemini-3.1-flash-lite`; earlier Pro preview references are superseded for
  this workflow.
- ADK sessions are run-local execution state only. The prediction cache is the durable artifact for resume, audit, scoring, and Label Studio handoff.
- `GetSessionConfig(num_recent_events=60)` returns exactly 30 user/model pairs when events alternate as user audio then model transcript.
- Installed ADK `InMemorySessionService.get_session(..., config=GetSessionConfig(num_recent_events=N))` returns a copied session with events sliced to the last `N`; retaining older events in the in-memory session does not increase the model-visible event window.
- Each segment invocation should allow exactly one LLM call through ADK. The runner must set `RunConfig(max_llm_calls=1, get_session_config=GetSessionConfig(num_recent_events=60))`.
- In ADK 2.1, a root `LlmAgent` run through `Runner` must be `mode="chat"` or leave mode unset. Construct the review-ranking root agent with `mode="chat"` and no tools; enforce one model call per segment with `RunConfig(max_llm_calls=1)`.
- Leave `include_contents` at ADK's default so the model receives relevant session history controlled by `GetSessionConfig(num_recent_events=60)`. Do not set `include_contents="none"` or manually rebuild the whole prior context inside each current message.
- Use ADK's default Gemini model path. Do not enable `Gemini(use_interactions_api=True)` because the prediction cache and ADK session already define the workflow's durable and run-local state.
- Replayed model events should use `author=<agent_name>` with `content.role="model"`; `author="model"` works but logs unknown-agent warnings.
- ADK context caching is experimental and did not report cached tokens in smoke usage metadata. Do not rely on cache savings.
- There is no strict duration policy. Use exactly up to 30 prior successful segments, regardless of total audio duration.
- Cached failed predictions from an older or incompatible context policy should not block the ADK transition run.
- Same-policy cached failed predictions should be retried only when the operator passes `--retry-errors`.
- Same-policy cached failed predictions are not valid model hypotheses. When
  `--retry-errors` is not set, they remain excluded/audit rows rather than
  being ranked with empty prediction text. Successful empty predictions are
  different and are ranked normally.
- Empty model responses are retried, but if empty output remains after `max_empty_attempts=2`, treat the empty transcript text as valid: store it as a successful empty prediction with `prediction_text=""` and `error=""`, append it to the source-group ADK session as a linked empty model transcript, and do not convert it to `[UNINTELLIGIBLE]`. Successful empty predictions are eligible as prior context.
- ADK prediction extraction uses only events where `event.is_final_response()` is true. Concatenate text parts from final-response events with spaces, strip the result, ignore non-text parts, and treat no final text as empty output. Intermediate and partial events are never cached as predictions.
- ADK replaces the direct GenAI review-ranking prediction path for both `run` and `preflight`. Do not maintain a parallel user-facing `run-adk` command.
- `rank_gemini.py run` is the authoritative ranking command and must process
  the full review dataset. Do not expose `--limit` or any row-sampling option on
  `run`, because partial inference cannot produce a correct global WER ranking.
- `rank_gemini.py preflight` is a bounded smoke command, not an authoritative
  ranking handoff. It may expose `--sample-size` to cap cost and should keep the
  cheaper `gemini-3.1-flash-lite` default. It still writes ranked/excluded
  artifacts under a fresh smoke prefix so the output schema and upload path are
  validated before the full run.
- `rank_gemini.py run` writes ranked/excluded outputs only after the full review
  dataset has finished prediction processing. If inference fails partway
  through, flush prediction-cache progress, exit nonzero, and do not write or
  truncate `ranked.jsonl`, `ranked.csv`, or `excluded.jsonl`.
- `rank_gemini.py run`, `rank-cache`, and `preflight` require fresh final output
  paths. Allow `prediction_cache.jsonl` to exist for resume/cache scoring, but
  fail before reading or mutating cache state if any requested `ranked.jsonl`,
  `ranked.csv`, or `excluded.jsonl` output already exists. This prevents stale
  completion artifacts from being mistaken for the current handoff.
- Delete the old direct GenAI review-ranking helper and tests in this change. Keep `model[vertex]` and `common.vertex` for non-review direct GenAI/Vertex batch, tuning, and notebook paths.
- Treat canonical manifests as one combined review dataset. Source manifest
  labels and source split labels must not partition ranking, cache selection,
  ADK sessions, or prior-context eligibility. Remove `split` from review-pool,
  ranked, Label Studio, reviewed export, and correction-overlay output schemas.
- Duplicate exact audio segments can happen when source manifests overlap, a
  dataset version includes repeated rows, or two model-ready GCS URIs point to
  identical clipped audio. Duplicate detection is based on exact audio content
  identity, not URI equality. Keep `build_review_pool.py` failing on duplicate
  `audio_segment_id` values and writing a duplicate report instead of silently
  deduplicating, because downstream correction overlay identity is
  `audio_segment_id`, duplicate rows distort review volume, and the review
  dataset is expected to contain zero duplicate exact audio segments. Do not add
  a harmless-duplicate exception for identical transcript/provenance rows.
- Keep `dataset_name` in review outputs as provenance only. It must not
  partition ranking, cache selection, ADK sessions, or prior-context
  eligibility.
- Keep `source_window_id`, `offset`, `duration`, and `row_index` in review
  outputs. They are provenance/order fields, not dataset partitions. Same-source
  context ordering is defined by `source_group` plus `row_index`.
- Review workflow CLI artifact paths are GCS-only. Reject non-`gs://` values
  for `build_review_pool.py` (`--manifest-jsonl`, `--review-pool-jsonl`,
  `--duplicates-jsonl`), `rank_gemini.py` (`--review-pool-jsonl`,
  `--prediction-cache-jsonl`, `--ranked-jsonl`, `--ranked-csv`,
  `--excluded-jsonl`), `package_label_studio.py` (`--ranked-jsonl`,
  optional `--correction-overlay-jsonl`, `--tasks-json`,
  `--label-config-xml`, `--readme-md`, `--preview-csv`, `--bucket-uri`),
  `parse_label_studio_export.py` (`--reviewed-jsonl`,
  `--errors-jsonl`), and `build_correction_overlay.py` (`--reviewed-jsonl`,
  `--overlay-jsonl`, `--summary-json`, `--errors-jsonl`). Local temporary files
  remain allowed only as internal implementation details while
  uploading/downloading GCS objects.
  `parse_label_studio_export.py --label-studio-export-json` may be a local path
  because it is the raw Label Studio UI export consumed only by that parser and
  is not a shared human-readable review artifact.
- `rank-cache` is ADK-era cache scoring for this workflow. Do not migrate or specially support old direct GenAI cache rows; they should be excluded as incompatible under the ADK effective prompt/context fingerprints.
- The ADK effective prompt fingerprint must include the system prompt, current user prompt, and fixed ADK linkage/wrapper text for prior audio, prior predictions, and the current segment. These wrapper constants must be importable without ADK so `rank-cache` can validate cache compatibility without installing `model[adk]`.
- The prediction cache is append-only history. Multiple rows for the same audio segment may coexist; consumers must select the latest successful row compatible with the active model, prompt fingerprint, context policy fingerprint, event cap, and context fingerprint. A later incompatible row or compatible failure row must not hide an earlier compatible success.
- Active cache entry selection for the current row must require the current row's `audio_segment_id` and `model_ready_audio_uri` to match the cache entry. Do not require current-row `source_group` or `row_index` to match beyond the active context fingerprint. Prior-context replay still requires the prior entry's `source_group` and `row_index` to match the prior row being replayed.
- ADK ranking must preserve the existing ranked/excluded output schema consumed
  by Label Studio packaging except for the intentional removal of `split`.
  Values such as fingerprints and `num_recent_events` may change, but do not
  add ADK session IDs, usage metadata, or prompt wrapper details to ranked rows.
- Review scoring and packaging helpers should tolerate additive input fields
  such as review-pool GCS metadata, but every output artifact must be projected
  onto its explicit schema. Extra input fields, including any future provenance
  additions, must not leak into ranked, excluded, Label Studio, reviewed export,
  or correction-overlay outputs.
- Unknown additive input fields are ignored by default. Validation fails for
  missing/invalid required fields, duplicate exact audio, malformed selected
  reviewed annotations, incompatible cache entries, and non-`gs://` shared
  artifact paths, not merely because an input row has extra fields.
- Required identity/provenance fields with `None` are invalid and must fail
  validation rather than being stringified to `"None"`. Empty transcript text is
  valid only where explicitly allowed: canonical/reference `text` may be `""`,
  Label Studio `submitted_transcript` may be `""`, and model
  `prediction_text` may be `""` after the empty-response policy.
- Canonical/reference `text=""` rows are valid review-pool input, but preserve
  existing Colab ranking behavior: references that are empty after scoring
  normalization are excluded from WER calculation, ranked output, and Label
  Studio packaging. Empty-reference rows with a compatible successful
  prediction use `exclusion_reason="empty_normalized_reference"`; rows without
  a compatible successful prediction may keep the existing
  `missing_or_incompatible_prediction_cache` reason. Empty reference text is
  not malformed; `text=None` is malformed.
- Empty-reference rows still receive ADK/Gemini predictions during `run` and
  `preflight` so their successful predictions can preserve same-source context
  continuity for later rows. Inference eligibility is broader than Label Studio
  packaging eligibility.
- Context eligibility depends on successful prediction status, source group,
  and row order, not Label Studio packaging eligibility. A successful
  empty-reference row remains eligible as prior context for later same-source
  rows.
- Empty-reference excluded rows keep compatible prediction/cache metadata in
  `excluded.jsonl` when available, including newly generated successful
  predictions from the current ADK run. These rows are not Label Studio review
  candidates; their presence in `excluded.jsonl` is the audit trail explaining
  why they were withheld from packaging.
- Do not trim canonical manifest string fields during review-pool construction.
  Treat them as source data; trimming URIs, source IDs, dataset names, or
  reference transcripts could hide upstream data issues or change identity.
  Only Label Studio `submitted_transcript` is normalized by trimming leading and
  trailing whitespace.
- Keep the pre-Label Studio human-readable artifact as `preview.csv` for queue
  inspection only. Do not build a separate clickable audio preview UI in this
  change; Label Studio is the authoritative surface for audio playback,
  transcript editing, and review status.
- Do not expose Gemini `prediction_text` to human reviewers in Label Studio
  tasks, `preview.csv`, README text, reviewed export rows, or correction overlay
  rows. The model prediction is used for WER ranking and ADK context only; human
  review should compare audio against the reference transcript without model
  suggestion bias.
- Keep `prediction_text` in ranking audit artifacts such as ranked JSONL/CSV
  and `excluded.jsonl` where the operator needs to inspect WER scoring, failed
  predictions, and ranking quality before Label Studio packaging.
- Keep the reference transcript in `preview.csv` as `reference_transcript` for
  queue inspection. This is not model suggestion bias because it is the existing
  transcript under review.
- Label Studio tasks and `preview.csv` keep review task provenance needed for
  audit and debugging: rank, WER metrics, audio segment ID, source window ID,
  model-ready and original audio URIs, offset, duration, source group,
  row index, dataset name, model ID, prompt/context fingerprints,
  `num_recent_events`, `context_fingerprint`, and `cache_created_at`.
- Label Studio packaging consumes ranked rows plus optional correction-overlay
  reviewed facts for already-reviewed audio. `excluded.jsonl` is audit/debug
  output and must not be packaged for human review.
- `package_label_studio.py --limit` is a required explicit review batch size.
  Select the highest-ranked rows after skipping previously reviewed audio
  segment IDs. If fewer unreviewed ranked rows are available, package fewer
  tasks and do not backfill from excluded rows.
- The Label Studio package README includes a review package summary with the
  requested batch size, packaged task count, and previously reviewed skip count.
  Keep `preview.csv` as the row-level queue preview for packaged tasks.
- Multiple Label Studio batches can be built from the same complete
  `ranked.jsonl` by passing the latest correction overlay/reviewed facts to
  skip newly reviewed audio. Rerun Gemini/ranking only when the review dataset,
  prompt, model, context policy, or source audio changes.
- `rank_gemini.py run` processes all review-pool rows so the ranked output is a
  complete global ordering. `rank_gemini.py preflight --sample-size` may process
  a bounded sample for smoke validation only; do not use preflight artifacts as
  the human-review handoff.
- A Label Studio annotation explicitly marked `Reviewed` with an empty
  submitted transcript is valid reviewed input. Do not describe it as an
  exclusion decision, and do not encode `exclude_from_future_inputs` or similar
  policy fields in Label Studio reviewed rows.
- The Label Studio transcript `TextArea` should be prefilled with the reference
  transcript, use `maxSubmissions="1"` to reduce duplicate transcription
  results, and must not set `required="true"` so reviewers can explicitly
  submit an empty corrected transcript.
- Label Studio `review_status` choices remain required. The reviewer must
  explicitly choose `Reviewed` or `Skip`; an empty transcript is valid only when
  the selected status is `Reviewed`.
- `label_studio_export` trims leading and trailing whitespace from
  `submitted_transcript`, preserves internal spacing, and stores whitespace-only
  submissions as `submitted_transcript=""`.
- Reviewed export rows preserve both `original_reference_transcript` and
  `submitted_transcript` so downstream audit can compare before and after
  without rereading ranked artifacts.
- When a Label Studio task has multiple completed annotations, the export
  parser uses the latest completed annotation whose review status is
  `Reviewed`. `Skip` annotations do not produce corrected transcript facts and
  do not suppress an earlier `Reviewed` annotation.
- `Skip` annotations do not make an audio segment previously reviewed for
  future Label Studio packaging. Only reviewed transcript facts suppress future
  packaging.
- Label Studio export parsing is all-or-nothing for reviewed output. If any
  selected reviewed annotation/task is malformed, write structured parse errors,
  write an empty reviewed JSONL, return nonzero, and do not write partial
  reviewed transcript rows. Writing the empty reviewed JSONL prevents stale
  reviewed artifacts at the same GCS path from being mistaken as current output.
- Correction overlay identity is `audio_segment_id`. If reviewed rows contain
  duplicate `audio_segment_id` values from repeated Label Studio imports or
  exports, the overlay uses the latest reviewed annotation for that audio
  segment across all rows.
- Correction overlay output is schema-simplified and policy-neutral: omit
  `overlay_status`, omit `overlay_action`, and emit `replacement_transcript`
  for the latest reviewed annotation. An empty reviewed transcript is a valid
  replacement with `replacement_transcript=""`.
- Correction overlay includes every latest reviewed transcript fact, including
  reviewed transcripts that are unchanged from the original reference.
- Correction overlay generation is all-or-nothing. If any reviewed row is
  malformed, write row-level validation errors to `--errors-jsonl` and do not
  produce a partial overlay artifact that could be mistaken for authoritative
  corrections.
- Source-group concurrency is supported with `--source-workers`. Each source
  group is still processed serially by row order, and workers share a
  single-process prediction-cache flush path.
- Prediction cache flushing remains global every N newly generated entries within a single process, plus a final flush. A prediction cache path must have only one writer process because GCS append is implemented as read-current plus rewrite-full. Do not add GCS object-generation preconditions or distributed locking in this PR.
- Prediction cache is the resumable progress artifact. Ranked JSONL, ranked CSV,
  and excluded JSONL are completion artifacts and must be written only after the
  full prediction pass succeeds.
- Ranked/excluded final outputs must be fresh for `run`, `rank-cache`, and
  `preflight`. Check for existing `--ranked-jsonl`, `--ranked-csv`, and
  `--excluded-jsonl` before starting ADK inference or cache scoring, and fail
  nonzero if any exist. Do not apply this freshness check to
  `--prediction-cache-jsonl`; it is the resume/cache input/output.
- `rank-cache` requires complete prediction cache coverage for every row in the
  supplied review pool. Each row must have either an active compatible
  successful prediction or an active compatible failed prediction record. If any
  row is missing compatible cache coverage, fail nonzero and do not write
  ranked/excluded outputs.
- All newly generated cache entries from one `run` or `preflight` invocation share a single `created_at` timestamp, matching the existing CLI behavior. Do not add a separate run ID field.
- `--request-timeout-ms` is a per-segment ADK invocation timeout, not a GenAI HTTP client timeout. On timeout, cancel the ADK async generator, recreate/replay the source-group in-memory session, and retry under the normal `max_attempts=3` retry policy.
- Retry ownership belongs to `run_source_group_predictions_adk()`, not ADK/Gemini configured retry options. Do not set ADK workflow `RetryConfig` or `Gemini(retry_options=...)` initially; use the explicit outer retry loop with session replay and bounded backoff.

## Files

- Modify: `model/pyproject.toml`
- Modify: `model/scripts/review/build_review_pool.py`
- Modify: `model/colabs/common/review.py`
- Modify: `model/colabs/common/label_studio_review.py`
- Modify: `model/colabs/common/label_studio_export.py`
- Create: `model/colabs/common/gemini_config.py`
- Modify: `model/colabs/common/prompts.py`
- Modify: `model/colabs/common/vertex.py`
- Modify: `model/colabs/common/ranking.py`
- Create: `model/colabs/common/adk_ranking.py`
- Delete: `model/colabs/common/gemini_ranking.py`
- Modify: `model/scripts/review/rank_gemini.py`
- Modify: `model/scripts/review/package_label_studio.py`
- Modify: `model/scripts/review/parse_label_studio_export.py`
- Modify: `model/scripts/review/build_correction_overlay.py`
- Test: `model/scripts/review/tests/test_build_review_pool_cli.py`
- Test: `model/colabs/common/tests/test_review.py`
- Test: `model/colabs/common/tests/test_label_studio_review.py`
- Test: `model/colabs/common/tests/test_label_studio_export.py`
- Test: `model/colabs/common/tests/test_correction_overlay.py`
- Test: `model/colabs/common/tests/test_ranking.py`
- Test: `model/colabs/common/tests/test_adk_ranking.py`
- Delete: `model/colabs/common/tests/test_gemini_ranking.py`
- Test: `model/scripts/review/tests/test_package_label_studio_cli.py`
- Test: `model/scripts/review/tests/test_parse_label_studio_export_cli.py`
- Test: `model/scripts/review/tests/test_build_correction_overlay_cli.py`
- Test: `model/scripts/review/tests/test_rank_gemini_cli.py`

## Task 1: Add ADK Runtime Dependency

- [ ] **Step 1: Add a failing dependency expectation test or import check**

Run this before editing to confirm the current environment lacks ADK:

```bash
safe-run -- uv run --project model --extra scoring python -c "import importlib.util; raise SystemExit(0 if importlib.util.find_spec('google.adk') is None else 1)"
```

Expected: command exits `0`, confirming ADK is not currently provided by the model package.

- [ ] **Step 2: Add an `adk` extra**

Modify `model/pyproject.toml`:

```toml
[project.optional-dependencies]
adk = ["google-adk>=2.1,<3"]
```

Do not add `adk` to the existing `all` extra in this change. Do not replace,
rename, or redefine the existing `vertex` extra. The existing `vertex` extra
still pins `google-genai>=2.3,<3` and remains owned by the direct GenAI/Vertex
batch, tuning, and notebook paths; combining it with ADK is not required for the
review runner.

- [ ] **Step 3: Verify ADK imports through the new extra**

Run:

```bash
safe-run -- uv run --project model --extra adk python -c "from google import adk; from google.adk.runners import Runner, RunConfig, GetSessionConfig; from google.adk.sessions import InMemorySessionService; print('adk ok')"
```

Expected: prints `adk ok`.

## Task 2: Remove Split-Specific Review Schema and CLI Terminology

- [ ] **Step 1: Update review-pool CLI tests**

Update `model/scripts/review/tests/test_build_review_pool_cli.py` to assert:

- help exposes repeated `--manifest-jsonl` inputs
- help does not expose `--train-jsonl` or `--eval-jsonl`
- the CLI accepts multiple `--manifest-jsonl` values, combines them into one
  review pool in argument order, and writes the same enriched review-pool
  schema
- manifest inputs and output artifact paths must be `gs://` URIs; local paths
  are rejected
- duplicate reports and GCS loading still work with generic manifest inputs
- review-pool rows do not include `split`
- review-pool rows keep `dataset_name`
- review-pool rows keep `source_window_id`, `offset`, `duration`, and
  `row_index`
- review-pool rows keep raw exact-audio audit metadata: `md5_hash`,
  `crc32c_hash`, `size`, `generation`, and `storage_url`
- ranked, Label Studio, reviewed export, and correction-overlay outputs do not
  carry raw GCS object metadata (`md5_hash`, `crc32c_hash`, `size`,
  `generation`, `storage_url`); they keep stable IDs, URIs, source-window
  metadata, dataset name, and transcript/review fields instead
- duplicate exact audio rows fail the builder and write `duplicates.jsonl`;
  tests should cover overlapping manifests and duplicate model-ready audio
  content at different `gs://` URIs rather than URI-only duplicate checks or
  silent deduplication; any duplicate exact audio row fails, even if reference
  transcript and provenance fields match
- duplicate reports include enough fields to diagnose all duplicate rows:
  `audio_segment_id`, `model_ready_audio_uri`, `original_audio_uri`,
  `source_window_id`, `offset`, `duration`, `source_group`, `row_index`,
  `dataset_name`, and `text`; do not include `split`
- duplicate reports do not need raw GCS object metadata such as `md5_hash`,
  `size`, or `generation`; `audio_segment_id` already captures exact content
  identity, and the report should stay focused on human-fixable manifest fields

Rename test methods and fixtures that encode split-specific manifests as the
review-pool interface. Fixture canonical rows may include `split` to mirror
source manifests, but review-pool outputs must drop it.

- [ ] **Step 2: Update review helper terminology and schema**

Update `model/colabs/common/review.py` docstrings and error text to refer to
canonical manifests or manifest labels, not split-specific manifests. Keep
`load_review_manifest_rows()` accepting a mapping of arbitrary manifest labels
so error messages can identify which input file had a malformed row.

Remove `split` from:

- required review row output fields
- duplicate report fields
- `ReviewManifestRow`
- `ReviewManifestRow.to_dict()`
- enriched review-pool rows

Do not require `split` in canonical input rows. If present, ignore it. Do not
use manifest labels or source split labels as context boundaries. The review
dataset is the combined rows across all provided manifests.

Keep `dataset_name` required in canonical input rows and emitted in review
outputs as provenance only.

Keep `source_window_id`, `offset`, `duration`, and `row_index` in review
outputs. `row_index` remains required for same-source ordering.

Reject `None` for required canonical identity/provenance fields such as
`model_ready_audio_uri`, `original_audio_uri`, `offset`, `duration`,
`source_group`, `row_index`, and `dataset_name`. Do not stringify `None` to
`"None"`. Canonical `text` may be an empty string, but `None` is malformed
unless a future source contract explicitly defines it.

Keep canonical rows with `text=""` in the review pool, but do not include empty
or normalization-empty references in WER calculation. They should be emitted to
`excluded.jsonl` and not packaged for Label Studio review. If the row has a
compatible successful prediction, use
`exclusion_reason="empty_normalized_reference"`; if it does not, keep the
existing cache-availability exclusion reason. Still send these rows to Gemini
during ADK prediction runs so successful predictions can be used as same-source
prior context for later rows.

When a compatible prediction exists for an empty-reference row, including one
generated in the current run, preserve the prediction/cache metadata in
`excluded.jsonl` for audit/debug.

Do not strip whitespace from canonical manifest string fields. Preserve source
values exactly in review-pool outputs; validation should catch missing/invalid
required fields, not silently rewrite them.

- [ ] **Step 3: Update review-pool CLI**

Update `model/scripts/review/build_review_pool.py`:

- change the module docstring and parser description to generic canonical
  manifests
- replace `--train-jsonl` and `--eval-jsonl` with repeated `--manifest-jsonl`
  (`action="append"`)
- require at least one `--manifest-jsonl`
- reject `--manifest-jsonl` values that are not `gs://` URIs
- reject `--review-pool-jsonl` and `--duplicates-jsonl` values that are not
  `gs://` URIs
- load manifests in argument order and pass them to
  `review.load_review_manifest_rows()` under generated labels such as
  `manifest-1`, `manifest-2`, ...
- preserve the current enriched review-pool output schema
  except for removing `split`

- [ ] **Step 4: Update downstream split schema consumers**

Update downstream review artifacts so `split` is not required or emitted:

- remove `split` from `common.ranking.RANKED_OUTPUT_FIELDS`
- ranked and excluded row builders tolerate extra review-pool input fields,
  including raw GCS metadata or any leftover `split`, but project outputs onto
  explicit schemas and do not emit `split` or raw GCS metadata
- tests should assert unknown additive input fields are ignored and do not leak
  into outputs
- tests should assert `None` in required identity/provenance fields fails
  validation rather than being stringified; empty string remains valid only for
  explicitly allowed transcript fields
- tests should assert canonical `text=""` rows are included in the review pool
  but excluded from WER-ranked output and Label Studio packaging; use
  `exclusion_reason="empty_normalized_reference"` when a compatible successful
  prediction exists, and allow the existing cache-availability reason when it
  does not
- tests should assert canonical string fields are preserved without trimming,
  while Label Studio `submitted_transcript` is trimmed at export-parse time
- keep `prediction_text` in ranked JSONL/CSV and `excluded.jsonl` as a ranking
  audit field
- remove `split` from Label Studio task/audit/preview fields in
  `common.label_studio_review`
- keep review task provenance in Label Studio task data and `preview.csv`: rank,
  WER metrics, audio IDs/URIs, source metadata, dataset name, model ID,
  prompt/context fingerprints, `num_recent_events`, `context_fingerprint`, and
  `cache_created_at`
- keep Gemini `prediction_text` out of Label Studio task data, preview CSV,
  README text, reviewed export rows, and correction overlay rows; retain tests
  that fail if the model hypothesis leaks into reviewer-facing or correction
  artifacts
- keep `reference_transcript` in `preview.csv`
- keep `package_label_studio.py` consuming ranked rows plus optional
  correction-overlay reviewed facts; excluded rows are audit/debug artifacts and
  are not packaged for Label Studio review
- make `package_label_studio.py --limit` required as the explicit review batch
  size; do not keep a hidden default package limit
- package the highest-ranked unreviewed rows by skipping audio segment IDs that
  already exist in the reviewed/correction overlay facts; `Skip` annotations do
  not suppress future packaging; do not backfill from excluded rows when fewer
  unreviewed ranked rows are available
- include a package summary in the generated README with requested batch size,
  packaged task count, and previously reviewed skip count; keep `preview.csv`
  as packaged task rows only
- support subsequent review batches from the same complete ranked output by
  applying the latest correction overlay/reviewed facts; do not rerun Gemini
  solely because a prior Label Studio batch completed
- remove `rank_gemini.py run --limit`; `run` must process all review-pool rows
  to produce a correct global WER ranking
- replace `preflight --limit` with `preflight --sample-size` so the bounded
  smoke-test behavior is not confused with authoritative ranking behavior
- remove the README line that assigns future-input exclusion meaning to
  `Reviewed` with an empty transcript; use no train/eval terminology and do not
  describe empty reviewed transcripts as exclusion decisions
- update Label Studio config/tests so the editable transcript `TextArea` is
  prefilled from `$reference_transcript`, includes `maxSubmissions="1"`, and
  does not set `required="true"`
- keep Label Studio `review_status` choices required in config/tests
- keep `preview.csv` as the only pre-Label Studio human-readable queue
  inspection artifact; do not add a separate audio preview page or signed-URL
  browser UI
- remove `split` from Label Studio reviewed/export parser field lists in
  `common.label_studio_export`
- keep `label_studio_export` parsing empty submitted transcript values as
  valid reviewed rows with `submitted_transcript=""`; when the reviewer
  explicitly deletes the prefilled text and marks the task `Reviewed`, use the
  empty string as the corrected transcription
- keep `label_studio_export` treating a `Reviewed` annotation with no
  transcription result as a parse error; missing transcription result is
  malformed export/config data, not unchanged text and not an empty reviewed
  transcript
- trim leading/trailing whitespace in parsed `submitted_transcript`; preserve
  internal spacing and convert whitespace-only submissions to `""`
- keep both `original_reference_transcript` and `submitted_transcript` in
  reviewed export rows for before/after audit
- keep parser selection based on the latest completed `Reviewed` annotation per
  task; `Skip` annotations are ignored for corrected transcript output and do
  not override older reviewed corrections
- keep `parse_label_studio_export.py` all-or-nothing for reviewed output:
  malformed selected reviewed annotations/tasks write structured errors,
  write an empty `--reviewed-jsonl`, return nonzero, and do not write partial
  reviewed transcript rows
- correction overlay output should naturally omit `split` through
  `label_studio_export.REVIEWED_OUTPUT_FIELDS`
- update correction overlay behavior so empty reviewed transcripts are included
  as valid replacement rows with `replacement_transcript=""`
- update correction overlay behavior so unchanged reviewed transcripts are also
  included as valid replacement rows; do not drop them as no-ops
- keep correction overlay focused on downstream replacement facts:
  `replacement_transcript` comes from the latest reviewed
  `submitted_transcript`; do not copy both before/after transcript fields into
  the overlay
- remove correction overlay `overlay_status` and `overlay_action` fields, along
  with related status/action summary fields such as `reviewed_edited`,
  `reviewed_unchanged`, `reviewed_empty`, and `overlay_actions`
- do not emit `overlay_action="exclude"` or any `exclude_from_future_inputs`
  policy field
- keep correction overlay de-duplication keyed by `audio_segment_id`; when the
  reviewed export contains duplicate audio segments, choose the latest reviewed
  annotation across all rows by completed time, updated time, annotation ID, and
  input order
- keep correction overlay validation all-or-nothing: malformed reviewed rows
  cause no consumable overlay rows to be written
- add `build_correction_overlay.py --errors-jsonl` as a required GCS output
  path. On success, write an empty errors JSONL. On validation failure, write
  structured row-level errors there, write the summary JSON, return nonzero,
  and do not write a consumable overlay JSONL.
- keep `dataset_name` in review-pool, ranked, Label Studio, reviewed export,
  and correction-overlay outputs

Update related tests and CLI fixtures:

- `model/colabs/common/tests/test_review.py`
- `model/colabs/common/tests/test_ranking.py`
- `model/colabs/common/tests/test_label_studio_review.py`
- `model/colabs/common/tests/test_label_studio_export.py`
- `model/colabs/common/tests/test_correction_overlay.py`
- `model/scripts/review/tests/test_package_label_studio_cli.py`
- `model/scripts/review/tests/test_parse_label_studio_export_cli.py`
- `model/scripts/review/tests/test_build_correction_overlay_cli.py`
- `model/scripts/review/tests/test_rank_gemini_cli.py`

- [ ] **Step 5: Enforce GCS-only artifact paths for downstream review CLIs**

Update `model/scripts/review/package_label_studio.py`,
`model/scripts/review/parse_label_studio_export.py`, and
`model/scripts/review/build_correction_overlay.py` so every shared CLI artifact
input and output is validated as a `gs://` URI before any load/write work
starts. Reject local shared-artifact paths with `argparse.ArgumentTypeError` or
a similarly clear parser error. Keep temporary local files only as internal
upload/download implementation details.

Required path validation:

- `package_label_studio.py`: `--ranked-jsonl`, optional
  `--correction-overlay-jsonl`, `--tasks-json`, `--label-config-xml`,
  `--readme-md`, `--preview-csv`, and `--bucket-uri`
- `parse_label_studio_export.py`: `--reviewed-jsonl` and `--errors-jsonl`;
  keep `--label-studio-export-json` accepting local paths and `gs://` URIs
  because it is the raw Label Studio UI export consumed only by the parser
- `build_correction_overlay.py`: `--reviewed-jsonl`, `--overlay-jsonl`,
  `--summary-json`, and `--errors-jsonl`

Update CLI tests so old local-success cases become local-rejection cases for
shared artifacts, keep local and mocked-GCS success coverage for
`--label-studio-export-json`, and keep or add mocked-GCS success coverage for
each script.

For `model/scripts/review/tests/test_build_correction_overlay_cli.py`, update
malformed-input coverage to assert `--errors-jsonl` is written with row-level
root-cause errors, `--summary-json` is still written, exit code is nonzero, and
no consumable `--overlay-jsonl` is produced.

For `model/scripts/review/tests/test_parse_label_studio_export_cli.py`, update
malformed-export coverage to assert `--errors-jsonl` is written with row-level
parse errors, `--reviewed-jsonl` is overwritten with an empty JSONL, and exit
code is nonzero.

Do not add this raw-export path exception to the generated Label Studio README;
the README should stay focused on import/review instructions, and parser CLI
help plus tests are sufficient.

- [ ] **Step 6: Run focused review schema tests**

Run:

```bash
safe-run -- env PYTHONPATH=model/colabs uv run --project model --extra scoring --with pytest python -m pytest \
  model/colabs/common/tests/test_review.py \
  model/colabs/common/tests/test_ranking.py \
  model/colabs/common/tests/test_label_studio_review.py \
  model/colabs/common/tests/test_label_studio_export.py \
  model/colabs/common/tests/test_correction_overlay.py \
  model/scripts/review/tests/test_build_review_pool_cli.py \
  model/scripts/review/tests/test_package_label_studio_cli.py \
  model/scripts/review/tests/test_parse_label_studio_export_cli.py \
  model/scripts/review/tests/test_build_correction_overlay_cli.py \
  model/scripts/review/tests/test_rank_gemini_cli.py \
  -q
```

Expected: all tests pass.

## Task 3: Change Context Policy to 30 Prior Segments

- [ ] **Step 1: Write/update ranking tests**

In `model/colabs/common/tests/test_ranking.py`, update the context-cap test so it constructs more than 30 prior successful same-source entries and expects exactly the last 30.

Expected assertions:

```python
self.assertEqual(len(context), 30)
self.assertEqual(context[0].audio_segment_id, "audio-471")
self.assertEqual(context[-1].audio_segment_id, "audio-500")
```

Also assert:

- changing the policy version invalidates the prior context-policy fingerprint
- changing an extra named prompt part changes the prompt fingerprint while the
  existing two-argument `prompt_fingerprint(system, user)` call remains valid
- cache history can contain multiple entries for the same audio segment, and the
  scorer selects the latest compatible successful entry instead of the last raw
  cache row
- a later incompatible cache entry does not hide an earlier compatible cache
  entry for the same audio segment
- a later compatible failed cache entry does not hide an earlier compatible
  successful cache entry for the same audio segment
- active cache selection rejects a cache entry whose `model_ready_audio_uri`
  differs from the current row, even when `audio_segment_id` and fingerprints
  match
- current-row cache reuse does not require matching `source_group` or
  `row_index` when the exact audio, prompt/model settings, and context
  fingerprint match
- prior-context selection and ranked output ignore source split labels when
  present in source manifests; same-source prior context can cross source
  manifest labels when source-group order says it is prior
- `context_fingerprint()` changes when a prior context entry's
  `model_ready_audio_uri` changes, even if its audio ID and prediction text are
  unchanged

- [ ] **Step 2: Update ranking constants and fingerprint policy**

In `model/colabs/common/ranking.py`, set:

```python
NUM_RECENT_EVENTS = 60
MAX_CONTEXT_ROWS = 30
CONTEXT_POLICY_VERSION = "same-source-prior-30-adk-v1"
```

Keep `context_policy_fingerprint()` including both `num_recent_events` and `max_context_rows`.

Update `context_fingerprint()` to hash ordered prior-context entries using:

- `audio_segment_id`
- `model_ready_audio_uri`
- `prediction_text`

The ADK request context includes the prior audio object and model transcript, so
cache compatibility must change when any of those values changes.

Update `prompt_fingerprint()` backward-compatibly so it can include named
prompt parts:

```python
def prompt_fingerprint(
    system_prompt: str,
    user_prompt: str,
    *,
    extra_parts: collections.abc.Mapping[str, str] | None = None,
) -> str: ...
```

The stable hash payload must include `extra_parts` when provided. Sort by key or
otherwise use a deterministic mapping so callers cannot change the fingerprint
by constructing the same parts in a different order.

Add cache-history helpers in `model/colabs/common/ranking.py`:

```python
PredictionCacheHistory = collections.abc.Mapping[
    str,
    collections.abc.Sequence[PredictionCacheEntry],
]

def build_cache_history(
    entries: collections.abc.Iterable[PredictionCacheEntry],
) -> dict[str, list[PredictionCacheEntry]]: ...

def latest_matching_cache_entry(
    entries: collections.abc.Sequence[PredictionCacheEntry],
    *,
    current_row: collections.abc.Mapping[str, object],
    model_id: str,
    prompt_fp: str,
    context_policy_fp: str,
    num_recent_events: int,
    context_fp: str,
    require_success: bool,
) -> PredictionCacheEntry | None: ...
```

`latest_matching_cache_entry(..., require_success=True)` returns the latest
compatible successful entry for the current row's exact `audio_segment_id` and
`model_ready_audio_uri`, and replaces direct dictionary lookup for scoring and
context. Compatible failed entries must not hide an earlier compatible success.
With `require_success=False`, the helper can find same-policy cached failure
rows for the ADK retry decision when no compatible success exists.
Preserve the existing `is_cache_entry_compatible()` helper as the success-only
predicate or implement it in terms of the new helper to minimize churn.

Add a cache coverage helper for `rank-cache`, such as
`validate_cache_coverage(...)`, that walks review rows in source order using
the same active context fingerprint calculation as scoring. For each row, first
look for a compatible successful prediction. If none exists, look for an active
compatible failed prediction record. Missing compatible success/failure coverage
is a command error, not an excluded ranking row, because `rank-cache` cannot
produce a trustworthy complete ranking from a spotty cache.

Update `score_ranked_rows()` to accept cache history rather than a single
entry-per-audio mapping. While walking rows in source order, it must compute the
current `context_fp` from already selected active successful entries, then select
the latest matching successful entry from that audio segment's history. Excluded
rows should include the latest matching failed entry for audit only when no
matching successful entry exists. Keep the existing excluded-row schema shape:
include existing cache metadata fields from the failed entry, such as
`prediction_text`, `model_id`, prompt/context fingerprints,
`num_recent_events`, and `cache_created_at`, but do not add ADK session details
or new failed-cache-specific fields.

- [ ] **Step 3: Run ranking tests**

Run:

```bash
safe-run -- env PYTHONPATH=model/colabs uv run --project model --extra scoring python -m pytest model/colabs/common/tests/test_ranking.py -q
```

Expected: all tests pass.

## Task 4: Add ADK Ranking Backend

- [ ] **Step 1: Extract shared Gemini generation config**

Create `model/colabs/common/gemini_config.py` with the existing values moved
unchanged from `common.vertex`:

```python
GEMINI_GENERATION_CONFIG = {"temperature": 0.0, "max_output_tokens": 512}

GEMINI_SAFETY_SETTINGS = [
    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
]
```

Update `model/colabs/common/vertex.py` to import these constants and keep the
same public names available from `common.vertex` for existing callers/tests:

```python
from common.gemini_config import GEMINI_GENERATION_CONFIG, GEMINI_SAFETY_SETTINGS
```

Do not add a Vertex-extra verification lane for this ADK implementation. Preserve
the existing `common.vertex.GEMINI_GENERATION_CONFIG` and
`common.vertex.GEMINI_SAFETY_SETTINGS` public names mechanically by importing
them from `common.gemini_config`; compile `common.vertex` in final verification.

- [ ] **Step 2: Create focused ADK helper tests**

Create `model/colabs/common/tests/test_adk_ranking.py` with tests for:

- `build_prior_user_content()` includes label text before the audio URI part.
- `build_prior_model_event()` uses `author=AGENT_NAME` and `content.role="model"`.
- `build_model_uri()` returns `projects/{project}/locations/{location}/publishers/google/models/{model_id}`.
- `build_agent()` uses `common.gemini_config` values for generation and safety
  settings.
- `build_agent()` uses root `mode="chat"` with no tools.
- `build_agent()` leaves `include_contents` at ADK's default history-inclusion behavior.
- `build_agent()` does not enable `Gemini(use_interactions_api=True)`.
- `build_agent()` does not configure ADK workflow `RetryConfig` or
  `Gemini(retry_options=...)`.
- `run_source_group_predictions_adk()` replays cache hits without calling the model.
- `run_source_group_predictions_adk()` stores successful ADK final responses as compatible `PredictionCacheEntry` rows.
- `run_source_group_predictions_adk()` extracts prediction text only from ADK final-response events, concatenating text parts with spaces and ignoring non-text parts.
- `run_source_group_predictions_adk()` records error entries and does not add failed rows to later compatible context.
- `run_source_group_predictions_adk()` retries a failed current segment before advancing to the next row.
- `run_source_group_predictions_adk()` recreates/replays the in-memory session before each retry so a failed current user-only event cannot contaminate the retry or later context.
- `run_source_group_predictions_adk()` omits a row from later context only after all retry attempts fail.
- `run_source_group_predictions_adk()` stores exhausted empty responses as successful empty predictions, not failed predictions.
- `run_source_group_predictions_adk()` adds successful empty predictions to
  later same-source context and appends them to the source-group ADK session as
  linked empty model transcripts.
- `run_source_group_predictions_adk()` uses `RunConfig(max_llm_calls=1, get_session_config=GetSessionConfig(num_recent_events=60))`.
- `run_source_group_predictions_adk()` retries incompatible cached error entries, but skips compatible same-policy error entries unless `retry_errors=True`.
- compatible cache hits append the cached current row's linked user/model pair
  into the source-group ADK session before advancing, so later rows see the
  same context as they would after a live successful model call.

Use fake runner/session objects where possible. For tests that need ADK `Content`, run them under `model[adk]`.

- [ ] **Step 3: Create `common.adk_ranking`**

Add ADK review-ranking prompt wrapper constants to `model/colabs/common/prompts.py`
so both the ADK runner and cache-only scoring can import them without requiring
`model[adk]`:

```python
GEMINI_ADK_PRIOR_AUDIO_LABEL_TEMPLATE = (
    "Prior segment {ordinal} ({audio_segment_id}). Audio:"
)
GEMINI_ADK_PRIOR_TRANSCRIPT_LABEL_TEMPLATE = (
    "Transcript for prior segment {ordinal} ({audio_segment_id}): {prediction_text}"
)
GEMINI_ADK_CURRENT_SEGMENT_INSTRUCTION = (
    "Current segment. Transcribe only this current audio. "
    "Prior segments are context only."
)
GEMINI_ADK_REVIEW_PROMPT_PARTS = {
    "prior_audio_label_template": GEMINI_ADK_PRIOR_AUDIO_LABEL_TEMPLATE,
    "prior_transcript_label_template": GEMINI_ADK_PRIOR_TRANSCRIPT_LABEL_TEMPLATE,
    "current_segment_instruction": GEMINI_ADK_CURRENT_SEGMENT_INSTRUCTION,
}
```

Then create `model/colabs/common/adk_ranking.py` with these public pieces:

```python
DEFAULT_FULL_MODEL = "gemini-3.5-flash"
DEFAULT_PREFLIGHT_MODEL = "gemini-3.1-flash-lite"
DEFAULT_AGENT_NAME = "radio_transcript_session_agent"
DEFAULT_APP_NAME = "radio_transcript_review"
DEFAULT_USER_ID = "radio_transcription_review_worker"

def build_model_uri(project: str, location: str, model_id: str) -> str: ...
def build_agent(...): ...
def build_prior_user_content(entry: ranking.PredictionCacheEntry, ordinal: int): ...
def build_prior_model_event(entry: ranking.PredictionCacheEntry, ordinal: int, *, agent_name: str = DEFAULT_AGENT_NAME): ...
def build_current_content(row: collections.abc.Mapping[str, object]): ...
async def run_source_group_predictions_adk(...): ...
```

`build_agent()` must construct an ADK `LlmAgent` with:

- `name=DEFAULT_AGENT_NAME`
- Vertex-routed Gemini model URI from `build_model_uri()`
- `instruction=prompts.GEMINI_TRANSCRIBE_SYSTEM_PROMPT.strip()`
- `generate_content_config` built from `common.gemini_config`
- `mode="chat"` because ADK 2.1 rejects a root `LlmAgent` with
  `mode="single_turn"` when run through `Runner`
- `tools=[]`
- no ADK workflow `RetryConfig`
- no `Gemini(retry_options=...)`

`build_prior_user_content()` must produce a user content object equivalent to:

```python
types.Content(
    role="user",
    parts=[
        types.Part.from_text(
            text=prompts.GEMINI_ADK_PRIOR_AUDIO_LABEL_TEMPLATE.format(
                ordinal=ordinal,
                audio_segment_id=entry.audio_segment_id,
            )
        ),
        types.Part.from_uri(
            file_uri=entry.model_ready_audio_uri,
            mime_type="audio/flac",
        ),
    ],
)
```

`build_prior_model_event()` must produce an ADK event equivalent to:

```python
Event(
    author=agent_name,
    content=types.Content(
        role="model",
        parts=[
            types.Part.from_text(
                text=prompts.GEMINI_ADK_PRIOR_TRANSCRIPT_LABEL_TEMPLATE.format(
                    ordinal=ordinal,
                    audio_segment_id=entry.audio_segment_id,
                    prediction_text=entry.prediction_text,
                )
            )
        ],
    ),
)
```

`build_current_content()` must include:

```text
Current segment. Transcribe only this current audio. Prior segments are context only.
```

before the current audio URI part.

- [ ] **Step 4: Implement source-group execution**

`run_source_group_predictions_adk()` must:

- group rows with `ranking.ordered_source_rows()`
- process rows in source order per source group
- ignore source split labels for ADK session grouping and prior-context
  eligibility when present in source manifests
- process source groups concurrently up to `--source-workers`, while preserving
  row order within each source group
- use `InMemorySessionService`
- create one session per source group, recreating it after errors
- retain all successful source-group user/model pairs in the in-memory session during the run; do not physically prune the session after each row
- never persist or reuse ADK session IDs across process runs
- reconstruct all run-local ADK session state from compatible prediction cache entries
- use `RunConfig(max_llm_calls=1, get_session_config=GetSessionConfig(num_recent_events=60))`
- use a root ADK `LlmAgent` built with `mode="chat"` and no tools
- leave ADK content inclusion at its default so session history is visible
  through `GetSessionConfig(num_recent_events=60)`
- do not enable `Gemini(use_interactions_api=True)`; ADK sessions and the
  prediction cache own state for this workflow
- use full Vertex model resource names from `build_model_uri()`; do not set or
  rely on process-global Google GenAI Vertex environment variables
- use an active prompt fingerprint computed with `prompts.GEMINI_ADK_REVIEW_PROMPT_PARTS`
- compute `context_fp` from the last 30 successful same-source compatible cache entries
- on compatible cache hit selected from cache history for the current row's exact `audio_segment_id` and `model_ready_audio_uri`, append that cached current row's linked user/model events into the source-group ADK session and skip model execution
- treat cache entries with non-empty `error` as non-context rows
- retry cached error entries automatically when their model, prompt, context policy, `num_recent_events`, or context fingerprint is incompatible with the active ADK run
- for same-policy cached error entries selected from cache history only because no compatible success exists, skip model execution by default and emit an excluded/audit row through existing scoring behavior; do not rank them with empty prediction text; retry only when `retry_errors=True`
- `retry_errors=True` applies only when no compatible successful cache entry exists for that audio segment; never retry a row that already has a compatible success solely because a later compatible failure is present
- if an audio segment has multiple cache rows, prefer the latest matching successful entry; a later incompatible entry or compatible failed entry must not force a model call when an earlier compatible success exists
- on cache miss, call `runner.run_async()` and extract the final response text
- extract text only from ADK events where `event.is_final_response()` is true,
  concatenate final-response text parts with spaces, strip the result, ignore
  non-text parts, and treat no final text as empty output
- wrap each ADK `runner.run_async(...)` consumption in
  `asyncio.timeout(request_timeout_ms / 1000)` when `request_timeout_ms` is set
- on timeout, call `await agen.aclose()` on the ADK async generator, treat the
  timeout as retryable, and recreate/replay the in-memory source-group session
  before retrying
- retry the current row before advancing when `runner.run_async()` raises, times out, or returns empty final text
- when recreating a session for retry, replay only previously accepted
  successful source-group pairs; never replay the failed current attempt's user
  message as prior context
- use `max_attempts=3` for retryable exceptions and `max_empty_attempts=2` for empty final text
- use explicit bounded backoff for retryable exceptions/timeouts, such as
  `1s`, `2s`, `4s` with small jitter; do not back off between empty-response
  retries unless they raise or time out
- when `max_empty_attempts` is exhausted, create a successful cache entry with
  `prediction_text=""` and `error=""`; do not store `[UNINTELLIGIBLE]` unless
  the model explicitly returns it
- treat successful empty predictions as context-eligible, because eligibility is
  based on success/failure rather than transcript length
- treat successful predictions for empty-reference rows as context-eligible,
  because context eligibility is independent of Label Studio packaging
  eligibility
- append successful empty predictions to the source-group ADK session as linked
  user/model pairs with empty model transcript text before advancing
- before each retry, create a fresh in-memory ADK session for the source group
  and replay all prior successful same-source pairs already accepted for that
  group, excluding the failed current attempt
- create a single error cache entry only after all retry attempts for that row are exhausted
- omit failed-after-retry entries from later context
- call `on_new_entry` for every newly generated cache entry
- use one `created_at` timestamp for all new cache entries produced by a single
  CLI invocation
- do not add usage metadata to `PredictionCacheEntry`, do not create a usage sidecar, and do not add logs beyond normal progress/errors
- log normal progress at start, per source group, and final summary only; do not log every audio row unless all retry attempts fail

- [ ] **Step 5: Run ADK helper tests**

Run:

```bash
safe-run -- env PYTHONPATH=model/colabs uv run --project model --extra scoring --extra adk --with pytest python -m pytest model/colabs/common/tests/test_adk_ranking.py -q
```

Expected: all tests pass.

## Task 5: Replace `rank_gemini.py` Prediction Commands With ADK

- [ ] **Step 1: Write CLI tests**

Update `model/scripts/review/tests/test_rank_gemini_cli.py` to assert:

- `run` calls `common.adk_ranking.run_source_group_predictions_adk()`.
- `preflight` calls the same ADK backend as `run`, with the same prompt
  fingerprint, cache-history semantics, retry behavior, and source-order rules.
- `run`, `preflight`, and `rank-cache` accept the existing artifact arguments
  and reject non-`gs://` values for `--review-pool-jsonl`,
  `--prediction-cache-jsonl`, `--ranked-jsonl`, `--ranked-csv`, and
  `--excluded-jsonl`.
- `run` accepts project/location, model, cache flush, request timeout, and retry
  options.
- `run` does not expose `--limit`, `--sample-size`, or any other row-sampling
  option; it passes all review-pool rows to ADK inference and scoring.
- `preflight --sample-size` passes at most that many review-pool rows to ADK
  inference for smoke validation only.
- `run` and `preflight` expose `--source-workers`, defaulting to 16, to process
  independent source groups concurrently.
- `preflight` keeps cheaper defaults: `gemini-3.1-flash-lite` and a small
  sample size.
- both `run` and `preflight` expose `--retry-errors`.
- `run` preserves cache flushing behavior.
- cache flushing is global every N new entries with a final flush, not deferred
  until each source group completes.
- when ADK inference raises before the full review dataset is processed, `run`
  performs the final prediction-cache flush, returns or exits nonzero, and does
  not call `_score_and_write()`.
- failed partial `run` attempts do not create, overwrite, truncate, or upload
  `ranked.jsonl`, `ranked.csv`, or `excluded.jsonl`.
- `run`, `preflight`, and `rank-cache` fail before inference or cache scoring
  when any final output path already exists: `--ranked-jsonl`, `--ranked-csv`,
  or `--excluded-jsonl`.
- `run`, `preflight`, and `rank-cache` allow `--prediction-cache-jsonl` to
  already exist and use it for resume/cache scoring.
- the CLI keeps the single-writer cache contract and does not implement GCS
  object-generation preconditions or distributed locking.
- `run` writes ranked/excluded outputs through existing `_score_and_write()`
  while preserving the output schema used by Label Studio packaging except for
  the intentional removal of `split`.
- prediction commands import ADK lazily so `rank-cache` still works without ADK installed.
- prediction commands and `rank-cache` use the same ADK effective prompt fingerprint,
  including `prompts.GEMINI_ADK_REVIEW_PROMPT_PARTS`, without requiring ADK
  imports for `rank-cache`.
- `rank_gemini.py` no longer imports or references `common.gemini_ranking`.
- cache loading preserves append-only history as `dict[str, list[PredictionCacheEntry]]`.
- `rank-cache` can rank from a cache file containing multiple rows for the same
  audio ID without allowing a later incompatible row to hide an earlier
  compatible row.
- `rank-cache` uses an earlier compatible successful row when a later compatible
  failed row exists for the same audio ID.
- `rank-cache` treats old direct GenAI cache rows as incompatible; no direct
  GenAI cache migration or compatibility shim is required.
- `rank-cache` rejects a cache entry whose `model_ready_audio_uri` differs from
  the current row, even when other compatibility fields match.
- `rank-cache` can reuse a current-row cache entry when exact audio and context
  fingerprint match, even if the cache entry's stored `source_group` or
  `row_index` differs from the current review row.
- `rank-cache` fails nonzero and writes no ranked/excluded outputs when any
  review-pool row lacks both a compatible successful cache entry and a
  compatible failed cache entry.
- `rank-cache` treats compatible failed cache entries as complete coverage for
  the row, but those rows remain excluded/audit rows rather than ranked rows.
- `--retry-errors` does not retry rows that already have a compatible
  successful cache entry, even if cache history also contains compatible
  failures for the same audio ID.

- [ ] **Step 2: Update parser commands**

In `model/scripts/review/rank_gemini.py`, keep the existing `run` and
`preflight` subcommand names, remove `--limit`, add `--sample-size` only to
`preflight`, and add `--retry-errors` to both prediction commands:

```python
run.add_argument(
    "--retry-errors",
    action="store_true",
    help="Retry same-policy cached failed predictions instead of preserving them as failures.",
)
run.set_defaults(func=_run_predicting_command)
```

Prefer a small helper such as `_add_retry_errors_arg()` so `run` and `preflight`
stay aligned.

Do not add a row-limiting argument to `run`. The full command must process the
entire review pool so the output ranking is globally correct. `preflight`
sample artifacts are smoke-test artifacts and must not be packaged for Label
Studio review.

Validate all artifact arguments as `gs://` URIs for `run`, `preflight`, and
`rank-cache`; local paths are rejected before reading or writing artifacts.

For `run`, `preflight`, and `rank-cache`, check `blob_exists` for
`--ranked-jsonl`, `--ranked-csv`, and `--excluded-jsonl` before invoking the ADK
backend or cache scoring. If any final output already exists, print a clear
error, return nonzero, and do not read/append/write the prediction cache during
that invocation. Existing `--prediction-cache-jsonl` is allowed and loaded
normally when final outputs are fresh.

For `rank-cache`, after loading the review pool and prediction cache history but
before `_score_and_write()`, validate complete cache coverage. If any row lacks
both a compatible successful entry and a compatible failed entry under the active
model/prompt/context policy, print a concise error with a count and a few audio
IDs, return nonzero, and write no ranked/excluded outputs.

- [ ] **Step 3: Replace `_run_predicting_command()` internals**

Use `asyncio.run()` inside `_run_predicting_command()` to call the ADK backend.
Keep the existing cache load, cache flush, scoring, and output writing patterns.
For `run`, pass the full `review_rows` list. For `preflight`, slice only by
`args.sample_size`.

Keep `_score_and_write()` after the ADK backend returns successfully. Do not
write partial ranked/excluded artifacts inside the inference loop. On backend
exception, the `finally` block should flush pending cache entries, then the
command should fail nonzero without writing ranked/excluded outputs.

Change `_load_cache()` to preserve cache history:

```python
def _load_cache(path: str) -> dict[str, list[ranking.PredictionCacheEntry]]:
    return ranking.build_cache_history(
        ranking.PredictionCacheEntry(**row)
        for row in _load_jsonl_with_missing_policy(path, missing_ok=True)
    )
```

Update `_run_predicting_command()`, `_score_and_write()`, and ADK runner call
sites to pass cache history, not a single-entry mapping.

Pass `args.request_timeout_ms` to the ADK backend as a per-segment invocation
timeout. Do not pass it to a GenAI client or describe it as HTTP options in
help text after the ADK migration.

Import `common.adk_ranking` lazily inside `_run_predicting_command()` so `rank-cache` still works without ADK installed.

Compute the active prompt fingerprint in both `_run_predicting_command()` and
`_score_and_write()` as:

```python
prompt_fp = ranking.prompt_fingerprint(
    prompts.GEMINI_TRANSCRIBE_SYSTEM_PROMPT,
    prompts.GEMINI_TRANSCRIBE_USER_PROMPT,
    extra_parts=prompts.GEMINI_ADK_REVIEW_PROMPT_PARTS,
)
```

Set the prediction-command default cache flush interval to `50`. ADK replay makes frequent durable checkpoints more valuable than large in-memory batches.

Remove `common.gemini_ranking` from the script-level import list and all CLI
test patches/fakes. CLI tests should patch or fake `common.adk_ranking`
instead.

- [ ] **Step 4: Delete old direct GenAI review helper**

Delete:

```text
model/colabs/common/gemini_ranking.py
model/colabs/common/tests/test_gemini_ranking.py
```

Do not delete `model/colabs/common/vertex.py` or change `model[vertex]`; those
remain owned by non-review direct GenAI/Vertex paths.

- [ ] **Step 5: Run CLI tests**

Run:

```bash
safe-run -- env PYTHONPATH=model/colabs uv run --project model --extra scoring --extra adk --with pytest python -m pytest model/scripts/review/tests/test_rank_gemini_cli.py -q
```

Expected: all tests pass.

## Task 6: Smoke Validate Against Vertex

- Do not live-run `build_review_pool.py` as part of this smoke step. The
  review-pool builder changes are covered by focused CLI/helper tests with
  mocked GCS. Vertex smoke validation starts from an existing review-pool
  artifact and validates the ADK prediction/ranking path.

- [ ] **Step 1: Run preflight with ADK**

Use a fresh output prefix and a small sample size:

```bash
safe-run -- env PYTHONPATH=model/colabs uv run --project model --extra scoring --extra adk python model/scripts/review/rank_gemini.py preflight \
  --project automatic-hawk-481415-m9 \
  --location global \
  --sample-size 5 \
  --review-pool-jsonl gs://wd-transcription-data/sft/dataset_versions/radio-transcription-sft-v20260528/reference_transcript_review/20260603T051822Z/review_pool.jsonl \
  --prediction-cache-jsonl gs://wd-transcription-data/sft/dataset_versions/radio-transcription-sft-v20260528/reference_transcript_review/ADK_SMOKE_PREFIX/prediction_cache.jsonl \
  --ranked-jsonl gs://wd-transcription-data/sft/dataset_versions/radio-transcription-sft-v20260528/reference_transcript_review/ADK_SMOKE_PREFIX/ranked.jsonl \
  --ranked-csv gs://wd-transcription-data/sft/dataset_versions/radio-transcription-sft-v20260528/reference_transcript_review/ADK_SMOKE_PREFIX/ranked.csv \
  --excluded-jsonl gs://wd-transcription-data/sft/dataset_versions/radio-transcription-sft-v20260528/reference_transcript_review/ADK_SMOKE_PREFIX/excluded.jsonl
```

Replace `ADK_SMOKE_PREFIX` with a timestamped prefix before running.

Expected: command exits `0`, produces ranked rows, and records `num_recent_events=60`.

- [ ] **Step 2: Do not partial-smoke `run`**

Do not run `rank_gemini.py run` with a hidden or temporary row limit. Once
`run` has no `--limit`, invoking it means processing the full review dataset
with `gemini-3.5-flash`. Use `preflight --sample-size` for smoke validation;
run the full command only when ready to pay for the complete ranking.

## Task 7: Final Verification

- [ ] **Step 1: Compile changed Python files**

Run:

```bash
safe-run -- python3 -m py_compile \
  model/colabs/common/review.py \
  model/colabs/common/label_studio_review.py \
  model/colabs/common/label_studio_export.py \
  model/colabs/common/correction_overlay.py \
  model/colabs/common/prompts.py \
  model/colabs/common/vertex.py \
  model/colabs/common/ranking.py \
  model/colabs/common/gemini_config.py \
  model/colabs/common/adk_ranking.py \
  model/scripts/review/build_review_pool.py \
  model/scripts/review/rank_gemini.py \
  model/scripts/review/package_label_studio.py \
  model/scripts/review/parse_label_studio_export.py \
  model/scripts/review/build_correction_overlay.py
```

Expected: no output and exit `0`.

- [ ] **Step 2: Run focused test suite**

Run:

```bash
safe-run -- env PYTHONPATH=model/colabs uv run --project model --extra scoring --extra adk --with pytest python -m pytest \
  model/colabs/common/tests/test_review.py \
  model/colabs/common/tests/test_ranking.py \
  model/colabs/common/tests/test_label_studio_review.py \
  model/colabs/common/tests/test_label_studio_export.py \
  model/colabs/common/tests/test_correction_overlay.py \
  model/colabs/common/tests/test_adk_ranking.py \
  model/scripts/review/tests/test_build_review_pool_cli.py \
  model/scripts/review/tests/test_package_label_studio_cli.py \
  model/scripts/review/tests/test_parse_label_studio_export_cli.py \
  model/scripts/review/tests/test_build_correction_overlay_cli.py \
  model/scripts/review/tests/test_rank_gemini_cli.py \
  -q
```

Expected: all tests pass.

- [ ] **Step 3: Run formatting/lint check only on changed files**

Run:

```bash
safe-run -- uv run ruff check \
  model/colabs/common/review.py \
  model/colabs/common/label_studio_review.py \
  model/colabs/common/label_studio_export.py \
  model/colabs/common/correction_overlay.py \
  model/colabs/common/prompts.py \
  model/colabs/common/vertex.py \
  model/colabs/common/ranking.py \
  model/colabs/common/gemini_config.py \
  model/colabs/common/adk_ranking.py \
  model/scripts/review/build_review_pool.py \
  model/scripts/review/rank_gemini.py \
  model/scripts/review/package_label_studio.py \
  model/scripts/review/parse_label_studio_export.py \
  model/scripts/review/build_correction_overlay.py \
  model/colabs/common/tests/test_review.py \
  model/colabs/common/tests/test_ranking.py \
  model/colabs/common/tests/test_label_studio_review.py \
  model/colabs/common/tests/test_label_studio_export.py \
  model/colabs/common/tests/test_correction_overlay.py \
  model/colabs/common/tests/test_adk_ranking.py \
  model/scripts/review/tests/test_build_review_pool_cli.py \
  model/scripts/review/tests/test_package_label_studio_cli.py \
  model/scripts/review/tests/test_parse_label_studio_export_cli.py \
  model/scripts/review/tests/test_build_correction_overlay_cli.py \
  model/scripts/review/tests/test_rank_gemini_cli.py
```

Expected: no lint errors.

## Execution Notes

- Do not use `model[vertex]` for the ADK runner unless the dependency conflict is intentionally resolved in a separate change.
- Do not redefine `model[vertex]` as an alias for ADK in this plan. Use `model[adk]` for review-ranking commands and leave existing direct Vertex flows on `model[vertex]`.
- Do not use `VertexAiSessionService` or create Agent Engines for this review job. The validated path uses `InMemorySessionService`.
- Do not set `GOOGLE_GENAI_USE_VERTEXAI`, `GOOGLE_CLOUD_PROJECT`, or
  `GOOGLE_CLOUD_LOCATION` inside the ranking CLI. Route by passing a full Vertex
  model resource name to ADK.
- Do not persist ADK session IDs. Resume by replaying successful compatible cache entries into a fresh in-memory session.
- Do not implement a duration cap. The model-visible context rule is count-based: up to 30 prior successful same-source segments.
- Do not physically prune in-memory ADK sessions during normal source-group processing. `GetSessionConfig(num_recent_events=60)` limits the events read for each model call, so older retained events only affect local memory, not model-token cost.
- Do not allow multi-call ADK agent loops for one audio segment. Use `max_llm_calls=1`.
- Do not attach tools to the review-ranking ADK agent. Use root `mode="chat"` because ADK 2.1 rejects root `mode="single_turn"` under `Runner`.
- Do not configure ADK workflow `RetryConfig` or `Gemini(retry_options=...)` for
  the first ADK implementation. Revisit Gemini `retry_options` only if smoke or
  full-run logs show repeated transient 429/5xx/network failures.
- Do not assume cached-token savings. Do not add usage sidecars or detailed token logging in this implementation.
- Keep progress logging compact: start summary, per-source-group summary, final ranked/excluded/cache-write summary, and exhausted per-audio errors only.
- Treat `--request-timeout-ms` as an ADK invocation timeout. It must cancel
  `runner.run_async()` cleanly with `agen.aclose()` and enter the same retry
  path as other retryable per-segment exceptions.
- Use a fresh GCS output prefix for the ADK run because prior `1000`-event cache rows are intentionally incompatible.
- Expose `--source-workers` for ADK review ranking. Process source groups
  concurrently up to the requested worker count, while keeping each source
  group serial by row order.
- Do not run multiple writer processes against the same `--prediction-cache-jsonl`
  path. Use distinct output prefixes for concurrent experiments.
- Do not add GCS object-generation preconditions or lock files for prediction-cache
  writes in this PR.
- Delete the older `common.gemini_ranking` module and tests as part of this implementation. Do not delete `common.vertex` or remove the `model[vertex]` extra.

## Follow-Ups

- Consider automatic worker downshift if future runs show sustained
  quota/resource-exhausted failures at high source-worker counts.

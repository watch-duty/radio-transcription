# Collector Authoring Guide

This guide explains the collector contract and the failure-classification
policy. The code is still the source of truth:

- `backend/pipeline/ingestion/models.py` defines `CapturedChunk`,
  `SourceObservation`, `CaptureResources`, `CollectorFn`, and `FeedFailure`.
- `backend/pipeline/storage/feed_store.py` defines `SourceType` and
  `FeedStatusReason`.
- `backend/pipeline/ingestion/router.py` defines the VM collector registry.
- `backend/pipeline/ingestion/settings.py` defines which source types the VM
  fleet claims through `_DEFAULT_CAPS`.
- `backend/pipeline/ingestion/main.py` enforces the registry/caps invariant at
  startup.

If this document disagrees with those files or their tests, the code and tests
win. Update this guide when a behavior change would make the guidance
misleading.

## Feed Failure Runtime Boundary

VM collectors have one job: turn a source-specific stream or polling API into
`CapturedChunk` audio values, emit `SourceObservation` for successful non-audio
source checks, or raise a typed `FeedFailure` for known feed-level failures.
The runtime owns lifecycle state, leases, GCS upload, Pub/Sub publish, progress
bookmarks, heartbeats, retries after failure, and quarantine telemetry.

Do not write feed lifecycle state from a collector. A collector should yield
valid capture events or report source-specific feed failure evidence through
`FeedFailure`.

Runtime-side `_PipelineFailure` is separate from `FeedFailure`. It represents
post-capture system failures after the collector already obtained source data,
and the runtime records those as `system_pipeline_error`.

Echo is the exception to the VM runtime shape: it runs as a synchronous Cloud
Function. It still writes the same status-reason fields through
`SyncFeedStore`, so admin-facing semantics stay consistent.

## Status Reason Policy

`feeds.status` remains lifecycle and scheduling state. `feeds.status_reason`
is a nullable, current abnormal-condition label that helps operators answer:
"is this caused by the upstream source, or by the ingestion system?" Successful
async progress, successful Echo heartbeat/progress, and manual reset clear
stale status reasons.

`quarantine_reason` is different. It preserves the short raw forensic reason
on quarantine transitions. Do not parse it for canonical ownership, and do not
replace it with `status_reason`.

Use source-owned reasons when the source or its provider cannot currently
supply usable audio for this feed:

| Reason | Use when |
|--------|----------|
| `source_offline` | The configured upstream feed exists as a concept, but the direct ingestion endpoint currently has no audio/feed available. |
| `source_unreachable` | The source/provider endpoint is persistently unavailable, failing, or unreachable after collector-owned retry policy. |
| `source_rate_limited` | The source/provider is refusing requests due to rate limiting after collector-owned backoff/retry policy. |

Use system-owned reasons when the ingestion system needs action or code/configuration is
the likely owner:

| Reason | Use when |
|--------|----------|
| `system_authentication_failed` | Configured credentials, tokens, or partner auth are rejected. |
| `system_configuration_invalid` | The feed row is missing or has an invalid source-specific identifier, URL, or required configuration. |
| `system_collector_error` | The collector cannot turn apparently available source data into a chunk, or all item failures are mixed/ambiguous. |
| `system_pipeline_error` | Runtime or Echo post-capture processing fails after source data was obtained, such as GCS upload, Pub/Sub publish, staging, duration probing, or heartbeat writes. |
| `system_unexpected_error` | Defensive fallback for bugs or untyped exceptions that should become typed in a future collector fix. |

## Observation Boundaries

Classify only from evidence inside the ingestion path. Direct stream/API
responses, ffmpeg stderr for the stream endpoint, a same-endpoint probe, a
poll page, or a persistent connection failure streak are valid evidence.
Public catalog pages or secondary control-plane APIs can be useful for manual
debugging, but they should not override direct ingestion observations.

Per-item failures are usually not feed-level failures. Skip isolated 404s,
corrupt files, and one-off download failures when other eligible items in the
same observation boundary succeed. Promote to a feed-level failure only when
every eligible attempted item in that boundary fails:

- If all failures have the same canonical reason, promote that reason.
- If all attempted items failed but reasons are mixed, promote
  `system_collector_error` with the raw reason `mixed_item_failures`.
- If no eligible items were attempted, or at least one item succeeded, do not
  record a feed failure.

The helper for this policy is `ItemBatchOutcome`.

## Successful Empty Polls

Polling collectors distinguish poll/fetch success from audio production:

- If the poll/fetch fails, count it as a collector-local failure.
- If the poll/fetch succeeds and the response has no source items to process,
  reset the collector-local failure streak and yield `SourceObservation`.
- If the poll/fetch succeeds and the response has at least one source item,
  keep the existing item handling behavior for that source.

`SourceObservation` is not an audio chunk. The runtime must not upload, publish,
or count it as audio progress. It may clear stale persisted failure state when
the leased feed is dirty (`failure_count > 0` or `status_reason IS NOT NULL`).

For Broadcastify Calls, the source item is a call entry in the API `calls`
array. A missing or non-list `calls` field is treated as an empty page under the
collector's current extraction semantics. A missing `lastPos` is still a
successful observation but does not advance a resume cursor.

For Fire Notifications, yield `SourceObservation` only when the poll succeeds
and `files == []`. Non-empty file lists continue through `_process_file_list`.

## Failure Classification Model

`FailureClassification` is neutral terminal evidence: a canonical
`FeedStatusReason` plus a bounded reason tag. It is not item-scoped or
feed-scoped by itself.

`ItemFailure` applies item scope to a `FailureClassification`. Use it when an
individual object, call, file, or media URL fails inside a collector-owned
batch. `ItemBatchOutcome` owns the "all attempted items failed" promotion
rule.

`FeedFailure` applies feed scope. Raise it only after the collector has enough
source-specific evidence to report the current feed-level condition to the
runtime.

Shared failure classifiers classify evidence only. Collectors still own:

- retry and backoff policy;
- same-endpoint probes;
- item versus feed escalation;
- final reason-prefix selection for the endpoint or stage.

## Endpoint/Stage Policy

Use endpoint/stage-specific reason prefixes so on-call output says where the
evidence came from without carrying high-cardinality data:

| Reason pattern | Use when |
|----------------|----------|
| `item_http_<status>` | A discrete downloaded item, media file, call recording, or object fails with a terminal HTTP status. |
| `item_download_failed` | A discrete item download exhausts retries without terminal HTTP status evidence, such as repeated connection drops or timeouts. |
| `calls_api_http_<status>` | Broadcastify Calls API or metadata endpoint status is terminal after its retry policy. |
| `fn_api_http_<status>` | Fire Notifications poll/list endpoint status is terminal after its retry policy. |
| `stream_http_<status>` | A direct stream endpoint or same-endpoint probe returns a terminal HTTP status. |
| `ffmpeg_exit_<n>` | ffmpeg exits non-zero without stronger HTTP/probe evidence. |
| `ffmpeg_signal_<n>` | ffmpeg is terminated by POSIX signal `n`. |
| `capture_timeout` | Stream capture exceeds the collector-owned read timeout. |
| `mixed_item_failures` | Every attempted item failed, but item failures have mixed canonical reasons. |

The shared HTTP and ffmpeg classifiers are deliberately conservative. When an
endpoint has source-specific semantics, define a local policy near the
collector code. For example, Icecast stream `404` is `source_offline`, while a
poll endpoint `404` may be invalid configuration and an item URL `404` may be
only one stale object.

Do not duplicate exact HTTP policy tables in this guide. The `HTTPStatusPolicy`
instances in code and their tests are the source of truth; this document should
explain why policies are scoped by endpoint/stage, not restate every mapping.

Reason strings must stay short, bounded, and safe for operator surfaces. Do not
include URLs, ffmpeg stderr blobs, stack traces, tokens, object IDs, timestamps,
request bodies, signed URLs, feed IDs, call IDs, or secrets in `reason`.

## Adding a VM Collector

1. Add the source type if it is new:
   - add a `SourceType` enum member;
   - add seed data in `terraform/modules/alloydb/sql/ingestion/006_seed_source_types.sql`;
   - add a `_DEFAULT_CAPS` entry if VM workers should claim it;
   - add a `_COLLECTORS` entry in `router.py`;
   - update topic routing if the source is continuous instead of segmented.
2. Implement the `CollectorFn` signature from `models.py`.
3. Use `CaptureResources.http_session` for async HTTP. Do not create hidden
   long-lived sessions per feed unless the source-specific transport requires
   it and the collector owns its cleanup.
4. Generate a stable `session_id` at the source's natural continuity boundary:
   stream connection, websocket connection, polling invocation, or source file.
5. Fill `CapturedChunk.receipt_time` when the source exposes a useful arrival
   time, and fill `resume_position` only when the source has a cursor that is
   better than `chunk_end_time`.
6. Retry and back off inside the collector for source-owned transport issues.
   Raise `FeedFailure` only after the source-specific policy says the
   feed-level observation is persistent or systemic.
7. Use `missing_source_feed_id_failure`, `collector_failure`, and
   `ItemBatchOutcome` instead of open-coded exception strings and counters.
8. Add focused tests beside the collector. Tests should cover chunk success,
   each feed-level `FeedStatusReason` mapping, skip/non-failure paths,
   shutdown behavior, and item-failure aggregation if the collector downloads
   per-item files.
9. Update router/settings tests if a new source type changes the registry,
   caps, or topic-routing behavior.

For Echo-like synchronous ingestion, do not register a VM collector. Keep its
classification in the Cloud Function path and write reasons through
`SyncFeedStore`.

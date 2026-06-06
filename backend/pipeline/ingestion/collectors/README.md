# Collector Authoring Guide

This guide explains the collector contract and the failure-classification
policy. The code is still the source of truth:

- `backend/pipeline/ingestion/models.py` defines `CapturedChunk`,
  `CaptureResources`, `CollectorFn`, and `FeedFailure`.
- `backend/pipeline/storage/feed_store.py` defines `SourceType` and
  `FeedStatusReason`.
- `backend/pipeline/ingestion/source_runtime_specs.py` defines data-only
  source metadata: VM claimability, default caps, URL-base env/default, and
  topic kind.
- `backend/pipeline/ingestion/router.py` defines the VM collector registry.
- `backend/pipeline/ingestion/settings.py` derives VM claim caps from
  `SourceRuntimeSpec`.
- `backend/pipeline/ingestion/main.py` enforces the registry/caps invariant at
  startup.

If this document disagrees with those files or their tests, the code and tests
win. Update this guide when a behavior change would make the guidance
misleading.

## Feed Failure Runtime Boundary

VM collectors have one job: turn a source-specific stream or polling API into
`CapturedChunk` values, or raise a typed `FeedFailure` for known
feed-level failures. The runtime owns lifecycle state, leases, GCS upload,
Pub/Sub publish, progress bookmarks, heartbeats, retries after failure, and
quarantine telemetry.

Do not write feed lifecycle state from a collector. A collector should either
yield valid chunks or report source-specific feed failure evidence through
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

## Request Attempts and Collector Boundaries

Name retry-budget constants as `*_MAX_ATTEMPTS`. The value means total
attempts, including the first request, not "retries after the first request".
For the current aiohttp collectors, Broadcastify Calls list/API requests, Fire
Notifications poll/list requests, and discrete item media downloads use three
total attempts unless the code documents a source-specific exception. Shared
aiohttp helpers own retry mechanics for those paths; source-specific
collectors still supply endpoint policy, payload validation, and item/feed
escalation.

List/poll endpoints are feed-scoped after their request retry budget is
exhausted. They should raise typed `FeedFailure` directly, not return
`ItemFailure`, and not keep local consecutive-failure wrappers. Broadcastify
Calls and Fire Notifications follow this model for their API/list endpoints.
Broadcastify Calls refreshes the shared JWT once on a terminal auth response
and then retries the Calls API request once with the refreshed token; a second
auth failure is terminal.

Item media downloads are item-scoped. Broadcastify Calls call media, Fire
Notifications MP3 objects, and OpenMHz M4A objects all return `ItemFailure`
for classified terminal item failures and use `ItemBatchOutcome` (or OpenMHz's
continuous item-failure window) to decide whether the feed should be blamed.
Repeated item transport exceptions without terminal HTTP status evidence use
the bounded reason `item_download_failed`. Empty `200 OK` item bodies are
non-failure skips unless the collector code explicitly validates otherwise.

Async aiohttp collectors should reuse `CaptureResources.http_session`; the
runtime owns session lifecycle and shutdown. OpenMHz is the current
source-specific exception because its websocket/source transport and Wasabi
access use `curl_cffi`; it owns and closes that session locally and does not
use the aiohttp helper layer.

`EVENT_TYPE_CALL_DOWNLOAD_FAILED` is item-download-only. Do not emit it for
list/poll API failures, JWT failures, websocket reconnect failures, or
post-capture runtime failures.

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

The shared HTTP and ffmpeg classifiers are deliberately conservative. The
default HTTP policy treats unmapped 4xx statuses as `system_collector_error`
because item URLs, API endpoints, and streams use different 4xx semantics.
When an endpoint has source-specific semantics, define a local policy near the
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
   - add a `SourceRuntimeSpec` entry with claimability, cap, URL metadata, and
     topic kind;
   - add a `_COLLECTORS` entry in `router.py` if VM workers should claim it.
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
   `ItemBatchOutcome` instead of open-coded exception strings and item-batch
   counters.
8. Add focused tests beside the collector. Tests should cover chunk success,
   each feed-level `FeedStatusReason` mapping, skip/non-failure paths,
   shutdown behavior, and item-failure aggregation if the collector downloads
   per-item files.
9. Update source-runtime, router, and settings tests if a new source type
   changes VM claimability, caps, URL metadata, or topic-routing behavior.

For Echo-like synchronous ingestion, do not register a VM collector. Keep its
classification in the Cloud Function path and write reasons through
`SyncFeedStore`.

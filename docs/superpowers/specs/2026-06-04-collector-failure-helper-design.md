# Collector Failure Helper Design

## Context

PR #570 adds typed failure classification to OpenMHz and Fire Notifications.
The review comments identify duplicated helper code and unclear naming:

- Fire Notifications maps HTTP status codes separately for polling and audio
  downloads.
- Broadcastify Calls, OpenMHz, and Fire Notifications each download discrete
  audio items and need the same item-download result shape.
- The helper name `normalize_download_result` collides with pipeline audio
  normalization terminology.

Broadcastify Calls already has related compatibility helpers from the preceding
collector-classification work, so this design covers all affected item-based
collectors. Broadcastify Feeds is routed through the Icecast stream collector;
it has stream endpoint failures, not per-item download failures.

## Problem

The real maintenance risk is semantic drift in failure classification policy.
Collectors should not independently decide how the same item-download status
code maps to `FeedStatusReason`, because those decisions are operator-facing.

At the same time, not every HTTP status response has the same meaning. A Fire
Notifications poll `404` can indicate invalid source configuration. An item
download `404` can be a missing object, object race, or unavailable recording
inside an otherwise healthy feed. The shared code must preserve that evidence
boundary instead of blindly reusing a poll helper for item downloads.

## Goals

- Centralize repeated result and failure-classification primitives.
- Share item-download failure handling where behavior is common.
- Keep source API, poll, and stream endpoint status mapping local to each
  collector.
- Preserve collector-specific retry loops and lifecycle behavior.
- Keep isolated per-item failures from becoming feed-level failures unless
  `ItemBatchOutcome` promotes them at an observation boundary or item failure
  window.
- Align helper naming away from `normalize` where the code is not audio
  normalization.

## Non-Goals

- Do not introduce a collector base class or lifecycle framework.
- Do not change feed lifecycle policy.
- Do not change OpenMHz websocket reconnect behavior.
- Do not change Fire Notifications file-list polling behavior.
- Do not make Broadcastify API fetch payloads or constructed chunks use the
  audio-specific `ItemDownloadResult`.
- Do not add a shared API, poll, or stream endpoint status classifier.

## Design

Add shared primitives to
`backend/pipeline/ingestion/collectors/failure_classification.py`:

- `ItemDownloadResult`: frozen dataclass with `audio_bytes: bytes | None` and
  `failure: ItemFailure | None`.
- `standardize_item_download_result(result: ItemDownloadResult | bytes | None)
  -> ItemDownloadResult`: adapts legacy test doubles and optional download
  results into the typed result wrapper.
- `item_download_http_failure(status: int, *, reason_prefix: str =
  "item_http") -> ItemFailure`: maps terminal item-download HTTP responses.
- `raise_item_failure(failure: ItemFailure) -> NoReturn`: raises a typed
  `CollectorFailure` from an item failure.

The item-download helper encodes the shared discrete-audio-item context:

- `item_download_http_failure(403)` returns
  `system_authentication_failed` with `item_http_403`.
- `item_download_http_failure(429)` returns `source_rate_limited` with
  `item_http_429`.
- `item_download_http_failure(404)` returns `source_unreachable` with the
  bounded raw reason `item_http_404`.
- `item_download_http_failure(503)` returns `source_unreachable` with the
  bounded raw reason `item_http_503`.

Network errors, shutdown-interrupted downloads, and retry exhaustion should keep
using collector-local `item_download_failed` results because there is no
terminal HTTP status to preserve.

Broadcastify Calls, OpenMHz, and Fire Notifications should import
`ItemDownloadResult`, `standardize_item_download_result`,
`item_download_http_failure`, and `raise_item_failure` for discrete audio item
downloads.

`ItemBatchOutcome` should continue to cover both supported per-item promotion
contexts:

- **Observation boundary**: a natural source batch, such as one Broadcastify
  Calls API page or one Fire Notifications file-list poll.
- **Item failure window**: a collector-defined consecutive eligible item-failure
  streak when the source has no natural batch, such as OpenMHz call downloads
  since the last successful yielded chunk.

Broadcastify Feeds/Icecast should not use `ItemDownloadResult` or
`ItemBatchOutcome`; its failures are stream endpoint failures classified from
ffmpeg stderr or same-URL probe evidence.

Fire Notifications should keep poll status classification local, but rename
`_poll_status_failure` to `_classify_poll_status_failure` for consistency with
the source-specific classifier naming used by other collectors.

Broadcastify Calls should keep API fetch classification local. Broadcastify
Feeds/Icecast should keep stream endpoint classification local. These source
endpoint mappings encode source contracts and should not be folded into the
item-download helper.

Broadcastify Calls should rename compatibility helpers from
`_normalize_fetch_result` and `_normalize_call_chunk_result` to
`_standardize_fetch_result` and `_standardize_call_chunk_result`. Those result
types carry API payloads and constructed chunks, so they should remain
collector-local.

## Testing

Add shared helper tests in `test_failure_classification.py` for:

- `standardize_item_download_result` accepts `ItemDownloadResult`, `bytes`, and
  `None`.
- item-download status mapping for 403, 429, 404, and 503.
- `raise_item_failure` raises `CollectorFailure` with the same status reason
  and raw reason.

Update collector tests to import the shared `ItemDownloadResult` where they
stub item-download results. Existing tests should continue to cover:

- Fire Notifications poll `404` remains configuration-invalid.
- Fire Notifications item-download `404` remains an item failure.
- Broadcastify Calls API fetch classification remains source-specific.
- OpenMHz item failures still promote only after the existing threshold.
- Successful item processing still prevents or resets feed-level promotion.
- Broadcastify Feeds/Icecast continues to classify stream endpoint failures
  separately and does not emit `call_download_failed`.

## Maintenance Impact

This keeps the shared layer small. Future collectors can reuse the result and
classification primitives without inheriting a lifecycle abstraction that does
not fit their source. Item-download result handling and classification become
easier to audit because the common item-download shape and mapping are tested
directly, while source endpoint contracts remain visible in each collector.

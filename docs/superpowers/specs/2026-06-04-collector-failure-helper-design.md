# Collector Failure Helper Design

## Context

PR #570 adds typed failure classification to OpenMHz and Fire Notifications.
The review comments identify duplicated helper code and unclear naming:

- Fire Notifications maps HTTP status codes separately for polling and audio
  downloads.
- OpenMHz and Fire Notifications each define the same download result wrapper
  and compatibility adapter.
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
- Make API/poll failures and item-download failures explicit contexts.
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
- Do not make Broadcastify result payloads use the audio-specific
  `DownloadResult`.

## Design

Add shared primitives to
`backend/pipeline/ingestion/collectors/failure_classification.py`:

- `DownloadResult`: frozen dataclass with `audio_bytes: bytes | None` and
  `failure: ItemFailure | None`.
- `standardize_download_result(result: DownloadResult | bytes | None)
  -> DownloadResult`: adapts legacy test doubles and optional download results
  into the typed result wrapper.
- `item_download_http_failure(status: int, *, reason_prefix: str =
  "item_http", fallback_reason: str = "item_download_failed") -> ItemFailure`:
  maps item download HTTP responses.
- `api_http_failure(status: int, *, reason_prefix: str,
  classify_4xx_as_configuration: bool = True) -> ItemFailure`: maps source API,
  poll, or fetch endpoint responses.
- `raise_item_failure(failure: ItemFailure) -> NoReturn`: raises a typed
  `CollectorFailure` from an item failure.

The HTTP helpers encode separate evidence contexts:

- `item_download_http_failure(403)` returns
  `system_authentication_failed` with `item_http_403`.
- `item_download_http_failure(429)` returns `source_rate_limited` with
  `item_http_429`.
- `item_download_http_failure(404)` returns `source_unreachable` with the
  fallback item-download reason.
- `api_http_failure(404, reason_prefix="fn_api_http")` returns
  `system_configuration_invalid` with `fn_api_http_404`.
- `api_http_failure(503, reason_prefix="fn_api_http")` returns
  `source_unreachable` with `fn_api_http_503`.

OpenMHz and Fire Notifications should import `DownloadResult`,
`standardize_download_result`, `item_download_http_failure`, and
`raise_item_failure`.

`ItemBatchOutcome` should continue to cover both supported per-item promotion
contexts:

- **Observation boundary**: a natural source batch, such as one Broadcastify
  Calls API page or one Fire Notifications file-list poll.
- **Item failure window**: a collector-defined consecutive eligible item-failure
  streak when the source has no natural batch, such as OpenMHz call downloads
  since the last successful yielded chunk.

Broadcastify Feeds/Icecast should not use `DownloadResult` or
`ItemBatchOutcome`; its failures are stream endpoint failures classified from
ffmpeg stderr or same-URL probe evidence.

Fire Notifications should delete `_poll_status_failure` and call
`api_http_failure(status, reason_prefix="fn_api_http")` directly at the poll
failure site.

Broadcastify Calls should rename compatibility helpers from
`_normalize_fetch_result` and `_normalize_call_chunk_result` to
`_standardize_fetch_result` and `_standardize_call_chunk_result`. Those result
types carry API payloads and chunks, so they should remain collector-local.

## Testing

Add shared helper tests in `test_failure_classification.py` for:

- `standardize_download_result` accepts `DownloadResult`, `bytes`, and `None`.
- item-download status mapping for 403, 429, 404, and 503.
- API/poll status mapping for 403, 429, 404, and 503.
- `raise_item_failure` raises `CollectorFailure` with the same status reason
  and raw reason.

Update collector tests to import the shared `DownloadResult` where they stub
download results. Existing tests should continue to cover:

- Fire Notifications poll `404` remains configuration-invalid.
- Fire Notifications item-download `404` remains an item failure.
- OpenMHz item failures still promote only after the existing threshold.
- Successful item processing still prevents or resets feed-level promotion.
- Broadcastify Feeds/Icecast continues to classify stream endpoint failures
  separately and does not emit `call_download_failed`.

## Maintenance Impact

This keeps the shared layer small. Future collectors can reuse the result and
classification primitives without inheriting a lifecycle abstraction that does
not fit their source. Classification policy becomes easier to audit because the
context-sensitive mappings live in one module and are tested directly.

# Failure Classifier Deep Research Prompt

Use this prompt in a fresh Gemini, ChatGPT, or Claude conversation. It includes
the codebase context needed for deep research without relying on prior
conversation history.

```text
I need deep technical research and design advice for failure/error classification in a Python audio ingestion pipeline. Assume you have zero prior knowledge of my codebase. I will provide all relevant context below.

## Product/Operational Context

We run an ingestion pipeline that captures public-safety radio audio from multiple upstream sources. Each source has its own collector. A collector turns an upstream stream/API/file list into audio chunks for the runtime.

Current source collectors include:

1. Broadcastify Calls
   - Polls a Broadcastify Calls API for discrete call metadata.
   - Each call has an audio URL.
   - Downloads each call audio item.
   - API fetch failures and per-item download failures have different meanings.

2. Broadcastify/Icecast stream feeds
   - Uses ffmpeg to capture/segment continuous stream audio.
   - ffmpeg failures may include HTTP errors in stderr, process exit codes, or signal exits.
   - Sometimes we probe the same stream URL after ambiguous ffmpeg failure.

3. OpenMHz
   - Receives discrete call events, then downloads call audio from media URLs.
   - Per-item audio download failure should not necessarily mean feed failure.

4. Fire Notifications
   - Polls an authenticated HTTP endpoint for available MP3 files.
   - Downloads individual MP3 files.
   - Poll endpoint HTTP failure and item download HTTP failure can have different ownership/meaning.

The runtime owns:
- leasing feeds to workers
- GCS upload
- Pub/Sub publish
- progress bookmarks
- heartbeat
- failure counting
- quarantine

Collectors own:
- source connections
- source-specific retries/backoff
- source-specific failure classification
- yielding valid audio chunks
- raising typed feed-level failures only when the collector has enough evidence

## Current Domain Model

The pipeline distinguishes three related concepts:

### `CollectorFailure`

A feed-level exception raised by a collector when the whole feed/source should be marked as failing or quarantined by the runtime.

It has:

```python
class CollectorFailure(Exception):
    status_reason: FeedStatusReason
    reason: str
```

- `status_reason` is a bounded canonical operator-facing category.
- `reason` is a short raw reason/detail string for debugging and quarantine records.
- `reason` must stay low-ish cardinality. It should not contain URLs, secrets, stack traces, stderr blobs, or high-cardinality data.

### `ItemFailure`

A failure for one discrete item, such as one call audio file or one MP3 file.

```python
@dataclass(frozen=True)
class ItemFailure:
    status_reason: FeedStatusReason
    reason: str
```

An isolated item failure should usually be skipped/logged, not raised as a feed-level failure. It can become feed-level only when all attempted items in one observation boundary fail.

### `ItemBatchOutcome`

Tracks item attempts and failures for a source observation boundary, such as one API page or one file-list poll.

Current intended policy:
- If no eligible items were attempted, do not record feed failure.
- If at least one item succeeds, do not record feed failure.
- If all attempted items fail with the same canonical `FeedStatusReason`, promote that reason.
- If all attempted items fail but canonical reasons are mixed, promote `system_collector_error` with raw reason `mixed_item_failures`.

## Canonical Feed Status Reasons

`FeedStatusReason` is a bounded enum stored in the database as `feeds.status_reason`.

```python
class FeedStatusReason(StrEnum):
    SOURCE_OFFLINE = "source_offline"
    SOURCE_UNREACHABLE = "source_unreachable"
    SOURCE_RATE_LIMITED = "source_rate_limited"
    SYSTEM_AUTHENTICATION_FAILED = "system_authentication_failed"
    SYSTEM_CONFIGURATION_INVALID = "system_configuration_invalid"
    SYSTEM_COLLECTOR_ERROR = "system_collector_error"
    SYSTEM_PIPELINE_ERROR = "system_pipeline_error"
    SYSTEM_UNEXPECTED_ERROR = "system_unexpected_error"
```

Intended semantics:

- `source_offline`
  - The upstream feed exists conceptually, but the direct ingestion endpoint currently has no audio/feed available.

- `source_unreachable`
  - The upstream source/provider endpoint is persistently unavailable, failing, or unreachable after collector-owned retry policy.

- `source_rate_limited`
  - The upstream source/provider is refusing requests due to rate limiting after collector-owned backoff/retry policy.

- `system_authentication_failed`
  - Our credentials, tokens, or partner auth are rejected.

- `system_configuration_invalid`
  - Our feed row/config is missing or has an invalid source-specific identifier, URL, or required configuration.

- `system_collector_error`
  - The collector cannot turn apparently available source data into a chunk, or all item failures are mixed/ambiguous.

- `system_pipeline_error`
  - Runtime post-capture processing fails after source data was obtained, such as GCS upload, Pub/Sub publish, or progress bookmark writes.

- `system_unexpected_error`
  - Defensive fallback for bugs or untyped exceptions that should become typed later.

## Current Problem

HTTP status classification and ffmpeg failure classification are duplicated or mixed across collectors.

Examples:

### Current item HTTP classification helper

There is currently a helper like:

```python
def item_download_http_failure(status: int, *, reason_prefix: str = "item_http") -> ItemFailure:
    reason = f"{reason_prefix}_{status}"
    if status in {401, 403}:
        return ItemFailure(FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED, reason)
    if status == 429:
        return ItemFailure(FeedStatusReason.SOURCE_RATE_LIMITED, reason)
    return ItemFailure(FeedStatusReason.SOURCE_UNREACHABLE, reason)
```

This helper is somewhat useful because it centralizes policy, but it also constructs `ItemFailure` and chooses the reason string. That may be too coupled.

### Fire Notifications poll endpoint has different mapping

For Fire Notifications API polling, current behavior is closer to:

```python
def _classify_poll_status_failure(status: int) -> ItemFailure:
    reason = f"fn_api_http_{status}"
    if status in {401, 403}:
        return ItemFailure(FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED, reason)
    if status == 429:
        return ItemFailure(FeedStatusReason.SOURCE_RATE_LIMITED, reason)
    if 400 <= status < 500:
        return ItemFailure(FeedStatusReason.SYSTEM_CONFIGURATION_INVALID, reason)
    return ItemFailure(FeedStatusReason.SOURCE_UNREACHABLE, reason)
```

So 404 on the poll endpoint may mean our configured channel/path is invalid, not just a missing item.

### Icecast stream HTTP status has different mapping

For Icecast stream failures, current local behavior is closer to:

```python
def _classify_stream_http_status(status: int) -> CollectorFailure | None:
    reason = f"stream_http_{status}"
    if status in {401, 403}:
        return CollectorFailure(FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED, reason)
    if status == 404:
        return CollectorFailure(FeedStatusReason.SOURCE_OFFLINE, reason)
    if status == 429:
        return CollectorFailure(FeedStatusReason.SOURCE_RATE_LIMITED, reason)
    if 500 <= status <= 599:
        return CollectorFailure(FeedStatusReason.SOURCE_UNREACHABLE, reason)
    return None
```

So 404 on a stream endpoint may mean source offline, while 404 on Fire Notifications poll may mean invalid config, while 404 on a per-item audio URL may just mean stale/missing item.

### FFMPEG classification is currently Icecast-local

Icecast uses ffmpeg to capture streams. ffmpeg failures may expose:
- HTTP status evidence in stderr, e.g. `HTTP error 404 Not Found`, `Server returned 503 Service Unavailable`, or `HTTP/1.1 429`.
- positive exit code, e.g. `ffmpeg_exit_8`.
- negative exit code from signal, e.g. `ffmpeg_signal_9`.
- no useful stderr, in which case a same-URL probe may give more evidence.
- probe status 200 with ffmpeg failure may imply collector/media processing issue, not source outage.

Current behavior roughly:
- If ffmpeg stderr includes HTTP status, classify it as stream HTTP failure.
- If no useful stderr but same-URL probe returns mapped status, use probe classification.
- If probe returns 200 or inconclusive, preserve raw ffmpeg reason like `ffmpeg_exit_8` as `system_collector_error`.
- Negative exit code maps to `ffmpeg_signal_N`.

## Desired Direction

We want separate failure classifier files/modules so future classifiers can be added easily.

Proposed structure:

```text
backend/pipeline/ingestion/collectors/failure_classifiers/
  __init__.py
  http_status.py
  ffmpeg.py
```

Naming preference:
- Use "failure classifier," not "error classifier," because the codebase uses `CollectorFailure`, `ItemFailure`, and `FeedStatusReason`.

Initial idea:

1. HTTP status classifier:
   - Maps HTTP status code to canonical `FeedStatusReason`.
   - Does NOT construct `ItemFailure` or `CollectorFailure`.
   - Does NOT choose collector-specific reason strings.
   - Allows collector-specific mapping customization.

2. FFMPEG classifier:
   - Classifies ffmpeg stderr/exit/probe evidence.
   - Should reuse HTTP status classifier for HTTP status evidence.
   - May return `CollectorFailure` because ffmpeg failures are stream/feed-level evidence, but I am open to a better design if research suggests returning a neutral classification object instead.

3. Collectors keep reason-string construction local.
   - Examples:
     - `calls_api_http_404`
     - `calls_audio_http_404`
     - `stream_http_404`
     - `fn_api_http_404`
     - `ffmpeg_exit_8`
     - `ffmpeg_signal_9`

4. Do not create a shared HTTP downloader/retry abstraction as part of this change.
   - This task is classification only.
   - Retry loops should remain collector-owned.

## Important Design Questions

Please research and answer:

1. What are current industry-standard patterns for error/failure classification in ingestion systems, API clients, observability systems, and media/streaming pipelines?

2. Should the classifier return:
   - just `FeedStatusReason`
   - a custom object like `FailureClassification(status_reason, reason?)`
   - `ItemFailure`
   - `CollectorFailure`
   - something else?

3. How should we represent collector-specific overrides?
   Options include:
   - dict of exact status overrides: `{404: FeedStatusReason.SOURCE_OFFLINE}`
   - dataclass policy object
   - function callback
   - enum/category presets
   - class-based strategy
   - registry/plugin framework

4. Should `404` have a default?
   - For item downloads, 404 often means stale/missing object.
   - For stream endpoint, 404 may mean source offline.
   - For API poll endpoint, 404 may mean system configuration invalid.
   What default minimizes mistakes?

5. Should `1xx`, `2xx`, `3xx`, and unknown statuses return `None` or some fallback category?

6. How should ffmpeg failure classification be structured?
   - Should it parse stderr only?
   - Should it accept explicit `exit_code`/`raw_reason`?
   - Should it know about probe results?
   - Should probe handling remain in Icecast collector?
   - Should it return `CollectorFailure` or a neutral classification?

7. How do we avoid high-cardinality labels and preserve on-call usefulness?

8. How do we keep the abstraction simple enough for future collectors?

## Research Requirements

Use authoritative/current sources where possible and cite links. Useful areas:
- Google API error model / AIP guidance
- gRPC canonical status codes
- OpenTelemetry semantic conventions for errors/exceptions
- AWS, Azure, Google Cloud retry/transient fault handling
- SRE/observability guidance on low-cardinality labels and actionable alerts
- ffmpeg documentation or reliable behavior references for stderr, protocol errors, exit codes/signals
- Any relevant API client design patterns for separating protocol classification from domain ownership

Clearly separate:
- documented fact from sources
- your design inference
- tradeoffs/opinions

## Please Produce

1. Problem framing:
   - What problem are we actually solving?
   - What are we not solving?

2. Industry-standard patterns:
   - canonical category vs raw reason/detail
   - low-cardinality labels
   - protocol evidence vs domain ownership
   - retry/transient vs terminal classification
   - item-level vs feed-level failure
   - media/ffmpeg caveats

3. Evaluation of design options:
   - all classifiers in one file
   - `failure_classifiers/` package with one module per evidence type
   - generic registry/plugin framework
   - dataclass policy vs plain kwargs vs class strategy vs callback
   - classifier returning `FeedStatusReason` vs custom object vs `ItemFailure`/`CollectorFailure`

4. Recommended design:
   - exact module layout
   - exact function/type names
   - exact return types
   - exact policy customization API
   - how reason strings should be built
   - how ffmpeg classifier should use HTTP classifier
   - what should remain collector-owned

5. Concrete Python API sketch:
   - `http_status.py`
   - `ffmpeg.py`
   - examples for BCFY Calls, OpenMHz, Fire Notifications, Icecast

6. Default mapping policy:
   - 100-399
   - 400
   - 401
   - 403
   - 404
   - 408
   - 409
   - 410
   - 423
   - 425
   - 426
   - 429
   - 500
   - 502
   - 503
   - 504
   - unknown/nonstandard status

7. Test plan:
   - unit tests for HTTP status classifier
   - unit tests for ffmpeg classifier
   - collector regression tests
   - edge cases

8. Risks:
   - wrong 404 semantics
   - hiding source-specific context
   - over-centralizing retry behavior
   - high-cardinality reasons
   - brittle ffmpeg stderr parsing
   - future classifier growth

9. Final decision-complete implementation plan:
   - concise but detailed enough for an engineer to implement without making design decisions.

Be critical. If an abstraction is not worth it, say so. Optimize for long-term maintainability, correctness of on-call/operator diagnosis, and avoiding overengineering.
```

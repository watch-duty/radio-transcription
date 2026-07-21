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

## Overview

> [!IMPORTANT]
> **Common Architecture Misconceptions**:
> 1. **`bcfy_feeds` vs `bcfy_calls`**: `bcfy_feeds` represents continuous audio streams handled by [`icecast_collector.py`](icecast/icecast_collector.py). Do not confuse it with `bcfy_calls` (Broadcastify Calls), which is a separate REST-based polling collector [`bcfy_calls_collector.py`](bcfy_calls/bcfy_calls_collector.py) capturing discrete pre-segmented calls that do **NOT** pass through Dataflow segmentation.
> 2. **Icecast Collector Scope**: `bcfy_feeds` is currently the primary continuous audio source captured via `icecast_collector.py`. Future/additional Icecast-protocol streams (`icecast`) use this same collector. Continuous streams (`bcfy_feeds` and `icecast`) are the **only** sources processed by the downstream Dataflow continuous audio segmentation pipeline.

### Ingestion Collector Module Mapping

| Collector Module | Handled `source_type` Values | Stream Architecture | Processed by Dataflow Segmentation? |
| :--- | :--- | :--- | :--- |
| [`icecast_collector.py`](icecast/icecast_collector.py) | `bcfy_feeds` *(primary)*, `icecast` *(future)* | Continuous Icecast-protocol streams | **YES** |
| [`bcfy_calls_collector.py`](bcfy_calls/bcfy_calls_collector.py) | `bcfy_calls` | Discrete call REST polling API | **NO** |
| [`openmhz/collector.py`](openmhz/collector.py) | `openmhz` | Discrete call polling API | **NO** |
| [`fire_notifications/collector.py`](fire_notifications/collector.py) | `fire_notifications` | Event notification stream | **NO** |
| [`echo/main.py`](echo/main.py) | `echo` | Archival push (Cloud Function) | **NO** |


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
and the runtime classifies those failures through `failure_policy` before
choosing the budgeted or non-budgeted store path. GCS upload and bookmark-write
failures retain `system_pipeline_error` and remain non-budgeted. Pub/Sub
publish failures after a successful bookmark use
`pipeline_publish_after_bookmark_failed`, record `replay_missing=true` and
`data_gap_known=true`, and remain outside the feed quarantine budget because
the source feed did not cause the already-advanced bookmark/publish gap.

Echo is the exception to the VM runtime shape: it runs as a synchronous Cloud
Function. It writes the same canonical status-reason field through
`SyncFeedStore` and routes recorded failures through `failure_policy` before
choosing the budgeted or non-budgeted sync-store path. Echo v1 centralizes its
object-notification completion policy in the handler and returns success for
object-scoped and pipeline failures after a best-effort non-budgeted status
recording attempt so one object cannot quarantine the feed or create a retry
loop.

## Runtime Control-Plane Contract

VM collectors run inside `CollectorRuntime`; they should not claim feeds,
renew leases, or build their own unbounded startup queues. Lease Admission is
the runtime's pre-claim backpressure boundary: each lease-loop cycle limits the
new primary plus recovery leases admitted before feed tasks are created.
Collector code should assume it receives an already leased feed and should
preserve the runtime's ownership of lease acquisition, fencing, heartbeat, and
shutdown behavior.

Collector startup work must avoid creating synchronous herds against shared
external systems. If many feed tasks share a blocking dependency such as a
credential lookup, token refresh, or source-control call, coordinate that work
at the async level before entering the shared thread pool. A cache, cooperative
`asyncio.Lock`, or per-source limiter is preferable to letting every feed task
start the same blocking operation at once.

Worker Health is the worker-local `/healthz` signal. It remains tied to the
worker event loop and heartbeat freshness, so collector code should yield
regularly, respect cancellation, and keep blocking work out of the event loop.
Do not rely on VM Health to hide real worker stalls.

VM Health is the VM-level same-image health agent used by the MIG health check.
It probes all configured local Worker Health endpoints by HTTP status and
protects the VM from immediate autohealing until every configured worker has
been continuously unhealthy for 600 seconds. That hysteresis absorbs transient
overload; it does not make worker-level stalls acceptable.

Recovery acquisition remains primary-first for v1. If primary acquisition keeps
filling the Lease Admission budget, recovery rows can wait behind continuous
primary backlog. Treat that as an explicit residual risk and future tuning area,
not as something an individual collector should work around locally.

## Worker Cap Calibration

`SourceRuntimeSpec.default_cap` is a per-source, per-worker lease limit. It is
not a fleet-size target. `CollectorSettings.max_feeds_per_worker` is the final
worker-wide limit and defaults to 800, while `CAP_<SOURCE_TYPE>` can override an
individual source cap. Feed inventory and VM count determine how many workers
are needed after a safe per-worker cap is established; they must not be used to
derive the cap itself.

### Why Fire Notifications Defaults to 600

There is no controlled Fire Notifications-only resource ramp as of July 2026,
so 600 is a conservative proxy rather than a measured saturation point. The
closest measured collector is `bcfy_calls`: both are segmented HTTP polling
collectors, reuse the runtime-owned HTTP session, keep a bounded 1,000-item
per-feed deduplication deque, and have no long-running subprocess per feed.
Fire polls every 30-35 seconds versus every 10 seconds for `bcfy_calls`.

Fire does add burst work that the proxy does not cover: it downloads every new
MP3 and runs `ffprobe` in the shared thread pool. Its upstream API and download
rate limits have not been load-tested either. For that reason, the default
copies the existing comparator's configured cap instead of extrapolating to the
800-task worker ceiling or the 900 configured `openmhz` cap.

The runtime currently limits same-host HTTP concurrency to 64 and admits 20
new feed tasks per lease cycle, but those are queueing controls rather than
proof of capacity. Fire does not separately limit `ffprobe` concurrency within
the 512-thread shared executor, and neither file-list length nor downloaded MP3
size has a collector-level bound. A synchronized item-arrival burst can
therefore be limited by subprocess CPU, HTTP queueing, or audio-buffer memory
before steady polling CPU becomes a problem.

The provisional choice is:

```text
fire cap = existing configured bcfy_calls cap = 600
worker slots Fire cannot consume = 800 - 600 = 200
```

The 200-slot difference is a useful mixed-admission consequence, not an
independently measured input to the cap. It does not reserve CPU, memory, HTTP,
or thread-pool capacity for Fire download/`ffprobe` bursts.

The proxy comes from the April 16, 2026 mono-source ramps in commit `9360b46c`,
`EXPERIMENT_1B_REPORT.md` section 5.8. `docker stats` reports 100 CPU percentage
points as one vCPU, so `vCPU/feed = CPU percentage points/feed / 100`.
The ramp measured resource slopes at 200, 500, and 1,000 `bcfy_calls` feeds; it
did not derive or validate 600 as that collector's saturation cap.

| Source | Default cap | Historical CPU percentage points/feed | Historical vCPU/feed | Historical RSS/feed |
|--------|------------:|--------------------------------------:|---------------------:|--------------------:|
| `bcfy_feeds` | 240 | 0.156 | 0.00156 | about 16.9 MiB |
| `bcfy_calls` | 600 | 0.009 | 0.00009 | 0.40 MiB |
| `openmhz` | 900 configured; 800 worker-effective | 0.100 | 0.00100 | 2.805 MiB |
| `fire_notifications` | 600 | not measured | not measured | not measured |

At 600 feeds, the `bcfy_calls` fit implies about 0.054 marginal vCPU and
240 MiB marginal RSS. Its fitted RSS intercept was about 155 MiB, making the
total fitted worker RSS about 395 MiB. These figures justify using
`bcfy_calls` as a low-cost proxy; they do not prove the same coefficients for
Fire.

The historical ramps used one `n2-standard-4` VM, three points per source,
10-minute measurement windows, and disabled Pub/Sub. They were run once on one
day and used glibc allocation; the current image uses jemalloc. The current
`bcfy_feeds` audio path has also changed since that experiment, so its
historical coefficient must not be treated as a current measurement. The
source report lives on `origin/experiment/1b-stream-copy`; retrieve it with:

```shell
git fetch origin experiment/1b-stream-copy
git show 9360b46c:model/data/wildfire_catalog/EXPERIMENT_1B_REPORT.md
```

### Updating a Cap

Replace the proxy with a measured limit before raising Fire above 600:

1. Run a Fire-only stepped ramp with the current production image, worker
   resource limits, Pub/Sub enabled, representative file arrival rates, and a
   restart/claim burst. Use at least five feed-count levels and repeat the ramp
   on more than one worker or day.
2. After warmup, measure sustained and peak cgroup CPU and RSS, event-loop
   drift, poll latency and error/rate-limit rates, downloaded bytes, concurrent
   downloads and `ffprobe` calls, file-list and MP3 size distributions,
   thread-pool and HTTP-connector wait, lease-loop latency, and publish
   throughput. Include a multi-hour soak to detect memory growth.
3. Fit the steady-state models `CPU(N) = C0 + c*N` and
   `RSS(N) = M0 + m*N`. Construct a conservative upper prediction envelope for
   each complete model, including uncertainty in both intercept and slope, or
   use the worst repeated ramp. Do not combine an upper-bound slope with an
   optimistic point-estimate intercept.
4. Choose explicit CPU and memory budgets below the worker limits so startup
   and item-arrival bursts retain headroom. Calculate:

   ```text
   N_cpu        = max N where CPU_upper(N) <= CPU_budget
   N_memory     = max N where RSS_upper(N) <= RSS_budget
   N_event_loop = max tested N meeting drift and heartbeat guardrails
   N_global     = max_feeds_per_worker - mixed_source_reserve
   N_final      = round_down(
       min(N_cpu, N_memory, N_event_loop, N_upstream, N_global)
   )
   ```

   `N_upstream` is the largest count that stays inside provider rate limits and
   acceptable poll/download latency. Set `mixed_source_reserve` to zero only
   for a source-dedicated worker. Choose and justify any nonzero reserve from the
   expected source mix; 200 is only the current consequence of the provisional
   Fire cap. `CPU_budget` applies to total worker cgroup CPU, including executor
   threads and child processes, not just the event-loop thread. The historical
   experiment targeted about 80 `docker stats` CPU percentage points.
   `RSS_budget` should stay at or below the memory-watchdog pause threshold
   times a finite cgroup limit, currently 70%. The checked-in deployment does
   not set a per-container memory limit, so configure one before relying on that
   threshold or define and validate an explicit host-derived per-worker RSS
   budget. Round down to a feed count that was actually exercised by the ramp.
5. Validate the proposed cap in a representative mixed-source ramp. Use one
   shared worker baseline rather than summing the intercept from each
   mono-source fit:

   ```text
   CPU_mixed = C0_shared + sum(source_count * source_CPU_slope)
   RSS_mixed = M0_shared + sum(source_count * source_RSS_slope)
   ```

6. Canary `N_final` with `CAP_FIRE_NOTIFICATIONS`, including a simultaneous
   restart test. Promote it to `default_cap` only after CPU, RSS, event-loop,
   error-rate, and upstream-limit guardrails remain healthy, then update this
   section and the cap tests with the new evidence.

## Status Reason Policy

`feeds.status` remains lifecycle and scheduling state. `feeds.status_reason`
is a nullable, current abnormal-condition label that helps operators answer:
"is this caused by the upstream source, or by the ingestion system?" Successful
async progress, successful Echo heartbeat/progress, and manual reset clear
stale status reasons.

`status_reason_detail` is different. It preserves bounded diagnostic text
for the current abnormal condition. Do not
parse it for canonical ownership, do not treat it as a stable code, and do not
replace it with `status_reason`.

Status-reason prefixes are semantic owner namespaces: `source_` for external
source/provider conditions, `system_` for Watch Duty-owned system conditions,
and `pipeline_` for post-capture pipeline conditions. Ownership is not the
same as retry, quarantine, or logging policy.

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
| `system_authentication_failed` | Configured credentials, tokens, or partner auth are rejected by the upstream provider. |
| `system_configuration_invalid` | The feed row is missing or has an invalid source-specific identifier, URL, or required configuration. |
| `system_source_configuration_invalid` | A source control-plane or provider API response says the configured feed/source path is invalid, but v1 keeps it non-budgeted because provider-side changes may recover without feed-row edits. |
| `system_runtime_configuration_invalid` | Shared runtime, deployment, environment, source-class, or transport configuration is invalid and retry is not expected to repair it. |
| `system_credential_access_failed` | Watch Duty could not retrieve or access internal credentials, such as Secret Manager access failure; this is not the same as upstream provider credential rejection. |
| `system_source_payload_invalid` | A successful source response or downloaded source media violates the collector payload contract, but v1 keeps it non-budgeted because the response may be transient, provider-owned, or later auto-recovered by a deploy. |
| `system_collector_error` | The collector cannot turn apparently available source data into a chunk, all item failures are mixed/ambiguous, or an Echo duration/probe failure is limited to one object. |
| `system_pipeline_error` | Runtime or Echo post-capture processing fails after source data was obtained, such as GCS upload, bookmark writes, Echo download/staging/publisher/publish failures, or heartbeat writes. |
| `system_unexpected_error` | Defensive fallback for bugs or untyped exceptions that should become typed in a future collector fix. |

Use pipeline-owned reasons when capture has already succeeded and the remaining
work belongs to a replay/hold lane, not to feed health:

| Reason | Use when |
|--------|----------|
| `pipeline_publish_after_bookmark_failed` | The runtime bookmarked captured audio but could not publish the corresponding Pub/Sub message. This records a known downstream gap, sets `replay_missing=true` and `data_gap_known=true`, and does not consume feed quarantine budget. |

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
  `system_collector_error` with the detail `mixed_item_failures`.
- If no eligible items were attempted, or at least one item succeeded, do not
  record a feed failure.

The helper for this policy is `ItemBatchOutcome`.

## Successful Empty Polls

Polling collectors distinguish poll/fetch success from audio production:

- If the poll/fetch fails, count it as a collector-local failure.
- If the poll/fetch succeeds and the response has no source items to process,
  reset the collector-local failure streak and yield `SourceObservation`.
- If the poll/fetch succeeds and every returned source item is skipped before
  any item attempt (for example, all items were already seen), treat that as a
  successful non-audio source observation.
- If the poll/fetch succeeds and at least one source item is attempted, keep
  the existing item handling behavior for that source.

`SourceObservation` is not an audio chunk. The runtime must not upload, publish,
or count it as audio progress. It may clear stale persisted failure state when
the leased feed is dirty (`failure_count > 0` or `status_reason IS NOT NULL`).

For Broadcastify Calls, the source item is a call entry in the API `calls`
array. A missing `calls` field is treated as an empty page; a present non-list
`calls` field is malformed source data. A missing `lastPos` is still a
successful observation but does not advance a resume cursor.

For Fire Notifications, yield `SourceObservation` when the poll succeeds and
`files == []`, or when a non-empty file list produces no attempted items because
all files were skipped before download. Non-empty file lists with at least one
attempted item continue through `_process_file_list` item handling. Downloaded
MP3 bytes that ffprobe cannot parse are item-scoped
`system_source_payload_invalid` failures; they promote only through the normal
all-attempted-items-failed observation boundary.

## Failure Classification Model

`FailureInfo` is a lightweight container for a canonical `FeedStatusReason`
plus status reason detail before feed scope is applied. The text is operator
diagnostic material, not a machine-readable tag.

`ItemFailure` is an item-scoped failure value. Use it when an individual
object, call, file, or media URL fails inside a collector-owned batch.
`ItemBatchOutcome` owns the "all attempted items failed" promotion rule.

`FeedFailure` applies feed scope. Raise it only after the collector has enough
source-specific evidence to report the current feed-level condition to the
runtime.

Shared failure classifiers own evidence-specific classification, and may render
diagnostics for that evidence type, such as ffmpeg exit/signal/timeout details.
Collectors and source helpers still own status reason detail around source
operations because they know the operation, available exception text, captured
stderr tail, and source-specific semantics.
For ffmpeg and ffprobe subprocess failures, shared helpers should expose or
render bounded process evidence; collectors should log the source-scoped
operation context and decide whether that evidence is item-scoped or
feed-scoped.
`backend.pipeline.ingestion.status_reason_detail` owns only shared storage-boundary
helpers: exception detail formatting and the database storage cap. It must not
grow source-specific message construction helpers.
Collectors still own:

- retry and backoff policy;
- same-endpoint probes;
- item versus feed escalation;
- final status reason detail construction for source-specific operations.

## Status Reason Detail Policy

Status reason details should be useful for on-call debugging. Include the direct
evidence that explains the failure: terminal HTTP status and reason phrase,
exception class/message after retries are exhausted, ffmpeg exit/signal/timeout
details, and the bounded stderr tail when it materially explains an ffmpeg
failure.

Do not derive status reason details from Python stack frames or function names.
Build them at the call site that has the evidence. Shared helpers may render
generic operations they own, such as item media downloads or JSON fetches.
Collectors render source-specific operations, such as stream capture and
same-stream probes.

Do not truncate status reason detail in collectors or failure objects.
`FeedFailure` and runtime `_PipelineFailure` carry full diagnostics; async and
sync feed stores cap the text immediately before persisting it.

Do not branch on status reason detail. If later behavior depends on a
classification, carry typed information such as HTTP status, ffmpeg failure
kind, exit code, signal number, or a local probe outcome. For example, Icecast
stream capture uses typed ffmpeg failure info to decide whether to run a
same-stream probe; it does not parse strings like `ffmpeg_signal_9`.

The shared HTTP and ffmpeg classifiers are deliberately conservative. When an
endpoint has source-specific semantics, define a local policy near the
collector code. For example, Icecast stream `404` is `source_offline`, while a
poll endpoint `404` may be invalid configuration and an item URL `404` may be
only one stale object.

An Icecast "no finalized segment within timeout" event is ambiguous by itself:
it means the collector did not observe a completed segment file, not that the
source is conclusively offline. Classify it as a source condition only when
terminal stream evidence, such as stderr HTTP status or the existing
same-stream probe, supports that classification.

Do not duplicate exact HTTP policy tables in this guide. The `HTTPStatusPolicy`
instances in code and their tests are the source of truth; this document should
explain why policies are scoped by endpoint/stage, not restate every mapping.

Do not append raw HTTP response bodies, full ffmpeg stderr, stack traces, or
large request/response bodies. Exception text and bounded stderr tails may be
preserved when they are the direct diagnostic evidence for the failure episode;
storage applies the final status-reason-detail cap immediately before persistence.

## Shared Collector Helpers

Use the focused helpers at source boundaries where their contracts match:

- `control_flow.sleep_or_cancel` replaces local boolean sleep helpers. It returns
  after a normal timeout and raises `asyncio.CancelledError` when shutdown
  interrupts the wait. Shutdown is a stop condition, not an item or feed
  failure.
- Completed item download helpers should return `bytes | ItemFailure`.
  `None` is not an item-download result. Use
  `item_downloads.item_http_failure` to build an item-scoped failure from
  terminal item HTTP evidence and `item_downloads.item_download_failed` when
  retries exhaust without terminal HTTP evidence.
- `payloads.extract_optional_item_list` is for optional item arrays in
  successful polling payloads. Missing fields mean an empty observation; present
  non-list fields raise a bounded malformed-payload `FeedFailure`.
- `telemetry.emit_call_download_failed` is the single call-download-failed SLO
  emit point. Collectors pass only bounded feed metadata.

Collectors still own transport choice, retry loops, source-specific backoff,
same-endpoint probes, and item-to-feed promotion. Do not move HTTP sessions,
`curl_cffi` behavior, websocket handling, ffmpeg execution, or
`ItemBatchOutcome` promotion into the shared helpers.

## Adding a VM Collector

1. Add the source type if it is new:
   - add a `SourceType` enum member;
   - add seed data in `terraform/modules/alloydb/sql/ingestion/006_seed_source_types.sql`;
   - add a `_DEFAULT_CAPS` entry if VM workers should claim it;
   - add a `_COLLECTORS` entry in `router.py`;
   - update topic routing if the source is continuous instead of segmented.
2. Implement the `CollectorFn` signature from `models.py`.
3. Use `CaptureResources.http_session` for ordinary async HTTP. A
   collector-owned `curl_cffi` session is acceptable only when the
   source-specific transport requires browser impersonation, websocket
   handshake behavior, or another capability the runtime aiohttp session does
   not provide; in that case the collector must own cleanup in `finally`.
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

Minimum tests for an item-downloading VM collector:

- completed downloads return `bytes | ItemFailure`, never `None`;
- shutdown during a retry wait or active source request raises
  `asyncio.CancelledError` and does not emit `call_download_failed`;
- terminal item HTTP statuses and retry exhaustion use
  `item_downloads.item_http_failure` and
  `item_downloads.item_download_failed`;
- missing optional poll item lists are empty observations, while present
  non-list item lists raise a malformed-payload `FeedFailure` through
  `payloads.extract_optional_item_list`;
- partial item success suppresses `ItemBatchOutcome` promotion, all attempted
  item failures promote, and mixed canonical reasons promote as
  `mixed_item_failures`;
- invalid downloaded media remains item-scoped until the observation boundary
  promotes it;
- completed item failures call `telemetry.emit_call_download_failed` instead of
  building `call_download_failed` JSON locally.

For Echo-like synchronous ingestion, do not register a VM collector. Keep its
classification in the Cloud Function path and write reasons through
`SyncFeedStore`.

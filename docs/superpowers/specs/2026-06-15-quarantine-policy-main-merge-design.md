# Quarantine Policy Main Merge Design

Date: 2026-06-15

## Objective

Merge the v1 quarantine policy into latest `origin/main` after the large
post-June-12 ingestion changes, without a database schema migration.

The merged design must preserve main's improved collector semantics while
adding v1's missing runtime/storage guardrail: non-feed-actionable failures
must not consume the feed quarantine budget.

## Current Context

The v1 quarantine design was started against a repo shape from before the
June 12 and June 13 ingestion changes. Since then, main added or changed:

- source runtime metadata through `SourceRuntimeSpec`;
- stronger collector/runtime contracts;
- source-specific HTTP and ffmpeg failure classifiers;
- shared collector helpers for HTTP requests, item downloads, payload parsing,
  control-flow sleeps, and telemetry;
- `ItemBatchOutcome`-based item-to-feed promotion;
- `SourceObservation` semantics for successful non-audio source checks;
- GCS retry behavior with explicit timeouts, idempotent 412 handling, and 404
  translation;
- warning-level handling for non-actionable external source observations to
  reduce Cloud Logging error groups and alert fatigue.

Those changes mean v1 should not re-own source classification. The merge should
layer runtime/storage policy routing after collectors have already produced
typed evidence.

## Core Rule

Only feed-owned, feed-scoped, feed-configuration failures may increment
`failure_count` or quarantine a feed.

All other known failure classes must remain visible and retryable, but they must
use a non-budgeted path.

## Ownership Model

### Collectors Own Evidence Extraction

Collectors and source-specific helpers own:

- endpoint-specific HTTP status semantics;
- ffmpeg and same-endpoint probe interpretation;
- shared auth and token behavior;
- item-scoped versus feed-scoped escalation;
- source-specific retry and backoff;
- when to yield `CapturedChunk`;
- when to yield `SourceObservation`;
- when to raise a typed `FeedFailure`.

Examples:

- Icecast stream 404 can mean `source_offline`.
- Broadcastify Calls API 404 can mean invalid source configuration.
- Item media 404 can be a stale object and should stay item-scoped unless the
  whole observation boundary fails.

This knowledge stays near the source collector. The runtime policy layer must
not duplicate source-specific HTTP tables.

### Failure Policy Owns Pure Intent

`backend/pipeline/ingestion/failure_policy.py` owns the shared vocabulary and a
side-effect-free classifier.

Evidence stays intentionally small:

- `owner_scope`
- `failure_scope`
- `endpoint_kind`
- optional `pipeline_stage`

The policy function receives `status_reason + evidence` and returns a
`FailurePolicyDecision` containing:

- `policy_intent`
- `executed_action`
- `feed_budget_eligible`
- `quarantine_feed`

The policy module must not call storage, logging, Pub/Sub, telemetry, alerting,
or collector-specific parsing code.

Do not add `reason_family` for this merge. Current routing works from
`status_reason` and structured evidence. `quarantine_reason` remains raw
forensic text and must not drive routing.

### Runtime Owns Execution

`CollectorRuntime` owns the routing choke point:

- `FeedFailure` uses collector-provided evidence.
- `_PipelineFailure` uses runtime-provided pipeline evidence.
- untyped exceptions use conservative telemetry-gap evidence.

Runtime executes exactly one existing-schema side effect:

- budgeted decisions call `report_feed_failure(...)`;
- all other decisions call `release_non_budgeted_failure(...)`.

Runtime also emits structured telemetry for policy decisions and publish gaps.

### Storage Owns State Transitions

Storage keeps two distinct failure write paths:

- `report_feed_failure(...)`
  - increments `failure_count`;
  - schedules exponential retry while below threshold;
  - transitions to `quarantined` when threshold is reached;
  - writes `quarantine_reason` only on quarantine transition.
- `release_non_budgeted_failure(...)`
  - writes `status='failing'`;
  - writes `failure_count=0`;
  - writes `retry_after`;
  - writes `status_reason`;
  - releases the lease;
  - never writes `quarantine_reason`.

`report_feed_failure(...)` remains the only path that can consume quarantine
budget.

### Frontend And API Own Compatibility Only

No lifecycle status is added. Frontend and API layers only tolerate the new
status reason:

- backend/OpenAPI enum includes `pipeline_publish_after_bookmark_failed`;
- shared TypeScript types and conversion allowlist include the reason;
- API controller preserves backend `status_reason` as frontend `statusReason`;
- UI status indicator can display the reason;
- `failing` and `quarantined` still map to UI `error`.

## Architecture

The merge should be a narrow additive policy layer on latest main:

1. Keep `failure_policy.py` pure and small.
2. Keep collector-specific classification in collectors and latest-main helper
   modules.
3. Keep `SourceRuntimeSpec` focused on runtime metadata: source type, topic
   kind, claimability, caps, URL base.
4. Do not move failure policy into `SourceRuntimeSpec`; routing depends on
   live evidence, not static source metadata.
5. Route all runtime failures through a policy decision before storage mutation.
6. Preserve main's low-noise logging for non-actionable external source
   observations.

## Routing Design

### Captured Audio Path

When a collector yields `CapturedChunk`, runtime owns GCS upload, bookmark, and
Pub/Sub publish.

- GCS upload failure:
  - `status_reason=system_pipeline_error`
  - `owner_scope=pipeline`
  - `pipeline_stage=gcs_upload`
  - non-budgeted release
- bookmark failure:
  - `status_reason=system_pipeline_error`
  - `owner_scope=pipeline`
  - `pipeline_stage=bookmark_write`
  - non-budgeted release
- Pub/Sub publish failure after bookmark:
  - `status_reason=pipeline_publish_after_bookmark_failed`
  - `owner_scope=pipeline`
  - `pipeline_stage=pubsub_publish`
  - non-budgeted release
  - policy intent `hold_for_replay`
  - emit `post_bookmark_publish_failure`
  - `replay_missing=true`
  - `data_gap_known=true`

This does not implement durable replay. It makes the known gap explicit without
misclassifying it as feed health.

### Source Observation Path

When a collector yields `SourceObservation`, runtime must not upload or publish.
It records the observation only when it is useful:

- the leased feed has stale failure state, or
- the observation carries `resume_position`.

This preserves latest-main successful-empty-poll behavior and prevents clean
cursor-bearing observations from being dropped.

### Typed FeedFailure Path

When a collector raises typed `FeedFailure`, runtime classifies using the
collector evidence:

- feed-owned configuration invalid:
  - budgeted `report_feed_failure(...)`;
  - may quarantine after threshold.
- source offline, unreachable, or rate-limited:
  - non-budgeted release;
  - no quarantine telemetry;
  - low-noise source observation logging.
- credential-scope or source-class failure:
  - non-budgeted release;
  - policy intent `open_breaker`;
  - no actual breaker state in this merge.
- pipeline-owned collector evidence:
  - non-budgeted release.
- unknown owner:
  - telemetry-gap non-budgeted release.

### Untyped Exception Path

Runtime must not infer ownership from stack traces, exception strings, or
`quarantine_reason`.

Untyped exceptions route to:

- `status_reason=system_unexpected_error`
- `owner_scope=unknown`
- `failure_scope=unknown`
- `endpoint_kind=unknown`
- policy intent `telemetry_gap`
- non-budgeted release

This preserves visibility while making missing typed evidence explicit.

## Edge Cases

### Source Offline / Unreachable / Rate Limited

These are not feed quarantine in v1. They use non-budgeted release with retry.
Known external source observations should not create noisy traceback error
groups.

### Shared Auth And Credential Failures

Shared credentials are not per-feed failures. They use credential-scope evidence
and non-budgeted release with breaker intent telemetry. Actual fleet-wide
breaker state remains out of scope.

### Feed Configuration Invalid

This remains the narrow feed-budgeted path. Missing source-specific identifiers
or invalid feed row configuration can quarantine because a human/admin can fix
the feed.

### Item-Scoped Failures

Item failures do not consume feed budget. `ItemBatchOutcome` is the promotion
boundary. A feed-level failure is promoted only when every attempted item in
the observation boundary fails.

Mixed item failures stay non-budgeted. If a collector still surfaces ambiguous
item evidence as `system_collector_error`, v1 policy prevents that evidence from
becoming feed quarantine.

### Pub/Sub Ordering Key Pauses

Publisher helpers may attempt `resume_publish(...)`, but any remaining
publish-after-bookmark failure is pipeline-owned. It must not count against the
feed quarantine budget.

### Echo

Echo remains out of scope for this merge. Echo uses sync storage semantics and
should not block the VM ingestion policy merge.

## What Changes From Latest Main

The merge adds or preserves these v1 concepts on top of latest main:

- strict policy evidence on known `FeedFailure` values;
- `_PipelineFailure` carrying `status_reason` and policy evidence;
- pure `failure_policy.py`;
- runtime policy decision telemetry;
- non-budgeted release storage method and SQL;
- post-bookmark publish-gap telemetry;
- `pipeline_publish_after_bookmark_failed`;
- focused API/UI compatibility for that status reason.

## What Does Not Change

This merge does not include:

- database schema migration;
- new feed lifecycle status;
- durable replay/outbox;
- actual source-class or credential breaker state;
- persistent audit table;
- broad UI redesign;
- parsing `quarantine_reason`;
- moving source HTTP/ffmpeg semantics into runtime policy;
- Echo parity.

## Merge Hygiene

Resolve conflicts by favoring latest-main collector semantics and layering v1
policy routing after typed evidence exists.

Specific merge rules:

- preserve `SourceRuntimeSpec` as metadata only;
- preserve main's collector helper boundaries;
- preserve warning/info logging for non-actionable external source observations;
- keep `quarantine_reason` storage-boundary capping in storage helpers;
- avoid truncating collector diagnostic reason text before storage;
- update stale planning docs that still describe the old all-failures-through-
  `report_feed_failure(...)` behavior.

## Testing Strategy

Use focused tests that prove contracts, not incident labels.

Backend tests:

- `failure_policy` pure classification:
  - feed config maps to quarantine/feed budget;
  - Pub/Sub pipeline maps to hold-for-replay intent and no feed budget;
  - credential/source-class maps to breaker intent and no feed budget;
  - unknown maps to telemetry gap and no feed budget;
  - source-owned observations map to suppressed retry and no feed budget.
- runtime:
  - feed-config `FeedFailure` calls `report_feed_failure(...)`;
  - non-actionable `FeedFailure` calls `release_non_budgeted_failure(...)`;
  - `_PipelineFailure` never calls `report_feed_failure(...)`;
  - post-bookmark Pub/Sub failure emits policy and data-gap telemetry;
  - non-budgeted decisions never emit `feed_quarantined`;
  - untyped exceptions become telemetry gaps;
  - clean `SourceObservation(resume_position=...)` persists cursor.
- storage:
  - non-budgeted release writes `status='failing'`;
  - writes `failure_count=0`;
  - writes `retry_after` and `status_reason`;
  - does not write `quarantine_reason`;
  - only `REPORT_FAILURE_SQL` increments failure count.
- collector compatibility:
  - latest-main collector semantic tests keep passing;
  - add only narrow expectations where policy evidence is now required.

Frontend/API tests:

- backend/OpenAPI status reason parity;
- shared TypeScript type/allowlist includes
  `pipeline_publish_after_bookmark_failed`;
- API controller preserves backend `status_reason` as frontend `statusReason`;
- UI tooltip renders `Pipeline Publish Failed After Bookmark`;
- no new lifecycle status is introduced.

Recommended verification commands:

- targeted storage pytest;
- targeted runtime/failure-policy pytest;
- collector semantic tests touched by merge conflicts;
- `frontend/common` build;
- frontend API typecheck and focused feed-controller test;
- transcription UI typecheck and focused status-indicator test;
- `git diff --check`.

Avoid broad Docker, emulator, E2E, or local-stack runs unless explicitly
requested.

## Success Criteria

- All v1 quarantine-policy requirements pass on latest main.
- No non-budgeted path calls `report_feed_failure(...)`.
- No non-budgeted path emits `feed_quarantined`.
- Post-bookmark publish gap is explicitly logged as a known unreplayed gap.
- Feed-config quarantine still works.
- Latest-main collector semantic tests remain valid.
- Frontend/API tolerate the new status reason without changing lifecycle UI.
- Worktree remains free of DB migrations for this merge.

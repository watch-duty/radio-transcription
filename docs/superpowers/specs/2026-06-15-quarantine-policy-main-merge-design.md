# Quarantine Policy Main Merge Design

Date: 2026-06-15

## Objective

Merge the v1 quarantine policy into the current synced codebase after the large
post-June-12 ingestion changes, without a database schema migration.

The merged design must preserve the current improved collector semantics while
adding v1's missing runtime/storage guardrail: only quarantine-budgeted
failures may consume the feed quarantine budget.

## Current Context

The v1 quarantine design was started against a repo shape from before the
June 12 and June 13 ingestion changes. Since then, the codebase added or
changed:

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

An ingestion failure may increment `failure_count` only when policy decides:

- retry, backoff, or probing is not expected to restore progress; and
- an operator can fix the condition.

The operator fix may be a feed-row correction, batch admin action, credential
repair, code/deploy change, schema/serializer fix, or internal pipeline repair.

An ingestion failure must use the non-budgeted path when:

- retry, backoff, or probing may recover; or
- the condition is outside operator control, such as an external source being
  offline, unreachable, or rate-limited.

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

Keep the routing table central and explicit. Changing a status/evidence
combination from quarantine-budgeted to non-budgeted, or the reverse, should be
a one-row policy change in `failure_policy.py`, not a collector refactor.

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
status reasons:

- backend/OpenAPI enum includes `pipeline_publish_after_bookmark_failed`;
- backend/OpenAPI enum includes `system_runtime_configuration_invalid`;
- backend/OpenAPI enum includes `system_credential_access_failed`;
- backend/OpenAPI enum includes `system_source_payload_invalid`;
- shared TypeScript types and conversion allowlist include the reasons;
- API controller preserves backend `status_reason` as frontend `statusReason`;
- UI status indicator can display the reasons;
- `failing` and `quarantined` still map to UI `error`.

## Architecture

The merge should be a narrow additive policy layer on the current synced
codebase:

1. Keep `failure_policy.py` pure and small.
2. Keep collector-specific classification in collectors and current helper
   modules.
3. Keep `SourceRuntimeSpec` focused on runtime metadata: source type, topic
   kind, claimability, caps, URL base.
4. Do not move failure policy into `SourceRuntimeSpec`; routing depends on
   live evidence, not static source metadata.
5. Route all runtime failures through a policy decision before storage mutation.
6. Preserve the current low-noise logging for non-actionable external source
   observations.

## Policy Table

The v1 routing table is keyed by `status_reason` plus policy evidence. The
table should be encoded in one place in `failure_policy.py`.

| Status reason / evidence | V1 route | Rationale |
|---|---|---|
| `source_offline` | Non-budgeted | External source condition; retry, probe, or upstream recovery is expected. |
| `source_unreachable` | Non-budgeted | External source/provider condition or transient reachability condition. |
| `source_rate_limited` | Non-budgeted | External provider backoff condition. |
| `system_unexpected_error` with `unknown` evidence | Non-budgeted | Missing typed evidence; keep as telemetry gap. |
| `system_credential_access_failed` | Non-budgeted by default | Secret/credential-store access may be transient; flip this row later if evidence proves retry cannot recover. |
| `system_collector_error` with item, stream, or unknown evidence | Non-budgeted by default | Ambiguous item/media/ffmpeg fallback evidence does not prove operator-fixable failure. |
| `system_authentication_failed` after explicit auth/access refusal | Quarantine-budgeted | Collector-local retry, token refresh, or reconnect policy has been exhausted; retry alone is not expected to restore progress. |
| `system_configuration_invalid` | Quarantine-budgeted | Feed-row or source-specific feed configuration requires operator correction. |
| `system_runtime_configuration_invalid` | Quarantine-budgeted | Shared runtime/deploy/source-class configuration requires operator correction. |
| `system_source_payload_invalid` | Quarantine-budgeted | Successful source payload violates collector contract; repeating the same request is not expected to help. |
| `system_pipeline_error` for GCS upload or bookmark write | Non-budgeted | Failure occurs before the post-bookmark publish gap; existing retry/backoff path should remain available. |
| `pipeline_publish_after_bookmark_failed` with Pub/Sub publish stage | Quarantine-budgeted | Bookmark advanced, publish failed, and recovery is no longer just normal claiming. |

The table intentionally makes shared/internal failures quarantine-budgeted when
retry will not fix them. V1 uses feed quarantine as the stop-claiming mechanism;
admin tooling can batch re-enable affected feeds after the operator repair.

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
  - budgeted `report_feed_failure(...)`
  - policy intent `quarantine_feed`
  - emit `post_bookmark_publish_failure`
  - `replay_missing=true`
  - `data_gap_known=true`

This does not implement durable replay. It stops normal claiming through the v1
quarantine mechanism and makes the known gap explicit. The source feed may be
healthy; the repair belongs to the pipeline/operator workflow.

### Source Observation Path

When a collector yields `SourceObservation`, runtime must not upload or publish.
It records the observation only when it is useful:

- the leased feed has stale failure state, or
- the observation carries `resume_position`.

This preserves current successful-empty-poll behavior and prevents clean
cursor-bearing observations from being dropped.

### Typed FeedFailure Path

When a collector raises typed `FeedFailure`, runtime classifies using the
collector evidence:

- feed configuration invalid:
  - budgeted `report_feed_failure(...)`;
  - may quarantine after threshold.
- runtime configuration invalid:
  - budgeted `report_feed_failure(...)`;
  - may quarantine after threshold.
- source payload contract invalid:
  - budgeted `report_feed_failure(...)`;
  - may quarantine after threshold.
- terminal auth/access refusal after collector-local retry or refresh:
  - budgeted `report_feed_failure(...)`;
  - may quarantine after threshold.
- credential-store access failure:
  - non-budgeted release by default;
  - one policy-table row can change this later.
- source offline, unreachable, or rate-limited:
  - non-budgeted release;
  - no quarantine telemetry;
  - low-noise source observation logging.
- ambiguous `system_collector_error` item, stream, or unknown evidence:
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

These are not quarantine-budgeted in v1, even when they persist. They use
non-budgeted release with retry, backoff, or probing. Known external source
observations should not create noisy traceback error groups.

### Shared Auth And Credential Failures

Explicit auth/access refusals become quarantine-budgeted only after
collector-local retry, token refresh, or reconnect policy has been exhausted.
Examples include Calls API `401/403`, Icecast stream `401/403`, Fire
Notifications poll `401/403`, OpenMHz WebSocket upgrade `401/403`, and
all-items-failed media `401/403` observation boundaries.

Credential-store access failures are different. `calls_jwt_secret_access_failed`
should use `system_credential_access_failed` and remain non-budgeted by default
because the current evidence cannot prove retry forever will not recover.

### Feed Configuration Invalid

`system_configuration_invalid` is for feed-row or source-specific feed
configuration such as missing `source_feed_id`, Calls API `404`, and Fire
Notifications poll `400/404`. It is quarantine-budgeted.

`system_runtime_configuration_invalid` is for shared deploy/source-class
configuration such as missing Broadcastify credentials, missing Calls JWT config,
missing Fire Notifications env config, missing Fire Notifications S3 base, or an
invalid OpenMHz transport setting. It is also quarantine-budgeted in v1.

The split is for diagnosis and future routing. If shared runtime configuration
later gets a source-class hold, one policy-table row can move it out of per-feed
quarantine.

### Source Payload Invalid

`system_source_payload_invalid` is for successful source responses that violate
collector payload expectations, such as Calls API non-JSON or invalid `calls`
shape and Fire Notifications invalid JSON or invalid `files` shape. It is
quarantine-budgeted because repeating the same request is not expected to make
the payload processable.

### Item-Scoped Failures

Item failures do not consume feed budget. `ItemBatchOutcome` is the promotion
boundary. A feed-level failure is promoted only when every attempted item in
the observation boundary fails.

Mixed item failures stay non-budgeted. If a collector still surfaces ambiguous
item evidence as `system_collector_error`, v1 policy prevents that evidence from
becoming feed quarantine.

### Pub/Sub Ordering Key Pauses

Publisher helpers may attempt `resume_publish(...)`, but any remaining
publish-after-bookmark failure is pipeline-owned and quarantine-budgeted in v1.
The detailed reason can still identify paused ordering key, schema validation,
or generic publish failure without splitting the status reason yet.

### Unexpected System Failures

`system_unexpected_error` is the residual fallback for untyped bugs or missing
classification evidence. It is non-budgeted until a future change replaces it
with a more precise status reason and policy evidence.

### Echo

Echo remains out of scope for this merge. Echo uses sync storage semantics and
should not block the VM ingestion policy merge.

## What Changes From Latest Main

The merge adds or preserves these v1 concepts on top of the current synced
codebase:

- strict policy evidence on known `FeedFailure` values;
- `_PipelineFailure` carrying `status_reason` and policy evidence;
- pure `failure_policy.py`;
- central status-plus-evidence policy table;
- runtime policy decision telemetry;
- non-budgeted release storage method and SQL;
- post-bookmark publish-gap telemetry;
- `pipeline_publish_after_bookmark_failed`;
- `system_runtime_configuration_invalid`;
- `system_credential_access_failed`;
- `system_source_payload_invalid`;
- focused API/UI compatibility for those status reasons.

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
- splitting post-bookmark publish failures into schema, ordering-key, and generic
  status reasons;
- Echo parity.

## Merge Hygiene

Resolve conflicts by favoring current collector semantics and layering v1
policy routing after typed evidence exists.

Specific merge rules:

- preserve `SourceRuntimeSpec` as metadata only;
- preserve current collector helper boundaries;
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
  - runtime config maps to quarantine/feed budget;
  - explicit terminal auth/access maps to quarantine/feed budget;
  - credential-store access maps to non-budgeted release;
  - source payload invalid maps to quarantine/feed budget;
  - Pub/Sub publish-after-bookmark maps to quarantine/feed budget;
  - GCS/bookmark pipeline errors map to non-budgeted release;
  - ambiguous collector errors map to non-budgeted release;
  - unknown maps to telemetry gap and no feed budget;
  - external source conditions map to non-budgeted release.
- runtime:
  - feed-config `FeedFailure` calls `report_feed_failure(...)`;
  - runtime-config `FeedFailure` calls `report_feed_failure(...)`;
  - source-payload-invalid `FeedFailure` calls `report_feed_failure(...)`;
  - terminal auth/access `FeedFailure` calls `report_feed_failure(...)`;
  - non-actionable `FeedFailure` calls `release_non_budgeted_failure(...)`;
  - GCS/bookmark `_PipelineFailure` calls `release_non_budgeted_failure(...)`;
  - post-bookmark Pub/Sub failure calls `report_feed_failure(...)`;
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
  - current collector semantic tests keep passing;
  - add only narrow expectations where policy evidence is now required.

Frontend/API tests:

- backend/OpenAPI status reason parity;
- shared TypeScript type/allowlist includes
  `pipeline_publish_after_bookmark_failed`;
- shared TypeScript type/allowlist includes
  `system_runtime_configuration_invalid`,
  `system_credential_access_failed`, and
  `system_source_payload_invalid`;
- API controller preserves backend `status_reason` as frontend `statusReason`;
- UI tooltip renders all new reasons;
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

- All v1 quarantine-policy requirements pass on the current synced codebase.
- No non-budgeted path calls `report_feed_failure(...)`.
- No non-budgeted path emits `feed_quarantined`.
- Post-bookmark publish gap increments the quarantine budget and is explicitly
  logged as a known unreplayed gap.
- Feed-config quarantine still works.
- Runtime-config, terminal-auth/access, and source-payload-invalid quarantine
  paths work.
- Current collector semantic tests remain valid.
- Frontend/API tolerate the new status reasons without changing lifecycle UI.
- Worktree remains free of DB migrations for this merge.

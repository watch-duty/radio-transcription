# Phase 5 — Producer And Runtime Routing Merge Research

**Phase:** 05-producer-and-runtime-routing-merge
**Date:** 2026-06-15
**Status:** Complete

## Research Question

What does the planner need to know to safely update producer status mappings and runtime `_PipelineFailure` routing without broad schema, replay, breaker, or compatibility changes?

## Findings

### Runtime Routing

- `backend/pipeline/ingestion/collector_runtime.py` already has the required primitives: `_pipeline_policy_evidence(...)`, `_telemetry_gap_evidence()`, `_record_feed_failure(...)`, `_record_non_budgeted_failure(...)`, `_emit_policy_decision(...)`, and `_is_feed_quarantine_decision(...)`.
- The collector `FeedFailure` catch arm already classifies the failure and branches to `_record_feed_failure(...)` or `_record_non_budgeted_failure(...)`.
- The `_PipelineFailure` catch arm still classifies the failure but unconditionally calls `_record_non_budgeted_failure(...)`. This is the smallest runtime change for Phase 5.
- Existing Pub/Sub post-bookmark publish tests currently assert the old v1 non-budgeted path. These should become RED tests for v1.1: `report_feed_failure(...)` called, `release_non_budgeted_failure(...)` not called, no `retry_after` storage release for the Pub/Sub route, and quarantine telemetry emitted only when the store returns `"quarantined"`.
- The canonical policy-decision log can preserve `replay_missing=true` and `data_gap_known=true` on the budgeted Pub/Sub path by adding equivalent optional flags to `_record_feed_failure(...)` or by a small local wrapper in the `_PipelineFailure` branch. The old `post_bookmark_publish_failure` event should not be a v1.1 routing requirement.

### Producer Splits

- `backend/pipeline/ingestion/collectors/failure_classification.py` already maps the new enum values to owner scopes, and `policy_evidence_for_status_reason(...)` will keep evidence coherent when producer labels change.
- Broadcastify Calls has three clear split points:
  - missing JWT env config: `system_runtime_configuration_invalid`
  - Secret Manager access failure: `system_credential_access_failed`
  - malformed successful API payload: `system_source_payload_invalid`
- Fire Notifications has clear split points:
  - missing shared S3/auth env config: `system_runtime_configuration_invalid`
  - malformed successful poll payload or JSON: `system_source_payload_invalid`
- Icecast has a clear shared-runtime split:
  - missing Broadcastify stream credentials: `system_runtime_configuration_invalid`
  - missing feed `source_feed_id` remains `system_configuration_invalid`
  - ffmpeg process fallback remains `system_collector_error`
- OpenMHz has one clear payload split:
  - source-provided invalid media URLs: `system_source_payload_invalid`
  - invalid selected transport remains feed/source configuration invalid
  - reconnect exhaustion and explicit source transport failures stay source-owned or auth-owned according to existing classifiers
- Broad or ambiguous cases should stay broad:
  - `mixed_item_failures`
  - `duration_probe_failed`
  - ffmpeg timeout/signal/exit without source-probe evidence
  - item HTTP fallbacks without a clear new enum

### External Pub/Sub Semantics

- Pub/Sub schema publishing docs say schema mismatches return `INVALID_ARGUMENT`; unchanged invalid payload retries are not expected to help.
- Pub/Sub error-code docs describe `INVALID_ARGUMENT` as a request-invalid failure that will fail again if retried unchanged.
- Pub/Sub ordering-key docs say failed ordered publishes affect queued and future publishes for that key until publishing is resumed.

These support preserving explicit data-gap telemetry for Pub/Sub post-bookmark publish failures while routing the failure through the existing feed budget in v1.1.

## Validation Architecture

Use focused backend tests only. Do not require Docker, testcontainers, OpenAPI generation, frontend builds, or full local integration stacks.

| Requirement | Automated Proof |
| --- | --- |
| RUN-11 | Runtime tests show `_PipelineFailure` classifies through `failure_policy.classify_failure_policy(...)` and branches by `is_feed_quarantine(...)`. |
| RUN-12 | Runtime tests show Pub/Sub post-bookmark publish failure calls `report_feed_failure(...)`. |
| RUN-13 | Runtime tests show GCS upload and bookmark write call `release_non_budgeted_failure(...)`, not `report_feed_failure(...)`. |
| RUN-14 | Runtime/storage tests show non-budgeted failures set retry-after and reset stale failure count. |
| RUN-15 | Runtime tests show budgeted Pub/Sub failures respect `feed_failure_threshold` and emit quarantine telemetry only on `"quarantined"`. |
| RUN-16 | Runtime tests do not require `post_bookmark_publish_failure`; policy decision telemetry carries `replay_missing` and `data_gap_known`. |
| TEST-13 | Existing publish-after-bookmark tests updated to assert `report_feed_failure(...)`. |
| TEST-14 | Runtime tests cover source, ambiguous collector, GCS/bookmark, credential-access, and telemetry-gap non-budgeted paths. |
| TEST-15 | Collector tests prove Calls, Fire Notifications, Icecast, and OpenMHz producer split values. |

## Recommended Plan Split

1. `05-01`: Producer status split tests and implementation.
2. `05-02`: Runtime `_PipelineFailure` policy execution.
3. `05-03`: Cross-route regression verification for non-budgeted reset and quarantine telemetry boundaries.

## Research Complete

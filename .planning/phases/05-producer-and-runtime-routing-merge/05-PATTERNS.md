# Phase 05 Pattern Map

**Phase:** 05-producer-and-runtime-routing-merge
**Date:** 2026-06-15

## Runtime Routing Patterns

| Target | Closest Existing Analog | Pattern To Preserve |
| --- | --- | --- |
| `_PipelineFailure` catch arm in `collector_runtime.py` | `FeedFailure` catch arm in `_process_feed(...)` | Classify `status_reason + policy_evidence`; branch by `failure_policy.is_feed_quarantine(decision)`; call `_record_feed_failure(...)` or `_record_non_budgeted_failure(...)`. |
| Pub/Sub post-bookmark publish failure | `_process_captured_chunk(...)` Pub/Sub `_PipelineFailure` construction | Preserve `FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED` with `EndpointKind.PUBSUB_PUBLISH` and `PipelineStage.PUBSUB_PUBLISH`. |
| Non-budgeted pipeline failures | GCS upload/bookmark `_PipelineFailure` construction | Keep `FeedStatusReason.SYSTEM_PIPELINE_ERROR`, `EndpointKind.GCS_UPLOAD`/`BOOKMARK_WRITE`, and `PipelineStage.GCS_UPLOAD`/`BOOKMARK_WRITE`. |
| Quarantine telemetry | `_record_feed_failure(...)` existing threshold behavior | Emit `quarantine_telemetry.emit_quarantine_event(...)` only when store returns `"quarantined"`. |

## Producer Mapping Patterns

| Target | Closest Existing Analog | Pattern To Preserve |
| --- | --- | --- |
| Calls JWT env/config split | `_get_jwt_token()` typed `collector_failure(...)` | Keep local evidence construction; change status to `SYSTEM_RUNTIME_CONFIGURATION_INVALID` with credential/source-class-compatible endpoint evidence as required by policy. |
| Calls Secret Manager access split | `_get_jwt_token()` exception handler | Change status to `SYSTEM_CREDENTIAL_ACCESS_FAILED`; preserve reason `calls_jwt_secret_access_failed`. |
| Calls source payload split | `_fetch_calls(...)` and payload validation | Set `invalid_payload_status_reason=FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID`; preserve no-retry behavior. |
| Shared payload helper split | `collectors/payloads.py::extract_optional_item_list(...)` | Malformed successful source payload raises a `FeedFailure` with source payload status and endpoint/failure scope supplied by caller. |
| Fire runtime config split | `fire_notifications_collector(...)` env setup | Missing shared env/auth config becomes runtime configuration invalid; missing `source_feed_id` remains feed configuration invalid. |
| Icecast runtime config split | `_build_auth_header()` | Missing shared Broadcastify credentials becomes runtime configuration invalid; missing feed IDs remain feed configuration invalid. |
| OpenMHz payload split | `_download_m4a(...)` invalid media URL guard | Invalid source-provided media URL becomes source payload invalid; invalid selected transport remains feed configuration invalid. |

## Test Patterns

| Test Area | Existing Test Files | Pattern |
| --- | --- | --- |
| Producer mapping tests | `backend/pipeline/ingestion/collectors/tests/test_*_collector.py` | Update existing assertions rather than adding duplicate scenario-only tests. |
| Runtime store call tests | `backend/pipeline/ingestion/tests/test_collector_runtime.py` | Assert `report_feed_failure` versus `release_non_budgeted_failure`; telemetry assertions are secondary. |
| Storage reset tests | `backend/pipeline/storage/tests/test_feed_store.py` | Assert final row state for `failure_count`, `status`, `retry_after`, `status_reason`, `worker_id`, and `quarantine_reason`. |

## Constraints

- Do not add `reason_family`.
- Do not add database migrations.
- Do not update OpenAPI/frontend/generated files in Phase 5.
- Do not add durable replay, source-class breaker state, or persistent audit tables.
- Do not parse `quarantine_reason` for routing.

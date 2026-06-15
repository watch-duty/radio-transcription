# Phase 2 Pattern Map: Runtime Routing And Telemetry

**Date:** 2026-06-15
**Status:** Complete

## Runtime Routing Pattern

File: `backend/pipeline/ingestion/collector_runtime.py`

- `_process_feed(...)` is the side-effect router.
- `FeedFailure` catch arm should classify collector-provided evidence and call
  `_record_feed_failure(...)` only when the decision is feed quarantine.
- `_PipelineFailure` catch arm should always use
  `_record_non_budgeted_failure(...)`.
- Generic `Exception` catch arm should synthesize UNKNOWN evidence through
  `_telemetry_gap_evidence()` and use `_record_non_budgeted_failure(...)`.
- `_record_feed_failure(...)` is the only runtime wrapper that may call
  `report_feed_failure(...)` and emit quarantine telemetry.
- `_record_non_budgeted_failure(...)` is the suppressed retry path and should
  call `release_non_budgeted_failure(...)`.

## Policy Classification Pattern

File: `backend/pipeline/ingestion/failure_policy.py`

- Policy owns pure classification and predicates only.
- No DB, Pub/Sub, telemetry, alerting, lease release, or runtime mutation goes
  in this file.
- `OwnerScope.PIPELINE` must never be feed-budget eligible.
- `OwnerScope.CREDENTIAL_SCOPE` and `OwnerScope.SOURCE_CLASS` use
  `PolicyIntent.OPEN_BREAKER` but execute only
  `ExecutedAction.RELEASE_NON_BUDGETED_FAILURE` in v1.
- `OwnerScope.FEED` plus feed-level configuration evidence is the narrow
  budgeted quarantine lane.
- `OwnerScope.UNKNOWN` routes to telemetry gap and non-budgeted release.

## Storage Pattern

Files:

- `backend/pipeline/storage/feed_store.py`
- `backend/pipeline/storage/feed_queries.py`

`report_feed_failure(...)` is the quarantine-budget path.
`release_non_budgeted_failure(...)` is the suppressed retry path.

Phase 2 should not add a DB migration. It should reuse the Phase 1 storage
primitive and rely on Phase 3 for storage-focused compatibility tests.

## Telemetry Pattern

File: `backend/pipeline/ingestion/collector_runtime.py`

- Runtime logs structured `json_fields`.
- `feed_failure_policy_decision` is emitted for every routed failure.
- `post_bookmark_publish_failure` is emitted only for post-bookmark Pub/Sub
  publish gaps.
- Telemetry mirrors decisions; it never chooses routing.
- Keep stable telemetry assertions small: status reason, owner scope, failure
  scope, endpoint kind, policy intent, executed action, retry delay, source
  type, and publish-gap replay flags.

## Test Pattern

File: `backend/pipeline/ingestion/tests/test_collector_runtime.py`

- Use existing classes:
  - `TestFeedFailureContract`
  - `TestProcessFeedRetry`
  - `TestProcessFeedQuarantine`
- Assert routing by store calls, not by log text:
  - budgeted: `report_feed_failure.assert_awaited...`
  - non-budgeted: `release_non_budgeted_failure.assert_awaited...`
- Patch `_non_budgeted_retry_after()` to a sentinel when a test needs exact
  retry propagation.
- Use `assertLogs(..., level=logging.INFO)` only for telemetry contract tests.

File: `backend/pipeline/ingestion/tests/test_failure_policy.py`

- Keep classifier tests pure.
- Add missing predicate/action split assertions here instead of duplicating
  policy conditions in runtime tests.

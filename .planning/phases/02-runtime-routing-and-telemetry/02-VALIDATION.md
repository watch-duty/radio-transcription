---
phase: 02
slug: runtime-routing-and-telemetry
status: draft
nyquist_compliant: true
wave_0_complete: true
created: 2026-06-15
---

# Phase 2 Validation Architecture

## Test Framework

- Framework: `pytest`
- Runtime tests: `unittest.IsolatedAsyncioTestCase`
- Config: `pyproject.toml`
- Host-stability wrapper: `safe-run --`

## Phase Gate Command

Run the narrow phase gate after all Phase 2 plans execute:

```bash
safe-run -- uv run python -m pytest \
  backend/pipeline/ingestion/tests/test_failure_policy.py \
  backend/pipeline/ingestion/tests/test_collector_runtime.py::TestFeedFailureContract \
  backend/pipeline/ingestion/tests/test_collector_runtime.py::TestProcessFeedRetry \
  backend/pipeline/ingestion/tests/test_collector_runtime.py::TestProcessFeedQuarantine \
  -q -n 0
```

This intentionally avoids broad local integration stacks.

## Plan Validation Map

| Plan | Requirements | Primary Validation |
|------|--------------|--------------------|
| 02-01 | POL-04, RUN-01, RUN-07 | Policy classifier tests plus runtime store-call assertions proving only feed-owned quarantine decisions use `report_feed_failure(...)`. |
| 02-02 | RUN-02, RUN-03 | Runtime tests proving source-class, credential-scope, pipeline, and unknown failures use `release_non_budgeted_failure(...)` with retry timing and the right status reason. |
| 02-03 | RUN-04, RUN-05, RUN-06, TEL-01, TEL-02, TEL-03, TEL-04, TEL-05 | Structured log assertions for `feed_failure_policy_decision`, post-bookmark publish gap flags, and no quarantine telemetry on non-budgeted decisions. |

## Requirement-Specific Assertions

- POL-04: `FeedFailure` without evidence remains invalid; untyped runtime
  exceptions route to UNKNOWN telemetry gap and non-budgeted release.
- RUN-01: Runtime calls `report_feed_failure(...)` only for
  `PolicyIntent.QUARANTINE_FEED` and `OwnerScope.FEED`.
- RUN-02: Non-feed-actionable decisions call `release_non_budgeted_failure(...)`
  and never call `report_feed_failure(...)`.
- RUN-03: Pub/Sub publish failure after bookmark uses
  `FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED`.
- RUN-04: Publish-gap telemetry includes `policy_intent=hold_for_replay`.
- RUN-05: Publish-gap telemetry includes
  `executed_action=suppress_feed_quarantine_record_publish_gap`.
- RUN-06: Publish-gap telemetry includes `replay_missing=true` and
  `data_gap_known=true`.
- RUN-07: Feed-owned configuration evidence remains able to quarantine.
- TEL-01/TEL-02: Every routed failure emits `feed_failure_policy_decision` with
  status reason, evidence fields, intent, action, retry delay when applicable,
  and source type.
- TEL-03/TEL-04: Post-bookmark Pub/Sub publish gaps emit the dedicated gap
  event with replay flags.
- TEL-05: Non-budgeted decisions never emit `feed_quarantined`.

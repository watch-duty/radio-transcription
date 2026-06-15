# Phase 2 Research: Runtime Routing And Telemetry

**Date:** 2026-06-15
**Status:** Complete

## Question

How should Phase 2 route runtime failures through the Phase 1 policy
foundation with minimal code change?

## Findings

Phase 1 already created most of the surfaces Phase 2 needs:

- `backend/pipeline/ingestion/failure_policy.py` contains the pure vocabulary,
  decision dataclass, classifier, and predicates.
- `backend/pipeline/ingestion/models.py` rejects typed `FeedFailure` without
  `policy_evidence`.
- `backend/pipeline/storage/feed_store.py` exposes
  `release_non_budgeted_failure(...)`, and `report_feed_failure(...)` remains
  the quarantine-budget path.
- `backend/pipeline/ingestion/collector_runtime.py` already has helpers for
  policy evidence, non-budgeted retry, policy telemetry, post-bookmark publish
  gap telemetry, and `_PipelineFailure`.
- `backend/pipeline/ingestion/tests/test_collector_runtime.py` already has
  partial Phase 2 tests for post-bookmark Pub/Sub publish failure, feed-config
  quarantine, generic telemetry-gap exceptions, and pipeline stage failures.

The implementation plan should therefore be a reconciliation and hardening
plan, not a broad rewrite.

## Design Direction

Routing authority is:

```text
status_reason + FailurePolicyEvidence
  -> failure_policy.classify_failure_policy(...)
  -> FailurePolicyDecision
  -> runtime executes the matching store/log action
```

Telemetry is not routing authority. Logs are v1 audit output only.

The runtime should prove policy through side effects:

- Budgeted quarantine decisions call `report_feed_failure(...)`.
- Non-budgeted decisions call `release_non_budgeted_failure(...)`.
- Non-budgeted decisions never emit `feed_quarantined`.
- Post-bookmark publish gaps also emit `post_bookmark_publish_failure` with
  `replay_missing=true` and `data_gap_known=true`.

## Key Phase 2 Corrections

The Phase 2 requirement wording for POL-04 is older than the Phase 1 decision
to keep `FeedFailure` strict. Execution must preserve the stricter boundary:

- typed `FeedFailure` without evidence remains invalid and is tested at model
  construction time;
- untyped runtime exceptions synthesize UNKNOWN evidence and route to
  telemetry gap through the non-budgeted path.

This reconciles POL-04 with context decisions D-01 through D-03.

## Plan Split

1. Guard the budgeted quarantine path.
   Prove only feed-owned `quarantine_feed` decisions can reach
   `report_feed_failure(...)`, and feed-actionable configuration failures
   still quarantine.

2. Harden non-budgeted routing.
   Prove source-class, credential-scope, pipeline-owned, source-offline,
   rate-limit, capture-timeout, and unknown decisions route through
   `release_non_budgeted_failure(...)` with retry timing supplied.

3. Lock down telemetry.
   Prove `feed_failure_policy_decision` mirrors the decision and that
   post-bookmark Pub/Sub publish gaps record the v1 no-replay reality without
   emitting quarantine telemetry.

## Risks

- Over-contracting telemetry payloads could create long-term maintenance cost.
  Tests should assert stable fields required by the requirements, not every
  incidental log field.
- `PolicyIntent.OPEN_BREAKER` could be misread as real breaker execution.
  Tests and plan language must assert it pairs with
  `ExecutedAction.RELEASE_NON_BUDGETED_FAILURE` in v1.
- Exact retry windows should not become policy. Tests should assert the runtime
  supplies a retry time or a patched sentinel, not the 5-15 minute range.
- Existing Phase 1 hooks may already satisfy parts of Phase 2. Execution should
  add focused missing assertions and avoid refactoring working code unless a
  requirement cannot otherwise be proven.

## External Research

No external library, SDK, cloud API, or framework documentation is required for
this planning phase. The relevant decisions are repository policy decisions and
current code contracts.

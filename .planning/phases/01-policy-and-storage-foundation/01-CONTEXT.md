# Phase 1: Policy And Storage Foundation - Context

**Gathered:** 2026-06-15
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 1 delivers the foundation for strict, evidence-based failure policy:
structured policy types, a pure policy decision module, the minimal status
reason vocabulary needed now, and a non-budgeted storage path for suppressed
retry states. It does not execute runtime routing end-to-end, add durable
replay, create source-class breakers, add audit tables, or redesign feed
lifecycle status.

</domain>

<decisions>
## Implementation Decisions

### Policy Evidence Contract

- **D-01:** Create `backend/pipeline/ingestion/failure_policy.py` for policy
  vocabulary and pure policy logic. Do not keep these policy enums/classes in
  `models.py` long term.
- **D-02:** `failure_policy.py` owns `OwnerScope`, `FailureScope`,
  `EndpointKind`, `PolicyIntent`, `ExecutedAction`, `PipelineStage`,
  `FailurePolicyEvidence`, `FailurePolicyDecision`,
  `classify_failure_policy(status_reason, evidence)`, and pure predicates such
  as `is_feed_quarantine`, `is_feed_budget_eligible`, `is_pipeline_hold`, and
  `is_source_class_breaker`.
- **D-03:** `failure_policy.py` must not own runtime side effects. The runtime,
  stores, publisher helpers, breaker stores, hold/replay stores, telemetry, and
  alerting remain outside the policy module.
- **D-04:** `FailurePolicyEvidence` contains facts only: ownership, scope,
  endpoint, and optional pipeline stage. It must not contain the final policy
  verdict. `PolicyIntent`, `ExecutedAction`, and budget/quarantine booleans
  belong on `FailurePolicyDecision`.
- **D-05:** Typed `FeedFailure` is strict. It must require
  `policy_evidence`. There is no intentional known/classified failure path
  without policy evidence.
- **D-06:** Untyped runtime exceptions are not `FeedFailure`. Runtime may
  synthesize `UNKNOWN` / telemetry-gap policy evidence for those defensive
  fallback paths.
- **D-07:** Do not add `reason_family` in Phase 1. V1 routing can be decided
  from `status_reason` plus `owner_scope`, `failure_scope`, `endpoint_kind`,
  and `pipeline_stage`. Raw `reason` remains forensic detail only.
- **D-08:** Collector/source-specific code owns raw signal extraction and
  construction of typed failure evidence. Runtime owns execution of policy
  decisions. Storage owns DB state transitions.

### Non-Budgeted Storage Semantics

- **D-09:** Keep `failure_count` in v1, but shrink its meaning to
  "consecutive feed-budget-eligible failures only."
- **D-10:** Non-budgeted failure storage always sets `failure_count = 0`.
  Non-budgeted observations are not part of any feed quarantine episode and
  must clear old mixed-budget debt.
- **D-11:** `release_non_budgeted_failure(...)` writes `status='failing'`,
  `failure_count=0`, `retry_after`, and `status_reason`.
- **D-12:** `release_non_budgeted_failure(...)` releases the active lease
  (`worker_id = NULL`) and preserves scheduler metadata needed by the existing
  failing/recovery path, such as `unclaimed_since` if required by local SQL
  conventions.
- **D-13:** `release_non_budgeted_failure(...)` must never write
  `quarantine_reason`. Reset/progress flows own clearing quarantine-specific
  forensic data.
- **D-14:** `report_feed_failure(...)` remains the only storage path that
  increments the feed quarantine budget.
- **D-15:** Existing successful progress and `SourceObservation` stale-state
  clearing semantics must remain intact.

### Status Reason Vocabulary

- **D-16:** Add only status enum values needed by current code paths. Do not
  add speculative status reasons.
- **D-17:** For v1, add `pipeline_publish_after_bookmark_failed` because it has
  distinct semantics: capture/bookmark advanced, publish failed, a downstream
  data gap is known, and v1 has no replay.
- **D-18:** Allow the `pipeline_` prefix in `FeedStatusReason`, but keep it
  rare. `pipeline_*` means downstream post-capture consistency/replay
  semantics, not simply "code lives in the pipeline."
- **D-19:** `pipeline_*` status reasons must never increment feed budget and
  must never quarantine the feed.
- **D-20:** Status reason prefixes are operator taxonomy, not routing
  authority. Budget eligibility is decided by `FailurePolicyDecision`, not by
  `source_` / `system_` / `pipeline_` string prefixes.
- **D-21:** Do not rename `FeedStatusReason` in v1. The compatibility cost is
  higher than the benefit; document that it is an abnormal ingestion/feed
  processing reason, not necessarily feed health.

### the agent's Discretion

The agent may choose exact enum member names and helper function names within
the locked ownership boundary, as long as tests enforce that typed
`FeedFailure` requires evidence and that `pipeline_*` decisions cannot
increment feed budget.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project Scope

- `.planning/PROJECT.md` — Core value, v1 boundaries, and locked project-level
  decisions.
- `.planning/REQUIREMENTS.md` — Phase 1 requirement IDs and v2/out-of-scope
  boundaries.
- `.planning/ROADMAP.md` — Phase 1 scope, success criteria, and plan split.

### Codebase Maps

- `.planning/codebase/ARCHITECTURE.md` — Collector/runtime/storage boundaries
  and current coarse failure routing behavior.
- `.planning/codebase/CONCERNS.md` — Known quarantine-budget mismatch,
  post-bookmark Pub/Sub gap risk, and fragile storage/runtime areas.
- `.planning/codebase/STACK.md` — Runtime stack, storage stack, and local test
  guardrails.

### Source Files To Inspect

- `backend/pipeline/ingestion/models.py` — Current `FeedFailure` and capture
  boundary types.
- `backend/pipeline/ingestion/collectors/failure_classification.py` — Shared
  collector classification and item-to-feed promotion helpers.
- `backend/pipeline/ingestion/collector_runtime.py` — Runtime failure handling
  and `_PipelineFailure` behavior.
- `backend/pipeline/storage/feed_store.py` — `FeedStatusReason`,
  `report_feed_failure`, and feed lifecycle store methods.
- `backend/pipeline/storage/feed_queries.py` — Atomic feed lifecycle SQL,
  especially `REPORT_FAILURE_SQL`, `UPDATE_PROGRESS_SQL`, and
  `RECORD_SOURCE_OBSERVATION_SQL`.
- `backend/pipeline/ingestion/collectors/README.md` — Collector authoring
  boundary and failure-classification guidance.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets

- `FeedFailure` in `backend/pipeline/ingestion/models.py`: existing typed
  collector/runtime boundary; Phase 1 should make policy evidence required.
- `FailureClassification`, `ItemFailure`, and `ItemBatchOutcome` in
  `failure_classification.py`: existing pure classification pattern to mirror
  for policy decisions without side effects.
- `FeedStore.report_feed_failure(...)` and `REPORT_FAILURE_SQL`: existing
  budgeted quarantine path; should remain the only incrementing path.
- `UPDATE_PROGRESS_SQL` and `RECORD_SOURCE_OBSERVATION_SQL`: existing stale
  failure-state clearing paths; Phase 1 must preserve them.

### Established Patterns

- Storage operations use explicit SQL constants plus thin `FeedStore` methods.
  The non-budgeted release path should follow that pattern.
- Collector code should classify source-specific evidence before crossing the
  runtime boundary. Runtime should not parse raw reason strings.
- Runtime owns leases and side effects; helper/policy modules should stay pure
  unless already part of runtime execution.

### Integration Points

- New `failure_policy.py` integrates with `FeedFailure` and shared collector
  helpers.
- New storage SQL integrates beside `REPORT_FAILURE_SQL`, not by modifying the
  semantics of `report_feed_failure(...)`.
- New status enum value integrates through `FeedStatusReason` parsing and row
  mapping.

</code_context>

<specifics>
## Specific Ideas

- Do not add `reason_family` in Phase 1.
- Use `classify_failure_policy(status_reason, evidence)` as the pure
  classification boundary.
- Treat `pipeline_publish_after_bookmark_failed` as the only currently needed
  new `pipeline_*` status reason.
- Enforce in tests that `pipeline_*` decisions cannot increment feed budget or
  quarantine feeds.

</specifics>

<deferred>
## Deferred Ideas

- Durable publish outbox / hold-replay worker belongs to a later phase.
- Source-class / credential breaker persistence belongs to a later phase.
- Persistent structured policy audit table belongs to a later phase.
- Renaming `FeedStatusReason` can be reconsidered later if the compatibility
  surface is worth the churn.

</deferred>

---

*Phase: 1-Policy And Storage Foundation*
*Context gathered: 2026-06-15*

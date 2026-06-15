# Phase 2: Runtime Routing And Telemetry - Context

**Gathered:** 2026-06-15
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 2 routes runtime failures through the Phase 1 policy foundation. It
must make the runtime action follow
`status_reason + FailurePolicyEvidence -> classify_failure_policy(...) ->
FailurePolicyDecision`, so only feed-owned quarantine decisions can call
`report_feed_failure(...)`. All non-feed-actionable decisions use the
non-budgeted release path. Phase 2 also emits the v1 operational logs needed
to explain those decisions, including explicit post-bookmark publish-gap logs.

This phase does not add durable replay/outbox storage, real source-class
breaker persistence, canary probing, alert routing, a new lifecycle status, or
a persistent structured audit table.

</domain>

<decisions>
## Implementation Decisions

### Strict Failure Boundary

- **D-01:** Evidence-less `FeedFailure` remains invalid. Do not loosen the
  strict Phase 1 boundary to support old-style typed collector failures.
- **D-02:** The Phase 2 wording should split the old "unannotated
  FeedFailure" term into two separate behaviors: typed `FeedFailure` without
  evidence is rejected at the model boundary, while untyped runtime exceptions
  route to telemetry gap.
- **D-03:** Unknown runtime failures use explicit fallback evidence:
  `OwnerScope.UNKNOWN`, `FailureScope.UNKNOWN`, and `EndpointKind.UNKNOWN`.
  They route to `PolicyIntent.TELEMETRY_GAP` and the non-budgeted storage path.

### Breaker Intent Without Breaker State

- **D-04:** Keep `PolicyIntent.OPEN_BREAKER` for
  `OwnerScope.CREDENTIAL_SCOPE` and `OwnerScope.SOURCE_CLASS` decisions. This
  records the intended policy lane for shared/auth/source-class failures.
- **D-05:** In v1, `OPEN_BREAKER` does not mean breaker state exists. It must
  pair with `ExecutedAction.RELEASE_NON_BUDGETED_FAILURE`.
- **D-06:** Do not add breaker persistence, canary state, source-class pause
  mechanics, or alert routing in Phase 2. Those remain v2.
- **D-07:** Tests should assert the intent/action split so future maintainers
  cannot infer that `OPEN_BREAKER` executed a real breaker.

### Suppressed Retry Timing

- **D-08:** Keep non-budgeted retry timing intentionally coarse in v1. The
  current jittered 5-15 minute retry delay can remain as an implementation
  default, but Phase 2 should not make exact timing a rigid policy contract.
- **D-09:** Phase 2 tests should assert that `retry_after` is supplied and
  future/non-null where practical, not exact minute bounds.
- **D-10:** Do not add reason-specific or status-specific backoff in Phase 2.
  The important behavior is routing and budget isolation, not backoff tuning.

### Telemetry As Audit Output

- **D-11:** Telemetry must never be routing authority. Routing is based only on
  `status_reason`, `FailurePolicyEvidence`, and
  `failure_policy.classify_failure_policy(...)`.
- **D-12:** Prefer minimal stable telemetry for long-term maintenance. Emit
  enough to identify the decision and action: feed id, source type, status
  reason, policy intent, executed action, and the relevant evidence fields
  already needed by current code/tests.
- **D-13:** It is acceptable to keep richer existing telemetry fields if they
  are already implemented and tested, but planning should not add new fields
  just for completeness.
- **D-14:** `post_bookmark_publish_failure` must explicitly record
  `replay_missing=true` and `data_gap_known=true`. That event documents the
  v1 reality that bookmark advanced but durable replay is not available.
- **D-15:** Tests prove routing by asserting store method calls
  (`report_feed_failure(...)` versus `release_non_budgeted_failure(...)`).
  Telemetry tests only prove that the operational record mirrors the routing
  decision.

### the agent's Discretion

The agent may choose exact helper names, log payload ordering, and test class
split. Keep changes minimal and favor existing Phase 1 helpers already present
in `collector_runtime.py` and `failure_policy.py`.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project Scope

- `.planning/PROJECT.md` — Core value, v1 boundaries, and out-of-scope replay
  and breaker decisions.
- `.planning/REQUIREMENTS.md` — Phase 2 requirement IDs: POL-04, RUN-01
  through RUN-07, and TEL-01 through TEL-05.
- `.planning/ROADMAP.md` — Phase 2 goal, success criteria, and planned split.

### Prior Phase Decisions

- `.planning/phases/01-policy-and-storage-foundation/01-CONTEXT.md` — Locked
  Phase 1 decisions on strict evidence, no `reason_family`, non-budgeted
  storage, `pipeline_*` semantics, and status reason vocabulary.
- `.planning/phases/01-policy-and-storage-foundation/01-01-SUMMARY.md` —
  Policy/model/status primitives delivered in Phase 1.
- `.planning/phases/01-policy-and-storage-foundation/01-02-SUMMARY.md` —
  Collector evidence wiring and AST guard delivered in Phase 1.
- `.planning/phases/01-policy-and-storage-foundation/01-03-SUMMARY.md` —
  Non-budgeted storage primitive delivered in Phase 1.
- `.planning/phases/01-policy-and-storage-foundation/01-04-SUMMARY.md` —
  Storage recovery and increment-isolation verification delivered in Phase 1.

### Codebase Maps

- `.planning/codebase/ARCHITECTURE.md` — Runtime/collector/storage ownership
  boundaries and current feed failure routing risk.
- `.planning/codebase/CONCERNS.md` — Quarantine-budget mismatch,
  post-bookmark publish gap risk, paused ordering-key risk, and test gaps.
- `.planning/codebase/STACK.md` — Python/runtime/storage/test stack and local
  validation guardrails.

### Source Files To Inspect

- `backend/pipeline/ingestion/failure_policy.py` — Pure policy classifier,
  policy intents, executed actions, and predicates.
- `backend/pipeline/ingestion/models.py` — Strict `FeedFailure` boundary.
- `backend/pipeline/ingestion/collector_runtime.py` — Runtime routing,
  `_PipelineFailure`, non-budgeted release, and policy telemetry.
- `backend/pipeline/storage/feed_store.py` — `FeedStatusReason`,
  `report_feed_failure(...)`, and `release_non_budgeted_failure(...)`.
- `backend/pipeline/storage/feed_queries.py` — Budgeted and non-budgeted SQL
  paths.
- `backend/pipeline/ingestion/tests/test_collector_runtime.py` — Existing
  runtime tests, including partial Phase 2 behavior already added during
  Phase 1 execution.
- `backend/pipeline/ingestion/tests/test_failure_policy.py` — Policy decision
  tests that Phase 2 should extend only if needed.
- `backend/pipeline/common/gcp_helper.py` — Pub/Sub paused ordering-key
  resume handling and publish failure propagation.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets

- `failure_policy.classify_failure_policy(...)`: The routing source of truth.
  Phase 2 should call it and execute the returned decision; do not duplicate
  routing conditions in runtime.
- `CollectorRuntime._record_feed_failure(...)`: Existing budgeted path; Phase
  2 must guard this so only feed-owned quarantine decisions can reach it.
- `CollectorRuntime._record_non_budgeted_failure(...)`: Existing non-budgeted
  path using `FeedStore.release_non_budgeted_failure(...)`; Phase 2 should
  route all non-quarantine decisions here.
- `CollectorRuntime._emit_policy_decision(...)` and
  `_emit_post_bookmark_publish_failure(...)`: Existing telemetry helpers from
  Phase 1 execution. Prefer reconciling and testing them over replacing them.
- `FeedStore.release_non_budgeted_failure(...)`: Storage primitive that resets
  failure count and releases the lease without writing `quarantine_reason`.

### Established Patterns

- Runtime owns side effects. Policy stays pure.
- Storage uses explicit SQL constants and thin `FeedStore` wrappers.
- Tests should be focused and targeted with `safe-run --` and `-n 0`.
- Structured `json_fields` logs are the operational record for v1 because no
  audit table is added.

### Integration Points

- `FeedFailure` catch arm in `_process_feed`: classify the evidence and choose
  `_record_feed_failure` only for feed quarantine decisions.
- `_PipelineFailure` catch arm in `_process_feed`: always use non-budgeted
  release; Pub/Sub post-bookmark failures must set publish-gap flags.
- Generic `Exception` catch arm in `_process_feed`: synthesize UNKNOWN
  evidence and route to telemetry gap/non-budgeted release.
- Pub/Sub publish path in `_process_captured_chunk`: publish failures after
  bookmark should use
  `FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED`.

</code_context>

<specifics>
## Specific Ideas

- Phase 2 may be more of a reconciliation and test-hardening phase than a
  large implementation phase because the Phase 1 execution already introduced
  runtime routing and telemetry helpers.
- Keep exact retry delay tuning out of the Phase 2 contract.
- Keep telemetry simple unless richer fields are already present and
  low-maintenance.

</specifics>

<deferred>
## Deferred Ideas

- Durable publish outbox / hold-replay worker remains v2.
- Real source-class or credential breaker state remains v2.
- Breaker canary probes and alert routing remain v2.
- Persistent structured policy audit/event table remains v2.
- Rich UI/operator display of suppressed retry reasons remains a later
  operator-experience phase.

</deferred>

---

*Phase: 2-Runtime Routing And Telemetry*
*Context gathered: 2026-06-15*

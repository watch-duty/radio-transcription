# Phase 4: Strict Policy Table And Status Vocabulary - Context

**Gathered:** 2026-06-15
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 4 delivers the backend policy vocabulary and routing foundation for v1.1.
It replaces broad `OwnerScope` defaults in `failure_policy.py` with explicit,
fail-closed `status_reason + FailurePolicyEvidence` policy rows, and it adds
only the backend `FeedStatusReason` values needed for the current routing
split.

This phase does not update frontend/OpenAPI/generated/UI compatibility surfaces,
does not route runtime `_PipelineFailure` differently, does not split collector
producer mappings, and does not add replay, breaker state, audit tables,
database migrations, or `reason_family`.

</domain>

<decisions>
## Implementation Decisions

### Explicit Policy Rows

- **D-01:** `failure_policy.py` should encode routing as an explicit table or
  table-like list of status/evidence predicates. Do not preserve broad defaults
  such as "all credential scope opens breaker" or "all pipeline owner scope is
  non-budgeted" as the final routing mechanism.
- **D-02:** A policy row is keyed by `status_reason` plus an evidence predicate
  over existing facts: `owner_scope`, `failure_scope`, `endpoint_kind`, and
  optional `pipeline_stage`.
- **D-03:** If no policy row matches, route to `PolicyIntent.TELEMETRY_GAP`,
  `ExecutedAction.SUPPRESS_FEED_QUARANTINE_TELEMETRY_GAP`,
  `feed_budget_eligible=False`, and `quarantine_feed=False`.
- **D-04:** Do not raise in production for unmatched combinations. Tests should
  make missing rows visible, but runtime behavior should fail closed through
  the non-budgeted telemetry-gap decision.
- **D-05:** Keep policy pure. `failure_policy.py` returns decisions only; it
  must not call stores, Pub/Sub, telemetry, alerting, or collector-specific
  parsing code.
- **D-06:** Do not add `reason_family`. Current routing must be expressible from
  `status_reason` plus the existing evidence fields.

### Budgeted And Non-Budgeted Rows

- **D-07:** Quarantine-budgeted rows in Phase 4 are:
  `system_authentication_failed`, `system_configuration_invalid`,
  `system_runtime_configuration_invalid`, `system_source_payload_invalid`, and
  `pipeline_publish_after_bookmark_failed` when their required evidence row
  matches.
- **D-08:** Non-budgeted rows in Phase 4 are: `source_offline`,
  `source_unreachable`, `source_rate_limited`,
  `system_credential_access_failed`, broad `system_collector_error`, broad
  `system_pipeline_error`, and `system_unexpected_error`.
- **D-09:** `pipeline_publish_after_bookmark_failed` changes policy direction
  in v1.1: with Pub/Sub publish-stage pipeline evidence it should be
  quarantine-budgeted, using `PolicyIntent.QUARANTINE_FEED` and
  `ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET`. Runtime execution of that
  decision is Phase 5.
- **D-10:** `system_pipeline_error` remains non-budgeted in Phase 4 because it
  currently covers GCS upload and bookmark-write failures where retry/backoff
  should remain available.
- **D-11:** `system_credential_access_failed` remains non-budgeted by default
  because credential-store access may be transient. The table should make this
  easy to flip later with one row edit if production evidence proves retry
  cannot recover.

### Status Vocabulary Split

- **D-12:** Add backend enum values:
  `system_runtime_configuration_invalid`,
  `system_credential_access_failed`, and
  `system_source_payload_invalid`.
- **D-13:** Do not add speculative enum values. These three values are needed
  now because they make current routing clear for known producers.
- **D-14:** `system_configuration_invalid` remains feed-row/source-specific feed
  configuration invalid.
- **D-15:** `system_runtime_configuration_invalid` means shared runtime,
  deploy, environment, source-class, or transport configuration invalid.
- **D-16:** `system_credential_access_failed` means the collector/runtime failed
  to retrieve or access internal credentials, not that the upstream provider
  rejected those credentials.
- **D-17:** `system_source_payload_invalid` means a successful source response
  violates the collector's expected payload contract and repeating the same
  request is not expected to help.

### Compatibility Boundary

- **D-18:** Keep Phase 4 focused on backend policy and backend enum vocabulary.
  Do not update frontend types, UI labels, generated route metadata, or OpenAPI
  compatibility surfaces in this phase.
- **D-19:** Phase 4 tests should be scoped so backend policy/status behavior can
  be validated without forcing frontend/OpenAPI compatibility. Broader enum
  parity may be handled by follow-up compatibility work.
- **D-20:** Frontend/OpenAPI/generated/UI compatibility changes are deferred as
  follow-up compatibility work, currently represented by Phase 6 in the
  roadmap.

### the agent's Discretion

The agent may choose the exact policy-row representation, helper names, and
test parametrization. Prefer the smallest explicit structure that makes a
future route flip a one-row change and keeps unmatched combinations visibly
non-budgeted.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project Scope

- `.planning/PROJECT.md` — v1.1 core value, active milestone scope, and
  operator-actionable quarantine definition.
- `.planning/REQUIREMENTS.md` — Phase 4 requirements: `POL-11` through
  `POL-14`, `STAT-11` through `STAT-14`, `TEST-11`, and `TEST-12`.
- `.planning/ROADMAP.md` — Phase 4 goal, success criteria, and plan split.

### Prior Phase Decisions

- `.planning/phases/01-policy-and-storage-foundation/01-CONTEXT.md` — Locked
  foundation decisions on pure policy ownership, strict evidence, no
  `reason_family`, non-budgeted storage, and status vocabulary.
- `.planning/phases/02-runtime-routing-and-telemetry/02-CONTEXT.md` — Locked
  runtime-routing decisions and the v1 distinction between telemetry and
  routing authority.
- `.planning/phases/03-verification-and-compatibility/03-CONTEXT.md` — Prior
  compatibility/testing decisions; note that v1.1 changes the
  `pipeline_publish_after_bookmark_failed` route from non-budgeted to
  budgeted.

### Codebase Maps

- `.planning/codebase/ARCHITECTURE.md` — Collector/runtime/storage boundaries.
- `.planning/codebase/STACK.md` — Backend runtime and focused test tooling.
- `.planning/codebase/CONCERNS.md` — Quarantine-budget and policy mismatch
  risks.

### Source Files To Inspect

- `backend/pipeline/ingestion/failure_policy.py` — Current broad
  owner-scope-based classifier; primary Phase 4 implementation target.
- `backend/pipeline/storage/feed_store.py` — `FeedStatusReason` enum and feed
  status parsing.
- `backend/pipeline/ingestion/collectors/failure_classification.py` —
  `owner_scope_for_status_reason(...)` and evidence construction mapping that
  must understand new enum values.
- `backend/pipeline/ingestion/tests/test_failure_policy.py` — Existing policy
  tests to replace/extend with row-table and fail-closed cases.
- `backend/pipeline/ingestion/collectors/tests/test_failure_classification.py`
  — Owner-scope/evidence mapping tests for new statuses.
- `backend/pipeline/storage/tests/test_feed_store.py` — Backend status reason
  vocabulary tests; avoid expanding Phase 4 to frontend/OpenAPI compatibility
  unless planner explicitly scopes it.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets

- `FailurePolicyEvidence` and `FailurePolicyDecision` already have the right
  fields. Phase 4 should not reshape the evidence model.
- `PolicyIntent` and `ExecutedAction` already contain the required intents and
  actions for budgeted, non-budgeted, telemetry-gap, and publish-gap decisions.
- `FeedStatusReason` is a Python `StrEnum`; the database column is text, so new
  values do not require a DB migration.
- `policy_evidence_for_status_reason(...)` centralizes broad owner-scope
  defaults for collectors; it is the place to map new statuses to their default
  evidence owner scope.

### Established Patterns

- Policy modules stay pure and side-effect free.
- Collector/source code owns raw signal extraction; policy owns only
  structured intent classification.
- Runtime/store code owns execution of policy decisions.
- Focused tests should prove store/routing intent without running broad local
  stacks.

### Integration Points

- `classify_failure_policy(...)` should become the only policy route lookup.
- `owner_scope_for_status_reason(...)` needs new enum values so default
  evidence construction remains coherent.
- Existing policy predicates (`is_feed_quarantine`,
  `is_feed_budget_eligible`, `is_pipeline_hold`,
  `is_source_class_breaker`) should continue to work from the returned
  decision.
- Phase 5 will consume the Phase 4 decision table when routing
  `_PipelineFailure`; Phase 4 should make that route possible but not execute
  it in runtime yet.

</code_context>

<specifics>
## Specific Ideas

- Table rows should be easy to audit. A small tuple/dataclass list with
  status, predicate, intent, action, and budget booleans is acceptable.
- Tests should include an intentional mismatch such as
  `source_offline + owner_scope=FEED` to prove unsupported combinations fail
  closed to telemetry gap.
- Tests should cover `pipeline_publish_after_bookmark_failed` with Pub/Sub
  publish pipeline evidence as budgeted.
- Tests should cover `system_pipeline_error` with GCS/bookmark evidence as
  non-budgeted.
- Tests should verify that each current `FeedStatusReason` has at least one
  explicit route row or an intentional fallback expectation.

</specifics>

<deferred>
## Deferred Ideas

- Frontend/OpenAPI/generated/UI compatibility changes are deferred from Phase 4
  and should be handled as follow-up compatibility work.
- Runtime `_PipelineFailure` execution of the budgeted Pub/Sub publish route is
  Phase 5.
- Collector producer remapping for Calls, Fire Notifications, Icecast, and
  OpenMHz is Phase 5.
- Durable replay/outbox remains future work.
- Source-class/credential breaker state remains future work.
- Persistent structured audit events remain future work.

</deferred>

---

*Phase: 4-Strict Policy Table And Status Vocabulary*
*Context gathered: 2026-06-15*

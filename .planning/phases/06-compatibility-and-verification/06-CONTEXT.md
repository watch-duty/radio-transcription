# Phase 6: Compatibility And Verification - Context

**Gathered:** 2026-06-15
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 6 is now a backend-only milestone closeout, not a full API/UI
compatibility phase. It should update stale backend documentation that now
contradicts the v1.1 routing semantics, run focused backend verification, and
produce final phase/milestone evidence.

The current roadmap and requirements still name OpenAPI, generated API route
metadata, shared frontend TypeScript types, and UI status labels. The user
explicitly chose to defer those compatibility surfaces because this milestone
only needs backend changes. Downstream planning should not mark `COMP-11`
through `COMP-14` or `TEST-16` complete in this milestone; move or record them
as deferred follow-up compatibility work instead.

</domain>

<decisions>
## Implementation Decisions

### API/UI Compatibility Deferral

- **D-01:** Defer OpenAPI enum synchronization, generated TSOA route metadata,
  shared frontend status reason types, frontend allowlists, and UI status
  labels out of this backend-only milestone.
- **D-02:** Do not regenerate `frontend/api/openapi.yaml` or
  `frontend/api/src/generated/routes.ts` in Phase 6 unless a backend-only
  verification command unexpectedly requires it.
- **D-03:** Do not edit `frontend/common/src/types/feeds.ts`,
  `frontend/common/src/utils/statusUtils.ts`, or
  `frontend/transcription-ui/src/components/common/FeedStatusIndicator.tsx` in
  Phase 6.
- **D-04:** Treat `COMP-11`, `COMP-12`, `COMP-13`, `COMP-14`, and `TEST-16` as
  deferred compatibility requirements for a future frontend/API compatibility
  follow-up, not as Phase 6 success criteria for this milestone.

### Backend Documentation Closeout

- **D-05:** Update only the stale collector guide plus Phase 6 planning
  summaries/verification docs. The stale repo doc is
  `backend/pipeline/ingestion/collectors/README.md`.
- **D-06:** The collector guide must no longer say
  `pipeline_publish_after_bookmark_failed` "must not quarantine the feed."
  v1.1 intentionally routes Pub/Sub post-bookmark publish failures through the
  budgeted feed quarantine path because retry alone cannot repair the advanced
  bookmark/publish consistency gap.
- **D-07:** Keep the data-integrity wording: Pub/Sub post-bookmark publish
  failure still records a known downstream gap, and v1.1 still has no durable
  replay/outbox worker.

### Verification Boundary

- **D-08:** Use focused backend verification only. Do not require frontend
  typecheck/build/test, TSOA generation, OpenAPI parity, Docker, testcontainers,
  or broad local E2E/integration stacks for Phase 6.
- **D-09:** Phase 6 verification should include the final focused backend
  command from Phase 5, plus any backend docs hygiene check such as
  `git diff --check`.
- **D-10:** If existing tests that are normally part of full-suite CI still
  assert backend/OpenAPI parity, leave that as a documented deferred
  compatibility gap rather than expanding this milestone.

### the agent's Discretion

The agent may choose exact wording in the collector guide, exact backend-focused
verification commands, and whether to update project planning artifacts to make
the compatibility deferral explicit. Keep changes minimal and do not touch
frontend/generated/API compatibility files.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project Scope

- `.planning/PROJECT.md` — v1.1 core value, backend-only milestone direction,
  and out-of-scope items.
- `.planning/REQUIREMENTS.md` — contains the original Phase 6 compatibility
  requirements; downstream agents must account for the user-approved deferral
  in this context.
- `.planning/ROADMAP.md` — contains the original Phase 6 goal; downstream
  agents must account for the user-approved deferral in this context.

### Prior Phase Decisions

- `.planning/phases/04-strict-policy-table-and-status-vocabulary/04-CONTEXT.md`
  — Explicit policy rows, split status vocabulary, and backend-only Phase 4
  compatibility deferral.
- `.planning/phases/04-strict-policy-table-and-status-vocabulary/04-VERIFICATION.md`
  — Verified backend policy/status behavior and known compatibility deferrals.
- `.planning/phases/05-producer-and-runtime-routing-merge/05-CONTEXT.md` —
  Producer/runtime decisions and the Phase 6 hand-off.
- `.planning/phases/05-producer-and-runtime-routing-merge/05-VERIFICATION.md`
  — Final backend focused command and Phase 5 verified behavior.

### Codebase Maps

- `.planning/codebase/TESTING.md` — Focused backend validation expectations
  and local safety guidance.
- `.planning/codebase/CONVENTIONS.md` — Status reason conventions, collector
  documentation guidance, and focused test discipline.
- `.planning/codebase/STRUCTURE.md` — Backend/frontend/generated surface
  locations and where feed lifecycle/quarantine policy files live.

### Source Files To Inspect

- `backend/pipeline/ingestion/collectors/README.md` — Stale documentation to
  update; lines around the `_PipelineFailure` and pipeline-owned reason table
  still describe the old non-budgeted Pub/Sub route.
- `backend/pipeline/ingestion/failure_policy.py` — Authoritative v1.1 routing
  table.
- `backend/pipeline/ingestion/collector_runtime.py` — Runtime execution of
  `_PipelineFailure` decisions.
- `backend/pipeline/ingestion/tests/test_collector_runtime.py` — Focused
  runtime routing tests from Phase 5.
- `backend/pipeline/storage/tests/test_feed_store.py` — Backend storage/status
  tests; avoid frontend/OpenAPI parity expansion in this backend-only phase.

### Deferred Compatibility Surfaces

- `frontend/api/openapi.yaml` — Deferred OpenAPI enum synchronization.
- `frontend/api/src/generated/routes.ts` — Deferred TSOA generated route
  metadata synchronization.
- `frontend/common/src/types/feeds.ts` — Deferred shared TS status reason type
  synchronization.
- `frontend/common/src/utils/statusUtils.ts` — Deferred frontend status reason
  allowlist synchronization.
- `frontend/transcription-ui/src/components/common/FeedStatusIndicator.tsx` —
  Deferred UI label synchronization.
- `frontend/transcription-ui/src/components/common/FeedStatusIndicator.test.tsx`
  — Deferred frontend status reason display coverage.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets

- `backend/pipeline/ingestion/failure_policy.py` already defines
  `pipeline_publish_after_bookmark_failed` with Pub/Sub publish pipeline
  evidence as budgeted quarantine.
- `backend/pipeline/ingestion/collector_runtime.py` already records
  `replay_missing=true` and `data_gap_known=true` on the canonical policy
  decision event for post-bookmark Pub/Sub publish failures.
- Phase 5 final verification already provides the focused backend command to
  reuse for closeout.

### Established Patterns

- `status_reason` is canonical abnormal-condition state; `quarantine_reason`
  remains raw forensic detail and must not drive policy.
- Runtime/store behavior should be proven by store method calls and final row
  state before telemetry assertions.
- Collector docs should be updated when behavior changes make
  `backend/pipeline/ingestion/collectors/README.md` misleading.

### Integration Points

- The stale collector guide currently says runtime-side `_PipelineFailure`
  events route through non-budgeted pipeline lanes and that
  `pipeline_publish_after_bookmark_failed` must not quarantine the feed. That
  is wrong for v1.1 after Phase 5.
- Current frontend/OpenAPI surfaces still contain the older reason list and do
  not include `system_runtime_configuration_invalid`,
  `system_credential_access_failed`, or `system_source_payload_invalid`; this
  is intentionally deferred by the user for this backend-only milestone.

</code_context>

<specifics>
## Specific Ideas

- Update the collector guide to distinguish:
  - GCS upload and bookmark write `_PipelineFailure` events remain
    `system_pipeline_error` and non-budgeted.
  - Pub/Sub publish failure after bookmark uses
    `pipeline_publish_after_bookmark_failed`, records a known data gap, and is
    budgeted/quarantinable in v1.1.
- Keep wording precise: quarantine here means repeated failures cross the
  existing feed failure threshold; it does not mean a single Pub/Sub failure
  immediately quarantines the feed.
- Record in Phase 6 summary/verification that API/UI/generated compatibility is
  intentionally deferred from this backend-only milestone.

</specifics>

<deferred>
## Deferred Ideas

- `COMP-11`: Backend enum and OpenAPI `BackendFeedStatusReason`
  synchronization.
- `COMP-12`: Shared frontend status reason types and allowlists.
- `COMP-13`: Generated API route metadata synchronization.
- `COMP-14`: UI status indicator readable labels for new split reasons.
- `TEST-16`: API/UI/storage tests proving enum compatibility surfaces are
  synchronized.
- TSOA `generate-spec` / `generate-routes` work remains a future compatibility
  task, even though the repo has scripts for it.

</deferred>

---

*Phase: 6-Compatibility And Verification*
*Context gathered: 2026-06-15*

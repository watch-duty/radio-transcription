# Phase 3: Verification And Compatibility - Context

**Gathered:** 2026-06-15
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 3 proves the v1 quarantine policy behavior with focused storage,
runtime, and compatibility checks. It must close the pending test and status
compatibility requirements without changing the lifecycle model, adding replay,
adding source-class breaker state, creating a new audit table, or broadening
operator UI behavior.

This phase is verification-first. Code changes should be limited to missing
tests, missing API/UI/shared type tolerance for
`pipeline_publish_after_bookmark_failed`, and any small compatibility repair
needed to keep the latest `main` surfaces aligned with the backend enum.

</domain>

<decisions>
## Implementation Decisions

### Compatibility Surfaces

- **D-01:** Treat `pipeline_publish_after_bookmark_failed` as a backend status
  reason that compatibility surfaces must tolerate, not as a prompt for a
  broader operator UI redesign.
- **D-02:** The required compatibility target is backend/OpenAPI parity and
  compile/type compatibility across shared status surfaces. Update OpenAPI,
  shared TypeScript types, status conversion allowlists, and UI display text
  only where needed.
- **D-03:** Do not add frontend test coverage solely for the new pipeline
  status reason. Existing frontend tests should change only if they fail after
  the compatibility update.
- **D-04:** Preserve existing lifecycle behavior: `failing` and `quarantined`
  continue to map to UI `error`; do not add a new lifecycle status in Phase 3.

### Evidence Organization

- **D-05:** The Phase 3 implementation summary should use a hybrid evidence
  format: a requirement-indexed matrix as the authoritative checklist, plus
  scenario grouping for reviewer readability.
- **D-06:** The requirement matrix must map `STAT-02` and `TEST-01` through
  `TEST-08` to exact tests, files, or compatibility surfaces.
- **D-07:** The scenario grouping should cover the important policy behaviors:
  non-budgeted release, post-bookmark publish gap, feed-config quarantine,
  unknown telemetry gap, source/source-class non-quarantine routing, and
  non-quarantine telemetry suppression.
- **D-08:** Include the full original incident taxonomy as traceability, but
  map each incident category to a covered policy scenario rather than requiring
  a unique test for every historic label.
- **D-09:** Put full incident-taxonomy traceability in the Phase 3
  implementation summary, not in test comments, docstrings, or a new durable
  project document.

### the agent's Discretion

The agent may choose exact test names, assertion placement, and whether a
requirement is proven by an existing test or a new test. Keep tests
behavior-focused and avoid adding bespoke tests for duplicate incident labels
when the same policy lane is already covered.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project Scope

- `.planning/PROJECT.md` — Core value, active Phase 3 requirements, and
  validated Phase 1/2 decisions.
- `.planning/REQUIREMENTS.md` — Pending Phase 3 requirements: `STAT-02` and
  `TEST-01` through `TEST-08`.
- `.planning/ROADMAP.md` — Phase 3 goal, success criteria, and planned split.

### Prior Phase Decisions

- `.planning/phases/01-policy-and-storage-foundation/01-CONTEXT.md` — Locked
  decisions on strict evidence, no `reason_family`, non-budgeted storage,
  `pipeline_*` semantics, and status reason vocabulary.
- `.planning/phases/02-runtime-routing-and-telemetry/02-CONTEXT.md` — Locked
  decisions on runtime routing, non-budgeted execution, telemetry, and
  post-bookmark publish-gap semantics.

### Codebase Maps

- `.planning/codebase/TESTING.md` — Narrow verification expectations, test
  locations, and local test safety guidance.
- `.planning/codebase/CONVENTIONS.md` — Status reason and quarantine reason
  conventions, logging safety, and focused test discipline.
- `.planning/codebase/STRUCTURE.md` — Where feed lifecycle, runtime, storage,
  and frontend compatibility files live.

### Source Files To Inspect

- `backend/pipeline/ingestion/collector_runtime.py` — Runtime routing,
  policy telemetry, `_PipelineFailure`, and post-bookmark publish-gap behavior.
- `backend/pipeline/ingestion/failure_policy.py` — Pure policy classifier,
  policy intents, executed actions, and predicates.
- `backend/pipeline/ingestion/models.py` — Strict `FeedFailure` evidence
  boundary.
- `backend/pipeline/ingestion/tests/test_collector_runtime.py` — Runtime
  behavior tests and likely home for Phase 3 routing assertions.
- `backend/pipeline/storage/feed_store.py` — `FeedStatusReason`,
  `report_feed_failure(...)`, and `release_non_budgeted_failure(...)`.
- `backend/pipeline/storage/feed_queries.py` — SQL for budgeted and
  non-budgeted feed lifecycle writes.
- `backend/pipeline/storage/tests/test_feed_store.py` — Storage state tests
  and OpenAPI enum parity tests.
- `frontend/api/openapi.yaml` — OpenAPI status reason enum surface.
- `frontend/common/src/types/feeds.ts` — Shared frontend
  `BackendFeedStatusReason` type.
- `frontend/common/src/utils/statusUtils.ts` — Status reason conversion
  allowlist.
- `frontend/transcription-ui/src/components/common/FeedStatusIndicator.tsx` —
  Existing UI display mapping for status reasons.
- `frontend/transcription-ui/src/components/common/FeedStatusIndicator.test.tsx`
  — Existing frontend status tooltip tests; update only if existing behavior
  breaks.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets

- `FeedStore.release_non_budgeted_failure(...)`: Storage primitive that should
  prove `status='failing'`, `failure_count=0`, `retry_after`, and
  `status_reason` without writing `quarantine_reason`.
- `FeedStore.report_feed_failure(...)`: The only feed quarantine budget
  increment path; Phase 3 tests should distinguish this from non-budgeted
  release.
- `CollectorRuntime._record_non_budgeted_failure(...)`: Runtime path for
  non-feed-actionable decisions.
- `CollectorRuntime._record_feed_failure(...)`: Runtime path for
  feed-actionable quarantine decisions.
- `CollectorRuntime._emit_policy_decision(...)` and
  `_emit_post_bookmark_publish_failure(...)`: Existing structured logs that
  should be asserted where event contracts matter.
- `TestFeedStatusReason.test_matches_openapi_spec`: Existing backend parity
  test that enforces OpenAPI enum alignment.

### Established Patterns

- Verification should use narrow local commands with `safe-run --`; avoid
  broad local E2E, Docker, or integration stacks unless explicitly requested.
- Runtime policy behavior is proven by store method calls first, telemetry
  fields second.
- Storage behavior is proven by exact DB/query state assertions, especially
  `failure_count`, `status`, `retry_after`, `status_reason`, `worker_id`, and
  `quarantine_reason`.
- Compatibility changes should follow existing generated/spec surfaces instead
  of inventing a new operator status model.

### Integration Points

- Backend enum compatibility connects `FeedStatusReason` to
  `frontend/api/openapi.yaml`.
- Frontend type compatibility connects OpenAPI/shared status values to
  `frontend/common/src/types/feeds.ts` and
  `frontend/common/src/utils/statusUtils.ts`.
- Operator display compatibility connects the status reason value to
  `FeedStatusIndicator.tsx` without changing lifecycle status mapping.
- Phase 3 evidence connects test outcomes and compatibility files back to
  `STAT-02` and `TEST-01` through `TEST-08`.

</code_context>

<specifics>
## Specific Ideas

- The implementation summary should contain two tables:
  1. requirement-to-proof mapping for `STAT-02` and `TEST-01` through
     `TEST-08`
  2. incident-category-to-policy-scenario mapping for the full original
     quarantine taxonomy
- Do not make the incident taxonomy table a reason to add one-off tests for
  every historic label.
- Do not add Phase 3 frontend tests solely for
  `pipeline_publish_after_bookmark_failed`.

</specifics>

<deferred>
## Deferred Ideas

- Durable publish outbox / hold-replay worker remains v2.
- Real source-class or credential breaker state remains v2.
- Persistent structured policy audit/event table remains v2.
- Rich operator UX for suppressed retry states remains a later
  operator-experience phase.
- A dedicated long-lived incident taxonomy document can be reconsidered after
  v1 if the implementation summary is not enough for reviews.

</deferred>

---

*Phase: 3-Verification And Compatibility*
*Context gathered: 2026-06-15*

# Roadmap: Evidence-Based Quarantine Policy v1.1

## Overview

This milestone merges the latest strict quarantine policy design into the
current codebase. The work keeps the existing database schema, preserves
collector-owned source classification, and makes one central policy table decide
which `status_reason + evidence` combinations may consume feed quarantine
budget.

The roadmap continues numbering from the completed v1.0 phases. Phases 1-3 are
complete and retained in the project history; this milestone starts at Phase 4.

## Phases

- [ ] **Phase 4: Strict Policy Table And Status Vocabulary** - Replace broad
  routing defaults with explicit policy rows and add the currently needed
  status reason split values.
- [ ] **Phase 5: Producer And Runtime Routing Merge** - Update collector/runtime
  producers and route `_PipelineFailure` through the same budgeted/non-budgeted
  policy branch as collector failures.
- [ ] **Phase 6: Compatibility And Verification** - Update backend
  documentation and run focused verification for the merged behavior.

## Phase Details

### Phase 4: Strict Policy Table And Status Vocabulary

**Goal**: The policy module has explicit, fail-closed routing rows and the
backend status enum can express the split root-cause categories needed for
v1.1.

**Depends on**: Completed v1.0 Phase 3

**Requirements**: POL-11, POL-12, POL-13, POL-14, STAT-11, STAT-12, STAT-13,
STAT-14, TEST-11, TEST-12

**Success Criteria**:
1. Policy tests prove every current status reason has an intended route.
2. Unmatched status/evidence combinations return telemetry-gap non-budgeted
   release.
3. New enum values exist only for currently needed routing clarity.
4. No `reason_family` field or routing dependency is introduced.

**Plans**: 2 plans

Plans:
**Wave 1**
- [x] 04-01: Add policy-table tests and implement explicit fail-closed routing.

**Wave 2** *(blocked on Wave 1 completion)*
- [x] 04-02: Add split backend status reasons and update status/evidence owner
  mapping tests.

### Phase 5: Producer And Runtime Routing Merge

**Goal**: Current producers emit the more precise status reasons, and runtime
uses policy decisions consistently for collector and pipeline failures.

**Depends on**: Phase 4

**Requirements**: RUN-11, RUN-12, RUN-13, RUN-14, RUN-15, RUN-16, TEST-13,
TEST-14, TEST-15

**Success Criteria**:
1. Calls, Fire Notifications, Icecast, and OpenMHz produce the split enum values
   for the agreed root causes.
2. `pipeline_publish_after_bookmark_failed` calls `report_feed_failure(...)`
   and respects the existing failure threshold.
3. GCS upload, bookmark write, source observations, ambiguous collector errors,
   credential-access failures, and telemetry gaps remain non-budgeted.
4. The old special post-bookmark publish telemetry event is not required for
   v1.1 behavior.

**Plans**: 3 plans

Plans:
- [x] 05-01: Split source producer mappings and update collector tests.
- [x] 05-02: Route `_PipelineFailure` through policy and update runtime tests.
- [x] 05-03: Verify non-budgeted reset semantics and quarantine telemetry
  boundaries.

### Phase 6: Compatibility And Verification

**Goal**: Backend documentation reflects the final v1.1 policy semantics, and
focused backend checks prove the milestone is ready for code review.

**Depends on**: Phase 5

**Requirements**: DOC-11, VER-11, TEST-17

**Success Criteria**:
1. Collector authoring documentation no longer describes
   `pipeline_publish_after_bookmark_failed` as non-quarantining.
2. Focused backend tests and diff hygiene checks pass.
3. Phase summary/verification documents record that OpenAPI, generated API,
   shared frontend types, and UI labels are deferred follow-up work.
4. No frontend, OpenAPI, generated API metadata, database migration, or schema
   files are changed.

**Plans**: 2 plans

Plans:
- [ ] 06-01: Update backend collector documentation for final v1.1 semantics.
- [ ] 06-02: Run focused verification and update implementation summary/docs.

## Progress

**Execution Order:**
Phases execute in numeric order: 4 -> 5 -> 6

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 4. Strict Policy Table And Status Vocabulary | 2/2 | Complete | 2026-06-15 |
| 5. Producer And Runtime Routing Merge | 3/3 | Complete | 2026-06-15 |
| 6. Compatibility And Verification | 0/2 | Pending | — |

---
*Roadmap created: 2026-06-15*
*Last updated: 2026-06-15 after v1.1 milestone initialization*

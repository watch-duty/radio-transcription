# Roadmap: Evidence-Based Quarantine Policy

## Overview

This roadmap turns the quarantine redesign into a narrow v1 implementation:
first add the policy contract and non-budgeted storage primitive, then route
runtime failures through explicit policy decisions and telemetry, then close
the work with focused tests and compatibility checks. The v1 scope avoids a DB
migration, durable replay, and actual source-class breaker state.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned v1 work.
- Decimal phases: Urgent insertions, if needed later.

- [ ] **Phase 1: Policy And Storage Foundation** - Add structured policy
  primitives, status reason support, and the non-budgeted storage path.
- [ ] **Phase 2: Runtime Routing And Telemetry** - Route non-actionable
  failures away from quarantine and emit explicit policy/data-gap events.
- [ ] **Phase 3: Verification And Compatibility** - Prove behavior with narrow
  tests and update compatibility surfaces only where needed.

## Phase Details

### Phase 1: Policy And Storage Foundation
**Goal**: The codebase has explicit policy evidence types and a storage method
for suppressed retry states that cannot consume quarantine budget.
**Depends on**: Nothing (first phase)
**Requirements**: POL-01, POL-02, POL-03, STORE-01, STORE-02, STORE-03, STORE-04, STORE-05, STORE-06, STAT-01
**Success Criteria** (what must be TRUE):
  1. Runtime-facing failure decisions can be represented without parsing raw reason text.
  2. Storage can release a feed into `failing` with `failure_count=0`, `retry_after`, and status reason.
  3. The non-budgeted storage path never writes `quarantine_reason`.
  4. Existing progress and `SourceObservation` recovery semantics remain intact.
**Plans**: 3 plans

Plans:
- [ ] 01-01: Add policy evidence and status reason primitives.
- [ ] 01-02: Add non-budgeted storage SQL and `FeedStore` method.
- [ ] 01-03: Add foundation storage tests and preserve recovery semantics.

### Phase 2: Runtime Routing And Telemetry
**Goal**: Runtime failure handling routes each failure to a policy decision,
uses the non-budgeted path for non-feed-actionable conditions, and records
post-bookmark publish gaps explicitly.
**Depends on**: Phase 1
**Requirements**: POL-04, RUN-01, RUN-02, RUN-03, RUN-04, RUN-05, RUN-06, RUN-07, TEL-01, TEL-02, TEL-03, TEL-04, TEL-05
**Success Criteria** (what must be TRUE):
  1. Only feed-owned `quarantine_feed` decisions can call `report_feed_failure(...)`.
  2. Pipeline-owned, source-class, shared-auth, source-offline, rate-limit, unknown, and telemetry-gap decisions use the non-budgeted path.
  3. Post-bookmark Pub/Sub publish failure records both hold-for-replay intent and v1 replay-missing reality.
  4. Non-budgeted decisions never emit `feed_quarantined`.
**Plans**: 3 plans

Plans:
- [ ] 02-01: Add runtime policy routing helper and budgeted-quarantine guard.
- [ ] 02-02: Route pipeline and non-actionable source/system failures through suppressed retry.
- [ ] 02-03: Emit policy decision and post-bookmark publish-gap telemetry.

### Phase 3: Verification And Compatibility
**Goal**: The behavior is covered by focused tests and any affected API/UI/doc
surfaces tolerate the new status reason without broad lifecycle changes.
**Depends on**: Phase 2
**Requirements**: STAT-02, TEST-01, TEST-02, TEST-03, TEST-04, TEST-05, TEST-06, TEST-07, TEST-08
**Success Criteria** (what must be TRUE):
  1. Storage and runtime tests prove non-budgeted paths cannot increment quarantine budget.
  2. Tests prove post-bookmark publish gaps emit both policy and data-gap telemetry.
  3. Tests prove feed-config quarantine-eligible failures still use the budgeted path.
  4. Shared status/API/UI surfaces tolerate `pipeline_publish_after_bookmark_failed`.
  5. Narrow verification commands pass without running broad local stacks.
**Plans**: 3 plans

Plans:
- [ ] 03-01: Complete focused storage and runtime tests.
- [ ] 03-02: Update status compatibility surfaces and documentation if required.
- [ ] 03-03: Run narrow verification and prepare implementation summary.

## Progress

**Execution Order:**
Phases execute in numeric order: 1 -> 2 -> 3

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Policy And Storage Foundation | 0/3 | Not started | - |
| 2. Runtime Routing And Telemetry | 0/3 | Not started | - |
| 3. Verification And Compatibility | 0/3 | Not started | - |

---
*Roadmap created: 2026-06-14*
*Last updated: 2026-06-14 after initialization*

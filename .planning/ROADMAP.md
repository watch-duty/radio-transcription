# Roadmap: Feed Audit Events V1

## Overview

Feed Audit Events V1 establishes a durable backend audit ledger beside the existing current-state `feeds` model. The work starts by locking down the event contract and schema, then moves through transactional storage writes, service/API compatibility, runtime and Echo event integration, and final retention plus verification hardening.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Contract and Schema Foundation** - Define the Feed Audit Event contract, canonical diagnostic detail, deletion-safe audit schema, actor vocabulary, and per-feed ordering foundation.
- [ ] **Phase 2: Transactional Storage Writes** - Make admin/storage feed mutations create audit rows atomically with current-state changes.
- [ ] **Phase 3: Service and Compatibility Surface** - Preserve existing API/BFF/frontend compatibility while carrying trusted admin actor context and exposing canonical diagnostic detail.
- [ ] **Phase 4: Runtime Event Integration** - Add runtime, failure, quarantine, recovery, and Echo audit semantics without polluting history with lease churn.
- [ ] **Phase 5: Retention and Verification Hardening** - Enforce 18-month retention and prove the audit contract with focused automated tests.

## Phase Details

### Phase 1: Contract and Schema Foundation
**Goal**: The repository has a shared Feed Audit Event contract and database foundation that future storage, runtime, delivery, and timeline work can rely on.
**Depends on**: Nothing (first phase)
**Requirements**: AUD-02, AUD-03, DIAG-01, ACT-01, DOC-01, DOC-02, DOC-03
**Success Criteria** (what must be TRUE):
  1. The Feed Audit Event contract defines the action vocabulary, actor vocabulary, current-state versus audit-history terminology, diagnostic-detail semantics, retention policy, and v1 boundaries.
  2. The audit schema can identify an affected feed without relying on the current `feeds` row continuing to exist.
  3. The schema and contract support occurred time plus a stable per-feed sequence that future timelines can order by.
  4. The current feed schema exposes `status_reason_detail` as the canonical bounded diagnostic detail field.
  5. Future Watch Duty delivery and admin timeline work can derive consumer payloads from the domain audit contract without changing v1 audit meaning.
**Plans**: 3 plans

Plans:
**Wave 1**
- [x] 01-01-PLAN.md - Domain contract documentation and repository terminology.
- [x] 01-02-PLAN.md - SQL migration and HOT guard schema foundation.

**Wave 2** *(blocked on Wave 1 completion)*
- [x] 01-03-PLAN.md - Text-level contract verification tests.

### Phase 2: Transactional Storage Writes
**Goal**: Storage-owned feed mutations persist current-state changes and their audit events together for admin and service lifecycle actions.
**Depends on**: Phase 1
**Requirements**: AUD-04, EVT-01, EVT-02, EVT-03, EVT-04, EVT-05, CON-01, CON-02, CON-03, CON-04
**Success Criteria** (what must be TRUE):
  1. Feed create, meaningful update, deactivate, reset, and delete mutations each emit the expected audit event from the storage boundary.
  2. Audit rows preserve meaningful before and after values for audited changes.
  3. Feed deletion records a `feed.deleted` audit event before current-state storage removes the row.
  4. A successful audited mutation and its audit row commit together, while a failed or rolled-back mutation leaves no audit row behind.
  5. Concurrent audited mutations for the same feed produce unique deterministic per-feed ordering without service or runtime callers inserting audit rows directly.
**Plans**: 4 plans

Plans:
**Wave 1**
- [x] 02-01-PLAN.md - Contract/schema cleanup and audit SQL foundation.

**Wave 2** *(blocked on Wave 1 completion)*
- [ ] 02-02-PLAN.md - Transactional create/update writes and service actor fallback.

**Wave 3** *(blocked on Wave 2 completion)*
- [ ] 02-03-PLAN.md - Transactional deactivate/reset/delete writes.

**Wave 4** *(blocked on Wave 3 completion)*
- [ ] 02-04-PLAN.md - Rollback/concurrency integration coverage and final hardening.

### Phase 3: Service and Compatibility Surface
**Goal**: Existing feed API consumers remain compatible while admin-initiated changes carry trusted actor identity and canonical diagnostic detail.
**Depends on**: Phase 2
**Requirements**: DIAG-04, ACT-02, COMP-01, COMP-02, COMP-03
**Success Criteria** (what must be TRUE):
  1. Admin-initiated feed mutations preserve the authenticated admin identity when it is available at the trusted service boundary.
  2. Existing feed API callers continue receiving the current fields they depend on during the compatibility window.
  3. Feed API responses expose `status_reason_detail` without breaking existing clients.
  4. Existing BFF/frontend feed status, status-reason, and `quarantine_reason` behavior remains compatible with the backend change.
  5. Actor attribution cannot be forged through untrusted request body fields.
**Plans**: TBD
**UI hint**: yes

### Phase 4: Runtime Event Integration
**Goal**: Runtime and Echo paths produce the meaningful failure, quarantine, recovery, and no-noise audit behavior promised by v1.
**Depends on**: Phase 3
**Requirements**: AUD-01, EVT-06, EVT-07, EVT-08, EVT-09, DIAG-02, DIAG-03, ACT-03, COMP-04
**Success Criteria** (what must be TRUE):
  1. Across storage and runtime paths, the complete v1 set of meaningful feed mutations produces durable audit history.
  2. Persisted non-terminal feed failures emit failure audit events and set bounded diagnostic detail.
  3. A failure that crosses the quarantine threshold emits one `feed.quarantined` outcome event rather than duplicate failure and quarantine events.
  4. Successful runtime activity that clears previously persisted abnormal state emits a recovery audit event and clears diagnostic detail; clean success does not emit an audit event.
  5. Echo/sync ingestion paths receive equivalent v1 audit coverage, while routine worker lease churn, heartbeats, and clean progress paths do not emit default audit events.
**Plans**: TBD

### Phase 5: Retention and Verification Hardening
**Goal**: Feed audit events are retained for the required window and the implementation is proven against the v1 behavioral contract.
**Depends on**: Phase 4
**Requirements**: AUD-05, VER-01, VER-02, VER-03, VER-04, VER-05
**Success Criteria** (what must be TRUE):
  1. Audit rows are retained for 18 months and expired only through the approved retention mechanism.
  2. Automated tests verify audit events for feed create, update, deactivate, reset, delete, failure, quarantine, and recovery paths.
  3. Automated tests verify transaction rollback behavior and concurrent per-feed event ordering.
  4. Automated tests verify diagnostic-detail lifecycle, compatibility alias behavior, secret/detail bounding, delete-survival, and retention behavior.
  5. Automated tests verify that lease churn and clean heartbeat or progress paths do not emit default audit events.
**Plans**: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 1 -> 2 -> 3 -> 4 -> 5

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Contract and Schema Foundation | 3/3 | Complete | 2026-06-19 |
| 2. Transactional Storage Writes | 0/4 | Ready to execute | - |
| 3. Service and Compatibility Surface | 0/TBD | Not started | - |
| 4. Runtime Event Integration | 0/TBD | Not started | - |
| 5. Retention and Verification Hardening | 0/TBD | Not started | - |

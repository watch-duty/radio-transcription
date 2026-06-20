# Requirements: Feed Audit Events V1

**Defined:** 2026-06-19
**Core Value:** Operators can reconstruct meaningful feed lifecycle and
configuration changes from durable backend data instead of relying on
short-lived logs.

## v1 Requirements

### Audit History

- [x] **AUD-01**: The system records durable audit history for meaningful feed
  creation, configuration, lifecycle, failure, quarantine, recovery, reset,
  deactivation, and deletion events.
- [x] **AUD-02**: Each audited event identifies the affected feed even when the
  current feed row is later deleted.
- [x] **AUD-03**: Each audited event records when the event occurred and has a
  stable per-feed ordering that future timelines can use.
- [x] **AUD-04**: Audit history preserves the meaningful values before and
  after each audited change.
- [ ] **AUD-05**: Audit rows are retained for 18 months and expired only by the
  approved retention mechanism.

### Event Semantics

- [x] **EVT-01**: Feed creation emits one audit event.
- [x] **EVT-02**: Meaningful feed configuration changes emit audit events.
- [x] **EVT-03**: Feed deactivation emits one audit event.
- [x] **EVT-04**: Feed reset emits one audit event.
- [x] **EVT-05**: Feed deletion emits one audit event before the feed is
  removed from current-state storage.
- [x] **EVT-06**: Persisted non-terminal feed failures emit failure audit
  events.
- [x] **EVT-07**: A failure that causes quarantine emits one quarantine outcome
  event rather than duplicate failure and quarantine events for the same state
  change.
- [x] **EVT-08**: A feed emits a recovery audit event when successful runtime
  activity clears previously persisted abnormal failure state.
- [x] **EVT-09**: Routine worker lease churn, heartbeats, and clean successful
  runtime activity do not emit default audit events.

### Diagnostic Detail

- [x] **DIAG-01**: Current feed state includes a canonical bounded diagnostic
  detail field that can explain abnormal status for both quarantine and
  non-quarantine failures.
- [x] **DIAG-02**: Diagnostic detail follows the same lifecycle as the typed
  status reason: it is set with abnormal state and cleared when abnormal state
  is cleared.
- [x] **DIAG-03**: Persisted diagnostic detail is bounded and does not retain
  secrets, credentials, or unbounded provider responses.
- [x] **DIAG-04**: Existing feed API/BFF/frontend flows continue working while
  diagnostic-detail consumers migrate from legacy `quarantine_reason` to
  canonical `status_reason_detail`.

### Actor Attribution

- [x] **ACT-01**: Each audit event attributes the cause to a human admin,
  service component, runtime worker, scheduled job, or explicit unknown actor.
- [x] **ACT-02**: Admin-initiated feed mutations preserve the authenticated
  admin identity when that identity is available at the trusted service
  boundary.
- [x] **ACT-03**: Runtime-generated feed events use stable system actor values
  that distinguish runtime, source-specific, and service-originated changes.

### Consistency

- [x] **CON-01**: A successful audited feed mutation and its audit event commit
  together.
- [x] **CON-02**: A failed or rolled-back feed mutation does not leave behind an
  audit event.
- [x] **CON-03**: Concurrent audited mutations for the same feed preserve a
  unique, deterministic per-feed order.
- [x] **CON-04**: Audit event creation is owned by backend storage boundaries so
  service and runtime callers cannot accidentally create state/history drift.

### Compatibility

- [x] **COMP-01**: Existing feed API callers continue to receive the current
  fields they depend on during the compatibility window.
- [x] **COMP-02**: Feed API responses expose the new canonical diagnostic detail
  without breaking existing clients.
- [x] **COMP-03**: Existing frontend/BFF feed status and status-reason behavior
  remains compatible with the v1 backend change.
- [x] **COMP-04**: Echo and other sync ingestion paths receive equivalent audit
  coverage for the v1 event types they can produce.

### Contract And Documentation

- [x] **DOC-01**: Repository documentation defines the Feed Audit Event concept,
  action vocabulary, actor vocabulary, diagnostic-detail semantics, retention
  policy, and v1 boundaries.
- [x] **DOC-02**: The contract is written so future Watch Duty backend delivery
  and admin timeline work can derive consumer payloads without changing the
  v1 audit meaning.
- [x] **DOC-03**: Repository terminology distinguishes current feed state,
  audit history, typed status reasons, diagnostic detail, and the deprecated
  legacy quarantine reason field.

### Verification

- [ ] **VER-01**: Automated tests verify audit events for feed create, update,
  deactivate, reset, delete, failure, quarantine, and recovery paths.
- [ ] **VER-02**: Automated tests verify transaction rollback behavior and
  concurrent per-feed event ordering.
- [ ] **VER-03**: Automated tests verify diagnostic-detail lifecycle,
  public API migration away from `quarantine_reason`, and secret/detail
  bounding behavior.
- [ ] **VER-04**: Automated tests verify delete-survival and retention behavior.
- [ ] **VER-05**: Automated tests verify that lease churn and clean heartbeat or
  progress paths do not emit default audit events.

## v2 Requirements

### Delivery

- **DEL-01**: The Watch Duty backend can receive a stable subset of Feed Audit
  Events through an idempotent delivery path.
- **DEL-02**: Delivery attempts, retries, dead-letter state, and webhook
  signatures are observable without mutating canonical audit event meaning.

### Admin Timeline

- **TIME-01**: Authorized admins can query a paginated per-feed audit timeline.
- **TIME-02**: Authorized admins can filter feed audit history by event type,
  status reason, actor, and time window.
- **TIME-03**: The admin UI presents feed audit history in a readable timeline.

### Extended Auditability

- **EXT-01**: Additional feed event metadata can be added when it has stable
  source-of-truth meaning and does not expose secrets.
- **EXT-02**: Other domains such as rules, transcripts, audio segments, or
  notifications can define their own audit ledgers if product requirements
  demand them.

## Out of Scope

| Feature | Reason |
|---------|--------|
| Watch Duty backend webhook delivery | Requires receiver contract, signatures, retries, and delivery state; v1 only establishes durable audit data. |
| Admin timeline read APIs and UI | Requires product and authorization design beyond write-only v1. |
| Full feed event sourcing | Existing runtime and service paths depend on `feeds` as authoritative current state. |
| Routine lease/heartbeat event history | High-noise scheduler mechanics do not answer the Linear auditability need. |
| Synthetic baseline events for existing feeds | Would create misleading history at rollout time. |
| Cross-domain audit ledgers | This milestone is feed-only. |
| Tamper-evident or immutable archive storage | No compliance requirement currently justifies the added operational complexity. |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| AUD-01 | Phase 4 | Complete |
| AUD-02 | Phase 1 | Complete |
| AUD-03 | Phase 1 | Complete |
| AUD-04 | Phase 2 | Complete |
| AUD-05 | Phase 5 | Pending |
| EVT-01 | Phase 2 | Complete |
| EVT-02 | Phase 2 | Complete |
| EVT-03 | Phase 2 | Complete |
| EVT-04 | Phase 2 | Complete |
| EVT-05 | Phase 2 | Complete |
| EVT-06 | Phase 4 | Complete |
| EVT-07 | Phase 4 | Complete |
| EVT-08 | Phase 4 | Complete |
| EVT-09 | Phase 4 | Complete |
| DIAG-01 | Phase 1 | Complete |
| DIAG-02 | Phase 4 | Complete |
| DIAG-03 | Phase 4 | Complete |
| DIAG-04 | Phase 3 | Complete |
| ACT-01 | Phase 1 | Complete |
| ACT-02 | Phase 3 | Complete |
| ACT-03 | Phase 4 | Complete |
| CON-01 | Phase 2 | Complete |
| CON-02 | Phase 2 | Complete |
| CON-03 | Phase 2 | Complete |
| CON-04 | Phase 2 | Complete |
| COMP-01 | Phase 3 | Complete |
| COMP-02 | Phase 3 | Complete |
| COMP-03 | Phase 3 | Complete |
| COMP-04 | Phase 4 | Complete |
| DOC-01 | Phase 1 | Complete |
| DOC-02 | Phase 1 | Complete |
| DOC-03 | Phase 1 | Complete |
| VER-01 | Phase 5 | Pending |
| VER-02 | Phase 5 | Pending |
| VER-03 | Phase 5 | Pending |
| VER-04 | Phase 5 | Pending |
| VER-05 | Phase 5 | Pending |

**Coverage:**
- v1 requirements: 37 total
- Mapped to phases: 37
- Unmapped: 0

---
*Requirements defined: 2026-06-19*
*Last updated: 2026-06-19 after roadmap creation*

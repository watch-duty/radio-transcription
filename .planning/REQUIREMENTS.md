# Requirements: Evidence-Based Quarantine Policy

**Defined:** 2026-06-14
**Core Value:** On-call should be alerted only when the quarantined feed is likely something a human can fix at feed scope.

## v1 Requirements

### Policy Evidence

- [x] **POL-01**: Runtime failure routing uses structured policy evidence fields rather than `quarantine_reason` or raw reason text for quarantine and alert decisions.
- [x] **POL-02**: The policy evidence model includes `owner_scope`, `failure_scope`, `endpoint_kind`, `policy_intent`, and `executed_action`.
- [x] **POL-03**: The policy evidence model includes pipeline stage detail for pipeline-owned failures.
- [x] **POL-04**: Unannotated `FeedFailure` instances route to a non-budgeted telemetry-gap decision.

### Storage State

- [x] **STORE-01**: Storage exposes a non-budgeted failure release method that releases the lease and writes `status='failing'`.
- [x] **STORE-02**: The non-budgeted failure release method always writes `failure_count=0`.
- [x] **STORE-03**: The non-budgeted failure release method writes `retry_after` and `status_reason`.
- [x] **STORE-04**: The non-budgeted failure release method never writes `quarantine_reason`.
- [x] **STORE-05**: `report_feed_failure(...)` remains the only path that increments the feed quarantine budget.
- [x] **STORE-06**: Successful chunk progress and `SourceObservation` continue to clear stale failure count and status reason state.

### Runtime Routing

- [x] **RUN-01**: Runtime calls `report_feed_failure(...)` only when policy intent is `quarantine_feed` and owner scope is `feed`.
- [x] **RUN-02**: Runtime routes source-offline, shared-auth, rate-limit, capture-timeout, unknown, source-class, and pipeline-owned decisions through the non-budgeted path.
- [x] **RUN-03**: Pub/Sub publish failure after bookmark uses the non-budgeted path with status reason `pipeline_publish_after_bookmark_failed`.
- [x] **RUN-04**: Pub/Sub publish failure after bookmark records `policy_intent=hold_for_replay`.
- [x] **RUN-05**: Pub/Sub publish failure after bookmark records `executed_action=suppress_feed_quarantine_record_publish_gap`.
- [x] **RUN-06**: Pub/Sub publish failure after bookmark explicitly records that replay is not available in v1.
- [x] **RUN-07**: Budgeted feed quarantine remains available for feed-owned, feed-actionable configuration failures.

### Status Reasons

- [x] **STAT-01**: Backend status reason enum includes `pipeline_publish_after_bookmark_failed`.
- [ ] **STAT-02**: API/UI/shared status handling is updated only where necessary to tolerate the new status reason while preserving existing lifecycle status behavior.

### Telemetry

- [x] **TEL-01**: Runtime emits `feed_failure_policy_decision` for every routed failure.
- [x] **TEL-02**: `feed_failure_policy_decision` includes status reason, owner scope, failure scope, endpoint kind, policy intent, executed action, retry delay, and source type when available.
- [x] **TEL-03**: Runtime emits `post_bookmark_publish_failure` for post-bookmark Pub/Sub publish gaps.
- [x] **TEL-04**: `post_bookmark_publish_failure` includes `replay_missing=true` and `data_gap_known=true`.
- [x] **TEL-05**: Non-budgeted policy decisions never emit `feed_quarantined`.

### Tests

- [ ] **TEST-01**: Storage tests prove non-budgeted release writes `status='failing'`, `failure_count=0`, `retry_after`, and status reason.
- [ ] **TEST-02**: Storage tests prove non-budgeted release does not write `quarantine_reason`.
- [ ] **TEST-03**: Runtime tests prove Pub/Sub post-bookmark publish failure does not call `report_feed_failure(...)`.
- [ ] **TEST-04**: Runtime tests prove Pub/Sub post-bookmark publish failure emits both policy and publish-gap telemetry.
- [ ] **TEST-05**: Runtime tests prove source-offline/auth/rate-limit/unknown cases use the non-budgeted path.
- [ ] **TEST-06**: Runtime tests prove unannotated failures route to telemetry gap.
- [ ] **TEST-07**: Runtime tests prove feed-config quarantine-eligible failures still use the budgeted path.
- [ ] **TEST-08**: Runtime tests prove non-budgeted paths never emit `feed_quarantined`.

## v2 Requirements

### Durable Replay

- **REPLAY-01**: Captured post-bookmark publish failures are stored in a durable outbox or hold table.
- **REPLAY-02**: Replay worker publishes held messages in ordering-key order after the root cause is fixed.
- **REPLAY-03**: Replay backlog age and size are alertable.

### Breakers

- **BRK-01**: Shared auth failures open credential-scope or source-class breakers instead of per-feed failures.
- **BRK-02**: Source-class breakers support half-open canary probes.
- **BRK-03**: Breaker state suppresses repetitive feed-level alerts while preserving class-level pages.

### Audit Events

- **AUD-01**: Structured policy decision events are persisted in an audit/event table.
- **AUD-02**: Audit events support root-cause aggregation by owner scope, endpoint kind, and reason family.

### Operator Experience

- **OPS-01**: UI exposes status reason and suppressed retry state clearly enough for operators to distinguish quarantine from retry suppression.
- **OPS-02**: Runbooks link source-class, pipeline, and feed-actionable incidents to the correct remediation path.

## Out of Scope

| Feature | Reason |
|---------|--------|
| Durable outbox/DLQ/replay worker in v1 | Larger design touching idempotency, ordering, storage, and replay operations; v1 must explicitly log replay gaps instead. |
| Source-class breaker state in v1 | Requires shared/fleet-wide state and canary orchestration; v1 should first stop feed-budget damage. |
| New feed lifecycle status in v1 | Current `failing` status provides scheduler compatibility without migration. |
| Persistent structured audit table in v1 | Logs are sufficient for v1 policy verification; DB schema remains unchanged. |
| Echo parity in v1 | VM ingestion is the incident-heavy path and existing plan target. |
| Parsing `quarantine_reason` | Raw forensic strings are not stable policy keys. |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| POL-01 | Phase 1 | Complete |
| POL-02 | Phase 1 | Complete |
| POL-03 | Phase 1 | Complete |
| POL-04 | Phase 2 | Complete |
| STORE-01 | Phase 1 | Complete |
| STORE-02 | Phase 1 | Complete |
| STORE-03 | Phase 1 | Complete |
| STORE-04 | Phase 1 | Complete |
| STORE-05 | Phase 1 | Complete |
| STORE-06 | Phase 1 | Complete |
| RUN-01 | Phase 2 | Complete |
| RUN-02 | Phase 2 | Complete |
| RUN-03 | Phase 2 | Complete |
| RUN-04 | Phase 2 | Complete |
| RUN-05 | Phase 2 | Complete |
| RUN-06 | Phase 2 | Complete |
| RUN-07 | Phase 2 | Complete |
| STAT-01 | Phase 1 | Complete |
| STAT-02 | Phase 3 | Pending |
| TEL-01 | Phase 2 | Complete |
| TEL-02 | Phase 2 | Complete |
| TEL-03 | Phase 2 | Complete |
| TEL-04 | Phase 2 | Complete |
| TEL-05 | Phase 2 | Complete |
| TEST-01 | Phase 3 | Pending |
| TEST-02 | Phase 3 | Pending |
| TEST-03 | Phase 3 | Pending |
| TEST-04 | Phase 3 | Pending |
| TEST-05 | Phase 3 | Pending |
| TEST-06 | Phase 3 | Pending |
| TEST-07 | Phase 3 | Pending |
| TEST-08 | Phase 3 | Pending |

**Coverage:**
- v1 requirements: 32 total
- Mapped to phases: 32
- Unmapped: 0

---
*Requirements defined: 2026-06-14*
*Last updated: 2026-06-14 after initial definition*

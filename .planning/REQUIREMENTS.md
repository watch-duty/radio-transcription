# Requirements: Evidence-Based Quarantine Policy v1.1

**Defined:** 2026-06-15
**Core Value:** On-call should be alerted only when retry is not expected to fix the ingestion failure and a human/operator repair is required.

## v1.1 Requirements

### Policy Routing

- [x] **POL-11**: Failure policy routing is encoded as explicit `status_reason + evidence` rows.
- [x] **POL-12**: Only explicit quarantine-budgeted rows can return `INCREMENT_FEED_FAILURE_BUDGET`.
- [x] **POL-13**: Unmatched or unsupported `status_reason + evidence` combinations route to telemetry-gap non-budgeted release.
- [x] **POL-14**: Routing continues to use structured status/evidence fields and does not add `reason_family`.

### Status Reason Splits

- [x] **STAT-11**: Backend status reason enum includes `system_runtime_configuration_invalid`.
- [x] **STAT-12**: Backend status reason enum includes `system_credential_access_failed`.
- [x] **STAT-13**: Backend status reason enum includes `system_source_payload_invalid`.
- [x] **STAT-14**: Existing producer mappings are split only for current root causes that need clear routing.

### Runtime Behavior

- [x] **RUN-11**: `_PipelineFailure` uses the same policy branch as collector `FeedFailure`.
- [x] **RUN-12**: `pipeline_publish_after_bookmark_failed` with Pub/Sub publish evidence increments the quarantine budget through `report_feed_failure(...)`.
- [x] **RUN-13**: GCS upload and bookmark-write pipeline failures remain non-budgeted.
- [x] **RUN-14**: Non-budgeted failures continue to reset stale `failure_count` and release with `retry_after`.
- [x] **RUN-15**: Budgeted failures still use the existing feed failure threshold; they do not immediately quarantine before threshold.
- [x] **RUN-16**: The old special `post_bookmark_publish_failure` telemetry event is not required for v1.1 routing.

### Backend Closeout

- [x] **DOC-11**: Collector authoring documentation reflects final v1.1
  Pub/Sub post-bookmark publish semantics.
- [ ] **VER-11**: Phase 6 closeout records that API/UI/generated
  compatibility is deferred from this backend-only milestone.

### Verification

- [x] **TEST-11**: Policy tests cover every current status reason's intended budget route.
- [x] **TEST-12**: Policy tests prove unmatched combinations fall back to telemetry-gap non-budgeted release.
- [x] **TEST-13**: Runtime tests prove Pub/Sub publish-after-bookmark failure calls `report_feed_failure(...)`.
- [x] **TEST-14**: Runtime tests prove non-budgeted source, ambiguous collector, GCS/bookmark, credential-access, and telemetry-gap cases do not call `report_feed_failure(...)`.
- [x] **TEST-15**: Collector tests prove the new split enum values are produced by Calls, Fire Notifications, Icecast, and OpenMHz where applicable.
- [ ] **TEST-17**: Final focused backend verification and diff hygiene pass
  after documentation closeout.

## Future Requirements

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
- **AUD-02**: Audit events support root-cause aggregation by owner scope and endpoint kind.

### Deferred API/UI Compatibility

- **COMP-11**: Backend enum and OpenAPI `BackendFeedStatusReason` stay synchronized.
- **COMP-12**: Shared frontend status reason types and allowlists include the new status reasons.
- **COMP-13**: API generated route metadata is regenerated or otherwise synchronized with the OpenAPI/type changes.
- **COMP-14**: UI status indicator displays readable labels for the new reasons.
- **TEST-16**: API/UI/storage tests prove enum compatibility surfaces are synchronized.

## Out of Scope

| Feature | Reason |
|---------|--------|
| Database migration | `feeds.status_reason` is text and existing storage paths are sufficient. |
| Durable outbox/DLQ/replay worker | Larger design touching idempotency, ordering, storage, and replay operations. |
| Source-class breaker state | Requires shared/fleet-wide state and canary orchestration. |
| New feed lifecycle status | Existing `failing` and `quarantined` lifecycle states remain sufficient for v1.1. |
| Persistent structured audit table | Logs and tests are sufficient for this policy merge. |
| OpenAPI/frontend/generated compatibility | User deferred these surfaces from the backend-only v1.1 milestone. |
| `reason_family` | Current routing can be decided from status reason plus evidence. |
| ADR | User explicitly deferred ADR creation for this iteration. |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| POL-11 | Phase 4 | Complete |
| POL-12 | Phase 4 | Complete |
| POL-13 | Phase 4 | Complete |
| POL-14 | Phase 4 | Complete |
| STAT-11 | Phase 4 | Complete |
| STAT-12 | Phase 4 | Complete |
| STAT-13 | Phase 4 | Complete |
| STAT-14 | Phase 4 | Complete |
| RUN-11 | Phase 5 | Complete |
| RUN-12 | Phase 5 | Complete |
| RUN-13 | Phase 5 | Complete |
| RUN-14 | Phase 5 | Complete |
| RUN-15 | Phase 5 | Complete |
| RUN-16 | Phase 5 | Complete |
| DOC-11 | Phase 6 | Complete |
| VER-11 | Phase 6 | Pending |
| TEST-11 | Phase 4 | Complete |
| TEST-12 | Phase 4 | Complete |
| TEST-13 | Phase 5 | Complete |
| TEST-14 | Phase 5 | Complete |
| TEST-15 | Phase 5 | Complete |
| TEST-17 | Phase 6 | Pending |
| COMP-11 | Follow-up | Deferred |
| COMP-12 | Follow-up | Deferred |
| COMP-13 | Follow-up | Deferred |
| COMP-14 | Follow-up | Deferred |
| TEST-16 | Follow-up | Deferred |

**Coverage:**
- v1.1 requirements: 22 total
- Mapped to phases: 22
- Unmapped: 0

---
*Requirements defined: 2026-06-15*
*Last updated: 2026-06-15 after v1.1 milestone initialization*

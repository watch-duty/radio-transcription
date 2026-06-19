# Phase 4 Research: Runtime Event Integration

**Phase:** 04-runtime-event-integration
**Date:** 2026-06-19
**Status:** Complete

## Scope

Phase 4 needs runtime and Echo paths to produce durable audit events for
meaningful failure, quarantine, and recovery outcomes while suppressing lease
and heartbeat noise. This research focused on the current code paths that
mutate feed lifecycle state and on the edge cases introduced by leasing.

## Findings

### Existing Audit Foundation

`FeedStore` already has storage-owned audit primitives:

- `_audit_snapshot` serializes the maintained feed-row snapshot allowlist.
- `_audit_event_identity` derives the denormalized event identity columns.
- `_allocate_feed_sequence` provides per-feed ordering.
- `_insert_feed_audit_event` inserts `feed_audit_events` rows in the same
  transaction as create, update, reset, deactivate, and delete mutations.

Runtime events should reuse this pattern rather than letting the collector
runtime or Echo handler insert audit rows directly.

### Async Runtime Mutation Paths

The async runtime writes lifecycle state through:

- `FeedStore.update_feed_progress`
- `FeedStore.record_source_observation`
- `FeedStore.report_feed_failure`
- `FeedStore.release_non_budgeted_failure`

`report_feed_failure` currently increments `failure_count`, computes retry
backoff, switches to `failing` or `quarantined`, and writes legacy
`quarantine_reason` on threshold crossing. `release_non_budgeted_failure`
marks the feed `failing` without consuming failure budget. Success paths clear
failure state through progress or source-observation writes.

### Lease Mechanics Hide Logical Previous Status

Recovery and retry paths have a critical nuance: recovery claiming updates a
`failing` feed to `active` before runtime processing succeeds. The returned
leased feed keeps `failure_count` and `status_reason`, but it does not
currently expose the status that existed before the claim.

If Phase 4 compares only the row immediately before `report_feed_failure`, a
retry of the same failure would look like `active -> failing` every time and
would create noisy duplicate events. If Phase 4 compares only the row
immediately before a success write, recovery from failing would look like
`active -> active` with cleared reason fields.

The implementation needs an explicit claim-time carrier for the logical
previous status. Add `previous_status` to the `LeasedFeed` shape and return it
from both primary and recovery claim SQL. Runtime storage methods can then
decide whether the failure/recovery changed the meaningful
`(status, status_reason)` combination without synthesizing raw audit snapshots.

### Diagnostic Detail Lifecycle

The current storage helper caps `quarantine_reason`, but Phase 4 needs a
canonical `status_reason_detail` persistence boundary:

- set bounded detail for abnormal writes
- clear detail when abnormal state clears
- avoid storing obvious secrets or credential values
- keep `quarantine_reason` compatibility-only

The practical implementation is a small sanitizer in `feed_lifecycle.py` that
normalizes whitespace, redacts common token/credential patterns, and then uses
the existing 2048-character cap.

### Echo And Sync Store Parity

Echo uses `SyncFeedStore` and psycopg SQL, not async `FeedStore`. It currently:

- resolves Echo feeds with only `id`, `name`, `status`, and `created_at`
- records success with `record_heartbeat`
- records budgeted failures with `record_failure`
- records non-budgeted failures with `record_non_budgeted_failure`

To get parity, `resolve_echo_feed` must return enough prior state
(`status`, `failure_count`, `status_reason`) for the handler to pass into the
sync store. The sync store needs the same snapshot, sequence, action-selection,
and transaction rules as async storage, using psycopg parameter syntax and
`conn.transaction()`.

### Runtime Actors

The canonical Phase 4 actors are semantic service actors:

- `service:collector-runtime`
- `service:echo-ingestion`

GCP service-account identity is transport/provenance and remains a fallback
only when no semantic actor is known. Runtime event writers should require an
explicit actor for audited abnormal/recovery writes rather than silently using
`unknown:unknown` or the removed `system:` prefix.

## Implementation Guidance

1. Add `previous_status` to async leased-feed rows before adding event gates.
2. Add the diagnostic-detail sanitizer and SQL writes before wiring events, so
   audit snapshots naturally include the canonical detail field.
3. Centralize event action selection in storage helpers:
   `feed.failure_reported`, `feed.quarantined`, or no event for failure writes;
   `feed.recovered` or no event for success writes.
4. Wire collector runtime after storage signatures exist, passing
   `previous_status`, `failure_count`, `status_reason`, actor ID, and sanitized
   diagnostic source text.
5. Implement Echo parity separately in the sync store so psycopg mechanics do
   not complicate the async implementation.
6. Finish with contract tests and documentation updates that prove no lease,
   clean heartbeat, or clean progress noise was introduced.

## Risks And Mitigations

| Risk | Mitigation |
|------|------------|
| Retry of the same failure emits duplicate events | Compare against `previous_status` and previous `status_reason`, not only the current active lease row. |
| Recovery event is lost because claim already moved status to active | Carry `previous_status` from claim to success writes and require prior abnormal status for recovery. |
| Echo diverges from async runtime semantics | Add sync store tests that assert the same action-selection cases and actor IDs. |
| Diagnostic text persists secrets | Add a bounded sanitizer before writing `status_reason_detail`; avoid storing raw auth headers/tokens. |
| Runtime callers create audit rows directly | Keep all inserts in storage classes; runtime only passes causal inputs. |


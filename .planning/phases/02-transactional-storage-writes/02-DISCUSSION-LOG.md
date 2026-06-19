# Phase 2: Transactional Storage Writes - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-19T13:03:23Z
**Phase:** 2-Transactional Storage Writes
**Areas discussed:** Audit producer boundary, Snapshot shape, Actor handling, Per-feed sequence allocation, Actor vocabulary cleanup

---

## Audit Producer Boundary

| Option | Description | Selected |
|--------|-------------|----------|
| FeedStore-only event writer | `FeedStore` owns audit creation through private helpers; callers pass causal inputs but do not insert audit rows. | ✓ |
| Shared storage helper | A lower-level storage helper can be called by selected storage modules. | |
| Service layer constructs events | Service/runtime callers build audit events and pass them to storage. | |

**User's choice:** Accepted recommended FeedStore-owned boundary.
**Notes:** The selected boundary preserves CON-04 by preventing service/runtime
callers from drifting current-state writes and audit-history writes.

---

## Snapshot Shape

| Option | Description | Selected |
|--------|-------------|----------|
| Changed allowlisted fields only | Store only changed values for every action. | |
| Full allowlisted snapshot for every event | Store a full snapshot for every action. | |
| Hybrid allowlist | Full snapshot for create/delete; changed allowlisted fields for update/status lifecycle events. | ✓ |

**User's choice:** Accepted recommended hybrid allowlist model.
**Notes:** The user previously clarified that delete can rely on
`before_values` and should contain the feed row/subset that is easiest to
maintain long term. The final selected design uses the same maintained allowlist
for create/delete snapshots and smaller changed-field payloads for update,
deactivate, and reset.

---

## Actor Handling

| Option | Description | Selected |
|--------|-------------|----------|
| Required actor parameter with service fallback | Storage methods accept `actor_id`; service API mutations use a deliberate service actor until trusted user propagation lands. | ✓ |
| Unknown fallback by default | Missing actors default to `unknown:unknown`. | |
| Human actor only | Treat all admin calls as user actions even when trusted user ID is unavailable. | |

**User's choice:** Accepted recommended actor parameter with service fallback,
then clarified nullable user actors should not be used.
**Notes:** Current BFF requests know the browser user, but the Python feeds
service does not yet receive the causal user identity. The user accepted using
`service:feeds-service` in Phase 2 and later forwarding `user:google:<sub>` in
Phase 3. The user rejected fake nullable user forms; `user:null` and `user:`
must not be valid actor IDs.

---

## Per-Feed Sequence Allocation

| Option | Description | Selected |
|--------|-------------|----------|
| Transactional sequence table | Allocate sequence in the same transaction through `feed_audit_event_sequences`. | ✓ |
| Advisory locks | Use database advisory locks around sequence allocation. | |
| `MAX(feed_sequence) + 1` | Compute next sequence from existing audit rows. | |

**User's choice:** Accepted recommended transactional sequence table.
**Notes:** The selected design uses the sequence foundation added in Phase 1
and avoids the race-prone max-scan approach.

---

## Actor Vocabulary Cleanup

| Option | Description | Selected |
|--------|-------------|----------|
| Keep `system:` | Retain Phase 1's `system:<component>` namespace. | |
| Remove `system:` | Use clearer `service:`, `job:`, `gcp-sa:`, user, and unknown namespaces. | ✓ |

**User's choice:** "sounds good"
**Notes:** The user asked whether `system:` is still needed. The resulting
decision is to remove it before any audit rows exist, because it overlaps with
clearer categories and risks becoming vague.

---

## the agent's Discretion

- Exact helper names and SQL layout.
- Exact initial implementation of the maintained snapshot allowlist, as long
  as it includes meaningful feed fields plus source feed ID and tags and
  excludes noisy lease fields by default.
- Exact practical test split between mock-level storage tests and available
  database/component coverage.

## Deferred Ideas

- Trusted user actor forwarding from BFF to feeds service belongs to Phase 3.
- Runtime failure/quarantine/recovery and Echo/sync audit events belong to
  Phase 4.
- Retention enforcement and broad verification hardening belong to Phase 5.

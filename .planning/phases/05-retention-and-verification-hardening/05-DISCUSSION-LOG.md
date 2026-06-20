# Phase 5: Retention and Verification Hardening - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md - this log preserves the alternatives considered.

**Date:** 2026-06-20
**Phase:** 05-retention-and-verification-hardening
**Areas discussed:** Retention mechanism, Retention side effects, Sequence cleanup

---

## Retention Mechanism

| Option | Description | Selected |
|--------|-------------|----------|
| DB-owned pg_cron job | AlloyDB deletes expired audit rows on a schedule from a `*pg_cron*.sql` migration. | ✓ |
| App/script job | A service or deploy/CI job calls cleanup SQL periodically. | |
| Both, with script as manual backstop | pg_cron is canonical, with a manual/emergency helper. | |

**User's choice:** DB-owned pg_cron job.
**Notes:** This matches the existing AlloyDB retention/sweep pattern and keeps
the database responsible for the data it owns.

### Cadence And Batch Shape

| Option | Description | Selected |
|--------|-------------|----------|
| Daily bounded cleanup | Run once per day and delete expired rows in bounded batches. | ✓ |
| Hourly bounded cleanup | Reduce backlog more frequently with more scheduled DB work. | |
| Daily unbounded cleanup | Simpler SQL but potentially large transactions. | |

**User's choice:** Daily bounded cleanup.
**Notes:** The user selected the conservative batch shape.

### Expiry Timestamp

| Option | Description | Selected |
|--------|-------------|----------|
| `occurred_at` | Retention follows the event's domain time and documented timeline field. | ✓ |
| `created_at` | Retention follows insert time. | |
| Both as a guard | Delete only when both timestamps are older than 18 months. | |

**User's choice:** `occurred_at`.
**Notes:** This keeps retention aligned with the audit event contract.

### Backlog Catch-Up

| Option | Description | Selected |
|--------|-------------|----------|
| One bounded batch per daily run | Safest and simplest; large backlogs clear over multiple days. | ✓ |
| Loop bounded batches per run | Chunked but potentially much more work in one cron invocation. | |
| Small hourly batches until caught up | Faster catch-up with more frequent scheduled work. | |

**User's choice:** One bounded batch per daily run.
**Notes:** Exact batch size is left to planning/implementation discretion.

---

## Retention Side Effects

| Option | Description | Selected |
|--------|-------------|----------|
| Delete rows only | No archive, redaction rewrite, or tombstone event. | ✓ |
| Delete rows and write summary/tombstone | Preserve that older history existed by creating synthetic history. | |
| Redact payloads instead of delete | Keep timeline continuity but violate the clear retention requirement. | |

**User's choice:** Delete expired rows only.
**Notes:** Synthetic audit events remain out of scope.

### Sequence Gaps

| Option | Description | Selected |
|--------|-------------|----------|
| Keep gaps | Never renumber; retained `feed_sequence` values remain immutable. | ✓ |
| Renumber retained rows | Makes timelines contiguous but mutates audit history. | |
| Add a separate visible offset | Preserves original sequence and adds display-only continuity. | |

**User's choice:** Keep gaps.
**Notes:** Gaps after retention are expected and acceptable.

---

## Sequence Cleanup

| Option | Description | Selected |
|--------|-------------|----------|
| Compute from `MAX(feed_sequence) + 1` | Removes sequence rows but is race-prone under concurrent mutations. | |
| Put `next_sequence` on `feeds` | Adds hot-row churn and fails deleted-feed audit needs. | |
| Use only `occurred_at` and `id` | Simpler but changes the current ordering contract. | |
| Keep `feed_audit_event_sequences` | Preserve the current concurrency and ordering contract. | ✓ |

**User's choice:** Keep `feed_audit_event_sequences` for Phase 5.
**Notes:** The user questioned whether sequence numbers are needed at all. The
discussion clarified that this is an existing v1 contract decision, not a
retention-hardening detail. Reconsidering it is deferred.

### Sequence Row Pruning

| Option | Description | Selected |
|--------|-------------|----------|
| Prune orphaned rows for feeds with no retained audit events | Remove no-longer-useful sequence counters after all history expires. | |
| Never prune sequence rows | Simplest, but leaves permanent bookkeeping for deleted feeds. | |
| Prune only when feed is gone and no audit events remain | Conservative cleanup that avoids touching live feeds. | ✓ |

**User's choice:** Prune sequence rows only when the feed row no longer exists
and no retained audit events remain.
**Notes:** Live feeds keep sequence rows even if all old events expire.

---

## the agent's Discretion

- Exact cron expression, batch size, migration number/name, and SQL layout.
- Exact test split across static contract, unit, integration, and CI/human lanes.

## Deferred Ideas

- Revisit in a future phase whether explicit per-feed `feed_sequence` can be
  removed from the audit contract and replaced by client-side ordering on
  `occurred_at` plus event ID.

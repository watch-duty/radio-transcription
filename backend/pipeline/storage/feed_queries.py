"""SQL queries for feed storage operations."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from backend.pipeline.storage.feed_store import SourceType

UPDATE_PROGRESS_SQL = """\
UPDATE feeds
SET last_processed_filename = $1,
    last_bookmark_time = COALESCE($5, last_bookmark_time),
    failure_count = 0,
    status_reason_updated_at = CASE
        WHEN status_reason IS NOT NULL THEN NOW()
        ELSE status_reason_updated_at
    END,
    status_reason = NULL
WHERE id = $2 AND worker_id = $3 AND fencing_token = $4
"""

RECORD_SOURCE_OBSERVATION_SQL = """\
WITH current_state AS (
    SELECT id, worker_id, status, fencing_token
    FROM feeds
    WHERE id = $1
    FOR UPDATE
),
do_update AS (
    UPDATE feeds
    SET failure_count = 0,
        last_bookmark_time = GREATEST(last_bookmark_time, $4),
        status_reason_updated_at = CASE
            WHEN status_reason IS NOT NULL THEN NOW()
            ELSE status_reason_updated_at
        END,
        status_reason = NULL
    FROM current_state
    WHERE feeds.id = current_state.id
      AND current_state.worker_id = $2
      AND current_state.fencing_token = $3
      AND current_state.status = 'active'::feed_status
    RETURNING feeds.id
)
SELECT
    current_state.id,
    current_state.worker_id AS current_worker,
    current_state.status::text AS current_status,
    current_state.fencing_token AS current_fencing_token,
    (do_update.id IS NOT NULL) AS recorded
FROM current_state
LEFT JOIN do_update ON current_state.id = do_update.id;
"""

RENEW_HEARTBEATS_BATCH_DIAGNOSTIC_SQL = """\
WITH current_state AS (
    SELECT id, worker_id, status, last_heartbeat
    FROM feeds WHERE id = ANY($1::uuid[])
    FOR UPDATE
),
do_update AS (
    UPDATE feeds SET last_heartbeat = NOW()
    FROM current_state
    WHERE feeds.id = current_state.id
      AND current_state.worker_id = $2
      AND current_state.status = 'active'::feed_status
      AND (current_state.last_heartbeat IS NULL
           OR current_state.last_heartbeat < NOW() - INTERVAL '15 seconds')
    RETURNING feeds.id
)
SELECT
    current_state.id,
    current_state.worker_id AS current_worker,
    current_state.status::text AS current_status,
    (do_update.id IS NOT NULL) AS renewed
FROM current_state
LEFT JOIN do_update ON current_state.id = do_update.id;
"""

RELEASE_FEED_SQL = """\
UPDATE feeds
SET worker_id = NULL,
    status = 'unclaimed'::feed_status,
    unclaimed_since = NOW()
WHERE id = $1 AND worker_id = $2 AND fencing_token = $3
  AND status = 'active'::feed_status
"""

# SIGTERM drain: release every active lease still owned by this worker in
# one UPDATE. Primary use is _shutdown_sequence — after cancelling all
# feed tasks (whose CancelledError path skips the normal-completion
# release_feed), this single statement is what flips active rows back to
# unclaimed. Deactivated rows are terminal admin stops until reset, so
# release must not make them claimable.
#
# WHERE worker_id = $1 is authoritative for both cases, and the active
# status guard preserves operator deactivation if shutdown races a
# manual lifecycle change. unclaimed_since = NOW() matches the
# convention in RELEASE_FEED_SQL so the autoscaler's MIN(unclaimed_since)
# signal stays accurate across scale-in. No last_heartbeat write —
# heartbeat renewal is now the sole writer of that column (scaling plan
# §6.1).
RELEASE_FEEDS_BATCH_SQL = """\
UPDATE feeds
SET worker_id = NULL,
    status = 'unclaimed'::feed_status,
    unclaimed_since = NOW()
WHERE worker_id = $1
  AND status = 'active'::feed_status
"""

# Authoritative per-cycle replacement for the worker's in-memory
# _held_by_type counter. The worker calls this once per leasing iteration
# before _calculate_branch_limits so per-type LIMITs reflect DB truth,
# not a running Python count that has to be kept in sync across every
# path that mutates _feed_tasks (claim, reap, orphan cancel,
# sweep-reclaim, shutdown).
#
# Why DB truth beats an incremental counter: the PR #334 review cycles
# surfaced two silent leaks in the incremental pattern — one when the
# orphan path cancelled a running task without decrementing (commit
# c84b52d), one when the orphan task was already .done() between reap
# and re-lease (commit 32afbc2). Both were O(N) per-event drifts that
# never threw. Pulling the number from the DB each cycle makes the
# entire class of drift bugs structurally impossible.
#
# Cost: one extra round-trip per worker per lease_poll_interval_sec
# (default 5 s) → ~0.2 qps per worker → ~3.2 qps at 16-worker fleet.
# Served by idx_feeds_active (id) WHERE status='active' partial index
# (migration 018): ≤250 active rows per worker, sub-millisecond.
COUNT_HELD_BY_TYPE_SQL = """\
SELECT source_type, COUNT(*) AS n
FROM feeds
WHERE worker_id = $1 AND status = 'active'::feed_status
GROUP BY source_type
"""


# Primary per-type claim: one independent CTE per claim_type, each locking
# its own rows with its own LIMIT. UNION ALL happens in a `claimed` CTE
# across their IDs — the outer UPDATE joins the combined IDs. Each
# per-type CTE gets its own MATERIALIZED pin to keep the SKIP LOCKED
# subquery from being inlined and re-evaluated per outer row (which would
# defeat the LIMIT under nested-loop plans).
#
# Worker computes each branch's LIMIT as max(0, min(cap, cap - held,
# total_slack)) so the DB enforces per-type caps structurally — a worker
# cannot be handed more memory-heavy bcfy_feeds rows than the cap allows
# in a single claim call.
#
# **Why not a single-CTE UNION ALL with FOR NO KEY UPDATE on the combined
# query?** PostgreSQL does not allow FOR UPDATE / FOR NO KEY UPDATE on a
# UNION result (asyncpg raises FeatureNotSupportedError: "FOR NO KEY
# UPDATE is not allowed with UNION/INTERSECT/EXCEPT"). Per-CTE locking
# sidesteps this and also clarifies the intent: each branch's lock scope
# is its own source_type partition.
#
# The failing-retryable + active-abandoned paths are served by the
# separate build_acquire_feeds_recovery_sql factory, called when this
# primary path underfills.
#
# FOR NO KEY UPDATE (not FOR UPDATE): weaker lock, sufficient because we
# only mutate status/worker_id/fencing_token/last_heartbeat — none of
# which are primary/unique keys. Reduces lock-manager contention at peak.
#
# ORDER BY id within each branch exploits the shipped composite partial
# index `feeds_claim_by_type_idx ON feeds (source_type, id)
# WHERE status = 'unclaimed'` — no sort node, index-scan only.
#
# Params: $1=worker_id, $2..$(1+N)=per-type LIMITs in claim_types order.
# Source_type literals are inlined per branch so the planner can use
# the (source_type, id) WHERE status='unclaimed' partial composite
# index for an index-only scan; parametrizing them would defeat that.
def _build_claim_query(
    branches: list[str],
    branch_names: list[str],
    combined_cte_name: str,
) -> str:
    """Wrap pre-built per-type CTE branches in the outer UPDATE+SELECT.

    Shared scaffolding used by both ``build_acquire_feeds_batch_sql``
    (primary, unclaimed) and ``build_acquire_feeds_recovery_sql``
    (failing-retryable + active-abandoned). The differing parts are the
    per-branch WHERE / ORDER BY (encoded in ``branches``) and the
    combined-CTE name (``combined_cte_name`` = ``claimed`` or
    ``recovered``); the outer UPDATE that flips status to active and the
    final feed_properties JOIN are identical between paths.

    SAFETY: ``branches`` and ``branch_names`` contain interpolated
    ``SourceType.value`` strings (closed enum, never user input). The
    Bandit S608 SQL-injection warning is a false positive in this
    context — same justification as the per-builder noqa comments.
    """
    branches_sql = ",\n".join(branches) + ","
    union_sql = "\n        UNION ALL\n".join(
        f"        SELECT id FROM {n}"  # noqa: S608
        for n in branch_names
    )
    return (
        "WITH\n"  # noqa: S608
        f"{branches_sql}\n"
        f"    {combined_cte_name} AS MATERIALIZED (\n"
        f"{union_sql}\n"
        "    ),\n"
        "leased AS (\n"
        "    UPDATE feeds\n"
        "    SET status = 'active'::feed_status,\n"
        "        worker_id = $1,\n"
        "        fencing_token = fencing_token + 1,\n"
        "        last_heartbeat = NOW(),\n"
        "        retry_after = NULL\n"
        f"    FROM {combined_cte_name}\n"
        f"    WHERE feeds.id = {combined_cte_name}.id\n"
        "    RETURNING feeds.id, feeds.name, feeds.source_type,\n"
        "              feeds.last_processed_filename, feeds.last_bookmark_time,\n"
        "              feeds.fencing_token, feeds.failure_count,\n"
        "              feeds.status_reason\n"
        ")\n"
        "SELECT leased.id, leased.name, leased.source_type,\n"
        "       leased.last_processed_filename, leased.last_bookmark_time,\n"
        "       leased.fencing_token, leased.failure_count,\n"
        "       leased.status_reason, fpi.source_feed_id\n"
        "FROM leased\n"
        "JOIN feed_properties fpi ON fpi.feed_id = leased.id\n"
    )


def build_acquire_feeds_batch_sql(claim_types: Sequence[SourceType]) -> str:
    """Generate per-type batch-claim SQL for the given claim_types.

    Returns a SQL string with one MATERIALIZED CTE per source_type, a
    UNION-ALL `claimed` CTE collecting their IDs, and an outer UPDATE
    that JOINs feed_properties for the LeasedFeed return shape. The
    LIMIT for branch i is parameter ``$2 + i`` in claim_types iteration
    order; $1 is worker_id.
    """
    if not claim_types:
        msg = "claim_types must contain at least one SourceType"
        raise ValueError(msg)

    branches: list[str] = []
    branch_names: list[str] = []
    for i, t in enumerate(claim_types):
        limit_param = i + 2
        cte_name = f"{t.value}_claim"
        branch_names.append(cte_name)
        branches.append(
            f"    {cte_name} AS MATERIALIZED (\n"  # noqa: S608
            f"        SELECT id FROM feeds\n"
            f"        WHERE source_type = '{t.value}' AND status = 'unclaimed'::feed_status\n"
            f"        ORDER BY id\n"
            f"        LIMIT ${limit_param}\n"
            f"        FOR NO KEY UPDATE SKIP LOCKED\n"
            f"    )"
        )
    return _build_claim_query(branches, branch_names, "claimed")


# Recovery-path claim: failing-retryable + active-abandoned. Runs when
# the primary per-type CTE returns fewer rows than the worker's total
# slack. Mirrors the primary path's per-type CTE structure so per-type
# caps are enforced structurally — without per-type LIMITs the recovery
# sweep could push held > cap when one type is at-cap and another type
# is mostly failing-retryable. The sweep volumes themselves are small
# by construction (pg_cron drains active-abandoned at 30 s cadence;
# failure events are rare).
#
# Within each branch:
# - WHERE matches failing-retryable OR active-abandoned of that type.
# - ORDER BY retry_after ASC NULLS LAST, id prioritizes failing-retryable
#   (retry_after = past timestamp, set by REPORT_FAILURE_SQL) over
#   active-abandoned (retry_after = NULL, cleared on claim). Failing
#   rows are workers' only safety net — pg_cron does not reclaim them —
#   whereas active-abandoned has the 30 s pg_cron sweep as a backstop.
#   Within the failing bucket, oldest-retry first; within the
#   active-abandoned tail (NULL retry_after), id order for determinism.
# - LIMIT $X is the per-type recovery LIMIT (passed by the caller after
#   re-running _calculate_branch_limits with held + primary counts).
#
# source_type literals are inlined per branch (same pattern as primary)
# so the planner can use a `source_type = '<literal>'` constant. This
# also means the claim_types filter is implicit: types absent from the
# generator's input never get a CTE branch, so they're never claimed.
#
# Known performance limit: ORDER BY (retry_after, id) is served by
# idx_feeds_failing_retryable for the failing branch; active-abandoned
# uses idx_feeds_active (id) + filter on last_heartbeat + a sort. At
# structurally-small volumes (≤ ~500 failing-or-abandoned rows at a
# time, drained by pg_cron) the sort stays in work_mem and is cheap.
# If either volume spikes (pg_cron paused, failure storm), this query
# becomes an expensive seq/sort path.
#
# TODO(recovery-path-index): if recovery-path P99 exceeds 50 ms at  # noqa: TD003
# production load OR the pg_cron sweep is paused for extended windows,
# add migration:
#
#   CREATE INDEX CONCURRENTLY idx_feeds_recovery
#       ON feeds (retry_after, id)
#       WHERE status IN ('failing'::feed_status, 'active'::feed_status);
#
# The HOT protection CI check (terraform/modules/alloydb/sql/ci/
# hot_protection_check.sql) would also need a second allow-list entry
# for this index, since it covers retry_after on active rows (where
# retry_after is NULL and rarely mutated — low bloat in practice).
#
# MATERIALIZED is non-negotiable for the same planner reason as the
# primary CTE: without it, the planner can inline the CTE into the
# outer UPDATE and re-evaluate the SKIP LOCKED subquery per outer row,
# which would bypass the LIMIT.
#
# Params: $1=worker_id, $2=abandonment_interval, $3..$(2+N)=per-type
# recovery LIMITs in claim_types iteration order.
def build_acquire_feeds_recovery_sql(claim_types: Sequence[SourceType]) -> str:
    """Generate per-type recovery-claim SQL for the given claim_types.

    Mirrors ``build_acquire_feeds_batch_sql`` but each branch's WHERE
    matches failing-retryable OR active-abandoned (not unclaimed). The
    LIMIT for branch i is parameter ``$3 + i`` in claim_types iteration
    order; $1=worker_id, $2=abandonment_interval.
    """
    if not claim_types:
        msg = "claim_types must contain at least one SourceType"
        raise ValueError(msg)

    branches: list[str] = []
    branch_names: list[str] = []
    for i, t in enumerate(claim_types):
        limit_param = i + 3
        cte_name = f"{t.value}_recovery"
        branch_names.append(cte_name)
        branches.append(
            f"    {cte_name} AS MATERIALIZED (\n"  # noqa: S608
            f"        SELECT id FROM feeds\n"
            f"        WHERE source_type = '{t.value}'\n"
            f"          AND (\n"
            f"              (status = 'failing'::feed_status AND (retry_after IS NULL OR retry_after <= NOW()))\n"
            f"              OR (status = 'active'::feed_status AND last_heartbeat < NOW() - $2::interval)\n"
            f"          )\n"
            f"        ORDER BY retry_after ASC NULLS LAST, id\n"
            f"        LIMIT ${limit_param}\n"
            f"        FOR NO KEY UPDATE SKIP LOCKED\n"
            f"    )"
        )
    return _build_claim_query(branches, branch_names, "recovered")


REPORT_FAILURE_SQL = """\
UPDATE feeds
SET status = CASE WHEN failure_count + 1 >= $3
                  THEN 'quarantined'::feed_status
                  ELSE 'failing'::feed_status END,
    failure_count = failure_count + 1,
    worker_id = NULL,
    retry_after = CASE WHEN failure_count + 1 < $3
                       THEN NOW() + LEAST($5 * INTERVAL '1 second',
                            $6 * INTERVAL '1 second' * POWER(2, failure_count))
                            + (RANDOM() * INTERVAL '10 seconds')
                       ELSE NULL END,
    -- COALESCE protects against an edge call passing reason=None during
    -- the quarantine transition: keep the previously-recorded reason
    -- rather than overwriting it with NULL. A real reason still wins.
    quarantine_reason = CASE WHEN failure_count + 1 >= $3 THEN COALESCE($7, quarantine_reason) ELSE quarantine_reason END,
    status_reason = COALESCE($8, 'system_unexpected_error'),
    status_reason_updated_at = NOW()
WHERE id = $1 AND worker_id = $2 AND fencing_token = $4
  AND status = 'active'::feed_status
RETURNING status::text, failure_count, retry_after
"""

CREATE_FEED_SQL = """\
WITH new_feed AS (
    INSERT INTO feeds (name, source_type)
    VALUES ($1, $2)
    RETURNING id, name, source_type, status, status_reason,
              status_reason_updated_at, failure_count, worker_id,
              last_heartbeat, last_processed_filename,
              last_bookmark_time, created_at
),
new_props AS (
    INSERT INTO feed_properties (feed_id, source_feed_id, source_type, tags)
    SELECT id, $3, source_type, $4 FROM new_feed
    RETURNING source_feed_id, tags
)
SELECT nf.*, np.source_feed_id, np.tags
FROM new_feed nf
JOIN new_props np ON TRUE;
"""

GET_FEED_SQL = """\
SELECT f.id, f.name, f.source_type, f.status, f.status_reason,
       f.status_reason_updated_at, f.failure_count,
       f.worker_id, f.last_heartbeat, f.last_processed_filename,
       f.last_bookmark_time, f.created_at,
       fp.source_feed_id, fp.tags
FROM feeds f
JOIN feed_properties fp ON f.id = fp.feed_id
WHERE f.id = $1
"""

LIST_FEEDS_DESC_SQL = """\
SELECT f.id, f.name, f.source_type, f.status, f.status_reason,
       f.status_reason_updated_at, f.failure_count,
       f.worker_id, f.last_heartbeat, f.last_processed_filename,
       f.last_bookmark_time, f.created_at,
       fp.source_feed_id, fp.tags
FROM feeds f
JOIN feed_properties fp ON f.id = fp.feed_id
WHERE ($1::timestamptz IS NULL OR f.created_at < $1 OR (f.created_at = $1 AND f.id < $2))
  AND ($3::text[] IS NULL OR f.source_type = ANY($3))
  AND ($4::text[] IS NULL OR f.status::text = ANY($4))
  AND ($5::jsonb IS NULL OR fp.tags @> $5::jsonb)
  AND ($6::text IS NULL OR f.name ILIKE '%' || $6 || '%')
ORDER BY f.created_at DESC, f.id DESC
LIMIT $7
"""

LIST_FEEDS_ASC_SQL = """\
SELECT f.id, f.name, f.source_type, f.status, f.status_reason,
       f.status_reason_updated_at, f.failure_count,
       f.worker_id, f.last_heartbeat, f.last_processed_filename,
       f.last_bookmark_time, f.created_at,
       fp.source_feed_id, fp.tags
FROM feeds f
JOIN feed_properties fp ON f.id = fp.feed_id
WHERE ($1::timestamptz IS NULL OR f.created_at > $1 OR (f.created_at = $1 AND f.id > $2))
  AND ($3::text[] IS NULL OR f.source_type = ANY($3))
  AND ($4::text[] IS NULL OR f.status::text = ANY($4))
  AND ($5::jsonb IS NULL OR fp.tags @> $5::jsonb)
  AND ($6::text IS NULL OR f.name ILIKE '%' || $6 || '%')
ORDER BY f.created_at ASC, f.id ASC
LIMIT $7
"""


DEACTIVATE_FEED_SQL = """\
UPDATE feeds
SET status = 'deactivated'::feed_status
WHERE id = $1
"""
# TODO(hard-delete): remove transcripts PR https://linear.app/watchduty/issue/GOO-458/remaining-legacy-cleanup
DELETE_FEED_SQL = """\
WITH deleted_audio_segments AS (
    DELETE FROM audio_segments
    WHERE feed_id = $1
),
deleted_transcripts AS (
    DELETE FROM transcripts
    WHERE feed_id = $1
)
DELETE FROM feeds
WHERE id = $1
"""


RESET_FEED_SQL = """\
WITH updated AS (
    UPDATE feeds
    SET status = 'unclaimed'::feed_status,
        failure_count = 0,
        worker_id = NULL,
        unclaimed_since = NOW(),
        quarantine_reason = NULL,
        last_heartbeat = NOW(),
        status_reason_updated_at = CASE
            WHEN status_reason IS NOT NULL THEN NOW()
            ELSE status_reason_updated_at
        END,
        status_reason = NULL
    WHERE id = $1
    RETURNING id, name, source_type, status, failure_count, worker_id,
              status_reason, status_reason_updated_at, last_heartbeat,
              last_processed_filename, last_bookmark_time, created_at
)
SELECT u.*, fp.source_feed_id, fp.tags
FROM updated u
JOIN feed_properties fp ON fp.feed_id = u.id
"""

UPDATE_FEED_SQL = """\
WITH updated_feed AS (
    UPDATE feeds
    SET name = $2
    WHERE id = $1
    RETURNING id, name, source_type, status, status_reason,
              status_reason_updated_at, failure_count, worker_id,
              last_heartbeat, last_processed_filename,
              last_bookmark_time, created_at
),
updated_props AS (
    UPDATE feed_properties
    SET tags = $3
    WHERE feed_id = $1
    RETURNING source_feed_id, tags
)
SELECT uf.*, up.source_feed_id, up.tags
FROM updated_feed uf
JOIN updated_props up ON TRUE;
"""


COUNT_FEEDS_SQL = """\
SELECT COUNT(*)
FROM feeds f
JOIN feed_properties fp ON f.id = fp.feed_id
WHERE ($1::text[] IS NULL OR f.source_type = ANY($1))
  AND ($2::text[] IS NULL OR f.status::text = ANY($2))
  AND ($3::jsonb IS NULL OR fp.tags @> $3::jsonb)
  AND ($4::text IS NULL OR f.name ILIKE '%' || $4 || '%')
"""

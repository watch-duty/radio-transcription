"""SQL queries for feed storage operations."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from backend.pipeline.storage.feed_store import SourceType

LEASE_FEED_SQL = """\
WITH available_feed AS (
    SELECT id
    FROM feeds
    WHERE (
        status = 'unclaimed'::feed_status
        OR (status = 'failing'::feed_status AND (retry_after IS NULL OR retry_after <= NOW()))
        OR (status = 'active'::feed_status
            AND last_heartbeat < NOW() - INTERVAL '60 seconds')
    )
    AND ($2::text[] IS NULL OR source_type = ANY($2::text[]))
    ORDER BY (status = 'unclaimed'::feed_status) DESC,
             retry_after ASC NULLS FIRST,
             last_heartbeat ASC NULLS FIRST
    LIMIT 1
    FOR UPDATE SKIP LOCKED
),
leased AS (
    UPDATE feeds
    SET worker_id = $1,
        status = 'active'::feed_status,
        retry_after = NULL,
        last_heartbeat = NOW(),
        fencing_token = fencing_token + 1
    FROM available_feed
    WHERE feeds.id = available_feed.id
    RETURNING feeds.id, feeds.name, feeds.source_type,
              feeds.last_processed_filename, feeds.last_bookmark_time,
              feeds.fencing_token
)
SELECT leased.id, leased.name, leased.source_type,
       leased.last_processed_filename, leased.last_bookmark_time,
       leased.fencing_token, fpi.source_feed_id, fpi.external_id
FROM leased
JOIN feed_properties fpi ON fpi.feed_id = leased.id
"""

UPDATE_PROGRESS_SQL = """\
UPDATE feeds
SET last_processed_filename = $1,
    last_bookmark_time = COALESCE($5, last_bookmark_time),
    failure_count = 0
WHERE id = $2 AND worker_id = $3 AND fencing_token = $4
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
"""

# SIGTERM drain: release every lease still owned by this worker in one
# UPDATE. The WHERE worker_id = $1 form is authoritative — it catches any
# stragglers where an earlier per-feed release_feed call failed (transient
# DB error, asyncio task got reaped before the finally block could
# re-raise) and the row sits in the DB with worker_id=us until pg_cron
# reclaims it ~60 s later. Symmetric with count_held_by_type's DB-truth
# stance: the DB is the source of authority for which feeds we own.
#
# unclaimed_since = NOW() matches the convention in RELEASE_FEED_SQL so
# the autoscaler's MIN(unclaimed_since) signal stays accurate across
# scale-in. No last_heartbeat write — heartbeat renewal is now the sole
# writer of that column (scaling plan §6.1).
RELEASE_FEEDS_BATCH_SQL = """\
UPDATE feeds
SET worker_id = NULL,
    status = 'unclaimed'::feed_status,
    unclaimed_since = NOW()
WHERE worker_id = $1
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
# separate ACQUIRE_FEEDS_RECOVERY_SQL, called when this primary path
# underfills.
#
# FOR NO KEY UPDATE (not FOR UPDATE): weaker lock, sufficient because we
# only mutate status/worker_id/fencing_token/last_heartbeat — none of
# which are primary/unique keys. Reduces lock-manager contention at peak.
#
# ORDER BY id within each branch exploits the shipped composite partial
# index `feeds_claim_by_type_idx ON feeds (source_type, id)
# WHERE status = 'unclaimed'` — no sort node, index-scan only.
#
# md5-based ramp filter (not hashtext): md5() is documented stable across
# PostgreSQL minor-version upgrades; hashtext() has historically changed
# between major versions, which would silently re-shuffle feeds between
# ramp buckets mid-rollout (scaling plan §9.4).
#
# Params: $1=worker_id, $2=ramp_pct, $3..$(2+N)=per-type LIMITs in
# claim_types order. Source_type literals are inlined per branch so the
# planner can use the (source_type, id) WHERE status='unclaimed' partial
# composite index for an index-only scan; parametrizing them would
# defeat that.
def build_acquire_feeds_batch_sql(claim_types: Sequence[SourceType]) -> str:
    """Generate per-type batch-claim SQL for the given claim_types.

    Returns a SQL string with one MATERIALIZED CTE per source_type, a
    UNION-ALL `claimed` CTE collecting their IDs, and an outer UPDATE
    that JOINs feed_properties for the LeasedFeed return shape. The
    LIMIT for branch i is parameter ``$3 + i`` in claim_types iteration
    order; $1 is worker_id and $2 is ramp_pct.

    For the production set (BCFY_FEEDS, BCFY_CALLS, OPENMHZ in that
    order), the output is byte-identical to the previous static
    ACQUIRE_FEEDS_BATCH_SQL constant.
    """
    if not claim_types:
        msg = "claim_types must contain at least one SourceType"
        raise ValueError(msg)

    # SAFETY: The interpolated `t.value` strings come from the SourceType
    # enum, which is a closed set defined in feed_store.py. They are
    # never derived from user input. The Bandit S608 SQL-injection
    # warning is a false positive in this context — the closed-enum
    # invariant is enforced at compile time by mypy.
    branches: list[str] = []
    union_lines: list[str] = []
    for i, t in enumerate(claim_types):
        limit_param = i + 3
        branches.append(
            f"    {t.value}_claim AS MATERIALIZED (\n"  # noqa: S608
            f"        SELECT id FROM feeds\n"
            f"        WHERE source_type = '{t.value}' AND status = 'unclaimed'::feed_status\n"
            f"          AND (('x' || substr(md5(id::text), 1, 7))::bit(28)::integer) % 100 < $2\n"
            f"        ORDER BY id\n"
            f"        LIMIT ${limit_param}\n"
            f"        FOR NO KEY UPDATE SKIP LOCKED\n"
            f"    )"
        )
        union_lines.append(f"        SELECT id FROM {t.value}_claim")  # noqa: S608
    branches_sql = ",\n".join(branches) + ","
    union_sql = "\n        UNION ALL\n".join(union_lines)

    return (
        "WITH\n"  # noqa: S608
        f"{branches_sql}\n"
        "    claimed AS MATERIALIZED (\n"
        f"{union_sql}\n"
        "    ),\n"
        "leased AS (\n"
        "    UPDATE feeds\n"
        "    SET status = 'active'::feed_status,\n"
        "        worker_id = $1,\n"
        "        fencing_token = fencing_token + 1,\n"
        "        last_heartbeat = NOW(),\n"
        "        retry_after = NULL\n"
        "    FROM claimed\n"
        "    WHERE feeds.id = claimed.id\n"
        "    RETURNING feeds.id, feeds.name, feeds.source_type,\n"
        "              feeds.last_processed_filename, feeds.last_bookmark_time,\n"
        "              feeds.fencing_token\n"
        ")\n"
        "SELECT leased.id, leased.name, leased.source_type,\n"
        "       leased.last_processed_filename, leased.last_bookmark_time,\n"
        "       leased.fencing_token, fpi.source_feed_id, fpi.external_id\n"
        "FROM leased\n"
        "JOIN feed_properties fpi ON fpi.feed_id = leased.id\n"
    )

# Recovery-path claim: failing-retryable + active-abandoned. Runs when the
# primary per-type CTE (ACQUIRE_FEEDS_BATCH_SQL) returns fewer rows than
# the worker's total slack. No per-type cap here: failing and abandoned
# volumes are small by construction (pg_cron sweep drains active-abandoned
# at 30 s cadence; failure events are rare).
#
# ORDER BY retry_after ASC NULLS LAST, id prioritizes failing-retryable
# (retry_after = past timestamp, set by REPORT_FAILURE_SQL) over
# active-abandoned (retry_after = NULL, cleared on claim). Failing
# rows are workers' only safety net — pg_cron does not reclaim them —
# whereas active-abandoned has the 30 s pg_cron sweep as a backstop.
# Within the failing bucket, oldest-retry first; within the
# active-abandoned tail (NULL retry_after), id order for determinism.
#
# Same md5 ramp filter as the primary path — ramp changes affect both
# branches symmetrically, which keeps rollback semantics deterministic.
#
# Known performance limit: ORDER BY (retry_after, id) is served by the
# idx_feeds_failing_retryable partial index for the failing branch, but
# the active-abandoned branch relies on idx_feeds_active (id) followed by
# a filter on last_heartbeat + a sort. At the structurally-small volumes
# the design assumes (≤ ~500 failing-or-abandoned rows at a time, drained
# by pg_cron), the sort stays in work_mem and is cheap. If either volume
# ever spikes (pg_cron paused, failure storm), this query becomes an
# expensive seq/sort path.
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
# MATERIALIZED is non-negotiable for the same planner reason as the primary
# CTE: without it, the planner can inline the CTE into the outer UPDATE and
# re-evaluate the SKIP LOCKED subquery per outer row, which would bypass
# the LIMIT.
#
# source_type filter ($5): restricts the recovery sweep to the worker's
# claim_types. Without it, recovery would scoop up failing or
# active-abandoned rows of any source_type — including types this worker
# has no per-type cap for (so the per-type memory budget would be
# bypassed) and types served by a separate pipeline (e.g. ECHO via Cloud
# Function, never VM-leased). The primary CTE (ACQUIRE_FEEDS_BATCH_SQL)
# already filters per-type via the literal `source_type = '...'` in each
# branch; recovery now does the same, dynamically, via $5.
#
# Params: $1=worker_id, $2=abandonment_interval, $3=ramp_pct, $4=limit,
#         $5=claim_types (text[] of source_type values).
ACQUIRE_FEEDS_RECOVERY_SQL = """\
WITH recovered AS MATERIALIZED (
    SELECT id FROM feeds
    WHERE source_type = ANY($5::text[])
      AND (
          (status = 'failing'::feed_status AND (retry_after IS NULL OR retry_after <= NOW()))
          OR (status = 'active'::feed_status AND last_heartbeat < NOW() - $2::interval)
      )
      AND (('x' || substr(md5(id::text), 1, 7))::bit(28)::integer) % 100 < $3
    ORDER BY retry_after ASC NULLS LAST, id
    LIMIT $4
    FOR NO KEY UPDATE SKIP LOCKED
),
leased AS (
    UPDATE feeds
    SET status = 'active'::feed_status,
        worker_id = $1,
        fencing_token = fencing_token + 1,
        last_heartbeat = NOW(),
        retry_after = NULL
    FROM recovered
    WHERE feeds.id = recovered.id
    RETURNING feeds.id, feeds.name, feeds.source_type,
              feeds.last_processed_filename, feeds.last_bookmark_time,
              feeds.fencing_token
)
SELECT leased.id, leased.name, leased.source_type,
       leased.last_processed_filename, leased.last_bookmark_time,
       leased.fencing_token, fpi.source_feed_id, fpi.external_id
FROM leased
JOIN feed_properties fpi ON fpi.feed_id = leased.id
"""

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
                       ELSE NULL END
WHERE id = $1 AND worker_id = $2 AND fencing_token = $4
RETURNING status::text, failure_count, retry_after
"""

CREATE_FEED_SQL = """\
WITH new_feed AS (
    INSERT INTO feeds (name, source_type)
    VALUES ($1, $2)
    RETURNING id, name, source_type, status, failure_count, worker_id, last_heartbeat, last_processed_filename, last_bookmark_time, created_at
),
new_props AS (
    INSERT INTO feed_properties (feed_id, source_feed_id, external_id, source_type)
    SELECT id, $3, $4, source_type FROM new_feed
    RETURNING source_feed_id, external_id
)
SELECT nf.*, np.source_feed_id, np.external_id
FROM new_feed nf
JOIN new_props np ON TRUE;
"""

GET_FEED_SQL = """\
SELECT f.id, f.name, f.source_type, f.status, f.failure_count,
       f.worker_id, f.last_heartbeat, f.last_processed_filename,
       f.last_bookmark_time, f.created_at,
       fp.source_feed_id, fp.external_id
FROM feeds f
JOIN feed_properties fp ON f.id = fp.feed_id
WHERE f.id = $1
"""

LIST_FEEDS_SQL = """\
SELECT f.id, f.name, f.source_type, f.status, f.failure_count,
       f.worker_id, f.last_heartbeat, f.last_processed_filename,
       f.last_bookmark_time, f.created_at,
       fp.source_feed_id, fp.external_id
FROM feeds f
JOIN feed_properties fp ON f.id = fp.feed_id
ORDER BY f.created_at DESC
"""

DELETE_FEED_SQL = """\
DELETE FROM feeds
WHERE id = $1
"""

RESET_FEED_SQL = """\
WITH updated AS (
    UPDATE feeds
    SET status = 'unclaimed'::feed_status,
        failure_count = 0,
        worker_id = NULL,
        last_heartbeat = NOW()
    WHERE id = $1
    RETURNING id, name, source_type, status, failure_count, worker_id,
              last_heartbeat, last_processed_filename, last_bookmark_time, created_at
)
SELECT u.*, fp.source_feed_id, fp.external_id
FROM updated u
JOIN feed_properties fp ON fp.feed_id = u.id
"""

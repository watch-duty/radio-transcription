"""SQL queries for feed storage operations."""

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

# Primary per-type claim: three independent per-type CTEs, each locking its
# own rows with its own LIMIT. UNION ALL happens in a fourth CTE across
# their IDs — the outer UPDATE joins the combined IDs. Each per-type CTE
# gets its own MATERIALIZED pin to keep the SKIP LOCKED subquery from
# being inlined and re-evaluated per outer row (which would defeat the
# LIMIT under nested-loop plans).
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
# Params: $1=worker_id, $2=ramp_pct,
#         $3=limit_bcfy_feeds, $4=limit_bcfy_calls, $5=limit_openmhz.
ACQUIRE_FEEDS_BATCH_SQL = """\
WITH
    bcfy_feeds_claim AS MATERIALIZED (
        SELECT id FROM feeds
        WHERE source_type = 'bcfy_feeds' AND status = 'unclaimed'::feed_status
          AND (('x' || substr(md5(id::text), 1, 7))::bit(28)::integer) % 100 < $2
        ORDER BY id
        LIMIT $3
        FOR NO KEY UPDATE SKIP LOCKED
    ),
    bcfy_calls_claim AS MATERIALIZED (
        SELECT id FROM feeds
        WHERE source_type = 'bcfy_calls' AND status = 'unclaimed'::feed_status
          AND (('x' || substr(md5(id::text), 1, 7))::bit(28)::integer) % 100 < $2
        ORDER BY id
        LIMIT $4
        FOR NO KEY UPDATE SKIP LOCKED
    ),
    openmhz_claim AS MATERIALIZED (
        SELECT id FROM feeds
        WHERE source_type = 'openmhz' AND status = 'unclaimed'::feed_status
          AND (('x' || substr(md5(id::text), 1, 7))::bit(28)::integer) % 100 < $2
        ORDER BY id
        LIMIT $5
        FOR NO KEY UPDATE SKIP LOCKED
    ),
    claimed AS MATERIALIZED (
        SELECT id FROM bcfy_feeds_claim
        UNION ALL
        SELECT id FROM bcfy_calls_claim
        UNION ALL
        SELECT id FROM openmhz_claim
    ),
leased AS (
    UPDATE feeds
    SET status = 'active'::feed_status,
        worker_id = $1,
        fencing_token = fencing_token + 1,
        last_heartbeat = NOW(),
        retry_after = NULL
    FROM claimed
    WHERE feeds.id = claimed.id
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

# Recovery-path claim: failing-retryable + active-abandoned. Runs when the
# primary per-type CTE (ACQUIRE_FEEDS_BATCH_SQL) returns fewer rows than
# the worker's total slack. No per-type cap here: failing and abandoned
# volumes are small by construction (pg_cron sweep drains active-abandoned
# at 30 s cadence; failure events are rare). Ordering by retry_after ASC
# NULLS FIRST prioritizes unclaimed retries over drift between retry windows.
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
# Params: $1=worker_id, $2=abandonment_interval, $3=ramp_pct, $4=limit.
ACQUIRE_FEEDS_RECOVERY_SQL = """\
WITH recovered AS MATERIALIZED (
    SELECT id FROM feeds
    WHERE (
        (status = 'failing'::feed_status AND (retry_after IS NULL OR retry_after <= NOW()))
        OR (status = 'active'::feed_status AND last_heartbeat < NOW() - $2::interval)
    )
      AND (('x' || substr(md5(id::text), 1, 7))::bit(28)::integer) % 100 < $3
    ORDER BY retry_after ASC NULLS FIRST, id
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

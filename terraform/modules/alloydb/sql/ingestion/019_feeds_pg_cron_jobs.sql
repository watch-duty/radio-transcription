-- Hard prerequisite: the instance flag alloydb.enable_pg_cron=on must be set
-- via database_flags on the alloydb module. That change is owned by a
-- separate PR in radio-transcription-deployment and must land + apply FIRST,
-- before this migration is applied. Without it, CREATE EXTENSION pg_cron
-- fails and the Cloud Run schema-apply job exits non-zero.
-- Idempotent: CREATE EXTENSION IF NOT EXISTS + cron.schedule's name-based
-- upsert semantics (calling schedule() with an existing job_name updates
-- the existing row rather than inserting a duplicate).
--
-- File-naming convention (load-bearing): any migration whose application
-- requires pg_cron must have "pg_cron" in its filename. The substring is
-- matched by (a) the CI guard job in .github/workflows/ci.yml, (b) the
-- docker-compose postgres init script in local_dev/docker_postgres_init.sh,
-- and (c) the integration-test fixtures under backend/**/tests/ and
-- integration_tests/**/conftest.py. Any of those contexts runs against a
-- vanilla postgres image that lacks the pg_cron extension, so they skip
-- files matching *pg_cron*. If a future migration breaks this convention,
-- local tests and docker-compose will crash at CREATE EXTENSION with no
-- useful hint about where to apply the fix.
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Abandoned-lease sweep. Runs every 30 s, reclaims at most 500 rows per
-- invocation. The LIMIT 500 is load-bearing: a zonal outage can strand
-- ~4,000 leases; flipping all of them to 'unclaimed' in one transaction
-- would trigger a fleet-wide polling stampede. The LIMIT spreads the
-- status-flip over ~8 sweep cycles (~4 minutes), giving surviving workers
-- time to absorb the reclaimed feeds without AlloyDB contention.
--
-- Concurrency-safety: the CTE takes row locks with FOR UPDATE SKIP LOCKED,
-- not a bare IN-subquery. Two load-bearing properties:
--   (a) A bare `WHERE id IN (subquery)` form is vulnerable under READ
--       COMMITTED + EvalPlanQual: the subquery's row list is captured once,
--       then EPQ re-validates only the outer WHERE (`id IN cached_list`)
--       against rows concurrently modified by workers. A late heartbeat or
--       status transition that commits between subquery evaluation and the
--       outer UPDATE would be silently overwritten — e.g., a worker that
--       heartbeats at T=59.5s and the sweep that matched at T=60s would
--       still get flipped back to 'unclaimed'.
--   (b) FOR UPDATE locks rows while evaluating the WHERE clause, so the
--       status + last_heartbeat predicates see each row's live state.
--       SKIP LOCKED is the right modifier because workers actively writing
--       to a row (heartbeat, status transition) briefly hold that row's
--       lock — treating those rows as "not abandoned right now" and letting
--       the next sweep cycle pick them up if they're still stale is correct.
SELECT cron.schedule(
    'feeds-abandoned-lease-sweep',
    '30 seconds',
    $$
    -- AS MATERIALIZED is load-bearing: from PG 12 onward, unmarked CTEs
    -- are inlined into the outer query when the planner judges it
    -- profitable. Inlining a FOR UPDATE SKIP LOCKED + LIMIT CTE would
    -- let the planner push the LIMIT inside a nested-loop join, risking
    -- repeated scans that each lock-and-skip different rows. MATERIALIZED
    -- pins the CTE to one execution, so exactly the 500 rows we locked
    -- here are the ones the outer UPDATE operates on. Analogous to the
    -- claim CTE's MATERIALIZED requirement in the scaling plan §6.
    WITH abandoned AS MATERIALIZED (
        SELECT id, last_heartbeat FROM public.feeds
         WHERE status = 'active'::feed_status
           -- Echo feeds are event-driven/push-based (processed via Cloud Run triggers). They
           -- do not have continuous worker lease-loop heartbeats (heartbeats only occur on
           -- handler execution), so periods of silence are expected and they should not be swept
           AND source_type != 'echo'
           AND last_heartbeat < NOW() - INTERVAL '60 seconds'
         LIMIT 500
         FOR UPDATE SKIP LOCKED
    )
    UPDATE public.feeds
       SET status = 'unclaimed'::feed_status,
           worker_id = NULL,
           -- Preserve the true abandonment time rather than resetting to
           -- NOW(). The autoscaler's oldest-unclaimed-feed-age metric is
           -- MIN(unclaimed_since) WHERE status='unclaimed'; if the sweep
           -- stamped NOW() on a zonal outage's ~4k abandoned feeds, the
           -- metric would artificially reset to ~0 seconds right when
           -- scale-out is most needed. Using the abandoned worker's
           -- last_heartbeat is strictly better than the alternative of
           -- NOW() - INTERVAL '60 seconds' because it also reflects the
           -- case where the sweep runs behind (e.g., pg_cron paused),
           -- signaling the real age of the backlog rather than a fixed
           -- "at least 60s" floor.
           unclaimed_since = abandoned.last_heartbeat
      FROM abandoned
     WHERE feeds.id = abandoned.id;
    $$
);

-- Minute-cadence VACUUM. Required because heap_page_prune_opt reclaims
-- tuple bytes but does not shrink the line-pointer (ItemId) array; only
-- VACUUM pushes LP_DEAD → LP_UNUSED. On a 12k-row / ~430-page table each
-- run is tens of milliseconds. See scaling plan §6.
--
-- Deliberately NOT "VACUUM (ANALYZE)" as the scaling plan's verbatim SQL
-- suggested. ANALYZE every 60 seconds is redundant with autovacuum's
-- analyze path (autovacuum checks per naptime, default 60s) and
-- invalidates cached plans fleet-wide on every tick — a measurable cost
-- for the claim and heartbeat queries that re-plan from cold against a
-- freshly-analyzed pg_statistic. Bare VACUUM fulfills this job's sole
-- stated purpose (LP-array maintenance) and leaves statistics-freshness
-- to autovacuum, where it belongs.
SELECT cron.schedule(
    'feeds-vac',
    '* * * * *',
    'VACUUM public.feeds'
);

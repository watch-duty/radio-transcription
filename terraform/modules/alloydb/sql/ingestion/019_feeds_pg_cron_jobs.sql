-- Hard prerequisite: the instance flag alloydb.enable_pg_cron=on must be set
-- via database_flags on the alloydb module. That change is owned by a
-- separate PR in radio-transcription-deployment and must land + apply FIRST,
-- before this migration is applied. Without it, CREATE EXTENSION pg_cron
-- fails and the Cloud Run schema-apply job exits non-zero.
-- Idempotent: CREATE EXTENSION IF NOT EXISTS + cron.schedule's name-based
-- upsert semantics (calling schedule() with an existing job_name updates
-- the existing row rather than inserting a duplicate).
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Abandoned-lease sweep. Runs every 30 s, reclaims at most 500 rows per
-- invocation. The LIMIT 500 is load-bearing: a zonal outage can strand
-- ~4,000 leases; flipping all of them to 'unclaimed' in one transaction
-- would trigger a fleet-wide polling stampede. The LIMIT spreads the
-- status-flip over ~8 sweep cycles (~4 minutes), giving surviving workers
-- time to absorb the reclaimed feeds without AlloyDB contention.
SELECT cron.schedule(
    'feeds-abandoned-lease-sweep',
    '30 seconds',
    $$
    UPDATE feeds
       SET status = 'unclaimed'::feed_status,
           worker_id = NULL,
           unclaimed_since = NOW()
     WHERE id IN (
         SELECT id FROM feeds
          WHERE status = 'active'::feed_status
            AND last_heartbeat < NOW() - INTERVAL '60 seconds'
          LIMIT 500
     );
    $$
);

-- Minute-cadence VACUUM (ANALYZE). Required because heap_page_prune_opt
-- reclaims tuple bytes but does not shrink the line-pointer (ItemId) array;
-- only VACUUM pushes LP_DEAD → LP_UNUSED. On a 12k-row / ~430-page table
-- each run is tens of milliseconds. See scaling plan §6.
SELECT cron.schedule(
    'feeds-vac',
    '* * * * *',
    'VACUUM (ANALYZE) feeds'
);

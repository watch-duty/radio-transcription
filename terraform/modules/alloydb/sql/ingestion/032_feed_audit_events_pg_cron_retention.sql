-- Hard prerequisite: the AlloyDB instance flag alloydb.enable_pg_cron=on
-- must be set before this migration is applied.
-- File-naming convention: this migration requires pg_cron and therefore
-- must keep "pg_cron" in its filename.
CREATE EXTENSION IF NOT EXISTS pg_cron;

SELECT cron.schedule(
    'feed-audit-events-retention',
    '15 3 * * *',
    'CALL public.prune_feed_audit_events_retention()'
);

-- AUTOCOMMIT
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_feed_properties_bcfy_calls_membership
ON public.feed_properties USING btree
    (bcfy_calls_sid, bcfy_calls_group_id, feed_id)
WHERE source_type = 'bcfy_calls'
  AND bcfy_calls_is_trunked IS TRUE;

DO $index_health$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_index AS index_state
        WHERE index_state.indexrelid = pg_catalog.to_regclass(
                  'public.idx_feed_properties_bcfy_calls_membership'
              )
          AND index_state.indrelid = 'public.feed_properties'::regclass
          AND index_state.indisvalid
    ) THEN
        RAISE EXCEPTION USING
            MESSAGE = 'Broadcastify Calls membership index is missing or invalid',
            HINT =
                'Run DROP INDEX CONCURRENTLY ' ||
                'public.idx_feed_properties_bcfy_calls_membership; ' ||
                'then reapply the schema migrations';
    END IF;
END
$index_health$;

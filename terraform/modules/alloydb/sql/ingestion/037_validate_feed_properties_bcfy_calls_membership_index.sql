-- Fail closed if an interrupted concurrent build left an invalid same-named
-- index, without duplicating the complete index definition.
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
                'Run DROP INDEX CONCURRENTLY IF EXISTS ' ||
                'public.idx_feed_properties_bcfy_calls_membership; ' ||
                'then reapply the schema migrations';
    END IF;
END
$index_health$;

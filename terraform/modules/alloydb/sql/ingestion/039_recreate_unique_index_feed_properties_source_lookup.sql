-- AUTOCOMMIT
-- Recreate unique composite index on feed_properties(source_type, source_feed_id).
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_feed_properties_source_lookup
ON feed_properties(source_type, source_feed_id);

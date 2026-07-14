-- AUTOCOMMIT
-- Drop the unused, high-overhead GIN index on annotations(data) to prevent lock contention and connection pool starvation during concurrent waveform inserts.
DROP INDEX CONCURRENTLY IF EXISTS idx_annotations_data;

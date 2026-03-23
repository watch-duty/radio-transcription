-- Replace the old leasing index with one that includes retry_after.
-- The new index supports the per-status WHERE clause structure of the
-- updated lease queries.
DROP INDEX IF EXISTS idx_feeds_leasing;
CREATE INDEX idx_feeds_leasing ON feeds (status, retry_after, last_heartbeat)
WHERE status IN ('unclaimed'::feed_status, 'failing'::feed_status, 'active'::feed_status);

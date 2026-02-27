-- Idempotent: IF NOT EXISTS allows safe re-application during Terraform runs.
CREATE INDEX IF NOT EXISTS idx_feeds_leasing ON feeds (status, last_heartbeat)
    WHERE status IN ('unclaimed'::feed_status, 'failing'::feed_status);

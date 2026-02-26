CREATE INDEX idx_feeds_leasing ON feeds (status, last_heartbeat)
    WHERE status IN ('unclaimed'::feed_status, 'failing'::feed_status);

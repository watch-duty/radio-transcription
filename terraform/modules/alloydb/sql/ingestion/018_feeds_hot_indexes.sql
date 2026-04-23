-- Drop the HOT-blocking leasing index. It includes last_heartbeat, which is
-- mutated on every heartbeat; any UPDATE touching last_heartbeat under this
-- index costs a full index write. The three partial indexes below replace
-- its functionality without indexing any high-churn column.
-- Idempotent: DROP INDEX IF EXISTS.
DROP INDEX IF EXISTS idx_feeds_leasing;

-- Serves the "failing with retry elapsed" recovery branch.
-- retry_after is HOT-protected in principle but only mutated on failure
-- (rare) — bloat is acceptable, and this index is explicitly allow-listed
-- in the CI guard (see sql/ci/hot_protection_check.sql).
CREATE INDEX IF NOT EXISTS idx_feeds_failing_retryable
    ON feeds (retry_after)
    WHERE status = 'failing'::feed_status;

-- Serves the abandoned-lease sweep's scan of active rows.
CREATE INDEX IF NOT EXISTS idx_feeds_active
    ON feeds (id)
    WHERE status = 'active'::feed_status;

-- Composite partial index for the per-type UNION ALL MATERIALIZED CTE
-- (scaling plan §6). Supports deterministic per-branch ORDER BY id scans
-- without a sort node. Both indexed columns (source_type, id) are immutable
-- in the hot path — HOT-safe. Also serves the publisher's
-- MIN(unclaimed_since) aggregate as a partial-index scan.
CREATE INDEX IF NOT EXISTS feeds_claim_by_type_idx
    ON feeds (source_type, id)
    WHERE status = 'unclaimed'::feed_status;

-- Column for the autoscaler's oldest-unclaimed-feed-age signal.
-- Deliberately NOT indexed: mutated on every status transition to 'unclaimed'
-- (INSERT, sweep, SIGTERM lease-release). Indexing it would break HOT.
-- Idempotent: ADD COLUMN IF NOT EXISTS + SET DEFAULT + WHERE-guarded backfill.
ALTER TABLE feeds
    ADD COLUMN IF NOT EXISTS unclaimed_since TIMESTAMP WITH TIME ZONE;

-- Column default. Applied in a separate statement because ADD COLUMN IF NOT
-- EXISTS skips (including any new DEFAULT clause) if the column already
-- exists — a hazard on the Cloud Run schema-apply's repeated runs. SET
-- DEFAULT is idempotent on its own. With DEFAULT NOW(), every INSERT that
-- omits unclaimed_since (i.e., all current CREATE_FEED_SQL paths) captures
-- the moment the row became unclaimed — which, because status defaults to
-- 'unclaimed', is always the row's effective unclaimed-start time.
ALTER TABLE feeds
    ALTER COLUMN unclaimed_since SET DEFAULT NOW();

-- Backfill for rows that existed before this migration: seed unclaimed ones
-- with created_at so the publisher's MIN(unclaimed_since) aggregate has a
-- non-NULL value. New rows (post-migration) get NOW() via the column
-- default. The sweep (019) and the RELEASE queries (feed_queries.py) write
-- unclaimed_since explicitly on every status→unclaimed transition after
-- that.
UPDATE feeds
   SET unclaimed_since = created_at
 WHERE status = 'unclaimed'::feed_status
   AND unclaimed_since IS NULL;

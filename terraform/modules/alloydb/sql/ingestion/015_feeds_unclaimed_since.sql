-- Column for the autoscaler's oldest-unclaimed-feed-age signal.
-- Deliberately NOT indexed: mutated on every status transition to 'unclaimed'
-- (INSERT, sweep, SIGTERM lease-release). Indexing it would break HOT.
-- Idempotent: ADD COLUMN IF NOT EXISTS + WHERE-guarded backfill UPDATE.
ALTER TABLE feeds
    ADD COLUMN IF NOT EXISTS unclaimed_since TIMESTAMP WITH TIME ZONE;

-- Backfill: seed existing unclaimed rows with created_at so the publisher's
-- MIN(unclaimed_since) aggregate has a non-NULL value. Worker code will set
-- unclaimed_since on future status→unclaimed transitions.
UPDATE feeds
   SET unclaimed_since = created_at
 WHERE status = 'unclaimed'::feed_status
   AND unclaimed_since IS NULL;

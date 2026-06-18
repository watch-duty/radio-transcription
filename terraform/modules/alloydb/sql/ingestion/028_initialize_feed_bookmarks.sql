-- Ensure new polling-based feeds (bcfy_calls, fire_notifications) default to NOW()
-- for last_bookmark_time, while preserving NULL for other feed types (e.g. echo).
-- Note: 'echo' is push-based and filters historical records at ingestion time by comparing
-- event start_ts with feed created_at, so it doesn't use last_bookmark_time.

-- Drop any pre-existing default value from prior implementation attempts before setting up the trigger.
ALTER TABLE feeds ALTER COLUMN last_bookmark_time DROP DEFAULT;

CREATE OR REPLACE FUNCTION set_default_feed_bookmarks()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.source_type IN ('bcfy_calls', 'fire_notifications') AND NEW.last_bookmark_time IS NULL THEN
        NEW.last_bookmark_time := NOW();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_set_default_feed_bookmarks ON feeds;

CREATE TRIGGER trg_set_default_feed_bookmarks
BEFORE INSERT ON feeds
FOR EACH ROW
EXECUTE FUNCTION set_default_feed_bookmarks();

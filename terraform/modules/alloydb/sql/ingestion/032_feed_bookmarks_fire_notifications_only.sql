-- SID-managed Broadcastify Calls children must be born with a NULL
-- last_bookmark_time: the SID owner admits a new member only after
-- stamping it at a page boundary (SourceObservation), and a NOW()
-- cursor bypasses that adoption protocol. The 028 trigger cannot make
-- the distinction — it fires BEFORE INSERT on feeds, before the
-- feed_properties row exists to identify a maintained SID member — and
-- since all bcfy_calls creation now goes through the SID-managed path,
-- bcfy_calls no longer needs creation-time cursor seeding at all.
--
-- Narrow the trigger to fire_notifications, whose collector treats a
-- NULL cursor as "no time filter" and would otherwise ingest the
-- entire current file listing on a newly created feed.
--
-- Idempotent: CREATE OR REPLACE; the 028 trigger binding is unchanged.
CREATE OR REPLACE FUNCTION set_default_feed_bookmarks()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.source_type = 'fire_notifications' AND NEW.last_bookmark_time IS NULL THEN
        NEW.last_bookmark_time := NOW();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

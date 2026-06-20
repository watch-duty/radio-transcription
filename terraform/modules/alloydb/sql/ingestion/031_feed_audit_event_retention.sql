CREATE OR REPLACE PROCEDURE public.prune_feed_audit_events_retention()
LANGUAGE plpgsql
AS $$
BEGIN
    WITH expired_events AS MATERIALIZED (
        SELECT id
        FROM public.feed_audit_events
        WHERE occurred_at < NOW() - INTERVAL '18 months'
        ORDER BY occurred_at, id
        LIMIT 10000
        FOR UPDATE SKIP LOCKED
    )
    DELETE FROM public.feed_audit_events events
    USING expired_events
    WHERE events.id = expired_events.id;

    WITH orphaned_sequences AS MATERIALIZED (
        SELECT sequences.feed_id
        FROM public.feed_audit_event_sequences sequences
        WHERE NOT EXISTS (
            SELECT 1
            FROM public.feeds feeds
            WHERE feeds.id = sequences.feed_id
        )
          AND NOT EXISTS (
            SELECT 1
            FROM public.feed_audit_events events
            WHERE events.feed_id = sequences.feed_id
        )
        ORDER BY sequences.feed_id
        LIMIT 10000
        FOR UPDATE SKIP LOCKED
    )
    DELETE FROM public.feed_audit_event_sequences sequences
    USING orphaned_sequences
    WHERE sequences.feed_id = orphaned_sequences.feed_id;
END;
$$;

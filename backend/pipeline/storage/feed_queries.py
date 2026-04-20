"""SQL queries for feed storage operations."""

LEASE_FEED_SQL = """\
WITH available_feed AS (
    SELECT id
    FROM feeds
    WHERE (
        status = 'unclaimed'::feed_status
        OR (status = 'failing'::feed_status AND (retry_after IS NULL OR retry_after <= NOW()))
        OR (status = 'active'::feed_status
            AND last_heartbeat < NOW() - INTERVAL '60 seconds')
    )
    AND ($2::text[] IS NULL OR source_type = ANY($2::text[]))
    ORDER BY (status = 'unclaimed'::feed_status) DESC,
             retry_after ASC NULLS FIRST,
             last_heartbeat ASC NULLS FIRST
    LIMIT 1
    FOR UPDATE SKIP LOCKED
),
leased AS (
    UPDATE feeds
    SET worker_id = $1,
        status = 'active'::feed_status,
        retry_after = NULL,
        last_heartbeat = NOW(),
        fencing_token = fencing_token + 1
    FROM available_feed
    WHERE feeds.id = available_feed.id
    RETURNING feeds.id, feeds.name, feeds.source_type,
              feeds.last_processed_filename, feeds.last_bookmark_time,
              feeds.fencing_token
)
SELECT leased.id, leased.name, leased.source_type,
       leased.last_processed_filename, leased.last_bookmark_time,
       leased.fencing_token, fpi.source_feed_id
FROM leased
JOIN feed_properties fpi ON fpi.feed_id = leased.id
"""

UPDATE_PROGRESS_SQL = """\
UPDATE feeds
SET last_processed_filename = $1,
    last_bookmark_time = COALESCE($5, last_bookmark_time),
    last_heartbeat = NOW(),
    failure_count = 0
WHERE id = $2 AND worker_id = $3 AND fencing_token = $4
"""

RENEW_HEARTBEATS_BATCH_DIAGNOSTIC_SQL = """\
WITH current_state AS (
    SELECT id, worker_id, status
    FROM feeds WHERE id = ANY($1::uuid[])
    FOR UPDATE
),
do_update AS (
    UPDATE feeds SET last_heartbeat = NOW()
    FROM current_state
    WHERE feeds.id = current_state.id AND current_state.worker_id = $2
    RETURNING feeds.id
)
SELECT
    current_state.id,
    current_state.worker_id AS current_worker,
    current_state.status::text AS current_status,
    (do_update.id IS NOT NULL) AS renewed
FROM current_state
LEFT JOIN do_update ON current_state.id = do_update.id;
"""

RELEASE_FEED_SQL = """\
UPDATE feeds
SET worker_id = NULL,
    status = 'unclaimed'::feed_status,
    last_heartbeat = NOW()
WHERE id = $1 AND worker_id = $2 AND fencing_token = $3
"""

RELEASE_FEEDS_BATCH_SQL = """\
UPDATE feeds
SET worker_id = NULL,
    status = 'unclaimed'::feed_status,
    last_heartbeat = NOW()
WHERE worker_id = $1 AND status = 'active'::feed_status
"""

ACQUIRE_FEEDS_BATCH_SQL = """\
WITH available_feeds AS (
    SELECT id
    FROM feeds
    WHERE (
        status = 'unclaimed'::feed_status
        OR (status = 'failing'::feed_status AND (retry_after IS NULL OR retry_after <= NOW()))
        OR (status = 'active'::feed_status
            AND last_heartbeat < NOW() - $2::interval)
    )
    AND ($4::text[] IS NULL OR source_type = ANY($4::text[]))
    ORDER BY (status = 'unclaimed'::feed_status) DESC,
             retry_after ASC NULLS FIRST,
             last_heartbeat ASC NULLS FIRST
    LIMIT $3
    FOR UPDATE SKIP LOCKED
),
leased AS (
    UPDATE feeds
    SET worker_id = $1,
        status = 'active'::feed_status,
        retry_after = NULL,
        last_heartbeat = NOW(),
        fencing_token = fencing_token + 1
    FROM available_feeds
    WHERE feeds.id = available_feeds.id
    RETURNING feeds.id, feeds.name, feeds.source_type,
              feeds.last_processed_filename, feeds.last_bookmark_time,
              feeds.fencing_token
)
SELECT leased.id, leased.name, leased.source_type,
       leased.last_processed_filename, leased.last_bookmark_time,
       leased.fencing_token, fpi.source_feed_id
FROM leased
JOIN feed_properties fpi ON fpi.feed_id = leased.id
"""

REPORT_FAILURE_SQL = """\
UPDATE feeds
SET status = CASE WHEN failure_count + 1 >= $3
                  THEN 'quarantined'::feed_status
                  ELSE 'failing'::feed_status END,
    failure_count = failure_count + 1,
    worker_id = NULL,
    last_heartbeat = NOW(),
    retry_after = CASE WHEN failure_count + 1 < $3
                       THEN NOW() + LEAST($5 * INTERVAL '1 second',
                            $6 * INTERVAL '1 second' * POWER(2, failure_count))
                            + (RANDOM() * INTERVAL '10 seconds')
                       ELSE NULL END
WHERE id = $1 AND worker_id = $2 AND fencing_token = $4
RETURNING status::text, failure_count, retry_after
"""

CREATE_FEED_SQL = """\
WITH new_feed AS (
    INSERT INTO feeds (name, source_type)
    VALUES ($1, $2)
    RETURNING id, name, source_type, status, failure_count, worker_id, last_heartbeat, last_processed_filename, last_bookmark_time, created_at
),
new_props AS (
    INSERT INTO feed_properties (feed_id, source_feed_id, external_id, source_type)
    SELECT id, $3, $4, source_type FROM new_feed
    RETURNING source_feed_id, external_id
)
SELECT nf.*, np.source_feed_id, np.external_id
FROM new_feed nf
JOIN new_props np ON TRUE;
"""

GET_FEED_SQL = """\
SELECT f.id, f.name, f.source_type, f.status, f.failure_count,
       f.worker_id, f.last_heartbeat, f.last_processed_filename,
       f.last_bookmark_time, f.created_at,
       fp.source_feed_id, fp.external_id
FROM feeds f
JOIN feed_properties fp ON f.id = fp.feed_id
WHERE f.id = $1
"""

LIST_FEEDS_SQL = """\
SELECT f.id, f.name, f.source_type, f.status, f.failure_count,
       f.worker_id, f.last_heartbeat, f.last_processed_filename,
       f.last_bookmark_time, f.created_at,
       fp.source_feed_id, fp.external_id
FROM feeds f
JOIN feed_properties fp ON f.id = fp.feed_id
ORDER BY f.created_at DESC
"""

DELETE_FEED_SQL = """\
DELETE FROM feeds
WHERE id = $1
"""

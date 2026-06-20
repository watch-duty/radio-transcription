"""Synchronous feed lifecycle SQL queries.

psycopg v3 uses ``%s`` parameters, so these stay separate from the async
``feed_queries`` module which uses asyncpg's ``$1`` parameter style. Echo
lifecycle writes also rely on terminal-state guards instead of VM lease
fencing.
"""

RESOLVE_ECHO_FEED_SQL = """\
SELECT f.id, f.name, f.status, f.failure_count, f.status_reason, f.created_at
FROM feeds f
JOIN feed_properties fp ON fp.feed_id = f.id
WHERE fp.source_feed_id = %s
AND fp.source_type = 'echo'
AND f.source_type = 'echo'
"""

HEARTBEAT_SQL = """\
UPDATE feeds
SET last_heartbeat = NOW(),
    failure_count = CASE WHEN failure_count > 0 THEN 0 ELSE failure_count END,
    status = 'active'::feed_status,
    status_reason_updated_at = CASE
        WHEN status_reason IS NOT NULL OR status_reason_detail IS NOT NULL
            THEN NOW()
        ELSE status_reason_updated_at
    END,
    status_reason_detail = NULL,
    status_reason = NULL
WHERE id = %s
  AND status NOT IN ('quarantined'::feed_status, 'deactivated'::feed_status)
"""

# Backoff formula: base * 2^(failure_count), capped at max, plus 0-10s jitter.
# Matches REPORT_FAILURE_SQL in feed_queries.py minus worker_id/fencing_token.
RECORD_FAILURE_SQL = """\
UPDATE feeds
SET status = CASE WHEN failure_count + 1 >= %s
                  THEN 'quarantined'::feed_status
                  ELSE 'failing'::feed_status END,
    failure_count = failure_count + 1,
    last_heartbeat = NOW(),
    retry_after = CASE WHEN failure_count + 1 < %s
                       THEN NOW() + LEAST(
                            %s * INTERVAL '1 second',
                            %s * INTERVAL '1 second' * POWER(2, failure_count)
                       ) + (RANDOM() * INTERVAL '10 seconds')
                       ELSE NULL END,
    status_reason = COALESCE(%s, 'system_unexpected_error'),
    status_reason_detail = %s,
    status_reason_updated_at = CASE
        WHEN status_reason IS DISTINCT FROM COALESCE(%s, 'system_unexpected_error')
            THEN NOW()
        ELSE status_reason_updated_at
    END
WHERE id = %s
  AND status NOT IN ('quarantined'::feed_status, 'deactivated'::feed_status)
"""

RECORD_NON_BUDGETED_FAILURE_SQL = """\
UPDATE feeds
SET status = 'failing'::feed_status,
    failure_count = 0,
    last_heartbeat = NOW(),
    retry_after = NULL,
    status_reason = %s,
    status_reason_detail = %s,
    status_reason_updated_at = CASE
        WHEN status_reason IS DISTINCT FROM %s THEN NOW()
        ELSE status_reason_updated_at
    END
WHERE id = %s
  AND status NOT IN ('quarantined'::feed_status, 'deactivated'::feed_status)
"""

GET_AUDIT_FEED_SNAPSHOT_SQL = """\
SELECT
    f.id,
    f.name,
    f.source_type,
    f.status,
    f.failure_count,
    f.retry_after,
    f.status_reason,
    f.status_reason_updated_at,
    f.status_reason_detail,
    f.quarantine_reason,
    f.last_bookmark_time,
    f.created_at,
    f.audit_revision AS feed_revision,
    fp.source_feed_id AS "feed_properties.source_feed_id",
    fp.tags AS "feed_properties.tags"
FROM feeds f
JOIN feed_properties fp ON fp.feed_id = f.id
WHERE f.id = %s
FOR UPDATE
"""

INSERT_FEED_AUDIT_EVENT_SQL = """\
INSERT INTO feed_audit_events (
    feed_id,
    action,
    actor_id,
    feed_revision,
    before_values,
    after_values,
    metadata
)
VALUES (
    %s,
    %s,
    %s,
    %s,
    %s::jsonb,
    %s::jsonb,
    COALESCE(%s::jsonb, '{}'::jsonb)
)
"""

"""Synchronous feed lifecycle SQL queries.

psycopg v3 uses ``%s`` parameters, so these stay separate from the async
``feed_queries`` module which uses asyncpg's ``$1`` parameter style. Echo
lifecycle writes also rely on terminal-state guards instead of VM lease
fencing.
"""
# ruff: noqa: S608

_AUDIT_SNAPSHOT_FIELDS = (
    ("id", "id"),
    ("name", "name"),
    ("source_type", "source_type"),
    ("status", "status"),
    ("failure_count", "failure_count"),
    ("retry_after", "retry_after"),
    ("status_reason", "status_reason"),
    ("status_reason_updated_at", "status_reason_updated_at"),
    ("status_reason_detail", "status_reason_detail"),
    ("quarantine_reason", "quarantine_reason"),
    ("last_bookmark_time", "last_bookmark_time"),
    ("created_at", "created_at"),
    ("feed_properties.source_feed_id", "source_feed_id"),
    ("feed_properties.tags", "tags"),
)


def _audit_snapshot_sql(alias: str) -> str:
    """Return a JSONB object expression for the maintained audit allowlist."""
    parts: list[str] = []
    for key, column in _AUDIT_SNAPSHOT_FIELDS:
        value = f"{alias}.{column}"
        if column in {"source_type", "status"}:
            value = f"{value}::text"
        if column == "tags":
            value = f"COALESCE({value}, '[]'::jsonb)"
        parts.append(f"        {key!r}, {value}")
    return "jsonb_build_object(\n" + ",\n".join(parts) + "\n    )"


def _audit_source_projection(alias: str = "f") -> str:
    """Return the feed fields needed to build an audit snapshot."""
    return (
        f"    {alias}.id,\n"
        f"    {alias}.name,\n"
        f"    {alias}.source_type,\n"
        f"    {alias}.status,\n"
        f"    {alias}.failure_count,\n"
        f"    {alias}.retry_after,\n"
        f"    {alias}.status_reason,\n"
        f"    {alias}.status_reason_updated_at,\n"
        f"    {alias}.status_reason_detail,\n"
        f"    {alias}.quarantine_reason,\n"
        f"    {alias}.last_bookmark_time,\n"
        f"    {alias}.created_at,\n"
        "    fp.source_feed_id,\n"
        "    fp.tags"
    )


_AUDIT_BEFORE_SNAPSHOT_SQL = _audit_snapshot_sql("before_row")
_AUDIT_AFTER_SNAPSHOT_SQL = _audit_snapshot_sql("after_row")

RESOLVE_ECHO_FEED_SQL = """\
SELECT f.id, f.name, f.status, f.failure_count, f.status_reason, f.created_at
FROM feeds f
JOIN feed_properties fp ON fp.feed_id = f.id
WHERE fp.source_feed_id = %s
AND fp.source_type = 'echo'
AND f.source_type = 'echo'
"""

HEARTBEAT_SQL = f"""\
WITH before_row AS (
    SELECT
{_audit_source_projection("f")},
        f.audit_revision
    FROM feeds f
    JOIN feed_properties fp ON fp.feed_id = f.id
    WHERE f.id = %s
      AND f.status NOT IN ('quarantined'::feed_status, 'deactivated'::feed_status)
    FOR UPDATE
),
updated AS (
    UPDATE feeds
    SET last_heartbeat = NOW(),
        audit_revision = CASE
            WHEN feeds.failure_count <> 0 OR feeds.status_reason IS NOT NULL THEN feeds.audit_revision + 1
            ELSE feeds.audit_revision
        END,
        failure_count = CASE WHEN feeds.failure_count > 0 THEN 0 ELSE feeds.failure_count END,
        status = 'active'::feed_status,
        status_reason_updated_at = CASE
            WHEN feeds.status_reason IS NOT NULL OR feeds.status_reason_detail IS NOT NULL
                THEN NOW()
            ELSE feeds.status_reason_updated_at
        END,
        status_reason_detail = NULL,
        status_reason = NULL
    FROM before_row
    WHERE feeds.id = before_row.id
    RETURNING feeds.id, feeds.name, feeds.source_type,
              feeds.status::text AS status, feeds.failure_count,
              feeds.retry_after, feeds.status_reason,
              feeds.status_reason_updated_at, feeds.status_reason_detail,
              feeds.quarantine_reason, feeds.last_bookmark_time,
              feeds.created_at, feeds.audit_revision AS feed_revision
),
after_row AS (
    SELECT u.*, fp.source_feed_id, fp.tags
    FROM updated u
    JOIN feed_properties fp ON fp.feed_id = u.id
),
audit_action AS (
    SELECT CASE
        WHEN (
            CASE
                WHEN before_row.status = 'active'::feed_status
                 AND (before_row.failure_count > 0 OR before_row.status_reason IS NOT NULL)
                THEN 'failing'::feed_status
                ELSE before_row.status
            END
        ) IN ('failing'::feed_status, 'quarantined'::feed_status)
        AND after_row.status NOT IN ('failing', 'quarantined')
        AND after_row.status_reason IS NULL
        AND after_row.failure_count = 0
        THEN 'feed.recovered'
        ELSE NULL
    END AS action
    FROM before_row
    JOIN after_row ON after_row.id = before_row.id
),
write_audit AS (
    INSERT INTO feed_audit_events (
        feed_id, action, actor_id, feed_revision,
        before_values, after_values
    )
    SELECT
        after_row.id,
        audit_action.action,
        %s::text,
        after_row.feed_revision,
{_AUDIT_BEFORE_SNAPSHOT_SQL},
{_AUDIT_AFTER_SNAPSHOT_SQL}
    FROM before_row
    JOIN after_row ON after_row.id = before_row.id
    CROSS JOIN audit_action
    WHERE %s::text IS NOT NULL
      AND audit_action.action IS NOT NULL
    RETURNING id
)
SELECT after_row.*
FROM after_row
"""

# Backoff formula: base * 2^(failure_count), capped at max, plus 0-10s jitter.
# Matches REPORT_FAILURE_SQL in feed_queries.py minus worker_id/fencing_token.
RECORD_FAILURE_SQL = f"""\
WITH before_row AS (
    SELECT
{_audit_source_projection("f")},
        f.audit_revision
    FROM feeds f
    JOIN feed_properties fp ON fp.feed_id = f.id
    WHERE f.id = %s
      AND f.status NOT IN ('quarantined'::feed_status, 'deactivated'::feed_status)
    FOR UPDATE
),
updated AS (
    UPDATE feeds
    SET status = CASE WHEN feeds.failure_count + 1 >= %s
                      THEN 'quarantined'::feed_status
                      ELSE 'failing'::feed_status END,
        audit_revision = feeds.audit_revision + 1,
        failure_count = feeds.failure_count + 1,
        last_heartbeat = NOW(),
        retry_after = CASE WHEN feeds.failure_count + 1 < %s
                           THEN NOW() + LEAST(
                                %s * INTERVAL '1 second',
                                %s * INTERVAL '1 second' * POWER(2, feeds.failure_count)
                           ) + (RANDOM() * INTERVAL '10 seconds')
                           ELSE NULL END,
        status_reason = COALESCE(%s, 'system_unexpected_error'),
        status_reason_detail = %s,
        status_reason_updated_at = CASE
            WHEN feeds.status_reason IS DISTINCT FROM COALESCE(%s, 'system_unexpected_error')
                THEN NOW()
            ELSE feeds.status_reason_updated_at
        END
    FROM before_row
    WHERE feeds.id = before_row.id
    RETURNING feeds.id, feeds.name, feeds.source_type,
              feeds.status::text AS status, feeds.failure_count,
              feeds.retry_after, feeds.status_reason,
              feeds.status_reason_updated_at, feeds.status_reason_detail,
              feeds.quarantine_reason, feeds.last_bookmark_time,
              feeds.created_at, feeds.audit_revision AS feed_revision
),
after_row AS (
    SELECT u.*, fp.source_feed_id, fp.tags
    FROM updated u
    JOIN feed_properties fp ON fp.feed_id = u.id
),
audit_action AS (
    SELECT CASE
        WHEN after_row.status = 'quarantined'
         AND (
            CASE
                WHEN before_row.status = 'active'::feed_status
                 AND (before_row.failure_count > 0 OR before_row.status_reason IS NOT NULL)
                THEN 'failing'::feed_status
                ELSE before_row.status
            END
         ) <> 'quarantined'::feed_status
         AND after_row.failure_count > before_row.failure_count
        THEN 'feed.quarantined'
        WHEN after_row.status = 'failing'
         AND (
            (
                CASE
                    WHEN before_row.status = 'active'::feed_status
                     AND (before_row.failure_count > 0 OR before_row.status_reason IS NOT NULL)
                    THEN 'failing'::feed_status
                    ELSE before_row.status
                END
            ) <> 'failing'::feed_status
            OR before_row.status_reason IS DISTINCT FROM after_row.status_reason
         )
        THEN 'feed.failure_reported'
        ELSE NULL
    END AS action
    FROM before_row
    JOIN after_row ON after_row.id = before_row.id
),
write_audit AS (
    INSERT INTO feed_audit_events (
        feed_id, action, actor_id, feed_revision,
        before_values, after_values
    )
    SELECT
        after_row.id,
        audit_action.action,
        %s::text,
        after_row.feed_revision,
{_AUDIT_BEFORE_SNAPSHOT_SQL},
{_AUDIT_AFTER_SNAPSHOT_SQL}
    FROM before_row
    JOIN after_row ON after_row.id = before_row.id
    CROSS JOIN audit_action
    WHERE %s::text IS NOT NULL
      AND audit_action.action IS NOT NULL
    RETURNING id
)
SELECT after_row.*
FROM after_row
"""

RECORD_NON_BUDGETED_FAILURE_SQL = f"""\
WITH before_row AS (
    SELECT
{_audit_source_projection("f")},
        f.audit_revision
    FROM feeds f
    JOIN feed_properties fp ON fp.feed_id = f.id
    WHERE f.id = %s
      AND f.status NOT IN ('quarantined'::feed_status, 'deactivated'::feed_status)
    FOR UPDATE
),
updated AS (
    UPDATE feeds
    SET status = 'failing'::feed_status,
        audit_revision = feeds.audit_revision + 1,
        failure_count = 0,
        last_heartbeat = NOW(),
        retry_after = NULL,
        status_reason = %s,
        status_reason_detail = %s,
        status_reason_updated_at = CASE
            WHEN feeds.status_reason IS DISTINCT FROM %s THEN NOW()
            ELSE feeds.status_reason_updated_at
        END
    FROM before_row
    WHERE feeds.id = before_row.id
    RETURNING feeds.id, feeds.name, feeds.source_type,
              feeds.status::text AS status, feeds.failure_count,
              feeds.retry_after, feeds.status_reason,
              feeds.status_reason_updated_at, feeds.status_reason_detail,
              feeds.quarantine_reason, feeds.last_bookmark_time,
              feeds.created_at, feeds.audit_revision AS feed_revision
),
after_row AS (
    SELECT u.*, fp.source_feed_id, fp.tags
    FROM updated u
    JOIN feed_properties fp ON fp.feed_id = u.id
),
audit_action AS (
    SELECT CASE
        WHEN after_row.status = 'quarantined'
         AND (
            CASE
                WHEN before_row.status = 'active'::feed_status
                 AND (before_row.failure_count > 0 OR before_row.status_reason IS NOT NULL)
                THEN 'failing'::feed_status
                ELSE before_row.status
            END
         ) <> 'quarantined'::feed_status
         AND after_row.failure_count > before_row.failure_count
        THEN 'feed.quarantined'
        WHEN after_row.status = 'failing'
         AND (
            (
                CASE
                    WHEN before_row.status = 'active'::feed_status
                     AND (before_row.failure_count > 0 OR before_row.status_reason IS NOT NULL)
                    THEN 'failing'::feed_status
                    ELSE before_row.status
                END
            ) <> 'failing'::feed_status
            OR before_row.status_reason IS DISTINCT FROM after_row.status_reason
         )
        THEN 'feed.failure_reported'
        ELSE NULL
    END AS action
    FROM before_row
    JOIN after_row ON after_row.id = before_row.id
),
write_audit AS (
    INSERT INTO feed_audit_events (
        feed_id, action, actor_id, feed_revision,
        before_values, after_values
    )
    SELECT
        after_row.id,
        audit_action.action,
        %s::text,
        after_row.feed_revision,
{_AUDIT_BEFORE_SNAPSHOT_SQL},
{_AUDIT_AFTER_SNAPSHOT_SQL}
    FROM before_row
    JOIN after_row ON after_row.id = before_row.id
    CROSS JOIN audit_action
    WHERE %s::text IS NOT NULL
      AND audit_action.action IS NOT NULL
    RETURNING id
)
SELECT after_row.*
FROM after_row
"""

"""Synchronous feed lifecycle SQL queries.

psycopg v3 uses ``%s`` parameters, so these stay separate from the async
``feed_queries`` module which uses asyncpg's ``$1`` parameter style. Echo
lifecycle writes also rely on terminal-state guards instead of VM lease
fencing.
"""
# ruff: noqa: S608

from backend.pipeline.storage import feed_audit_sql

_AUDIT_BEFORE_SNAPSHOT_SQL = feed_audit_sql.audit_snapshot_sql("before_row")
_AUDIT_AFTER_SNAPSHOT_SQL = feed_audit_sql.audit_snapshot_sql("after_row")
_AUDIT_ACTOR_CTE_SQL = """audit_actor AS (
    SELECT %s::text AS actor_id
)"""
_STATUS_REASON_INPUT_CTE_SQL = """status_reason_input AS (
    SELECT %s::text AS status_reason
)"""

RESOLVE_ECHO_FEED_SQL = """\
SELECT f.id, f.name, f.status, f.created_at
FROM feeds f
JOIN feed_properties fp ON fp.feed_id = f.id
WHERE fp.source_feed_id = %s
AND fp.source_type = 'echo'
AND f.source_type = 'echo'
"""

HEARTBEAT_SQL = f"""\
WITH before_row AS (
    SELECT
{feed_audit_sql.audit_source_projection("f")}
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
    RETURNING feeds.*, feeds.audit_revision AS feed_revision
),
after_row AS (
    SELECT u.*, fp.source_feed_id, COALESCE(fp.tags, '[]'::jsonb) AS tags
    FROM updated u
    JOIN feed_properties fp ON fp.feed_id = u.id
),
{feed_audit_sql.recovery_audit_action_cte(before_alias="before_row")},
{_AUDIT_ACTOR_CTE_SQL},
{
    feed_audit_sql.insert_feed_audit_event_cte(
        feed_id_sql="after_row.id",
        action_sql="audit_action.action",
        actor_id_sql="audit_actor.actor_id",
        feed_revision_sql="after_row.feed_revision",
        before_values_sql=_AUDIT_BEFORE_SNAPSHOT_SQL,
        after_values_sql=_AUDIT_AFTER_SNAPSHOT_SQL,
        from_sql=(
            "FROM before_row\n"
            "    JOIN after_row ON after_row.id = before_row.id\n"
            "    CROSS JOIN audit_action\n"
            "    CROSS JOIN audit_actor"
        ),
        where_sql="audit_action.action IS NOT NULL",
    )
}
SELECT after_row.*
FROM after_row
"""

# Backoff formula: base * 2^(failure_count), capped at max, plus 0-10s jitter.
# Matches REPORT_FAILURE_SQL in feed_queries.py minus worker_id/fencing_token.
# Failure writes always update current failure state and advance
# feeds.audit_revision. feed_audit_events rows are intentionally suppressed for
# repeated failures with the same status/status_reason, so event revisions can
# have gaps.
RECORD_FAILURE_SQL = f"""\
WITH before_row AS (
    SELECT
{feed_audit_sql.audit_source_projection("f")}
    FROM feeds f
    JOIN feed_properties fp ON fp.feed_id = f.id
    WHERE f.id = %s
      AND f.status NOT IN ('quarantined'::feed_status, 'deactivated'::feed_status)
    FOR UPDATE
),
{_STATUS_REASON_INPUT_CTE_SQL},
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
        status_reason = COALESCE(status_reason_input.status_reason, 'system_unexpected_error'),
        status_reason_detail = %s,
        status_reason_updated_at = CASE
            WHEN feeds.status_reason IS DISTINCT FROM COALESCE(status_reason_input.status_reason, 'system_unexpected_error')
                THEN NOW()
            ELSE feeds.status_reason_updated_at
        END
    FROM before_row
    CROSS JOIN status_reason_input
    WHERE feeds.id = before_row.id
    RETURNING feeds.*, feeds.audit_revision AS feed_revision
),
after_row AS (
    SELECT u.*, fp.source_feed_id, COALESCE(fp.tags, '[]'::jsonb) AS tags
    FROM updated u
    JOIN feed_properties fp ON fp.feed_id = u.id
),
{feed_audit_sql.failure_audit_action_cte()},
{_AUDIT_ACTOR_CTE_SQL},
{
    feed_audit_sql.insert_feed_audit_event_cte(
        feed_id_sql="after_row.id",
        action_sql="audit_action.action",
        actor_id_sql="audit_actor.actor_id",
        feed_revision_sql="after_row.feed_revision",
        before_values_sql=_AUDIT_BEFORE_SNAPSHOT_SQL,
        after_values_sql=_AUDIT_AFTER_SNAPSHOT_SQL,
        from_sql=(
            "FROM before_row\n"
            "    JOIN after_row ON after_row.id = before_row.id\n"
            "    CROSS JOIN audit_action\n"
            "    CROSS JOIN audit_actor"
        ),
        where_sql="audit_action.action IS NOT NULL",
    )
}
SELECT after_row.*
FROM after_row
"""

RECORD_NON_BUDGETED_FAILURE_SQL = f"""\
WITH before_row AS (
    SELECT
{feed_audit_sql.audit_source_projection("f")}
    FROM feeds f
    JOIN feed_properties fp ON fp.feed_id = f.id
    WHERE f.id = %s
      AND f.status NOT IN ('quarantined'::feed_status, 'deactivated'::feed_status)
    FOR UPDATE
),
{_STATUS_REASON_INPUT_CTE_SQL},
updated AS (
    UPDATE feeds
    SET status = 'failing'::feed_status,
        audit_revision = feeds.audit_revision + 1,
        failure_count = 0,
        last_heartbeat = NOW(),
        retry_after = NULL,
        status_reason = status_reason_input.status_reason,
        status_reason_detail = %s,
        status_reason_updated_at = CASE
            WHEN feeds.status_reason IS DISTINCT FROM status_reason_input.status_reason THEN NOW()
            ELSE feeds.status_reason_updated_at
        END
    FROM before_row
    CROSS JOIN status_reason_input
    WHERE feeds.id = before_row.id
    RETURNING feeds.*, feeds.audit_revision AS feed_revision
),
after_row AS (
    SELECT u.*, fp.source_feed_id, COALESCE(fp.tags, '[]'::jsonb) AS tags
    FROM updated u
    JOIN feed_properties fp ON fp.feed_id = u.id
),
{feed_audit_sql.failure_audit_action_cte()},
{_AUDIT_ACTOR_CTE_SQL},
{
    feed_audit_sql.insert_feed_audit_event_cte(
        feed_id_sql="after_row.id",
        action_sql="audit_action.action",
        actor_id_sql="audit_actor.actor_id",
        feed_revision_sql="after_row.feed_revision",
        before_values_sql=_AUDIT_BEFORE_SNAPSHOT_SQL,
        after_values_sql=_AUDIT_AFTER_SNAPSHOT_SQL,
        from_sql=(
            "FROM before_row\n"
            "    JOIN after_row ON after_row.id = before_row.id\n"
            "    CROSS JOIN audit_action\n"
            "    CROSS JOIN audit_actor"
        ),
        where_sql="audit_action.action IS NOT NULL",
    )
}
SELECT after_row.*
FROM after_row
"""

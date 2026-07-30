"""SQL for SID-aware Broadcastify Calls Feed admin mutations.

Every statement here runs after the caller has locked the parent
``ingestion_leases ('bcfy_calls', sid)`` row (``LOCK_LEASE_SQL``,
``FOR NO KEY UPDATE``) — the same parent-before-child order as the worker,
so admin and worker transactions cannot deadlock. ``membership_revision``
is cache invalidation, never an authority fence: bumped once per real
mutation, never on idempotent replay.
"""
# ruff: noqa: S608

from __future__ import annotations

from backend.pipeline.storage import feed_audit_sql

_AUDIT_BEFORE_SNAPSHOT_SQL = feed_audit_sql.audit_snapshot_sql("before_row")
_AUDIT_AFTER_SNAPSHOT_SQL = feed_audit_sql.audit_snapshot_sql("after_row")
_AUDIT_EVENT_RETURNING_SQL = (
    f"{feed_audit_sql.feed_audit_event_payload_sql()} AS feed_audit_event"
)
_AUDIT_EVENT_SELECT_SQL = feed_audit_sql.feed_audit_event_scalar_sql()

# The shared authority-reactivation rule: a reactivating parent becomes
# a clean, unowned ``unclaimed`` Lease with a fresh failure budget. The
# fencing token is never rewritten. Each pair is (column, clean value).
_LEASE_REACTIVATION_CLEAN_VALUES = (
    ("status", "'unclaimed'::feed_status"),
    ("worker_id", "NULL"),
    ("last_heartbeat", "NULL"),
    ("failure_count", "0"),
    ("retry_after", "NULL"),
    ("status_reason", "NULL"),
    ("status_reason_detail", "NULL"),
)


def _lease_reactivation_assignments(
    reactivate_when_sql: str,
    indent: str,
) -> str:
    """SET clauses applying the authority-reactivation rule.

    A parent matching ``reactivate_when_sql`` becomes clean
    ``unclaimed``; any other parent keeps every field exactly.
    """
    return ",\n".join(
        f"{indent}{column} = CASE\n"
        f"{indent}    WHEN {reactivate_when_sql}\n"
        f"{indent}        THEN {clean_value}\n"
        f"{indent}    ELSE ingestion_leases.{column}\n"
        f"{indent}END"
        for column, clean_value in _LEASE_REACTIVATION_CLEAN_VALUES
    )


def _lease_reactivation_pending_sql(indent: str) -> str:
    """Predicate: the authority-reactivation rule would change this row."""
    checks = f"\n{indent}    OR ".join(
        f"ingestion_leases.{column} IS DISTINCT FROM {clean_value}"
        for column, clean_value in _LEASE_REACTIVATION_CLEAN_VALUES
    )
    return (
        f"{indent}ingestion_leases.status <> 'active'::feed_status\n"
        f"{indent}AND (\n"
        f"{indent}    {checks}\n"
        f"{indent})"
    )


# Lock-free routing pre-read; the SID branch re-verifies this identity
# under the child lock, so a stale read degrades to not-found, never to
# a different parent. Legacy rows return no row and keep legacy paths.
GET_SID_MEMBERSHIP_KEY_SQL = """\
SELECT fp.bcfy_calls_sid AS sid
FROM feed_properties fp
WHERE fp.feed_id = $1
  AND fp.source_type = 'bcfy_calls'
  AND fp.bcfy_calls_is_trunked IS TRUE
  AND fp.bcfy_calls_sid IS NOT NULL
"""

# ON CONFLICT DO NOTHING keeps concurrent creates race-free: the loser
# sees no returned row and instead locks + bumps the existing parent.
INSERT_UNCLAIMED_PARENT_LEASE_SQL = """\
INSERT INTO ingestion_leases (
    source_type, lease_key, status, membership_revision
)
VALUES ('bcfy_calls', $1, 'unclaimed'::feed_status, 1)
ON CONFLICT (source_type, lease_key) DO NOTHING
RETURNING lease_key
"""

# A deactivated parent gaining an eligible member is an explicit
# reactivation: it becomes a clean unclaimed Lease with a fresh failure
# budget and no stale owner, its fence preserved. Every other lifecycle
# (active owner, failing backoff, quarantine) is preserved exactly.
REGISTER_MEMBER_ON_EXISTING_LEASE_SQL = f"""\
UPDATE ingestion_leases
SET membership_revision = membership_revision + 1,
{
    _lease_reactivation_assignments(
        "ingestion_leases.status = 'deactivated'::feed_status",
        indent="    ",
    )
},
    updated_at = NOW()
WHERE source_type = 'bcfy_calls'
  AND lease_key = $1
RETURNING membership_revision
"""

# The child is born active with no worker/heartbeat and a NULL cursor,
# so the SID owner adopts it at the next page boundary.
CREATE_SID_FEED_SQL = f"""\
WITH new_feed AS (
    INSERT INTO feeds (name, source_type, status, audit_revision)
    VALUES ($1, 'bcfy_calls', 'active'::feed_status, 1)
    RETURNING feeds.*, feeds.audit_revision AS feed_revision
),
new_props AS (
    INSERT INTO feed_properties (
        feed_id, source_feed_id, source_type, tags,
        bcfy_calls_sid, bcfy_calls_group_id, bcfy_calls_is_trunked
    )
    SELECT id, $2, source_type, $3, $4, $5, TRUE FROM new_feed
    RETURNING source_feed_id, tags
),
after_row AS (
    SELECT nf.*, np.source_feed_id, COALESCE(np.tags, '[]'::jsonb) AS tags,
           NULL::timestamptz AS last_speech_segment_timestamp
    FROM new_feed nf
    JOIN new_props np ON TRUE
),
{
    feed_audit_sql.insert_feed_audit_event_cte(
        feed_id_sql="after_row.id",
        action_sql="'feed.created'",
        actor_id_sql="$6::text",
        feed_revision_sql="after_row.feed_revision",
        before_values_sql="        '{}'::jsonb",
        after_values_sql=_AUDIT_AFTER_SNAPSHOT_SQL,
        from_sql="FROM after_row",
        returning_sql=_AUDIT_EVENT_RETURNING_SQL,
    )
}
SELECT after_row.*,
       {_AUDIT_EVENT_SELECT_SQL}
FROM after_row
"""

# The child keeps its identity and bookmark; a repeat is a full no-op.
# Only when no eligible (active/failing) sibling remains is the parent
# deactivated and unowned; otherwise its authority is preserved exactly.
DEACTIVATE_SID_CHILD_SQL = f"""\
WITH before_row AS (
    SELECT
{feed_audit_sql.audit_source_projection("f")}
    FROM feeds f
    JOIN feed_properties fp ON fp.feed_id = f.id
    WHERE f.id = $1
      AND fp.source_type = 'bcfy_calls'
      AND fp.bcfy_calls_is_trunked IS TRUE
      AND fp.bcfy_calls_sid = $2
    FOR NO KEY UPDATE OF f
),
updated AS (
    UPDATE feeds
    SET status = 'deactivated'::feed_status,
        audit_revision = feeds.audit_revision + 1
    FROM before_row
    WHERE feeds.id = before_row.id
      AND before_row.status <> 'deactivated'::feed_status
    RETURNING feeds.*, feeds.audit_revision AS feed_revision
),
after_row AS (
    SELECT u.*, fp.source_feed_id, COALESCE(fp.tags, '[]'::jsonb) AS tags
    FROM updated u
    JOIN feed_properties fp ON fp.feed_id = u.id
),
{
    feed_audit_sql.insert_feed_audit_event_cte(
        feed_id_sql="after_row.id",
        action_sql="'feed.deactivated'",
        actor_id_sql="$3::text",
        feed_revision_sql="after_row.feed_revision",
        before_values_sql=_AUDIT_BEFORE_SNAPSHOT_SQL,
        after_values_sql=_AUDIT_AFTER_SNAPSHOT_SQL,
        from_sql=(
            "FROM before_row\n"
            "    JOIN after_row ON after_row.id = before_row.id"
        ),
        returning_sql=_AUDIT_EVENT_RETURNING_SQL,
    )
},
remaining AS (
    SELECT EXISTS (
        SELECT 1
        FROM feed_properties fp
        JOIN feeds sibling ON sibling.id = fp.feed_id
        WHERE fp.source_type = 'bcfy_calls'
          AND fp.bcfy_calls_is_trunked IS TRUE
          AND fp.bcfy_calls_sid = $2
          AND fp.feed_id <> $1
          AND sibling.status IN (
              'active'::feed_status,
              'failing'::feed_status
          )
        LIMIT 1
    ) AS has_eligible_member
),
lease_update AS (
    UPDATE ingestion_leases
    SET membership_revision = ingestion_leases.membership_revision + 1,
        status = CASE
            WHEN remaining.has_eligible_member
                THEN ingestion_leases.status
            ELSE 'deactivated'::feed_status
        END,
        worker_id = CASE
            WHEN remaining.has_eligible_member
                THEN ingestion_leases.worker_id
        END,
        last_heartbeat = CASE
            WHEN remaining.has_eligible_member
                THEN ingestion_leases.last_heartbeat
        END,
        updated_at = NOW()
    FROM remaining, updated
    WHERE ingestion_leases.source_type = 'bcfy_calls'
      AND ingestion_leases.lease_key = $2
      AND updated.id IS NOT NULL
    RETURNING ingestion_leases.membership_revision
)
SELECT before_row.id,
       (updated.id IS NOT NULL) AS changed,
       (SELECT lease_update.membership_revision FROM lease_update)
           AS membership_revision,
       {_AUDIT_EVENT_SELECT_SQL}
FROM before_row
LEFT JOIN updated ON updated.id = before_row.id
"""

# Supported for active and inactive parents: the child becomes an
# enabled member with a cleared cursor for page-boundary re-adoption,
# and an inactive parent independently becomes clean unclaimed with its
# fence preserved — even when the child itself is already clean. An
# active parent keeps its owner, fence, and failure state. Only when
# neither the child nor the parent needs a change is the reset a full
# no-op (no audit event, no revision bump).
RESET_SID_CHILD_SQL = f"""\
WITH before_row AS (
    SELECT
{feed_audit_sql.audit_source_projection("f")}
    FROM feeds f
    JOIN feed_properties fp ON fp.feed_id = f.id
    WHERE f.id = $1
      AND fp.source_type = 'bcfy_calls'
      AND fp.bcfy_calls_is_trunked IS TRUE
      AND fp.bcfy_calls_sid = $2
    FOR NO KEY UPDATE OF f
),
lease_before AS (
    -- The caller already holds this row FOR NO KEY UPDATE
    -- (LOCK_LEASE_SQL), so this read is stable for the transaction.
    SELECT
        (
{_lease_reactivation_pending_sql("            ")}
        ) AS parent_needs_reset
    FROM ingestion_leases
    WHERE ingestion_leases.source_type = 'bcfy_calls'
      AND ingestion_leases.lease_key = $2
),
change AS (
    -- LEFT JOIN: an absent permanent Lease row (a configuration error
    -- surfaced by the health projection) degrades to child-only reset.
    SELECT
        before_row.*,
        (
            before_row.status <> 'active'::feed_status
            OR before_row.last_bookmark_time IS NOT NULL
            OR before_row.last_processed_filename IS NOT NULL
            OR before_row.failure_count <> 0
            OR before_row.retry_after IS NOT NULL
            OR before_row.status_reason IS NOT NULL
            OR before_row.status_reason_detail IS NOT NULL
            OR before_row.worker_id IS NOT NULL
            OR before_row.last_heartbeat IS NOT NULL
            OR COALESCE(lease_before.parent_needs_reset, FALSE)
        ) AS changed
    FROM before_row
    LEFT JOIN lease_before ON TRUE
),
updated AS (
    UPDATE feeds
    SET status = 'active'::feed_status,
        last_bookmark_time = NULL,
        last_processed_filename = NULL,
        failure_count = 0,
        retry_after = NULL,
        worker_id = NULL,
        last_heartbeat = NULL,
        unclaimed_since = NULL,
        audit_revision = feeds.audit_revision + 1,
        status_reason_updated_at = CASE
            WHEN feeds.status_reason IS NOT NULL
                 OR feeds.status_reason_detail IS NOT NULL THEN NOW()
            ELSE feeds.status_reason_updated_at
        END,
        status_reason = NULL,
        status_reason_detail = NULL
    FROM change
    WHERE feeds.id = change.id
      AND change.changed
    RETURNING feeds.*
),
after_row AS (
    SELECT u.*, fp.source_feed_id, COALESCE(fp.tags, '[]'::jsonb) AS tags,
           u.audit_revision AS feed_revision
    FROM updated u
    JOIN feed_properties fp ON fp.feed_id = u.id
),
{
    feed_audit_sql.insert_feed_audit_event_cte(
        feed_id_sql="after_row.id",
        action_sql="'feed.reset'",
        actor_id_sql="$3::text",
        feed_revision_sql="after_row.feed_revision",
        before_values_sql=_AUDIT_BEFORE_SNAPSHOT_SQL,
        after_values_sql=_AUDIT_AFTER_SNAPSHOT_SQL,
        from_sql=(
            "FROM before_row\n"
            "    JOIN after_row ON after_row.id = before_row.id"
        ),
        returning_sql=_AUDIT_EVENT_RETURNING_SQL,
    )
},
lease_update AS (
    UPDATE ingestion_leases
    SET membership_revision = ingestion_leases.membership_revision + 1,
{
    _lease_reactivation_assignments(
        "ingestion_leases.status <> 'active'::feed_status",
        indent="        ",
    )
},
        updated_at = NOW()
    FROM updated
    WHERE ingestion_leases.source_type = 'bcfy_calls'
      AND ingestion_leases.lease_key = $2
      AND updated.id IS NOT NULL
    RETURNING ingestion_leases.membership_revision
),
result_row AS (
    SELECT * FROM after_row
    UNION ALL
    SELECT before_row.*, before_row.audit_revision AS feed_revision
    FROM before_row
    JOIN change ON change.id = before_row.id
    WHERE NOT change.changed
)
SELECT result_row.*,
       (
           SELECT s.end_timestamp
           FROM audio_segments s
           WHERE s.feed_id = result_row.id
             AND s.classification = 'SPEECH'::audio_classification
           ORDER BY s.end_timestamp DESC, s.id DESC
           LIMIT 1
       ) AS last_speech_segment_timestamp,
       (SELECT lease_update.membership_revision FROM lease_update)
           AS membership_revision,
       {_AUDIT_EVENT_SELECT_SQL}
FROM result_row
"""

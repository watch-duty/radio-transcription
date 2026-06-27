"""Shared SQL fragments for feed audit event writes."""

from __future__ import annotations

# ruff: noqa: S608

FEED_AUDIT_NOTIFICATION_EVENT_TYPE = (
    "radio_transcription.feed_audit_notification"
)
FEED_AUDIT_NOTIFICATION_SCHEMA_VERSION = 1

AUDITED_FEED_STATE_FIELDS = (
    ("id", "id"),
    ("name", "name"),
    ("source_type", "source_type"),
    ("status", "status"),
    ("failure_count", "failure_count"),
    ("retry_after", "retry_after"),
    ("status_reason", "status_reason"),
    ("status_reason_updated_at", "status_reason_updated_at"),
    ("status_reason_detail", "status_reason_detail"),
    ("created_at", "created_at"),
    ("feed_properties.source_feed_id", "source_feed_id"),
    ("feed_properties.tags", "tags"),
)


def feed_audit_event_payload_sql(
    alias: str = "feed_audit_events",
) -> str:
    """Return the schema version 1 Feed Audit Notification JSONB payload."""
    return f"""jsonb_build_object(
        'event_type', {FEED_AUDIT_NOTIFICATION_EVENT_TYPE!r},
        'schema_version', {FEED_AUDIT_NOTIFICATION_SCHEMA_VERSION},
        'event_id', {alias}.id,
        'action', {alias}.action,
        'occurred_at', {alias}.occurred_at,
        'actor_id', {alias}.actor_id,
        'feed_id', {alias}.feed_id,
        'feed_revision', {alias}.feed_revision,
        'before_values', {alias}.before_values,
        'after_values', {alias}.after_values
    )"""


def feed_audit_event_scalar_sql(alias: str = "write_audit") -> str:
    """Return nullable notification payload from a one-row audit CTE."""
    return f"(SELECT {alias}.feed_audit_event FROM {alias}) AS feed_audit_event"


def audit_snapshot_sql(alias: str) -> str:
    """Return a JSONB object expression for the maintained audit allowlist."""
    parts: list[str] = []
    for key, column in AUDITED_FEED_STATE_FIELDS:
        value = f"{alias}.{column}"
        if column in {"source_type", "status"}:
            value = f"{value}::text"
        if column == "tags":
            value = f"COALESCE({value}, '[]'::jsonb)"
        parts.append(f"        {key!r}, {value}")
    return "jsonb_build_object(\n" + ",\n".join(parts) + "\n    )"


def audit_source_projection(alias: str = "f") -> str:
    """Return row-shaped feed state used by audit snapshot builders.

    Keep the durable audit JSON allowlist in ``AUDITED_FEED_STATE_FIELDS``.
    This projection intentionally carries the full feed row through mutation
    CTEs so adding a non-audited ``feeds`` column does not require touching
    every audited SQL statement.
    """
    return (
        f"    {alias}.*,\n"
        "    fp.source_feed_id,\n"
        "    COALESCE(fp.tags, '[]'::jsonb) AS tags"
    )


def effective_prior_status_sql(before_alias: str) -> str:
    """Return status expression that treats dirty active feeds as failing."""
    return f"""(
            CASE
                WHEN {before_alias}.status = 'active'::feed_status
                 AND ({before_alias}.failure_count > 0 OR {before_alias}.status_reason IS NOT NULL)
                THEN 'failing'::feed_status
                ELSE {before_alias}.status
            END
        )"""


def recovery_audit_action_cte(
    *,
    before_alias: str,
    after_alias: str = "after_row",
) -> str:
    """Return audit_action CTE for abnormal-to-normal recovery events."""
    return f"""audit_action AS (
    SELECT CASE
        WHEN {effective_prior_status_sql(before_alias)}::text IN ('failing', 'quarantined')
        AND {after_alias}.status::text NOT IN ('failing', 'quarantined')
        AND {after_alias}.status_reason IS NULL
        AND {after_alias}.failure_count = 0
        THEN 'feed.recovered'
        ELSE NULL
    END AS action
    FROM {before_alias}
    JOIN {after_alias} ON {after_alias}.id = {before_alias}.id
)"""


def failure_audit_action_cte(
    *,
    before_alias: str = "before_row",
    after_alias: str = "after_row",
) -> str:
    """Return audit_action CTE for failure and quarantine events."""
    effective_prior_status = effective_prior_status_sql(before_alias)
    return f"""audit_action AS (
    SELECT CASE
        WHEN {after_alias}.status::text = 'quarantined'
         AND {effective_prior_status}::text <> 'quarantined'
         AND {after_alias}.failure_count > {before_alias}.failure_count
        THEN 'feed.quarantined'
        WHEN {after_alias}.status::text = 'failing'
         AND (
            {effective_prior_status}::text <> 'failing'
            OR {before_alias}.status_reason IS DISTINCT FROM {after_alias}.status_reason
         )
        THEN 'feed.failure_reported'
        ELSE NULL
    END AS action
    FROM {before_alias}
    JOIN {after_alias} ON {after_alias}.id = {before_alias}.id
)"""


def insert_feed_audit_event_cte(
    *,
    feed_id_sql: str,
    action_sql: str,
    actor_id_sql: str,
    feed_revision_sql: str,
    before_values_sql: str,
    after_values_sql: str,
    from_sql: str,
    where_sql: str | None = None,
    returning_sql: str = "id",
) -> str:
    """Return write_audit CTE for the canonical feed audit insert."""
    where_clause = f"\n    WHERE {where_sql}" if where_sql else ""
    return f"""write_audit AS (
    INSERT INTO feed_audit_events (
        feed_id, action, actor_id, feed_revision,
        before_values, after_values
    )
    SELECT
        {feed_id_sql},
        {action_sql},
        {actor_id_sql},
        {feed_revision_sql},
{before_values_sql},
{after_values_sql}
    {from_sql}{where_clause}
    RETURNING {returning_sql}
)"""

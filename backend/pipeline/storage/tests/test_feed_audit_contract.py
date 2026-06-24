from __future__ import annotations

import pathlib
import re

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]

_ACTIONS = (
    "feed.created",
    "feed.updated",
    "feed.deactivated",
    "feed.reset",
    "feed.deleted",
    "feed.failure_reported",
    "feed.quarantined",
    "feed.recovered",
)


def _read(path: str) -> str:
    return (_REPO_ROOT / path).read_text(encoding="utf-8")


def _sql_without_comments(text: str) -> str:
    return re.sub(r"--.*$", "", text, flags=re.MULTILINE)


def _normalized_sql(text: str) -> str:
    return " ".join(_sql_without_comments(text).split())


def _feed_audit_events_table_sql(text: str) -> str:
    sql = _sql_without_comments(text)
    match = re.search(
        r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+feed_audit_events\s*"
        r"\((.*?)\);",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    assert match is not None
    return match.group(1)


def _markdown_section(text: str, heading: str) -> str:
    pattern = r"^" + re.escape(heading) + r"\n(?P<body>.*?)(?=^### |\Z)"
    match = re.search(pattern, text, flags=re.MULTILINE | re.DOTALL)
    assert match is not None
    return match.group("body")


def test_repository_glossary_defines_audit_terms() -> None:
    text = _read("CONTEXT.md")

    for heading in (
        "### Current Feed State",
        "### Audit History",
        "### Feed Audit Event",
        "### Status Reason Detail",
        "### Actor ID",
    ):
        assert heading in text

    feed_event_section = _markdown_section(text, "### Feed Audit Event")
    for token in (
        "feed-local `feed_revision`",
        "delivery, timeline APIs, and broader operational",
        "separate follow-up concerns",
    ):
        assert token in feed_event_section

    actor_section = _markdown_section(text, "### Actor ID")
    for token in (
        "`user:google:<sub>`",
        "`service_account:gcp:<service-account-unique-id>`",
        "`service_account:gcp:unresolved`",
        "`service_account:local:development`",
        "`FEED_AUDIT_ACTOR_ID=service_account:gcp:<service-account-unique-id>`",
        "GCP/IaC/deployment records",
    ):
        assert token in actor_section


def test_migration_defines_delete_safe_audit_schema() -> None:
    text = _read(
        "terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql"
    )
    sql = _sql_without_comments(text)
    normalized = _normalized_sql(text)

    for token in (
        "ADD COLUMN IF NOT EXISTS audit_revision BIGINT NOT NULL DEFAULT 0",
        "CREATE TABLE IF NOT EXISTS feed_audit_events",
        "feed_id UUID NOT NULL",
        "action TEXT NOT NULL",
        "actor_id TEXT NOT NULL",
        "before_values JSONB NOT NULL DEFAULT '{}'::jsonb",
        "after_values JSONB NOT NULL DEFAULT '{}'::jsonb",
        "feed_revision BIGINT NOT NULL",
        "occurred_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()",
        "feed_audit_events_revision_positive",
        "feed_audit_events_feed_revision_unique",
        "feed_audit_events_json_object_shape",
        "idx_feed_audit_events_feed_occurred_at",
        "ON feed_audit_events (feed_id, occurred_at DESC, feed_revision DESC)",
        "jsonb_typeof(before_values) = 'object'",
        "jsonb_typeof(after_values) = 'object'",
    ):
        assert token in normalized

    feed_fk_pattern = (
        r"REFERENCES\s+(?:ONLY\s+)?"
        r"(?:(?:[A-Za-z_][\w$]*|\"[^\"]+\")\.)?"
        r"(?:feeds|\"feeds\")(?=\s|\()"
    )
    for pattern in (
        feed_fk_pattern,
        r"ON\s+DELETE\s+CASCADE",
        r"DROP\s+COLUMN\s+quarantine_reason",
    ):
        assert re.search(pattern, sql, flags=re.IGNORECASE) is None


def test_migration_defines_actor_and_action_constraints() -> None:
    text = _read(
        "terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql"
    )
    sql = _sql_without_comments(text)

    for token in (
        "feed_audit_events_action_check",
        "feed_audit_events_actor_id_check",
        *_ACTIONS,
    ):
        assert token in sql


def test_migration_bounds_actor_id_storage_shape() -> None:
    text = _read(
        "terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql"
    )
    normalized = _normalized_sql(text)

    assert "char_length(actor_id) > 0" in normalized
    assert "char_length(actor_id) <= 512" in normalized
    assert "actor_id !~ '[[:space:]]'" in normalized


def test_migration_uses_schema_qualified_constraint_guards() -> None:
    text = _read(
        "terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql"
    )
    normalized = _normalized_sql(text)

    assert normalized.count("table_schema = current_schema()") >= 6


def test_migration_defines_status_reason_detail_and_hot_guard() -> None:
    migration = _read(
        "terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql"
    )
    hot_guard = _read(
        "terraform/modules/alloydb/sql/ci/hot_protection_check.sql"
    )
    sql = _sql_without_comments(migration)
    guard_sql = _sql_without_comments(hot_guard)
    normalized = _normalized_sql(migration)
    normalized_guard = _normalized_sql(hot_guard)

    for token in (
        "ADD COLUMN IF NOT EXISTS status_reason_detail TEXT",
        "feeds_status_reason_detail_length",
        "2048",
    ):
        assert token in normalized

    table_sql = _feed_audit_events_table_sql(migration)
    assert "status_reason_detail" not in table_sql

    assert "WITH guarded_columns(attname) AS" in guard_sql
    assert "('status_reason')" in guard_sql
    assert "('status_reason_updated_at')" in guard_sql
    assert "('status_reason_detail')" in guard_sql
    assert "('audit_revision')" in guard_sql
    assert "('retry_after')" in guard_sql
    for token in (
        "feed_indexes AS",
        "index_column_dependencies AS",
        "allowed_references AS",
        "'idx_feeds_failing_retryable'::text AS indexname",
        "'retry_after'::text AS attname",
        "'direct'::text AS reference_kind",
        "violations AS",
        "pg_depend",
        "d.objid = i.indexrelid",
        "d.refobjid = i.table_oid",
        "d.refobjsubid > 0",
        "x.indkey",
        "WHEN a.attnum = ANY(i.indkey) THEN 'direct'",
        "ELSE 'derived'",
    ):
        assert token in normalized_guard
    assert "c.relname <> " not in guard_sql
    assert "pg_get_expr" not in guard_sql
    assert r"\m" not in guard_sql
    assert r"\M" not in guard_sql
    assert (
        re.search(
            r"CREATE\s+INDEX(?:\s+IF\s+NOT\s+EXISTS)?\s+\S+\s+"
            r"ON\s+feeds\s*\([^;]*status_reason_detail",
            sql,
            flags=re.IGNORECASE,
        )
        is None
    )

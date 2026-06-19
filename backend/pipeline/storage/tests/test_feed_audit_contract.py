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

_ACTOR_STRINGS = (
    "user:google:",
    "user-email:",
    "service:",
    "job:",
    "gcp-sa:",
    "unknown:unknown",
)


def _read(path: str) -> str:
    return (_REPO_ROOT / path).read_text(encoding="utf-8")


def _sql_without_comments(text: str) -> str:
    return re.sub(r"--.*$", "", text, flags=re.MULTILINE)


def _normalized_sql(text: str) -> str:
    return " ".join(_sql_without_comments(text).split())


def test_documentation_defines_feed_audit_event_contract() -> None:
    text = _read("documentation/feed-audit-events.md")

    for token in (
        *_ACTIONS,
        *_ACTOR_STRINGS,
        "feed_audit_events",
        "before_values",
        "after_values",
        "occurred_at",
        "feed_sequence",
        "status_reason_detail",
        "quarantine_reason",
        "18 months",
        "Watch Duty",
        "admin timeline",
    ):
        assert token in text

    assert "system:" not in text


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


def test_migration_defines_delete_safe_audit_schema() -> None:
    text = _read(
        "terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql"
    )
    sql = _sql_without_comments(text)
    normalized = _normalized_sql(text)

    for token in (
        "CREATE TABLE IF NOT EXISTS feed_audit_events",
        "feed_id UUID NOT NULL",
        "feed_name VARCHAR(255) NOT NULL",
        "source_type TEXT NOT NULL REFERENCES source_types(slug)",
        "before_values JSONB NOT NULL DEFAULT '{}'::jsonb",
        "after_values JSONB NOT NULL DEFAULT '{}'::jsonb",
        "metadata JSONB NOT NULL DEFAULT '{}'::jsonb",
        "feed_sequence BIGINT NOT NULL",
        "occurred_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()",
        "feed_audit_events_feed_sequence_unique",
        "feed_audit_events_json_object_shape",
        "jsonb_typeof(before_values) = 'object'",
        "jsonb_typeof(after_values) = 'object'",
        "jsonb_typeof(metadata) = 'object'",
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
        r"pg_cron",
        r"dispatcher",
        r"webhook",
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
        *_ACTOR_STRINGS,
    ):
        assert token in sql

    assert "system:%" not in sql


def test_migration_rejects_malformed_actor_id_suffixes() -> None:
    text = _read(
        "terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql"
    )
    sql = _sql_without_comments(text)
    normalized = _normalized_sql(text)
    rejected_actor_ids = (
        "user:google:",
        "service:",
        "job:",
        "gcp-sa:",
        "user-email:",
    )

    assert "actor_id = 'unknown:unknown'" in sql
    assert "char_length(actor_id) <= 512" in normalized

    for prefix in rejected_actor_ids:
        suffix_pattern = (
            r"substring\s*\(\s*actor_id\s+FROM\s+char_length\('"
            + re.escape(prefix)
            + r"'\)\s*\+\s*1\s*\)"
        )
        assert f"actor_id LIKE '{prefix}%'" in normalized
        assert re.search(
            suffix_pattern + r"\s*<>\s*''",
            sql,
            flags=re.IGNORECASE,
        ), prefix
        assert re.search(
            suffix_pattern + r"\s*!~\s*'\[\[:space:\]\]'",
            sql,
            flags=re.IGNORECASE,
        ), prefix

    assert re.search(
        r"substring\s*\(\s*actor_id\s+FROM\s+"
        r"char_length\('user-email:'\)\s*\+\s*1\s*\)\s+LIKE\s+'%@%'",
        sql,
        flags=re.IGNORECASE,
    )


def test_replacement_migration_removes_system_actor_constraint() -> None:
    text = _read(
        "terraform/modules/alloydb/sql/ingestion/"
        "030_feed_audit_events_actor_constraint.sql"
    )
    sql = _sql_without_comments(text)
    normalized = _normalized_sql(text)

    assert "actor_id LIKE 'system:%'" in normalized
    assert "RAISE EXCEPTION" in normalized
    assert "clean or remap those rows" in normalized
    assert (
        "DROP CONSTRAINT IF EXISTS feed_audit_events_actor_id_check"
        in normalized
    )
    assert "ADD CONSTRAINT feed_audit_events_actor_id_check" in normalized

    recreated_constraint = sql.split(
        "ADD CONSTRAINT feed_audit_events_actor_id_check",
        maxsplit=1,
    )[1]
    assert "system:%" not in recreated_constraint


def test_migration_uses_schema_qualified_constraint_guards() -> None:
    text = _read(
        "terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql"
    )
    normalized = _normalized_sql(text)

    assert normalized.count("table_schema = current_schema()") >= 7


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
        "feed_audit_events_detail_length",
        "2048",
    ):
        assert token in normalized

    assert "WITH guarded_columns(attname) AS" in guard_sql
    assert "('status_reason_detail')" in guard_sql
    assert "x.indpred IS NOT NULL" in normalized_guard
    assert "pg_get_expr(x.indpred, x.indrelid)" in normalized_guard
    assert re.search(
        r"NOT\s*\(\s*c\.relname\s*=\s*'idx_feeds_failing_retryable'"
        r"\s+AND\s+g\.attname\s*=\s*'retry_after'\s*\)",
        guard_sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    assert "c.relname <> 'idx_feeds_failing_retryable'" not in guard_sql
    assert (
        re.search(
            r"CREATE\s+INDEX(?:\s+IF\s+NOT\s+EXISTS)?\s+\S+\s+"
            r"ON\s+feeds\s*\([^;]*status_reason_detail",
            sql,
            flags=re.IGNORECASE,
        )
        is None
    )

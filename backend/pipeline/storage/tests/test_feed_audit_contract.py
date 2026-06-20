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
    "service:",
)

_DEFERRED_ACTOR_STRINGS = (
    "unknown:unknown",
    "user-email:",
    "job:",
    "gcp-sa:",
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
        "`service:<name>`",
    ):
        assert token in actor_section

    for token in (
        "unknown actor",
        "GCP service account fallback",
        "scheduled job",
        "system:",
    ):
        assert token not in actor_section


def test_migration_defines_delete_safe_audit_schema() -> None:
    text = _read(
        "terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql"
    )
    sql = _sql_without_comments(text)
    normalized = _normalized_sql(text)
    table_sql = _feed_audit_events_table_sql(text)
    table_normalized = " ".join(table_sql.split())

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
        "idx_feed_audit_events_feed_revision",
        "jsonb_typeof(before_values) = 'object'",
        "jsonb_typeof(after_values) = 'object'",
    ):
        assert token in normalized

    for token in (
        "feed_audit_event_sequences",
        "feed_sequence",
        "feed_audit_events_feed_sequence_unique",
        "idx_feed_audit_events_feed_sequence",
    ):
        assert token not in normalized

    for token in (
        "feed_name",
        "source_type",
        "status feed_status",
        "status_reason TEXT",
        "status_reason_detail",
        "metadata",
    ):
        assert token not in table_normalized

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


def test_audit_schema_defers_delivery_and_retention_work() -> None:
    text = _read(
        "terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql"
    )
    normalized = _normalized_sql(text)
    table_normalized = " ".join(_feed_audit_events_table_sql(text).split())

    for token in (
        "id UUID",
        "feed_id UUID NOT NULL",
        "action TEXT NOT NULL",
        "actor_id TEXT NOT NULL",
        "occurred_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()",
        "feed_revision BIGINT NOT NULL",
        "before_values JSONB NOT NULL DEFAULT '{}'::jsonb",
        "after_values JSONB NOT NULL DEFAULT '{}'::jsonb",
        "created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()",
    ):
        assert token in table_normalized

    for token in (
        "feed_event_outbox",
        "feed_event_delivery",
        "delivery_status",
        "delivery_attempt",
        "next_attempt_at",
        "last_attempt_at",
        "delivered_at",
        "last_delivery_error",
        "last_response_code",
        "dead_letter",
        "dispatcher",
        "webhook",
        "cloudevents",
        "specversion",
        "dataschema",
        "datacontenttype",
        "pg_cron",
        "DELETE FROM feed_audit_events",
        "ON DELETE CASCADE",
    ):
        assert token.lower() not in normalized.lower()


def test_status_reason_detail_helper_is_not_broad_normalizer() -> None:
    text = _read("backend/pipeline/storage/feed_lifecycle.py")

    assert "def status_reason_detail_storage_value" in text
    assert "quarantine_reason.cap_quarantine_reason_for_storage" in text
    for token in (
        "_CREDENTIAL_RE",
        "_BEARER_TOKEN_RE",
        "Authorization",
        "password",
        "re.sub",
    ):
        assert token not in text


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
    for token in _DEFERRED_ACTOR_STRINGS:
        assert token not in sql


def test_migration_rejects_malformed_actor_id_suffixes() -> None:
    text = _read(
        "terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql"
    )
    sql = _sql_without_comments(text)
    normalized = _normalized_sql(text)
    rejected_actor_ids = (
        "user:google:",
        "service:",
    )

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

    for token in _DEFERRED_ACTOR_STRINGS:
        assert token not in sql


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

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
    "system:",
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
        "feed_sequence BIGINT NOT NULL",
        "occurred_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()",
        "feed_audit_events_feed_sequence_unique",
    ):
        assert token in normalized

    for pattern in (
        r"REFERENCES\s+feeds",
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

    for token in (
        "feed_audit_events_action_check",
        "feed_audit_events_actor_id_check",
        *_ACTIONS,
        *_ACTOR_STRINGS,
    ):
        assert token in text


def test_migration_rejects_empty_actor_id_suffixes() -> None:
    text = _read(
        "terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql"
    )
    sql = _sql_without_comments(text)
    rejected_actor_ids = (
        "user:google:",
        "service:",
        "system:",
        "job:",
        "gcp-sa:",
        "user-email:",
    )

    assert "actor_id = 'unknown:unknown'" in sql

    for prefix in rejected_actor_ids:
        branch_pattern = (
            r"actor_id\s+LIKE\s+'"
            + re.escape(prefix)
            + r"%'\s+AND\s+char_length\(actor_id\)\s*>\s*"
            + r"char_length\('"
            + re.escape(prefix)
            + r"'\)"
        )
        assert re.search(branch_pattern, sql, flags=re.IGNORECASE), prefix


def test_migration_defines_status_reason_detail_and_hot_guard() -> None:
    migration = _read(
        "terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql"
    )
    hot_guard = _read(
        "terraform/modules/alloydb/sql/ci/hot_protection_check.sql"
    )
    sql = _sql_without_comments(migration)
    normalized = _normalized_sql(migration)

    for token in (
        "ADD COLUMN IF NOT EXISTS status_reason_detail TEXT",
        "feeds_status_reason_detail_length",
        "feed_audit_events_detail_length",
        "2048",
    ):
        assert token in normalized

    assert "'status_reason_detail'" in hot_guard
    assert (
        re.search(
            r"CREATE\s+INDEX(?:\s+IF\s+NOT\s+EXISTS)?\s+\S+\s+"
            r"ON\s+feeds\s*\([^;]*status_reason_detail",
            sql,
            flags=re.IGNORECASE,
        )
        is None
    )

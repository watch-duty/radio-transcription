"""Tests for the SyncFeedStore."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import cast
from unittest.mock import MagicMock, patch

import pytest

from backend.pipeline.storage import quarantine_reason, sync_feed_queries
from backend.pipeline.storage.feed_store import FeedStatus, FeedStatusReason
from backend.pipeline.storage.sync_feed_store import SyncFeedStore

_ECHO_ACTOR_ID = (
    "service_account:gcp:echo-ingestion@example.iam.gserviceaccount.com"
)
_MISSING_ACTOR_ID = cast("str", None)


def _make_store(
    mock_conn: MagicMock,
    *,
    failure_threshold: int = 5,
    base_backoff_sec: int = 15,
    max_backoff_sec: int = 600,
) -> SyncFeedStore:
    """Build a SyncFeedStore backed by a mock connection."""
    connect_db = MagicMock(return_value=mock_conn)
    return SyncFeedStore(
        connect_db,
        failure_threshold=failure_threshold,
        base_backoff_sec=base_backoff_sec,
        max_backoff_sec=max_backoff_sec,
    )


def _make_mock_conn() -> MagicMock:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchone.return_value = None
    return conn


class TestResolveEchoFeed:
    def test_returns_typed_feed_row(self) -> None:
        conn = _make_mock_conn()
        feed_id = uuid.uuid4()
        created_at = datetime(2026, 1, 1, tzinfo=UTC)
        feed_row = {
            "id": feed_id,
            "name": "Fire CA",
            "status": "active",
            "created_at": created_at,
        }
        conn.execute.return_value.fetchone.return_value = feed_row
        store = _make_store(conn)

        result = store.resolve_echo_feed("fire-ca")

        assert result == {
            "id": feed_id,
            "name": "Fire CA",
            "status": FeedStatus.ACTIVE,
            "created_at": created_at,
        }
        conn.execute.assert_called_once()
        sql, params = conn.execute.call_args[0]
        assert "AND fp.source_type = 'echo'" in sql
        assert "AND f.source_type = 'echo'" in sql
        assert params == ("fire-ca",)

    def test_returns_none_for_unknown_channel(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(conn)

        result = store.resolve_echo_feed("unknown")

        assert result is None

    def test_raises_value_error_for_unknown_status(self) -> None:
        conn = _make_mock_conn()
        conn.execute.return_value.fetchone.return_value = {
            "id": uuid.uuid4(),
            "name": "Fire CA",
            "status": "not-a-status",
            "created_at": datetime(2026, 1, 1, tzinfo=UTC),
        }
        store = _make_store(conn)

        with pytest.raises(ValueError, match="Unknown feed status"):
            store.resolve_echo_feed("fire-ca")


class TestRecordHeartbeat:
    def test_executes_heartbeat_sql(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(conn)
        feed_id = uuid.uuid4()

        store.record_heartbeat(feed_id, actor_id=_ECHO_ACTOR_ID)

        conn.execute.assert_called_once()
        sql, params = conn.execute.call_args[0]
        assert (
            "status NOT IN ('quarantined'::feed_status, "
            "'deactivated'::feed_status)"
        ) in sql
        assert params == (feed_id, _ECHO_ACTOR_ID)

    def test_rejects_missing_actor_id(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(conn)

        with pytest.raises(ValueError, match="actor_id is required"):
            store.record_heartbeat(uuid.uuid4(), actor_id=_MISSING_ACTOR_ID)

        conn.execute.assert_not_called()


class TestRecordFailure:
    def test_executes_failure_sql_with_config(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(
            conn,
            failure_threshold=5,
            base_backoff_sec=15,
            max_backoff_sec=600,
        )
        feed_id = uuid.uuid4()

        store.record_failure(
            feed_id,
            actor_id=_ECHO_ACTOR_ID,
            reason="echo_pubsub_publish_failed",
            status_reason=FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        )

        conn.execute.assert_called_once()
        sql, params = conn.execute.call_args[0]
        assert (
            "status NOT IN ('quarantined'::feed_status, "
            "'deactivated'::feed_status)"
        ) in sql
        assert params == (
            feed_id,
            "system_pipeline_error",
            5,
            5,
            600,
            15,
            "echo_pubsub_publish_failed",
            _ECHO_ACTOR_ID,
        )

    def test_uses_custom_thresholds(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(
            conn,
            failure_threshold=10,
            base_backoff_sec=30,
            max_backoff_sec=1200,
        )
        feed_id = uuid.uuid4()

        store.record_failure(
            feed_id,
            actor_id=_ECHO_ACTOR_ID,
            reason="echo_heartbeat_write_failed",
            status_reason=FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        )

        params = conn.execute.call_args[0][1]
        assert params == (
            feed_id,
            "system_pipeline_error",
            10,
            10,
            1200,
            30,
            "echo_heartbeat_write_failed",
            _ECHO_ACTOR_ID,
        )

    def test_record_failure_allows_omitted_status_reason_for_compatibility(
        self,
    ) -> None:
        conn = _make_mock_conn()
        store = _make_store(conn)
        feed_id = uuid.uuid4()

        store.record_failure(
            feed_id,
            actor_id=_ECHO_ACTOR_ID,
            reason="raw",
        )

        assert conn.execute.call_args[0][1] == (
            feed_id,
            None,
            5,
            5,
            600,
            15,
            "raw",
            _ECHO_ACTOR_ID,
        )

    def test_caps_status_reason_detail_at_persistence_boundary(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(conn)
        feed_id = uuid.uuid4()
        long_reason = "x" * (quarantine_reason.MAX_QUARANTINE_REASON_LENGTH + 1)

        store.record_failure(
            feed_id,
            actor_id=_ECHO_ACTOR_ID,
            reason=long_reason,
        )

        reason_arg = conn.execute.call_args[0][1][6]
        assert len(reason_arg) == quarantine_reason.MAX_QUARANTINE_REASON_LENGTH
        assert reason_arg.endswith("[truncated]")

    def test_always_logs_failure(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(conn)
        feed_id = uuid.uuid4()

        with patch(
            "backend.pipeline.storage.sync_feed_store.logger"
        ) as mock_logger:
            store.record_failure(
                feed_id,
                actor_id=_ECHO_ACTOR_ID,
                reason="echo_recording_download_failed",
                status_reason=FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
            )

        mock_logger.warning.assert_called_once()
        extra = mock_logger.warning.call_args[1]["extra"]
        assert extra["feed_id"] == str(feed_id)
        assert extra["status_reason"] == "system_collector_error"
        assert extra["reason"] == "echo_recording_download_failed"

    def test_rejects_missing_actor_id(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(conn)

        with pytest.raises(ValueError, match="actor_id is required"):
            store.record_failure(uuid.uuid4(), actor_id=_MISSING_ACTOR_ID)

        conn.execute.assert_not_called()


class TestRecordNonBudgetedFailure:
    def test_executes_non_budgeted_failure_sql(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(conn)
        feed_id = uuid.uuid4()

        store.record_non_budgeted_failure(
            feed_id,
            actor_id=_ECHO_ACTOR_ID,
            status_reason=FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        )

        conn.execute.assert_called_once()
        sql, params = conn.execute.call_args[0]
        assert "failure_count = 0" in sql
        assert "retry_after = NULL" in sql
        assert "status_reason_updated_at = CASE" in sql
        assert (
            "WHEN feeds.status_reason IS DISTINCT FROM "
            "status_reason_input.status_reason THEN NOW()"
        ) in sql
        assert (
            "status NOT IN ('quarantined'::feed_status, "
            "'deactivated'::feed_status)"
        ) in sql
        assert "quarantine_reason =" not in sql
        assert params == (
            feed_id,
            "system_pipeline_error",
            None,
            _ECHO_ACTOR_ID,
        )

    def test_logs_non_budgeted_failure(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(conn)
        feed_id = uuid.uuid4()

        with patch(
            "backend.pipeline.storage.sync_feed_store.logger"
        ) as mock_logger:
            store.record_non_budgeted_failure(
                feed_id,
                actor_id=_ECHO_ACTOR_ID,
                status_reason=FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
            )

        mock_logger.info.assert_called_once()
        extra = mock_logger.info.call_args[1]["extra"]
        assert extra == {
            "feed_id": str(feed_id),
            "status_reason": "system_collector_error",
        }

    def test_rejects_missing_actor_id(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(conn)

        with pytest.raises(ValueError, match="actor_id is required"):
            store.record_non_budgeted_failure(
                uuid.uuid4(),
                actor_id=_MISSING_ACTOR_ID,
                status_reason=FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
            )

        conn.execute.assert_not_called()


class TestSyncAuditSql:
    def test_runtime_audit_insert_is_embedded_in_lifecycle_sql(self) -> None:
        for sql in (
            sync_feed_queries.HEARTBEAT_SQL,
            sync_feed_queries.RECORD_FAILURE_SQL,
            sync_feed_queries.RECORD_NON_BUDGETED_FAILURE_SQL,
        ):
            assert "INSERT INTO feed_audit_events" in sql
            assert "feed_revision" in sql
            assert "before_values" in sql
            assert "after_values" in sql

    def test_runtime_audit_actions_are_selected_in_sql(self) -> None:
        assert "THEN 'feed.recovered'" in sync_feed_queries.HEARTBEAT_SQL
        assert (
            "THEN 'feed.failure_reported'"
            in sync_feed_queries.RECORD_FAILURE_SQL
        )
        assert "THEN 'feed.quarantined'" in sync_feed_queries.RECORD_FAILURE_SQL

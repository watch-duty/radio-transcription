"""Tests for the SyncFeedStore."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from backend.pipeline.storage import quarantine_reason
from backend.pipeline.storage.feed_store import FeedStatusReason
from backend.pipeline.storage.sync_feed_store import SyncFeedStore


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
    def test_returns_feed_dict(self) -> None:
        conn = _make_mock_conn()
        feed_row = {
            "id": uuid.uuid4(),
            "status": "active",
            "failure_count": 0,
            "created_at": datetime(2026, 1, 1, tzinfo=UTC),
        }
        conn.execute.return_value.fetchone.return_value = feed_row
        store = _make_store(conn)

        result = store.resolve_echo_feed("fire-ca")

        assert result == feed_row
        conn.execute.assert_called_once()
        assert conn.execute.call_args[0][1] == ("fire-ca",)

    def test_returns_none_for_unknown_channel(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(conn)

        result = store.resolve_echo_feed("unknown")

        assert result is None


class TestRecordHeartbeat:
    def test_executes_heartbeat_sql(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(conn)
        feed_id = uuid.uuid4()

        store.record_heartbeat(feed_id)

        conn.execute.assert_called_once()
        sql, params = conn.execute.call_args[0]
        assert (
            "status NOT IN ('quarantined'::feed_status, "
            "'deactivated'::feed_status)"
        ) in sql
        assert params == (feed_id,)


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
            5,
            5,
            600,
            15,
            5,
            "echo_pubsub_publish_failed",
            "system_pipeline_error",
            "system_pipeline_error",
            feed_id,
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
            reason="echo_heartbeat_write_failed",
            status_reason=FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        )

        params = conn.execute.call_args[0][1]
        assert params == (
            10,
            10,
            1200,
            30,
            10,
            "echo_heartbeat_write_failed",
            "system_pipeline_error",
            "system_pipeline_error",
            feed_id,
        )

    def test_record_failure_allows_omitted_status_reason_for_compatibility(
        self,
    ) -> None:
        conn = _make_mock_conn()
        store = _make_store(conn)
        feed_id = uuid.uuid4()

        store.record_failure(feed_id, reason="raw")

        assert conn.execute.call_args[0][1] == (
            5,
            5,
            600,
            15,
            5,
            "raw",
            None,
            None,
            feed_id,
        )

    def test_caps_quarantine_reason_at_persistence_boundary(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(conn)
        feed_id = uuid.uuid4()
        long_reason = "x" * (quarantine_reason.MAX_QUARANTINE_REASON_LENGTH + 1)

        store.record_failure(feed_id, reason=long_reason)

        reason_arg = conn.execute.call_args[0][1][5]
        assert len(reason_arg) == quarantine_reason.MAX_QUARANTINE_REASON_LENGTH
        assert reason_arg.endswith("[truncated]")

    def test_always_logs_failure(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(conn)
        feed_id = uuid.uuid4()

        with patch(
            "backend.pipeline.storage.sync_feed_store.logger"
        ) as mock_logger:
            store.record_failure(feed_id)

        mock_logger.warning.assert_called_once()
        extra = mock_logger.warning.call_args[1]["extra"]
        assert extra["feed_id"] == str(feed_id)


class TestRecordNonBudgetedFailure:
    def test_executes_non_budgeted_failure_sql(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(conn)
        feed_id = uuid.uuid4()

        store.record_non_budgeted_failure(
            feed_id,
            status_reason=FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        )

        conn.execute.assert_called_once()
        sql, params = conn.execute.call_args[0]
        assert "failure_count = 0" in sql
        assert "retry_after = NULL" in sql
        assert "status_reason_updated_at = CASE" in sql
        assert "WHEN status_reason IS DISTINCT FROM %s THEN NOW()" in sql
        assert (
            "status NOT IN ('quarantined'::feed_status, "
            "'deactivated'::feed_status)"
        ) in sql
        assert "quarantine_reason" not in sql
        assert params == (
            "system_pipeline_error",
            "system_pipeline_error",
            feed_id,
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
                status_reason=FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
            )

        mock_logger.info.assert_called_once()
        extra = mock_logger.info.call_args[1]["extra"]
        assert extra == {
            "feed_id": str(feed_id),
            "status_reason": "system_collector_error",
        }

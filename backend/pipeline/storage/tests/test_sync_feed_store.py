"""Tests for the SyncFeedStore."""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

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
        }
        conn.execute.return_value.fetchone.return_value = feed_row
        store = _make_store(conn)

        result = store.resolve_echo_feed("fire-ca")

        assert result == feed_row
        conn.execute.assert_called_once()
        sql = conn.execute.call_args[0][0]
        assert "feed_properties" in sql
        assert "source_type" in sql
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
        sql = conn.execute.call_args[0][0]
        assert "last_heartbeat" in sql
        assert conn.execute.call_args[0][1] == (feed_id,)


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

        store.record_failure(feed_id)

        conn.execute.assert_called_once()
        sql = conn.execute.call_args[0][0]
        assert "failure_count + 1" in sql
        params = conn.execute.call_args[0][1]
        # Parameters: (threshold, threshold, max_backoff, base_backoff, feed_id)
        assert params == (5, 5, 600, 15, feed_id)

    def test_uses_custom_thresholds(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(
            conn,
            failure_threshold=10,
            base_backoff_sec=30,
            max_backoff_sec=1200,
        )
        feed_id = uuid.uuid4()

        store.record_failure(feed_id)

        params = conn.execute.call_args[0][1]
        assert params == (10, 10, 1200, 30, feed_id)

    def test_error_reason_logged_when_provided(self) -> None:
        from unittest.mock import patch

        conn = _make_mock_conn()
        store = _make_store(conn)
        feed_id = uuid.uuid4()

        with patch(
            "backend.pipeline.storage.sync_feed_store.logger"
        ) as mock_logger:
            store.record_failure(feed_id, error_reason="ffmpeg exit 8")

        mock_logger.warning.assert_called_once()
        extra = mock_logger.warning.call_args[1]["extra"]
        assert extra["error_reason"] == "ffmpeg exit 8"
        assert extra["feed_id"] == str(feed_id)

    def test_no_log_when_error_reason_is_none(self) -> None:
        from unittest.mock import patch

        conn = _make_mock_conn()
        store = _make_store(conn)
        feed_id = uuid.uuid4()

        with patch(
            "backend.pipeline.storage.sync_feed_store.logger"
        ) as mock_logger:
            store.record_failure(feed_id)

        mock_logger.warning.assert_not_called()

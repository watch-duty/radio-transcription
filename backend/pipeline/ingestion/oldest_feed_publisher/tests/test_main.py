from __future__ import annotations

from unittest import mock

import asyncpg
import pytest
from google.api_core.exceptions import GoogleAPIError

from backend.pipeline.ingestion.oldest_feed_publisher import main


@pytest.fixture
def configured(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set required env vars + reset module-level monitoring client."""
    monkeypatch.setattr(main, "PROJECT_ID", "test-project")
    monkeypatch.setattr(main, "ALLOYDB_HOST", "10.0.0.1")
    monkeypatch.setattr(main, "ALLOYDB_PORT", "6432")
    monkeypatch.setattr(main, "ALLOYDB_USER", "worker")
    monkeypatch.setattr(main, "ALLOYDB_DB", "appdb")
    monkeypatch.setattr(main, "ALLOYDB_PASSWORD", "s3cret")
    monkeypatch.setattr(main, "_monitoring_client", None)


def _make_mock_conn(
    fetchval_result: float | None = 0.0,
    fetchval_side_effect: type[BaseException] | BaseException | None = None,
) -> mock.AsyncMock:
    """Build an asyncpg connection mock with controllable fetchval behavior."""
    conn = mock.AsyncMock()
    if fetchval_side_effect is not None:
        conn.fetchval = mock.AsyncMock(side_effect=fetchval_side_effect)
    else:
        conn.fetchval = mock.AsyncMock(return_value=fetchval_result)
    conn.close = mock.AsyncMock()
    return conn


class TestSuccessPath:
    def test_success_returns_200_and_publishes(
        self,
        configured: None,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        del configured
        mock_conn = _make_mock_conn(fetchval_result=42.5)
        mock_publish = mock.AsyncMock()

        with (
            mock.patch.object(
                main.asyncpg,
                "connect",
                mock.AsyncMock(return_value=mock_conn),
            ),
            mock.patch.object(
                main.MonitoringClient,
                "write_time_series_double",
                mock_publish,
            ),
            caplog.at_level("INFO", logger=main.__name__),
        ):
            result = main.oldest_feed_publisher(mock.MagicMock())

        assert result == ("ok", 200)
        mock_publish.assert_awaited_once()
        # await_args is `_Call | None`; assert_awaited_once above
        # guarantees non-None at runtime.
        kwargs = mock_publish.await_args.kwargs  # ty: ignore[unresolved-attribute]
        assert kwargs["metric_type"] == main.METRIC_TYPE
        assert kwargs["value"] == 42.5
        assert kwargs["resource_labels"] == {"project_id": "test-project"}
        assert kwargs["resource_type"] == "global"
        # INFO log must surface the published value
        assert any("42.500" in r.message for r in caplog.records)
        mock_conn.close.assert_awaited_once()

    def test_zero_value_still_publishes(self, configured: None) -> None:
        del configured
        # COALESCE → 0.0 is a valid datapoint (empty unclaimed set), not
        # a sentinel — Publisher MUST still publish it.
        mock_conn = _make_mock_conn(fetchval_result=0.0)
        mock_publish = mock.AsyncMock()

        with (
            mock.patch.object(
                main.asyncpg,
                "connect",
                mock.AsyncMock(return_value=mock_conn),
            ),
            mock.patch.object(
                main.MonitoringClient,
                "write_time_series_double",
                mock_publish,
            ),
        ):
            result = main.oldest_feed_publisher(mock.MagicMock())

        assert result == ("ok", 200)
        mock_publish.assert_awaited_once()
        assert mock_publish.await_args.kwargs["value"] == 0.0  # ty: ignore[unresolved-attribute]


class TestFailurePaths:
    def test_connect_failure_returns_500_no_publish(
        self,
        configured: None,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        del configured
        mock_publish = mock.AsyncMock()

        with (
            mock.patch.object(
                main.asyncpg,
                "connect",
                mock.AsyncMock(
                    side_effect=ConnectionError("alloydb unreachable")
                ),
            ),
            mock.patch.object(
                main.MonitoringClient,
                "write_time_series_double",
                mock_publish,
            ),
            caplog.at_level("ERROR", logger=main.__name__),
        ):
            result = main.oldest_feed_publisher(mock.MagicMock())

        assert result == ("err", 500)
        mock_publish.assert_not_awaited()
        # Error log surfaced (logger.exception → ERROR level + traceback)
        assert any("publisher failed" in r.message for r in caplog.records)
        assert any(r.levelname == "ERROR" for r in caplog.records)

    def test_query_failure_closes_conn_no_publish(
        self, configured: None
    ) -> None:
        del configured
        mock_conn = _make_mock_conn(
            fetchval_side_effect=asyncpg.PostgresError("query failed")
        )
        mock_publish = mock.AsyncMock()

        with (
            mock.patch.object(
                main.asyncpg,
                "connect",
                mock.AsyncMock(return_value=mock_conn),
            ),
            mock.patch.object(
                main.MonitoringClient,
                "write_time_series_double",
                mock_publish,
            ),
        ):
            result = main.oldest_feed_publisher(mock.MagicMock())

        assert result == ("err", 500)
        mock_publish.assert_not_awaited()
        # The try/finally in _query_oldest_age must close the connection
        # even when the query raises.
        mock_conn.close.assert_awaited_once()

    def test_publish_failure_returns_500(self, configured: None) -> None:
        del configured
        mock_conn = _make_mock_conn(fetchval_result=12.3)
        mock_publish = mock.AsyncMock(
            side_effect=GoogleAPIError("monitoring quota exceeded")
        )

        with (
            mock.patch.object(
                main.asyncpg,
                "connect",
                mock.AsyncMock(return_value=mock_conn),
            ),
            mock.patch.object(
                main.MonitoringClient,
                "write_time_series_double",
                mock_publish,
            ),
        ):
            result = main.oldest_feed_publisher(mock.MagicMock())

        # Publish error must NOT be silently swallowed: response is 500.
        assert result == ("err", 500)
        mock_publish.assert_awaited_once()

    def test_missing_env_returns_500_before_db(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # Configure most env, then unset ALLOYDB_HOST to trigger validation.
        monkeypatch.setattr(main, "PROJECT_ID", "test-project")
        monkeypatch.setattr(main, "ALLOYDB_HOST", "")
        monkeypatch.setattr(main, "ALLOYDB_DB", "appdb")
        monkeypatch.setattr(main, "ALLOYDB_PASSWORD", "s3cret")
        monkeypatch.setattr(main, "_monitoring_client", None)

        mock_connect = mock.AsyncMock()
        mock_publish = mock.AsyncMock()

        with (
            mock.patch.object(main.asyncpg, "connect", mock_connect),
            mock.patch.object(
                main.MonitoringClient,
                "write_time_series_double",
                mock_publish,
            ),
            caplog.at_level("ERROR", logger=main.__name__),
        ):
            result = main.oldest_feed_publisher(mock.MagicMock())

        assert result == ("err", 500)
        # No DB connection attempt should be made when env validation fails.
        mock_connect.assert_not_awaited()
        mock_publish.assert_not_awaited()
        # The missing env-var name must appear in some log record
        # (either the exception message or its traceback).
        log_blob = "\n".join(
            (r.message + "\n" + (r.exc_text or "")) for r in caplog.records
        )
        assert "ALLOYDB_HOST" in log_blob

    def test_connect_args_include_pgbouncer_settings(
        self, configured: None
    ) -> None:
        del configured
        mock_conn = _make_mock_conn(fetchval_result=0.0)
        mock_connect = mock.AsyncMock(return_value=mock_conn)

        with (
            mock.patch.object(main.asyncpg, "connect", mock_connect),
            mock.patch.object(
                main.MonitoringClient,
                "write_time_series_double",
                mock.AsyncMock(),
            ),
        ):
            main.oldest_feed_publisher(mock.MagicMock())

        # await_args is `_Call | None`; the call above guarantees non-None.
        kwargs = mock_connect.await_args.kwargs  # ty: ignore[unresolved-attribute]
        assert kwargs["statement_cache_size"] == 0
        assert kwargs["timeout"] == main.CONNECT_TIMEOUT_SEC
        assert kwargs["timeout"] == 10.0
        # Verify fetchval was called with the statement timeout.
        fetchval_kwargs = mock_conn.fetchval.await_args.kwargs
        assert fetchval_kwargs["timeout"] == main.QUERY_TIMEOUT_SEC
        assert fetchval_kwargs["timeout"] == 5.0

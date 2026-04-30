from __future__ import annotations

import asyncio
from unittest import mock

import asyncpg
import pytest
from google.api_core.exceptions import GoogleAPIError

from backend.pipeline.ingestion.oldest_feed_publisher import main


@pytest.fixture
def configured(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set PROJECT_ID and reset module-level singletons between tests."""
    monkeypatch.setattr(main, "PROJECT_ID", "test-project")
    monkeypatch.setattr(main, "_monitoring_client", None)
    monkeypatch.setattr(main, "_pool", None)


def _make_mock_pool(
    fetchval_result: int | None = 0,
    fetchval_side_effect: type[BaseException] | BaseException | None = None,
) -> mock.AsyncMock:
    """Build an asyncpg pool mock with controllable fetchval behavior."""
    pool = mock.AsyncMock()
    if fetchval_side_effect is not None:
        pool.fetchval = mock.AsyncMock(side_effect=fetchval_side_effect)
    else:
        pool.fetchval = mock.AsyncMock(return_value=fetchval_result)
    return pool


class TestSuccessPath:
    def test_success_returns_200_and_publishes(
        self,
        configured: None,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        del configured
        # COUNT(*)::DOUBLE PRECISION returns integer-valued floats; 42.0 is
        # the realistic shape (vs. fractional values that latency would have).
        mock_pool = _make_mock_pool(fetchval_result=42)
        mock_publish = mock.AsyncMock()

        with (
            mock.patch.object(
                main,
                "create_pool_from_settings",
                mock.AsyncMock(return_value=mock_pool),
            ),
            mock.patch.object(
                main.MonitoringClient,
                "write_time_series",
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
        assert kwargs["value"] == 42
        assert kwargs["resource_labels"] == {"project_id": "test-project"}
        assert kwargs["resource_type"] == "global"
        # INFO log must surface the published count (rendered with %.0f)
        assert any("unclaimed_count=42" in r.message for r in caplog.records)
        # The QUERY constant must be the one passed to fetchval
        # (defends against a regression that swaps it for the old latency SQL).
        mock_pool.fetchval.assert_awaited_once()
        fetchval_args = mock_pool.fetchval.await_args
        assert fetchval_args.args[0] == main.QUERY
        assert "COUNT(*)" in main.QUERY
        assert "status = 'unclaimed'" in main.QUERY

    def test_zero_value_still_publishes(self, configured: None) -> None:
        del configured
        # COUNT(*) = 0 is a valid datapoint (no unclaimed feeds — fully
        # caught up), not a sentinel — Publisher MUST still publish it so
        # the autoscaler sees the queue is empty and can scale in.
        mock_pool = _make_mock_pool(fetchval_result=0)
        mock_publish = mock.AsyncMock()

        with (
            mock.patch.object(
                main,
                "create_pool_from_settings",
                mock.AsyncMock(return_value=mock_pool),
            ),
            mock.patch.object(
                main.MonitoringClient,
                "write_time_series",
                mock_publish,
            ),
        ):
            result = main.oldest_feed_publisher(mock.MagicMock())

        assert result == ("ok", 200)
        mock_publish.assert_awaited_once()
        assert mock_publish.await_args.kwargs["value"] == 0  # ty: ignore[unresolved-attribute]

    def test_pool_reused_across_invocations(self, configured: None) -> None:
        """Warm Cloud Run instances reuse the pool — only one create call."""
        del configured
        mock_pool = _make_mock_pool(fetchval_result=7)
        mock_create = mock.AsyncMock(return_value=mock_pool)

        with (
            mock.patch.object(main, "create_pool_from_settings", mock_create),
            mock.patch.object(
                main.MonitoringClient,
                "write_time_series",
                mock.AsyncMock(),
            ),
        ):
            main.oldest_feed_publisher(mock.MagicMock())
            main.oldest_feed_publisher(mock.MagicMock())
            main.oldest_feed_publisher(mock.MagicMock())

        # Three Scheduler ticks → one pool created, fetched three times.
        mock_create.assert_awaited_once()

    def test_persistent_event_loop_used_across_invocations(
        self, configured: None
    ) -> None:
        """Each handler invocation runs in the same module-level loop.

        REGRESSION GUARD: an earlier version used `asyncio.run(_run())`,
        which creates a fresh event loop per invocation. asyncpg pools are
        loop-bound — reusing a pool created in a closed loop raises
        "Task got Future attached to a different loop" or hangs on the
        loop-bound semaphore. This test fails if `_loop` is replaced by
        `asyncio.run` again.

        We capture the running loop id from inside the async coroutine
        across three invocations and assert they're all the same loop.
        """
        del configured
        captured_loop_ids: list[int] = []

        async def capturing_fetchval(*args: object, **kwargs: object) -> int:
            del args, kwargs
            captured_loop_ids.append(id(asyncio.get_running_loop()))
            return 1

        mock_pool = mock.AsyncMock()
        mock_pool.fetchval = mock.AsyncMock(side_effect=capturing_fetchval)
        mock_create = mock.AsyncMock(return_value=mock_pool)

        with (
            mock.patch.object(main, "create_pool_from_settings", mock_create),
            mock.patch.object(
                main.MonitoringClient,
                "write_time_series",
                mock.AsyncMock(),
            ),
        ):
            main.oldest_feed_publisher(mock.MagicMock())
            main.oldest_feed_publisher(mock.MagicMock())
            main.oldest_feed_publisher(mock.MagicMock())

        assert len(captured_loop_ids) == 3
        assert (
            captured_loop_ids[0] == captured_loop_ids[1] == captured_loop_ids[2]
        ), (
            f"Handler must reuse a persistent event loop across invocations "
            f"(got {captured_loop_ids}). asyncpg pools are loop-bound; "
            f"using asyncio.run would orphan the pool on the second tick."
        )
        assert captured_loop_ids[0] == id(main._loop)
        assert mock_pool.fetchval.await_count == 3


class TestFailurePaths:
    def test_pool_creation_failure_returns_500_no_publish(
        self,
        configured: None,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        del configured
        mock_publish = mock.AsyncMock()

        with (
            mock.patch.object(
                main,
                "create_pool_from_settings",
                mock.AsyncMock(
                    side_effect=ConnectionError("alloydb unreachable")
                ),
            ),
            mock.patch.object(
                main.MonitoringClient,
                "write_time_series",
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

    def test_query_failure_no_publish(self, configured: None) -> None:
        del configured
        mock_pool = _make_mock_pool(
            fetchval_side_effect=asyncpg.PostgresError("query failed")
        )
        mock_publish = mock.AsyncMock()

        with (
            mock.patch.object(
                main,
                "create_pool_from_settings",
                mock.AsyncMock(return_value=mock_pool),
            ),
            mock.patch.object(
                main.MonitoringClient,
                "write_time_series",
                mock_publish,
            ),
        ):
            result = main.oldest_feed_publisher(mock.MagicMock())

        assert result == ("err", 500)
        mock_publish.assert_not_awaited()
        # Pool checkout/release is asyncpg's job; we just need to verify the
        # query was attempted and the error didn't get swallowed.
        mock_pool.fetchval.assert_awaited_once()

    def test_publish_failure_returns_500(self, configured: None) -> None:
        del configured
        mock_pool = _make_mock_pool(fetchval_result=12)
        mock_publish = mock.AsyncMock(
            side_effect=GoogleAPIError("monitoring quota exceeded")
        )

        with (
            mock.patch.object(
                main,
                "create_pool_from_settings",
                mock.AsyncMock(return_value=mock_pool),
            ),
            mock.patch.object(
                main.MonitoringClient,
                "write_time_series",
                mock_publish,
            ),
        ):
            result = main.oldest_feed_publisher(mock.MagicMock())

        # Publish error must NOT be silently swallowed: response is 500.
        assert result == ("err", 500)
        mock_publish.assert_awaited_once()

    def test_missing_project_id_returns_500_before_db(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # PROJECT_ID is the only env var still validated inline in main.py;
        # the ALLOYDB_* vars now flow through AlloyDBSettings (validated at
        # asyncpg connection time). Unset PROJECT_ID and the function should
        # fail-fast before attempting any DB or pool work.
        monkeypatch.setattr(main, "PROJECT_ID", "")
        monkeypatch.setattr(main, "_monitoring_client", None)
        monkeypatch.setattr(main, "_pool", None)

        mock_create = mock.AsyncMock()
        mock_publish = mock.AsyncMock()

        with (
            mock.patch.object(main, "create_pool_from_settings", mock_create),
            mock.patch.object(
                main.MonitoringClient,
                "write_time_series",
                mock_publish,
            ),
            caplog.at_level("ERROR", logger=main.__name__),
        ):
            result = main.oldest_feed_publisher(mock.MagicMock())

        assert result == ("err", 500)
        # No pool creation should be attempted when PROJECT_ID is unset.
        mock_create.assert_not_awaited()
        mock_publish.assert_not_awaited()
        assert any("GOOGLE_CLOUD_PROJECT" in r.message for r in caplog.records)

    def test_pool_created_with_publisher_overrides(
        self, configured: None
    ) -> None:
        """Pool is sized for a 1-tick-per-minute service, not the worker fleet."""
        del configured
        mock_pool = _make_mock_pool(fetchval_result=0)
        mock_create = mock.AsyncMock(return_value=mock_pool)

        with (
            mock.patch.object(main, "create_pool_from_settings", mock_create),
            mock.patch.object(
                main.MonitoringClient,
                "write_time_series",
                mock.AsyncMock(),
            ),
        ):
            main.oldest_feed_publisher(mock.MagicMock())

        # Pool was created with the AlloyDBSettings.replace overrides:
        # min/max=1 (single connection for a per-minute service), and a
        # tighter 5s command_timeout (vs the default 30s for the worker fleet).
        mock_create.assert_awaited_once()
        settings_arg = mock_create.await_args.args[0]  # ty: ignore[unresolved-attribute]
        assert settings_arg.pool_min_size == 1
        assert settings_arg.pool_max_size == 1
        assert settings_arg.command_timeout_sec == main.QUERY_TIMEOUT_SEC
        assert settings_arg.command_timeout_sec == 5.0
        # The fetchval call also passes the per-query timeout explicitly.
        fetchval_kwargs = mock_pool.fetchval.await_args.kwargs
        assert fetchval_kwargs["timeout"] == main.QUERY_TIMEOUT_SEC

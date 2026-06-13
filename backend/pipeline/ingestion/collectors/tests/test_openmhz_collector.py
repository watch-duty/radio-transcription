from __future__ import annotations

import asyncio
import contextlib
import datetime
import unittest
import uuid
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemFailure,
)
from backend.pipeline.ingestion.collectors.openmhz._types import CallEvent
from backend.pipeline.ingestion.collectors.openmhz._ws_transport import (
    OpenMHzTransportError,
)
from backend.pipeline.ingestion.collectors.openmhz.collector import (
    MAX_ITEM_DOWNLOAD_FAILURES,
    MAX_RECONNECT_FAILURES,
    _download_m4a,
    openmhz_collector,
)
from backend.pipeline.ingestion.collectors.tests.conftest import (
    _default_resources,
    _require_captured_chunk,
)
from backend.pipeline.ingestion.models import FeedFailure, SourceObservation
from backend.pipeline.storage.feed_store import (
    FeedStatusReason,
    LeasedFeed,
    SourceType,
)

_TEST_FEED = LeasedFeed(
    id=uuid.UUID("12345678-1234-5678-1234-567812345678"),
    name="test-openmhz-wmata",
    source_type=SourceType.OPENMHZ,
    last_processed_filename=None,
    last_bookmark_time=None,
    fencing_token=1,
    failure_count=0,
    status_reason=None,
    source_feed_id="wmata",
)


def _make_call(
    call_id: str = "abc",
    length_sec: int = 5,
    url: str = "https://media2.openmhz.com/test.m4a",
) -> CallEvent:
    return CallEvent(
        id=call_id,
        talkgroup_num=34480,
        url=url,
        time=datetime.datetime(2026, 4, 2, 22, 57, 20, tzinfo=datetime.UTC),
        length_sec=length_sec,
        freq=490962500,
        src_list=[],
        short_name="wmata",
        emergency=False,
    )


@contextlib.asynccontextmanager
async def _mock_transport(
    calls: list[CallEvent],
) -> AsyncIterator[AsyncIterator[CallEvent]]:
    """Fake transport that yields scripted CallEvent objects."""

    async def _events() -> AsyncIterator[CallEvent]:
        for c in calls:
            yield c

    yield _events()


_COL_MOD = "backend.pipeline.ingestion.collectors.openmhz.collector"


def _require_item_failure(value: ItemFailure | bytes) -> ItemFailure:
    """Return a typed item failure for tests that intentionally expect one."""
    if not isinstance(value, ItemFailure):
        msg = f"Expected ItemFailure, got {value!r}"
        raise TypeError(msg)
    return value


class TestDownloadM4a(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.session = MagicMock()
        self.shutdown = asyncio.Event()

    async def test_valid_download_disables_redirects(self) -> None:
        resp = MagicMock(status_code=200, content=b"m4a")
        self.session.get = AsyncMock(return_value=resp)

        result = await _download_m4a(
            self.session,
            "https://media2.openmhz.com/test.m4a",
            self.shutdown,
        )

        self.assertEqual(result, b"m4a")
        self.session.get.assert_awaited_once_with(
            "https://media2.openmhz.com/test.m4a",
            timeout=30.0,
            allow_redirects=False,
        )

    async def test_rejects_non_openmhz_media_url(self) -> None:
        self.session.get = AsyncMock()

        result = await _download_m4a(
            self.session,
            "http://169.254.169.254/latest/meta-data/",
            self.shutdown,
        )

        failure = _require_item_failure(result)
        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(failure.reason, "invalid_openmhz_media_url")
        self.session.get.assert_not_called()

    async def test_rejects_malformed_media_url_as_item_failure(self) -> None:
        self.session.get = AsyncMock()

        result = await _download_m4a(
            self.session,
            "http://[::1",
            self.shutdown,
        )

        failure = _require_item_failure(result)
        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(failure.reason, "invalid_openmhz_media_url")
        self.session.get.assert_not_called()

    async def test_terminal_4xx_statuses_return_item_failures(self) -> None:
        cases = {
            401: FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
            403: FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
            404: FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        }
        for status, reason in cases.items():
            with self.subTest(status=status):
                resp = MagicMock(status_code=status)
                self.session.get = AsyncMock(return_value=resp)

                result = await _download_m4a(
                    self.session,
                    "https://media2.openmhz.com/test.m4a",
                    self.shutdown,
                )

                failure = _require_item_failure(result)
                self.assertIs(failure.status_reason, reason)
                self.assertEqual(failure.reason, f"item_http_{status}")

    @patch(f"{_COL_MOD}.control_flow.sleep_or_cancel", new_callable=AsyncMock)
    async def test_retryable_4xx_retry_success(
        self,
        mock_sleep: AsyncMock,
    ) -> None:
        mock_sleep.return_value = False
        for status in (408, 429):
            with self.subTest(status=status):
                mock_sleep.reset_mock(return_value=True, side_effect=True)
                mock_sleep.return_value = False
                resp_retryable = MagicMock(status_code=status)
                resp200 = MagicMock(status_code=200, content=b"m4a")
                self.session.get = AsyncMock(
                    side_effect=[resp_retryable, resp200]
                )

                result = await _download_m4a(
                    self.session,
                    "https://media2.openmhz.com/test.m4a",
                    self.shutdown,
                )

                self.assertEqual(result, b"m4a")
                self.assertEqual(self.session.get.call_count, 2)
                mock_sleep.assert_awaited_once()

    @patch(f"{_COL_MOD}.control_flow.sleep_or_cancel", new_callable=AsyncMock)
    async def test_retry_exhausted_5xx_returns_item_failure(
        self,
        mock_sleep: AsyncMock,
    ) -> None:
        mock_sleep.return_value = False
        resp = MagicMock(status_code=503)
        self.session.get = AsyncMock(return_value=resp)

        result = await _download_m4a(
            self.session,
            "https://media2.openmhz.com/test.m4a",
            self.shutdown,
        )

        failure = _require_item_failure(result)
        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(failure.reason, "item_http_503")
        self.assertEqual(self.session.get.call_count, 3)

    @patch(f"{_COL_MOD}.control_flow.sleep_or_cancel", new_callable=AsyncMock)
    async def test_network_errors_exhausted_return_item_failure(
        self,
        mock_sleep: AsyncMock,
    ) -> None:
        mock_sleep.return_value = False
        self.session.get = AsyncMock(side_effect=TimeoutError)

        result = await _download_m4a(
            self.session,
            "https://media2.openmhz.com/test.m4a",
            self.shutdown,
        )

        failure = _require_item_failure(result)
        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(failure.reason, "item_download_failed")
        self.assertEqual(self.session.get.call_count, 3)

    async def test_active_get_cancelled_when_shutdown_is_set(self) -> None:
        started = asyncio.Event()
        never_complete = asyncio.Future()

        async def _blocked_get(*_args: object, **_kwargs: object) -> object:
            started.set()
            await never_complete
            msg = "unreachable"
            raise AssertionError(msg)

        self.session.get = AsyncMock(side_effect=_blocked_get)

        download_task = asyncio.create_task(
            _download_m4a(
                self.session,
                "https://media2.openmhz.com/test.m4a",
                self.shutdown,
            )
        )
        await asyncio.wait_for(started.wait(), timeout=1.0)
        self.shutdown.set()

        with self.assertRaises(asyncio.CancelledError):
            await download_task

        self.session.get.assert_awaited_once_with(
            "https://media2.openmhz.com/test.m4a",
            timeout=30.0,
            allow_redirects=False,
        )

    async def test_parent_cancellation_cancels_active_get(self) -> None:
        started = asyncio.Event()
        cancelled = asyncio.Event()

        async def _blocked_get(*_args: object, **_kwargs: object) -> object:
            started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                cancelled.set()
                raise
            msg = "unreachable"
            raise AssertionError(msg)

        self.session.get = AsyncMock(side_effect=_blocked_get)

        download_task = asyncio.create_task(
            _download_m4a(
                self.session,
                "https://media2.openmhz.com/test.m4a",
                self.shutdown,
            )
        )
        await asyncio.wait_for(started.wait(), timeout=1.0)
        download_task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await download_task

        await asyncio.wait_for(cancelled.wait(), timeout=1.0)
        self.session.get.assert_awaited_once()

    @patch(f"{_COL_MOD}.control_flow.sleep_or_cancel", new_callable=AsyncMock)
    async def test_shutdown_during_retry_raises_cancelled_error(
        self,
        mock_sleep: AsyncMock,
    ) -> None:
        mock_sleep.side_effect = asyncio.CancelledError
        resp = MagicMock(status_code=503)
        self.session.get = AsyncMock(return_value=resp)

        with self.assertRaises(asyncio.CancelledError):
            await _download_m4a(
                self.session,
                "https://media2.openmhz.com/test.m4a",
                self.shutdown,
            )

        self.assertEqual(self.session.get.call_count, 1)
        mock_sleep.assert_awaited_once()


class TestOpenmhzCollector(unittest.IsolatedAsyncioTestCase):
    @patch(f"{_COL_MOD}.websocket_transport")
    async def test_dirty_feed_yields_observation_on_successful_connection(
        self,
        mock_transport: MagicMock,
    ) -> None:
        feed = cast(
            "LeasedFeed",
            {
                **_TEST_FEED,
                "failure_count": 1,
                "status_reason": FeedStatusReason.SOURCE_UNREACHABLE,
            },
        )
        mock_transport.side_effect = lambda *a, **kw: _mock_transport([])

        shutdown = asyncio.Event()
        events = []
        async for event in openmhz_collector(
            feed,
            shutdown,
            "https://api.openmhz.com/",
            _default_resources(),
        ):
            events.append(event)
            shutdown.set()

        self.assertEqual(events, [SourceObservation()])

    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}._download_m4a")
    async def test_yields_flac_and_call_time(
        self,
        mock_download: AsyncMock,
        mock_transport: MagicMock,
    ) -> None:
        call = _make_call(call_id="c1", length_sec=5)
        mock_transport.side_effect = lambda *a, **kw: _mock_transport([call])
        mock_download.return_value = b"fake-m4a-bytes"

        shutdown = asyncio.Event()
        results = []
        async for chunk in openmhz_collector(
            _TEST_FEED,
            shutdown,
            "https://api.openmhz.com/",
            _default_resources(),
        ):
            results.append(_require_captured_chunk(chunk))
            shutdown.set()

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].audio_bytes, b"fake-m4a-bytes")
        self.assertEqual(results[0].chunk_start_time, call.time)

    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}._download_m4a")
    async def test_skips_zero_length_calls(
        self,
        mock_download: AsyncMock,
        mock_transport: MagicMock,
    ) -> None:
        calls = [
            _make_call(call_id="zero", length_sec=0),
            _make_call(call_id="normal", length_sec=5),
        ]
        mock_transport.side_effect = lambda *a, **kw: _mock_transport(calls)
        mock_download.return_value = b"m4a"

        shutdown = asyncio.Event()
        results = []
        async for chunk in openmhz_collector(
            _TEST_FEED,
            shutdown,
            "https://api.openmhz.com/",
            _default_resources(),
        ):
            results.append(chunk)
            shutdown.set()

        self.assertEqual(len(results), 1)
        mock_download.assert_called_once()

    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}._download_m4a")
    async def test_skips_failed_download(
        self,
        mock_download: AsyncMock,
        mock_transport: MagicMock,
    ) -> None:
        calls = [
            _make_call(call_id="bad-url"),
            _make_call(call_id="good"),
        ]
        mock_transport.side_effect = lambda *a, **kw: _mock_transport(calls)
        mock_download.side_effect = [
            ItemFailure(
                FeedStatusReason.SOURCE_UNREACHABLE,
                "item_download_failed",
            ),
            b"m4a",
        ]

        shutdown = asyncio.Event()
        results = []
        async for chunk in openmhz_collector(
            _TEST_FEED,
            shutdown,
            "https://api.openmhz.com/",
            _default_resources(),
        ):
            results.append(chunk)
            shutdown.set()

        self.assertEqual(len(results), 1)
        self.assertEqual(mock_download.call_count, 2)

    @patch(f"{_COL_MOD}.control_flow.sleep_or_cancel", new_callable=AsyncMock)
    @patch(f"{_COL_MOD}._download_m4a")
    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}.AsyncSession")
    async def test_collector_owns_session_and_closes_it(
        self,
        mock_session_cls: MagicMock,
        mock_transport: MagicMock,
        mock_download: AsyncMock,
        _mock_sleep: AsyncMock,
    ) -> None:
        mock_session = AsyncMock()
        mock_session.close = AsyncMock()
        mock_session_cls.return_value = mock_session
        call = _make_call(call_id="session-owned", length_sec=5)
        mock_transport.side_effect = lambda *a, **kw: _mock_transport([call])
        mock_download.return_value = b"m4a"

        shutdown = asyncio.Event()
        chunks = []
        async for chunk in openmhz_collector(
            _TEST_FEED,
            shutdown,
            "https://api.openmhz.com/",
            _default_resources(),
        ):
            chunks.append(_require_captured_chunk(chunk))
            shutdown.set()

        mock_session_cls.assert_called_once_with()
        mock_session.close.assert_awaited_once()
        self.assertEqual(len(chunks), 1)

    @patch(f"{_COL_MOD}.telemetry.emit_call_download_failed")
    @patch(f"{_COL_MOD}._download_m4a")
    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}.AsyncSession")
    async def test_download_cancelled_propagates_without_telemetry(
        self,
        mock_session_cls: MagicMock,
        mock_transport: MagicMock,
        mock_download: AsyncMock,
        mock_emit: MagicMock,
    ) -> None:
        mock_session = AsyncMock()
        mock_session.close = AsyncMock()
        mock_session_cls.return_value = mock_session
        call = _make_call(call_id="cancelled", length_sec=5)
        mock_transport.side_effect = lambda *a, **kw: _mock_transport([call])
        mock_download.side_effect = asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            async for _ in openmhz_collector(
                _TEST_FEED,
                asyncio.Event(),
                "https://api.openmhz.com/",
                _default_resources(),
            ):
                pass

        mock_download.assert_awaited_once()
        mock_emit.assert_not_called()
        mock_session.close.assert_awaited_once()

    @patch(f"{_COL_MOD}._download_m4a")
    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}.AsyncSession")
    async def test_successful_download_after_shutdown_yields_no_chunk(
        self,
        mock_session_cls: MagicMock,
        mock_transport: MagicMock,
        mock_download: AsyncMock,
    ) -> None:
        mock_session = AsyncMock()
        mock_session.close = AsyncMock()
        mock_session_cls.return_value = mock_session
        call = _make_call(call_id="download-finished-after-shutdown")
        mock_transport.side_effect = lambda *a, **kw: _mock_transport([call])
        shutdown = asyncio.Event()

        async def _download_then_shutdown(*_args: object) -> bytes:
            shutdown.set()
            return b"m4a"

        mock_download.side_effect = _download_then_shutdown

        chunks = []
        async for chunk in openmhz_collector(
            _TEST_FEED,
            shutdown,
            "https://api.openmhz.com/",
            _default_resources(),
        ):
            chunks.append(chunk)

        self.assertEqual(chunks, [])
        mock_download.assert_awaited_once()
        mock_session.close.assert_awaited_once()

    async def test_raises_typed_failure_for_missing_source_feed_id(
        self,
    ) -> None:
        feed = LeasedFeed(
            id=uuid.uuid4(),
            name="no-id",
            source_type=SourceType.OPENMHZ,
            last_processed_filename=None,
            last_bookmark_time=None,
            fencing_token=1,
            failure_count=0,
            status_reason=None,
            source_feed_id=None,
        )
        shutdown = asyncio.Event()
        with self.assertRaises(FeedFailure) as ctx:
            async for _ in openmhz_collector(
                feed,
                shutdown,
                "https://api.openmhz.com/",
                _default_resources(),
            ):
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        )
        self.assertEqual(str(ctx.exception), "missing_source_feed_id")

    async def test_raises_typed_failure_for_blank_source_feed_id(
        self,
    ) -> None:
        feed = cast(
            "LeasedFeed",
            {
                **_TEST_FEED,
                "source_feed_id": "   ",
            },
        )
        shutdown = asyncio.Event()
        with self.assertRaises(FeedFailure) as ctx:
            async for _ in openmhz_collector(
                feed,
                shutdown,
                "https://api.openmhz.com/",
                _default_resources(),
            ):
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        )
        self.assertEqual(str(ctx.exception), "missing_source_feed_id")

    @patch.dict("os.environ", {"OPENMHZ_TRANSPORT": "bogus"})
    async def test_invalid_transport_raises_typed_configuration_failure(
        self,
    ) -> None:
        shutdown = asyncio.Event()

        with self.assertRaises(FeedFailure) as ctx:
            async for _ in openmhz_collector(
                _TEST_FEED,
                shutdown,
                "https://api.openmhz.com/",
                _default_resources(),
            ):
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        )
        self.assertEqual(str(ctx.exception), "invalid_openmhz_transport")

    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}.control_flow.sleep_or_cancel", new_callable=AsyncMock)
    async def test_raises_after_max_reconnect_failures(
        self,
        mock_sleep: AsyncMock,
        mock_transport: MagicMock,
    ) -> None:
        """Transport always raises -> collector escalates."""
        mock_transport.return_value.__aenter__ = AsyncMock(
            side_effect=OpenMHzTransportError("refused")
        )
        mock_transport.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_sleep.return_value = False

        shutdown = asyncio.Event()
        with self.assertRaises(FeedFailure) as ctx:
            async for _ in openmhz_collector(
                _TEST_FEED,
                shutdown,
                "https://api.openmhz.com/",
                _default_resources(),
            ):
                pass
        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertIn(
            "OpenMHz transport reconnect exhausted", str(ctx.exception)
        )
        self.assertIn("refused", str(ctx.exception))

        # The Nth failure raises before sleeping, so N-1 sleeps
        self.assertEqual(mock_sleep.call_count, MAX_RECONNECT_FAILURES - 1)

    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}.control_flow.sleep_or_cancel", new_callable=AsyncMock)
    async def test_websocket_upgrade_403_raises_auth_diagnostic(
        self,
        mock_sleep: AsyncMock,
        mock_transport: MagicMock,
    ) -> None:
        """Repeated WebSocket upgrade 403s keep provider diagnostic detail."""
        upgrade_message = (
            "Failed to perform, curl: (22) Refused WebSockets upgrade: "
            "403. See https://curl.se/libcurl/c/libcurl-errors.html "
            "token=secret-value"
        )
        upgrade_error = OpenMHzTransportError("openmhz_transport_error")
        upgrade_error.__cause__ = ConnectionError(upgrade_message)

        mock_transport.return_value.__aenter__ = AsyncMock(
            side_effect=upgrade_error
        )
        mock_transport.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_sleep.return_value = None

        shutdown = asyncio.Event()
        with self.assertRaises(FeedFailure) as ctx:
            async for _ in openmhz_collector(
                _TEST_FEED,
                shutdown,
                "https://api.openmhz.com/",
                _default_resources(),
            ):
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
        )
        self.assertIn(
            "WebSocket upgrade failed with HTTP 403", str(ctx.exception)
        )
        self.assertIn("Refused WebSockets upgrade: 403", str(ctx.exception))
        self.assertIn("token=secret-value", str(ctx.exception))

    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}.control_flow.sleep_or_cancel", new_callable=AsyncMock)
    async def test_raises_after_max_post_handshake_transport_failures(
        self,
        mock_sleep: AsyncMock,
        mock_transport: MagicMock,
    ) -> None:
        @contextlib.asynccontextmanager
        async def _failing_after_enter():
            async def _events():
                msg = "frame failure"
                raise OpenMHzTransportError(msg)
                yield  # pragma: no cover

            yield _events()

        mock_transport.side_effect = lambda *a, **kw: _failing_after_enter()
        mock_sleep.return_value = False

        shutdown = asyncio.Event()
        with self.assertRaises(FeedFailure) as ctx:
            async for _ in openmhz_collector(
                _TEST_FEED,
                shutdown,
                "https://api.openmhz.com/",
                _default_resources(),
            ):
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertIn(
            "OpenMHz transport reconnect exhausted", str(ctx.exception)
        )
        self.assertIn("frame failure", str(ctx.exception))
        self.assertEqual(mock_transport.call_count, MAX_RECONNECT_FAILURES)
        self.assertEqual(mock_sleep.call_count, MAX_RECONNECT_FAILURES - 1)

    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}._download_m4a")
    async def test_unexpected_collector_bug_propagates(
        self,
        mock_download: AsyncMock,
        mock_transport: MagicMock,
    ) -> None:
        call = _make_call(call_id="bug", length_sec=5)
        mock_transport.side_effect = lambda *a, **kw: _mock_transport([call])
        mock_download.side_effect = RuntimeError("collector bug")

        with self.assertRaisesRegex(RuntimeError, "collector bug"):
            async for _ in openmhz_collector(
                _TEST_FEED,
                asyncio.Event(),
                "https://api.openmhz.com/",
                _default_resources(),
            ):
                pass

    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}._download_m4a")
    async def test_item_download_failure_threshold_raises_typed_failure(
        self,
        mock_download: AsyncMock,
        mock_transport: MagicMock,
    ) -> None:
        calls = [
            _make_call(call_id=f"bad-{i}")
            for i in range(MAX_ITEM_DOWNLOAD_FAILURES)
        ]
        mock_transport.side_effect = lambda *a, **kw: _mock_transport(calls)
        mock_download.return_value = ItemFailure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "item_http_503",
        )

        shutdown = asyncio.Event()
        with self.assertRaises(FeedFailure) as ctx:
            async for _ in openmhz_collector(
                _TEST_FEED,
                shutdown,
                "https://api.openmhz.com/",
                _default_resources(),
            ):
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(str(ctx.exception), "item_http_503")
        self.assertEqual(mock_download.call_count, MAX_ITEM_DOWNLOAD_FAILURES)

    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}._download_m4a")
    async def test_item_download_failure_threshold_mixed_failures_promotes_collector_error(
        self,
        mock_download: AsyncMock,
        mock_transport: MagicMock,
    ) -> None:
        calls = [
            _make_call(call_id=f"mixed-bad-{i}")
            for i in range(MAX_ITEM_DOWNLOAD_FAILURES)
        ]
        mock_transport.side_effect = lambda *a, **kw: _mock_transport(calls)
        failures = [
            ItemFailure(
                FeedStatusReason.SOURCE_UNREACHABLE,
                "item_download_failed",
            ),
            ItemFailure(
                FeedStatusReason.SOURCE_RATE_LIMITED,
                "item_http_429",
            ),
        ]
        mock_download.side_effect = [
            failures[i % len(failures)]
            for i in range(MAX_ITEM_DOWNLOAD_FAILURES)
        ]

        shutdown = asyncio.Event()
        with self.assertRaises(FeedFailure) as ctx:
            async for _ in openmhz_collector(
                _TEST_FEED,
                shutdown,
                "https://api.openmhz.com/",
                _default_resources(),
            ):
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(str(ctx.exception), "mixed_item_failures")
        self.assertEqual(mock_download.await_count, MAX_ITEM_DOWNLOAD_FAILURES)

    @patch(f"{_COL_MOD}.control_flow.sleep_or_cancel", new_callable=AsyncMock)
    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}._download_m4a")
    async def test_successful_chunk_resets_item_failure_window(
        self,
        mock_download: AsyncMock,
        mock_transport: MagicMock,
        mock_sleep: AsyncMock,
    ) -> None:
        first_failures = MAX_ITEM_DOWNLOAD_FAILURES - 1
        second_failures = MAX_ITEM_DOWNLOAD_FAILURES - 1
        calls = [
            _make_call(call_id=f"failed-before-{i}")
            for i in range(first_failures)
        ]
        calls.append(_make_call(call_id="success"))
        calls.extend(
            _make_call(call_id=f"failed-after-{i}")
            for i in range(second_failures)
        )
        mock_transport.side_effect = lambda *a, **kw: _mock_transport(calls)
        item_failure = ItemFailure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "item_download_failed",
        )
        mock_download.side_effect = [
            *([item_failure] * first_failures),
            b"m4a",
            *([item_failure] * second_failures),
        ]
        shutdown = asyncio.Event()

        async def _stop_after_transport_exhaustion(*_args: object) -> None:
            shutdown.set()

        mock_sleep.side_effect = _stop_after_transport_exhaustion

        chunks = []
        async for chunk in openmhz_collector(
            _TEST_FEED,
            shutdown,
            "https://api.openmhz.com/",
            _default_resources(),
        ):
            chunks.append(_require_captured_chunk(chunk))

        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0].audio_bytes, b"m4a")
        self.assertEqual(
            mock_download.await_count,
            2 * MAX_ITEM_DOWNLOAD_FAILURES - 1,
        )

    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}._download_m4a")
    async def test_session_id_consistent_within_connection(
        self,
        mock_download: AsyncMock,
        mock_transport: MagicMock,
    ) -> None:
        """All chunks within one connection share the same session_id."""
        calls = [_make_call(call_id="c1"), _make_call(call_id="c2")]
        mock_transport.side_effect = lambda *a, **kw: _mock_transport(calls)
        mock_download.return_value = b"m4a"

        shutdown = asyncio.Event()
        results = []
        async for chunk in openmhz_collector(
            _TEST_FEED,
            shutdown,
            "https://api.openmhz.com/",
            _default_resources(),
        ):
            results.append(_require_captured_chunk(chunk))
            if len(results) == 2:
                shutdown.set()

        self.assertEqual(len(results), 2)
        self.assertIsNotNone(results[0].session_id)
        self.assertTrue(results[0].session_id)
        self.assertEqual(results[0].session_id, results[1].session_id)

    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}._download_m4a")
    @patch(f"{_COL_MOD}.control_flow.sleep_or_cancel", new_callable=AsyncMock)
    async def test_session_id_changes_on_reconnect(
        self,
        mock_sleep: AsyncMock,
        mock_download: AsyncMock,
        mock_transport: MagicMock,
    ) -> None:
        """After reconnection, chunks get a different session_id."""
        call = _make_call()
        call_count = 0

        def _transport_factory(*a, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First connection: yield one call then raise
                @contextlib.asynccontextmanager
                async def _failing():
                    async def _events():
                        yield call
                        msg = "transport error"
                        raise OpenMHzTransportError(msg)

                    yield _events()

                return _failing()
            # Second connection: yield one call
            return _mock_transport([call])

        mock_transport.side_effect = _transport_factory
        mock_download.return_value = b"m4a"
        mock_sleep.return_value = False

        shutdown = asyncio.Event()
        results = []
        async for chunk in openmhz_collector(
            _TEST_FEED,
            shutdown,
            "https://api.openmhz.com/",
            _default_resources(),
        ):
            results.append(_require_captured_chunk(chunk))
            if len(results) == 2:
                shutdown.set()

        self.assertEqual(len(results), 2)
        self.assertNotEqual(results[0].session_id, results[1].session_id)


class TestOpenmhzReceiptTimeStamp(unittest.IsolatedAsyncioTestCase):
    """RCPT-03: OpenMHZ stamps receipt_time at WS event arrival."""

    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}._download_m4a")
    @patch(f"{_COL_MOD}.datetime")
    async def test_stamps_receipt_time_at_event_arrival(
        self,
        mock_datetime: MagicMock,
        mock_download: AsyncMock,
        mock_transport: MagicMock,
    ) -> None:
        fixed_time = datetime.datetime(
            2026, 4, 22, 12, 0, 0, tzinfo=datetime.UTC
        )
        # Let timedelta / UTC pass through to the real module.
        mock_datetime.datetime.now.return_value = fixed_time
        mock_datetime.UTC = datetime.UTC
        mock_datetime.timedelta = datetime.timedelta

        call = _make_call(call_id="c1", length_sec=5)
        mock_transport.side_effect = lambda *a, **kw: _mock_transport([call])
        mock_download.return_value = b"m4a"

        shutdown = asyncio.Event()
        results = []
        async for chunk in openmhz_collector(
            _TEST_FEED,
            shutdown,
            "https://api.openmhz.com/",
            _default_resources(),
        ):
            results.append(_require_captured_chunk(chunk))
            shutdown.set()

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].receipt_time, fixed_time)


class TestOpenmhzCallDownloadFailedEmit(unittest.IsolatedAsyncioTestCase):
    """LOG-02: OpenMHZ emits call_download_failed at _download_m4a caller."""

    @patch(f"{_COL_MOD}.control_flow.sleep_or_cancel", new_callable=AsyncMock)
    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}._download_m4a")
    async def test_emits_call_download_failed_on_terminal_failure(
        self,
        mock_download: AsyncMock,
        mock_transport: MagicMock,
        mock_sleep: AsyncMock,
    ) -> None:
        call = _make_call(call_id="terminal-fail", length_sec=5)
        mock_transport.side_effect = lambda *a, **kw: _mock_transport([call])
        shutdown = asyncio.Event()

        async def _stop_after_reconnect_sleep(*_args: object) -> None:
            shutdown.set()

        mock_download.return_value = ItemFailure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "item_download_failed",
        )
        mock_sleep.side_effect = _stop_after_reconnect_sleep

        with self.assertLogs(
            "backend.pipeline.ingestion.collectors.openmhz.collector",
            level="WARNING",
        ) as cm:
            async for _ in openmhz_collector(
                _TEST_FEED,
                shutdown,
                "https://api.openmhz.com/",
                _default_resources(),
            ):
                pass  # no yield because download failed

        emits = [
            r for r in cm.records if r.getMessage() == "Call download failed"
        ]
        self.assertEqual(len(emits), 1)
        rec = cast("Any", emits[0])
        self.assertEqual(rec.json_fields["event_type"], "call_download_failed")
        self.assertEqual(rec.json_fields["feed_id"], str(_TEST_FEED["id"]))
        self.assertEqual(
            rec.json_fields["source_type"],
            _TEST_FEED["source_type"],
        )
        # Golden match
        import json  # noqa: PLC0415
        import pathlib  # noqa: PLC0415

        golden = json.loads(
            (
                pathlib.Path(__file__).resolve().parents[2]  # noqa: ASYNC240 -- sync file read in test is fine
                / "tests"
                / "golden"
                / "call_download_failed.json"
            ).read_text()
        )
        self.assertEqual(
            set(rec.json_fields.keys()),
            set(golden["expected_keys"]),
        )

    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}._download_m4a")
    async def test_no_emit_on_successful_download(
        self,
        mock_download: AsyncMock,
        mock_transport: MagicMock,
    ) -> None:
        call = _make_call(call_id="ok", length_sec=5)
        mock_transport.side_effect = lambda *a, **kw: _mock_transport([call])
        mock_download.return_value = b"fake-m4a-bytes"

        shutdown = asyncio.Event()
        with self.assertLogs(
            "backend.pipeline.ingestion.collectors.openmhz.collector",
            level="WARNING",
        ) as cm:
            async for _ in openmhz_collector(
                _TEST_FEED,
                shutdown,
                "https://api.openmhz.com/",
                _default_resources(),
            ):
                shutdown.set()
            # Placeholder emit so assertLogs doesn't raise on zero-record capture.
            from backend.pipeline.ingestion.collectors.openmhz import (  # noqa: PLC0415
                collector as _oc,
            )

            _oc.logger.warning("_test_placeholder_")

        emits = [
            r for r in cm.records if r.getMessage() == "Call download failed"
        ]
        self.assertEqual(emits, [])

    @patch(f"{_COL_MOD}.telemetry.emit_call_download_failed")
    @patch(f"{_COL_MOD}._download_m4a")
    @patch(f"{_COL_MOD}.websocket_transport")
    async def test_no_emit_during_shutdown(
        self,
        mock_transport: MagicMock,
        mock_download: AsyncMock,
        mock_emit: MagicMock,
    ) -> None:
        call = _make_call(call_id="shut-failing", length_sec=5)
        mock_transport.side_effect = lambda *a, **kw: _mock_transport([call])

        shutdown = asyncio.Event()

        async def _fail_after_shutdown(*_args: object) -> ItemFailure:
            shutdown.set()
            return ItemFailure(
                FeedStatusReason.SOURCE_UNREACHABLE,
                "item_download_failed",
            )

        mock_download.side_effect = _fail_after_shutdown

        async for _ in openmhz_collector(
            _TEST_FEED,
            shutdown,
            "https://api.openmhz.com/",
            _default_resources(),
        ):
            pass

        mock_emit.assert_not_called()

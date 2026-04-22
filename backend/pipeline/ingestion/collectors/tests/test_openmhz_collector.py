from __future__ import annotations

import asyncio
import contextlib
import datetime
import unittest
import uuid
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

from backend.pipeline.ingestion.collectors.openmhz._types import CallEvent
from backend.pipeline.ingestion.collectors.openmhz.collector import (
    MAX_RECONNECT_FAILURES,
    openmhz_collector,
)
from backend.pipeline.storage.feed_store import LeasedFeed, SourceType

_TEST_FEED = LeasedFeed(
    id=uuid.UUID("12345678-1234-5678-1234-567812345678"),
    name="test-openmhz-wmata",
    source_type=SourceType.OPENMHZ,
    last_processed_filename=None,
    last_bookmark_time=None,
    fencing_token=1,
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


class TestOpenmhzCollector(unittest.IsolatedAsyncioTestCase):
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
            _TEST_FEED, shutdown, "https://api.openmhz.com/"
        ):
            results.append(chunk)
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
            _TEST_FEED, shutdown, "https://api.openmhz.com/"
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
        mock_download.side_effect = [None, b"m4a"]

        shutdown = asyncio.Event()
        results = []
        async for chunk in openmhz_collector(
            _TEST_FEED, shutdown, "https://api.openmhz.com/"
        ):
            results.append(chunk)
            shutdown.set()

        self.assertEqual(len(results), 1)
        self.assertEqual(mock_download.call_count, 2)

    async def test_raises_value_error_for_missing_source_feed_id(
        self,
    ) -> None:
        feed = LeasedFeed(
            id=uuid.uuid4(),
            name="no-id",
            source_type=SourceType.OPENMHZ,
            last_processed_filename=None,
            last_bookmark_time=None,
            fencing_token=1,
            source_feed_id=None,
        )
        shutdown = asyncio.Event()
        with self.assertRaises(ValueError, msg="missing source_feed_id"):
            async for _ in openmhz_collector(
                feed, shutdown, "https://api.openmhz.com/"
            ):
                pass

    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}._sleep_or_shutdown", new_callable=AsyncMock)
    async def test_raises_after_max_reconnect_failures(
        self,
        mock_sleep: AsyncMock,
        mock_transport: MagicMock,
    ) -> None:
        """Transport always raises -> collector escalates."""
        mock_transport.return_value.__aenter__ = AsyncMock(
            side_effect=ConnectionError("refused")
        )
        mock_transport.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_sleep.return_value = False

        shutdown = asyncio.Event()
        with self.assertRaises(RuntimeError, msg="consecutively"):
            async for _ in openmhz_collector(
                _TEST_FEED, shutdown, "https://api.openmhz.com/"
            ):
                pass

        # The Nth failure raises before sleeping, so N-1 sleeps
        self.assertEqual(mock_sleep.call_count, MAX_RECONNECT_FAILURES - 1)

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
            _TEST_FEED, shutdown, "https://api.openmhz.com/"
        ):
            results.append(chunk)
            if len(results) == 2:
                shutdown.set()

        self.assertEqual(len(results), 2)
        self.assertIsNotNone(results[0].session_id)
        self.assertTrue(len(results[0].session_id) > 0)
        self.assertEqual(results[0].session_id, results[1].session_id)

    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}._download_m4a")
    @patch(f"{_COL_MOD}._sleep_or_shutdown", new_callable=AsyncMock)
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
                        raise ConnectionError(msg)

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
            _TEST_FEED, shutdown, "https://api.openmhz.com/"
        ):
            results.append(chunk)
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
            _TEST_FEED, shutdown, "https://api.openmhz.com/"
        ):
            results.append(chunk)
            shutdown.set()

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].receipt_time, fixed_time)


class TestOpenmhzCallDownloadFailedEmit(unittest.IsolatedAsyncioTestCase):
    """LOG-02: OpenMHZ emits call_download_failed at _download_m4a caller."""

    @patch(f"{_COL_MOD}._sleep_or_shutdown", new_callable=AsyncMock)
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
        mock_download.return_value = None  # terminal failure
        # After transport events exhaust, collector reconnects (while loop);
        # returning True from _sleep_or_shutdown signals shutdown, ending the loop.
        mock_sleep.return_value = True

        shutdown = asyncio.Event()
        with self.assertLogs(
            "backend.pipeline.ingestion.collectors.openmhz.collector",
            level="WARNING",
        ) as cm:
            async for _ in openmhz_collector(
                _TEST_FEED, shutdown, "https://api.openmhz.com/"
            ):
                pass  # no yield because download failed

        emits = [
            r for r in cm.records if r.getMessage() == "Call download failed"
        ]
        self.assertEqual(len(emits), 1)
        rec = emits[0]
        self.assertEqual(
            rec.json_fields["event_type"], "call_download_failed"
        )
        self.assertEqual(rec.json_fields["feed_id"], str(_TEST_FEED["id"]))
        self.assertEqual(
            rec.json_fields["source_type"], _TEST_FEED["source_type"]
        )
        # Golden match
        import json  # noqa: PLC0415
        import pathlib  # noqa: PLC0415

        golden = json.loads(
            (
                pathlib.Path(__file__).resolve().parents[2]
                / "tests"
                / "golden"
                / "call_download_failed.json"
            ).read_text()
        )
        self.assertEqual(
            set(rec.json_fields.keys()), set(golden["expected_keys"])
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
                _TEST_FEED, shutdown, "https://api.openmhz.com/"
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

    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}._download_m4a")
    async def test_no_emit_during_shutdown(
        self,
        mock_download: AsyncMock,
        mock_transport: MagicMock,
    ) -> None:
        call = _make_call(call_id="shut-failing", length_sec=5)
        mock_transport.side_effect = lambda *a, **kw: _mock_transport([call])

        shutdown = asyncio.Event()

        async def _download_then_shut(*args, **kwargs):
            # Simulate: shutdown gets set DURING the download, download returns None
            shutdown.set()
            return None

        mock_download.side_effect = _download_then_shut

        with self.assertLogs(
            "backend.pipeline.ingestion.collectors.openmhz.collector",
            level="WARNING",
        ) as cm:
            async for _ in openmhz_collector(
                _TEST_FEED, shutdown, "https://api.openmhz.com/"
            ):
                pass
            # Placeholder emit so assertLogs doesn't raise on zero-record capture.
            from backend.pipeline.ingestion.collectors.openmhz import (  # noqa: PLC0415
                collector as _oc,
            )

            _oc.logger.warning("_test_placeholder_")

        # Download returned None AND shutdown.is_set() is True -> no emit.
        emits = [
            r for r in cm.records if r.getMessage() == "Call download failed"
        ]
        self.assertEqual(emits, [])

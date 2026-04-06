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
from backend.pipeline.storage.feed_store import LeasedFeed

_TEST_FEED = LeasedFeed(
    id=uuid.UUID("12345678-1234-5678-1234-567812345678"),
    name="test-openmhz-wmata",
    source_type="openmhz",
    last_processed_filename=None,
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
    @patch(f"{_COL_MOD}.convert_to_flac")
    async def test_yields_flac_and_call_time(
        self,
        mock_convert: MagicMock,
        mock_download: AsyncMock,
        mock_transport: MagicMock,
    ) -> None:
        call = _make_call(call_id="c1", length_sec=5)
        mock_transport.side_effect = lambda *a, **kw: _mock_transport(
            [call]
        )
        mock_download.return_value = b"fake-m4a-bytes"
        mock_convert.return_value = b"fake-flac-bytes"

        shutdown = asyncio.Event()
        results = []
        async for flac, ts in openmhz_collector(
            _TEST_FEED, shutdown, "https://api.openmhz.com/"
        ):
            results.append((flac, ts))
            shutdown.set()

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][0], b"fake-flac-bytes")
        self.assertEqual(results[0][1], call.time)
        mock_convert.assert_called_once_with(b"fake-m4a-bytes", "m4a")

    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}._download_m4a")
    @patch(f"{_COL_MOD}.convert_to_flac")
    async def test_skips_zero_length_calls(
        self,
        mock_convert: MagicMock,
        mock_download: AsyncMock,
        mock_transport: MagicMock,
    ) -> None:
        calls = [
            _make_call(call_id="zero", length_sec=0),
            _make_call(call_id="normal", length_sec=5),
        ]
        mock_transport.side_effect = lambda *a, **kw: _mock_transport(
            calls
        )
        mock_download.return_value = b"m4a"
        mock_convert.return_value = b"flac"

        shutdown = asyncio.Event()
        results = []
        async for flac, ts in openmhz_collector(
            _TEST_FEED, shutdown, "https://api.openmhz.com/"
        ):
            results.append(flac)
            shutdown.set()

        self.assertEqual(len(results), 1)
        mock_download.assert_called_once()

    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}._download_m4a")
    @patch(f"{_COL_MOD}.convert_to_flac")
    async def test_skips_failed_download(
        self,
        mock_convert: MagicMock,
        mock_download: AsyncMock,
        mock_transport: MagicMock,
    ) -> None:
        calls = [
            _make_call(call_id="bad-url"),
            _make_call(call_id="good"),
        ]
        mock_transport.side_effect = lambda *a, **kw: _mock_transport(
            calls
        )
        mock_download.side_effect = [None, b"m4a"]
        mock_convert.return_value = b"flac"

        shutdown = asyncio.Event()
        results = []
        async for flac, ts in openmhz_collector(
            _TEST_FEED, shutdown, "https://api.openmhz.com/"
        ):
            results.append(flac)
            shutdown.set()

        self.assertEqual(len(results), 1)
        self.assertEqual(mock_download.call_count, 2)
        mock_convert.assert_called_once()

    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}._download_m4a")
    @patch(f"{_COL_MOD}.convert_to_flac")
    @patch(f"{_COL_MOD}._sleep_or_shutdown", new_callable=AsyncMock)
    async def test_skips_failed_flac_conversion(
        self,
        mock_sleep: AsyncMock,
        mock_convert: MagicMock,
        mock_download: AsyncMock,
        mock_transport: MagicMock,
    ) -> None:
        call = _make_call(call_id="corrupt")
        mock_transport.side_effect = lambda *a, **kw: _mock_transport(
            [call]
        )
        mock_download.return_value = b"corrupt-m4a"
        mock_convert.side_effect = Exception("pydub decode error")
        mock_sleep.return_value = True  # exit on first reconnect attempt

        shutdown = asyncio.Event()
        results = []
        async for flac, ts in openmhz_collector(
            _TEST_FEED, shutdown, "https://api.openmhz.com/"
        ):
            results.append(flac)

        self.assertEqual(len(results), 0)

    async def test_raises_value_error_for_missing_source_feed_id(
        self,
    ) -> None:
        feed = LeasedFeed(
            id=uuid.uuid4(),
            name="no-id",
            source_type="openmhz",
            last_processed_filename=None,
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
        mock_transport.return_value.__aexit__ = AsyncMock(
            return_value=False
        )
        mock_sleep.return_value = False

        shutdown = asyncio.Event()
        with self.assertRaises(RuntimeError, msg="consecutively"):
            async for _ in openmhz_collector(
                _TEST_FEED, shutdown, "https://api.openmhz.com/"
            ):
                pass

        # The Nth failure raises before sleeping, so N-1 sleeps
        self.assertEqual(
            mock_sleep.call_count, MAX_RECONNECT_FAILURES - 1
        )

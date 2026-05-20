from __future__ import annotations

import asyncio
import collections
import datetime
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from backend.pipeline.ingestion.collectors.fire_notifications import collector
from backend.pipeline.storage.feed_store import SourceType


class TestParseFilenameTimestamp(unittest.TestCase):
    def test_valid_filename(self) -> None:
        filename = "CHANNEL 2026-05-20 12-00-00.mp3"
        dt = collector._parse_filename_timestamp(filename, "CHANNEL")
        self.assertEqual(dt.year, 2026)
        self.assertEqual(dt.month, 5)
        self.assertEqual(dt.day, 20)
        self.assertEqual(dt.hour, 12)
        self.assertEqual(dt.minute, 0)
        self.assertEqual(dt.second, 0)
        self.assertEqual(dt.tzinfo, datetime.UTC)

    def test_filename_with_spaces_in_channel(self) -> None:
        filename = "SAN JOSE DISP 2026-05-20 12-00-00.mp3"
        dt = collector._parse_filename_timestamp(filename, "SAN JOSE DISP")
        self.assertEqual(dt.year, 2026)
        self.assertEqual(dt.month, 5)
        self.assertEqual(dt.day, 20)

    def test_invalid_filename(self) -> None:
        filename = "invalid_filename.mp3"
        with self.assertRaises(ValueError):
            collector._parse_filename_timestamp(filename, "CHANNEL")


class TestDownloadAudio(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.session = MagicMock()
        self.shutdown = asyncio.Event()

    async def test_success(self) -> None:
        resp = MagicMock(status_code=200, content=b"audio_data")
        self.session.get = AsyncMock(return_value=resp)

        data = await collector._download_audio(
            self.session, "http://url", self.shutdown
        )
        self.assertEqual(data, b"audio_data")

    async def test_non_retryable_4xx(self) -> None:
        resp = MagicMock(status_code=404)
        self.session.get = AsyncMock(return_value=resp)

        data = await collector._download_audio(
            self.session, "http://url", self.shutdown
        )
        self.assertIsNone(data)

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_5xx_retry_success(self, mock_sleep: AsyncMock) -> None:
        mock_sleep.return_value = False
        resp500 = MagicMock(status_code=500)
        resp200 = MagicMock(status_code=200, content=b"data")

        self.session.get = AsyncMock(side_effect=[resp500, resp200])

        data = await collector._download_audio(
            self.session, "http://url", self.shutdown
        )
        self.assertEqual(data, b"data")
        self.assertEqual(self.session.get.call_count, 2)

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_5xx_max_retries_fail(self, mock_sleep: MagicMock) -> None:
        mock_sleep.return_value = False
        resp500 = MagicMock(status_code=500)
        self.session.get = AsyncMock(return_value=resp500)

        data = await collector._download_audio(
            self.session, "http://url", self.shutdown
        )
        self.assertIsNone(data)
        self.assertEqual(
            self.session.get.call_count, collector._DOWNLOAD_MAX_RETRIES
        )


class TestProcessFileList(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.session = MagicMock()
        self.shutdown = asyncio.Event()
        self.feed = {
            "id": "feed-id",
            "source_type": SourceType.FIRE_NOTIFICATIONS,
        }
        self.processed_uuids = collections.deque(maxlen=1000)

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._download_audio",
        new_callable=AsyncMock,
    )
    async def test_process_files(self, mock_download: AsyncMock) -> None:
        mock_download.return_value = b"mp3_bytes"
        files = [
            {
                "type": "file",
                "name": "CHAN 2026-05-20 12-00-00.mp3",
                "uuid": "uuid1",
                "size": 1000,
            },
            {
                "type": "file",
                "name": "CHAN 2026-05-20 12-00-01.mp3",
                "uuid": "uuid2",
                "size": 1000,
            },
            {"type": "dir", "name": "some_dir"},
            {
                "type": "file",
                "name": "not_mp3.txt",
                "uuid": "uuid3",
                "size": 1000,
            },
        ]

        chunks = []
        async for chunk in collector._process_file_list(
            files,
            self.session,
            self.shutdown,
            "session-id",
            self.feed,  # type: ignore
            self.processed_uuids,
            "CHAN",
        ):
            chunks.append(chunk)

        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0].session_id, "session-id")
        self.assertEqual(chunks[0].audio_bytes, b"mp3_bytes")
        self.assertEqual(len(self.processed_uuids), 2)
        self.assertIn("uuid1", self.processed_uuids)
        self.assertIn("uuid2", self.processed_uuids)


if __name__ == "__main__":
    unittest.main()

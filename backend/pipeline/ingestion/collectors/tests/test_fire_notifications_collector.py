from __future__ import annotations

import asyncio
import collections
import datetime
import os
import unittest
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

from backend.pipeline.ingestion.collectors.batch_outcome import BatchOutcome
from backend.pipeline.ingestion.collectors.fire_notifications import collector
from backend.pipeline.ingestion.models import AudioMimeType
from backend.pipeline.ingestion.slo_contract import (
    EVENT_TYPE_BATCH_UNPRODUCTIVE,
)
from backend.pipeline.storage.feed_store import SourceType

if TYPE_CHECKING:
    import logging


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
        self.feed: dict[str, Any] = {
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
        with patch(
            "backend.pipeline.ingestion.collectors.fire_notifications.collector.get_audio_duration",
            return_value=30000,
        ) as mock_duration:
            async for chunk in collector._process_file_list(
                files,
                self.session,
                self.shutdown,
                "session-id",
                self.feed,  # type: ignore
                self.processed_uuids,
                "CHAN",
                "http://mock-s3-bucket",
            ):
                chunks.append(chunk)

        self.assertEqual(len(chunks), 2)
        self.assertEqual(mock_duration.call_count, 2)
        self.assertEqual(chunks[0].session_id, "session-id")
        self.assertEqual(chunks[0].audio_bytes, b"mp3_bytes")
        self.assertEqual(chunks[0].mime_type, AudioMimeType.MPEG)
        self.assertEqual(chunks[0].resume_position, chunks[0].chunk_end_time)
        self.assertEqual(len(self.processed_uuids), 2)
        self.assertIn("uuid1", self.processed_uuids)
        self.assertIn("uuid2", self.processed_uuids)

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._download_audio",
        new_callable=AsyncMock,
    )
    async def test_process_files_with_last_bookmark_time(
        self, mock_download: AsyncMock
    ) -> None:
        mock_download.return_value = b"mp3_bytes"
        self.feed["last_bookmark_time"] = datetime.datetime(
            2026, 5, 20, 12, 0, 0, tzinfo=datetime.UTC
        )
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
        ]

        chunks = []
        with patch(
            "backend.pipeline.ingestion.collectors.fire_notifications.collector.get_audio_duration",
            return_value=30000,
        ) as mock_duration:
            async for chunk in collector._process_file_list(
                files,
                self.session,
                self.shutdown,
                "session-id",
                self.feed,  # type: ignore
                self.processed_uuids,
                "CHAN",
                "http://mock-s3-bucket",
            ):
                chunks.append(chunk)

        self.assertEqual(len(chunks), 1)
        self.assertEqual(mock_duration.call_count, 1)
        self.assertEqual(
            chunks[0].chunk_start_time,
            datetime.datetime(2026, 5, 20, 12, 0, 1, tzinfo=datetime.UTC),
        )
        self.assertEqual(chunks[0].resume_position, chunks[0].chunk_end_time)

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._download_audio",
        new_callable=AsyncMock,
    )
    async def test_failed_download_does_not_mark_uuid_seen(
        self, mock_download: AsyncMock
    ) -> None:
        mock_download.return_value = None
        files = [
            {
                "type": "file",
                "name": "CHAN 2026-05-20 12-00-00.mp3",
                "uuid": "uuid1",
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
            "http://mock-s3-bucket",
        ):
            chunks.append(chunk)

        self.assertEqual(len(chunks), 0)
        self.assertEqual(len(self.processed_uuids), 0)
        self.assertNotIn("uuid1", self.processed_uuids)

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._download_audio",
        new_callable=AsyncMock,
    )
    async def test_failed_download_then_success_continues_and_tracks_outcome(
        self, mock_download: AsyncMock
    ) -> None:
        mock_download.side_effect = [None, b"mp3_bytes"]
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
        ]
        outcome = BatchOutcome()

        chunks = []
        with patch(
            "backend.pipeline.ingestion.collectors.fire_notifications.collector.get_audio_duration",
            return_value=30000,
        ):
            async for chunk in collector._process_file_list(
                files,
                self.session,
                self.shutdown,
                "session-id",
                self.feed,  # type: ignore
                self.processed_uuids,
                "CHAN",
                "http://mock-s3-bucket",
                outcome,
            ):
                chunks.append(chunk)

        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0].audio_bytes, b"mp3_bytes")
        self.assertEqual(outcome.attempted, 2)
        self.assertEqual(outcome.produced, 1)
        self.assertNotIn("uuid1", self.processed_uuids)
        self.assertIn("uuid2", self.processed_uuids)


@patch.dict(
    os.environ,
    {"FIRE_NOTIFICATIONS_S3_BASE": "http://mock-s3-bucket"},
)
class TestFireNotificationsCollector(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.shutdown = asyncio.Event()
        self.feed: dict[str, Any] = {
            "id": "feed-id",
            "source_type": SourceType.FIRE_NOTIFICATIONS,
            "source_feed_id": "CHAN",
            "name": "CHAN-feed",
        }
        self.resources = MagicMock()

    @staticmethod
    def _audio_file(file_uuid: str, second: int = 0) -> dict[str, Any]:
        return {
            "type": "file",
            "name": f"CHAN 2026-05-20 12-00-{second:02d}.mp3",
            "uuid": file_uuid,
            "size": 1000,
        }

    @staticmethod
    def _ok_response(files: list[dict[str, Any]]) -> MagicMock:
        resp = MagicMock(status_code=200)
        resp.json.return_value = {"files": files}
        return resp

    @staticmethod
    def _batch_unproductive_records(
        records: list[logging.LogRecord],
    ) -> list[logging.LogRecord]:
        return [
            r
            for r in records
            if getattr(r, "json_fields", {}).get("event_type")
            == EVENT_TYPE_BATCH_UNPRODUCTIVE
        ]

    def _collector_generator(self):
        return collector.fire_notifications_collector(
            self.feed,  # type: ignore
            self.shutdown,
            "http://base",
            self.resources,
        )

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.AsyncSession",
    )
    async def test_max_consecutive_failures_raises_source_unreachable(
        self, mock_session_cls: MagicMock, mock_sleep: AsyncMock
    ) -> None:
        mock_sleep.return_value = False  # Sleep normally
        mock_session = mock_session_cls.return_value
        mock_session.close = AsyncMock()
        mock_session.get = AsyncMock(
            side_effect=Exception("Connection failure")
        )

        collector_generator = collector.fire_notifications_collector(
            self.feed,  # type: ignore
            self.shutdown,
            "http://base",
            self.resources,
        )

        with self.assertRaises(RuntimeError) as ctx:
            async for _ in collector_generator:
                pass

        self.assertEqual(str(ctx.exception), "source_unreachable")
        self.assertEqual(mock_session.get.call_count, 10)

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.AsyncSession",
    )
    async def test_successful_poll_resets_consecutive_failures(
        self, mock_session_cls: MagicMock, mock_sleep: AsyncMock
    ) -> None:
        mock_sleep.return_value = False

        mock_session = mock_session_cls.return_value
        mock_session.close = AsyncMock()
        resp_fail = Exception("Connection failure")
        resp_ok = MagicMock(status_code=200)
        resp_ok.json.return_value = {"files": []}

        # 9 failures, 1 success, 2 failures, then we trigger shutdown to exit gracefully.
        side_effect = [resp_fail] * 9 + [resp_ok] + [resp_fail] * 2

        async def sleep_side_effect(event, duration):
            if mock_sleep.call_count >= 11:
                self.shutdown.set()
            return False

        mock_sleep.side_effect = sleep_side_effect
        mock_session.get = AsyncMock(side_effect=side_effect)

        collector_generator = collector.fire_notifications_collector(
            self.feed,  # type: ignore
            self.shutdown,
            "http://base",
            self.resources,
        )

        chunks = []
        async for chunk in collector_generator:
            chunks.append(chunk)

        self.assertEqual(len(chunks), 0)
        self.assertEqual(mock_session.get.call_count, 11)

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._download_audio",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.AsyncSession",
    )
    async def test_all_downloads_fail_emits_batch_unproductive_without_raising(
        self,
        mock_session_cls: MagicMock,
        mock_sleep: AsyncMock,
        mock_download: AsyncMock,
    ) -> None:
        mock_download.return_value = None
        mock_sleep.return_value = True
        mock_session = mock_session_cls.return_value
        mock_session.close = AsyncMock()
        resp = MagicMock(status_code=200)
        resp.json.return_value = {
            "files": [
                {
                    "type": "file",
                    "name": "CHAN 2026-05-20 12-00-00.mp3",
                    "uuid": "uuid1",
                    "size": 1000,
                }
            ]
        }
        mock_session.get = AsyncMock(return_value=resp)

        collector_generator = collector.fire_notifications_collector(
            self.feed,  # type: ignore
            self.shutdown,
            "http://base",
            self.resources,
        )

        with self.assertLogs(
            "backend.pipeline.ingestion.collectors.fire_notifications.collector",
            level="WARNING",
        ) as cm:
            chunks = [chunk async for chunk in collector_generator]

        self.assertEqual(chunks, [])
        self.assertEqual(mock_session.get.call_count, 1)
        emits = [
            r
            for r in cm.records
            if getattr(r, "json_fields", {}).get("event_type")
            == EVENT_TYPE_BATCH_UNPRODUCTIVE
        ]
        self.assertEqual(len(emits), 1)
        self.assertEqual(emits[0].json_fields["feed_id"], "feed-id")
        self.assertEqual(
            emits[0].json_fields["source_type"], self.feed["source_type"]
        )
        self.assertEqual(emits[0].json_fields["attempted"], 1)
        self.assertEqual(emits[0].json_fields["produced"], 0)
        self.assertEqual(emits[0].json_fields["reason"], "downloads_failing")

    @patch.object(collector, "_MAX_CONSECUTIVE_FAILURES", 3)
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._download_audio",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.AsyncSession",
    )
    async def test_unproductive_polls_raise_downloads_failing_after_threshold(
        self,
        mock_session_cls: MagicMock,
        mock_sleep: AsyncMock,
        mock_download: AsyncMock,
    ) -> None:
        mock_download.return_value = None
        mock_sleep.return_value = False
        mock_session = mock_session_cls.return_value
        mock_session.close = AsyncMock()
        mock_session.get = AsyncMock(
            return_value=self._ok_response([self._audio_file("uuid1")])
        )

        with self.assertLogs(
            "backend.pipeline.ingestion.collectors.fire_notifications.collector",
            level="WARNING",
        ) as cm:
            with self.assertRaises(RuntimeError) as ctx:
                async for _ in self._collector_generator():
                    pass

        self.assertEqual(str(ctx.exception), "downloads_failing")
        self.assertEqual(mock_session.get.call_count, 3)
        emits = self._batch_unproductive_records(cm.records)
        self.assertEqual(len(emits), 3)
        for emit in emits:
            self.assertEqual(emit.json_fields["reason"], "downloads_failing")
            self.assertEqual(emit.json_fields["attempted"], 1)
            self.assertEqual(emit.json_fields["produced"], 0)

    @patch.object(collector, "_MAX_CONSECUTIVE_FAILURES", 3)
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._download_audio",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.AsyncSession",
    )
    async def test_endpoint_failures_then_unproductive_poll_raises_downloads_failing(
        self,
        mock_session_cls: MagicMock,
        mock_sleep: AsyncMock,
        mock_download: AsyncMock,
    ) -> None:
        mock_download.return_value = None
        mock_sleep.return_value = False
        mock_session = mock_session_cls.return_value
        mock_session.close = AsyncMock()
        resp_ok = self._ok_response([self._audio_file("uuid1")])
        mock_session.get = AsyncMock(
            side_effect=[
                Exception("Connection failure"),
                Exception("Connection failure"),
                resp_ok,
            ]
        )

        with self.assertRaises(RuntimeError) as ctx:
            async for _ in self._collector_generator():
                pass

        self.assertEqual(str(ctx.exception), "downloads_failing")
        self.assertEqual(mock_session.get.call_count, 3)
        self.assertEqual(mock_download.call_count, 1)

    @patch.object(collector, "_MAX_CONSECUTIVE_FAILURES", 3)
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._download_audio",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.AsyncSession",
    )
    async def test_unproductive_polls_then_endpoint_failure_raises_source_unreachable(
        self,
        mock_session_cls: MagicMock,
        mock_sleep: AsyncMock,
        mock_download: AsyncMock,
    ) -> None:
        mock_download.return_value = None
        mock_sleep.return_value = False
        mock_session = mock_session_cls.return_value
        mock_session.close = AsyncMock()
        resp_ok = self._ok_response([self._audio_file("uuid1")])
        mock_session.get = AsyncMock(
            side_effect=[
                resp_ok,
                resp_ok,
                Exception("Connection failure"),
            ]
        )

        with self.assertRaises(RuntimeError) as ctx:
            async for _ in self._collector_generator():
                pass

        self.assertEqual(str(ctx.exception), "source_unreachable")
        self.assertEqual(mock_session.get.call_count, 3)
        self.assertEqual(mock_download.call_count, 2)

    @patch.object(collector, "_MAX_CONSECUTIVE_FAILURES", 3)
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._download_audio",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.AsyncSession",
    )
    async def test_silent_poll_resets_unproductive_streak(
        self,
        mock_session_cls: MagicMock,
        mock_sleep: AsyncMock,
        mock_download: AsyncMock,
    ) -> None:
        mock_download.return_value = None
        mock_session = mock_session_cls.return_value
        mock_session.close = AsyncMock()
        resp_unproductive = self._ok_response([self._audio_file("uuid1")])
        resp_silent = self._ok_response([])
        mock_session.get = AsyncMock(
            side_effect=[
                resp_unproductive,
                resp_unproductive,
                resp_silent,
                resp_unproductive,
                resp_unproductive,
            ]
        )

        async def sleep_side_effect(
            _event: asyncio.Event, _duration: float
        ) -> bool:
            if mock_sleep.call_count >= 5:
                self.shutdown.set()
            return False

        mock_sleep.side_effect = sleep_side_effect

        with self.assertLogs(
            "backend.pipeline.ingestion.collectors.fire_notifications.collector",
            level="WARNING",
        ) as cm:
            chunks = [chunk async for chunk in self._collector_generator()]

        self.assertEqual(chunks, [])
        self.assertEqual(mock_session.get.call_count, 5)
        emits = self._batch_unproductive_records(cm.records)
        self.assertEqual(len(emits), 4)
        for emit in emits:
            self.assertEqual(emit.json_fields["reason"], "downloads_failing")
            self.assertEqual(emit.json_fields["attempted"], 1)
            self.assertEqual(emit.json_fields["produced"], 0)

    @patch.object(collector, "_MAX_CONSECUTIVE_FAILURES", 3)
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.get_audio_duration",
        return_value=30000,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._download_audio",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.AsyncSession",
    )
    async def test_mixed_success_poll_resets_unproductive_streak(
        self,
        mock_session_cls: MagicMock,
        mock_sleep: AsyncMock,
        mock_download: AsyncMock,
        _mock_duration: MagicMock,
    ) -> None:
        mock_download.side_effect = [
            None,
            None,
            None,
            b"mp3_bytes",
            None,
            None,
        ]
        mock_session = mock_session_cls.return_value
        mock_session.close = AsyncMock()
        resp_unproductive = self._ok_response([self._audio_file("uuid1")])
        resp_mixed = self._ok_response(
            [self._audio_file("uuid1"), self._audio_file("uuid2", second=1)]
        )
        mock_session.get = AsyncMock(
            side_effect=[
                resp_unproductive,
                resp_unproductive,
                resp_mixed,
                resp_unproductive,
                resp_unproductive,
            ]
        )

        async def sleep_side_effect(
            _event: asyncio.Event, _duration: float
        ) -> bool:
            if mock_sleep.call_count >= 5:
                self.shutdown.set()
            return False

        mock_sleep.side_effect = sleep_side_effect

        with self.assertLogs(
            "backend.pipeline.ingestion.collectors.fire_notifications.collector",
            level="WARNING",
        ) as cm:
            chunks = [chunk async for chunk in self._collector_generator()]

        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0].audio_bytes, b"mp3_bytes")
        self.assertEqual(mock_session.get.call_count, 5)
        emits = self._batch_unproductive_records(cm.records)
        self.assertEqual(len(emits), 4)
        for emit in emits:
            self.assertEqual(emit.json_fields["reason"], "downloads_failing")
            self.assertEqual(emit.json_fields["attempted"], 1)
            self.assertEqual(emit.json_fields["produced"], 0)

    @patch.object(collector, "_MAX_CONSECUTIVE_FAILURES", 3)
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._download_audio",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.AsyncSession",
    )
    async def test_shutdown_interrupted_unproductive_poll_is_neutral(
        self,
        mock_session_cls: MagicMock,
        mock_sleep: AsyncMock,
        mock_download: AsyncMock,
    ) -> None:
        mock_sleep.return_value = False
        mock_session = mock_session_cls.return_value
        mock_session.close = AsyncMock()
        mock_session.get = AsyncMock(
            return_value=self._ok_response([self._audio_file("uuid1")])
        )
        download_count = 0

        async def download_side_effect(*_args: Any, **_kwargs: Any) -> None:
            nonlocal download_count
            download_count += 1
            if download_count == 3:
                self.shutdown.set()

        mock_download.side_effect = download_side_effect

        with self.assertLogs(
            "backend.pipeline.ingestion.collectors.fire_notifications.collector",
            level="WARNING",
        ) as cm:
            chunks = [chunk async for chunk in self._collector_generator()]

        self.assertEqual(chunks, [])
        self.assertEqual(mock_session.get.call_count, 3)
        emits = self._batch_unproductive_records(cm.records)
        self.assertEqual(len(emits), 2)
        for emit in emits:
            self.assertEqual(emit.json_fields["reason"], "downloads_failing")
            self.assertEqual(emit.json_fields["attempted"], 1)
            self.assertEqual(emit.json_fields["produced"], 0)

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._download_audio",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.AsyncSession",
    )
    async def test_silent_poll_does_not_emit_batch_unproductive(
        self,
        mock_session_cls: MagicMock,
        mock_sleep: AsyncMock,
        mock_download: AsyncMock,
    ) -> None:
        mock_sleep.return_value = True
        mock_session = mock_session_cls.return_value
        mock_session.close = AsyncMock()
        resp = MagicMock(status_code=200)
        resp.json.return_value = {"files": []}
        mock_session.get = AsyncMock(return_value=resp)

        collector_generator = collector.fire_notifications_collector(
            self.feed,  # type: ignore
            self.shutdown,
            "http://base",
            self.resources,
        )

        with self.assertNoLogs(
            "backend.pipeline.ingestion.collectors.fire_notifications.collector",
            level="WARNING",
        ):
            chunks = [chunk async for chunk in collector_generator]

        self.assertEqual(chunks, [])
        mock_download.assert_not_called()

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.get_audio_duration",
        return_value=30000,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._download_audio",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.AsyncSession",
    )
    async def test_mixed_failure_success_poll_yields_chunk_without_batch_unproductive(
        self,
        mock_session_cls: MagicMock,
        mock_sleep: AsyncMock,
        mock_download: AsyncMock,
        _mock_duration: MagicMock,
    ) -> None:
        mock_download.side_effect = [None, b"mp3_bytes"]
        mock_sleep.return_value = True
        mock_session = mock_session_cls.return_value
        mock_session.close = AsyncMock()
        resp = MagicMock(status_code=200)
        resp.json.return_value = {
            "files": [
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
            ]
        }
        mock_session.get = AsyncMock(return_value=resp)

        collector_generator = collector.fire_notifications_collector(
            self.feed,  # type: ignore
            self.shutdown,
            "http://base",
            self.resources,
        )

        with self.assertLogs(
            "backend.pipeline.ingestion.collectors.fire_notifications.collector",
            level="WARNING",
        ) as cm:
            chunks = [chunk async for chunk in collector_generator]

        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0].audio_bytes, b"mp3_bytes")
        emits = [
            r
            for r in cm.records
            if getattr(r, "json_fields", {}).get("event_type")
            == EVENT_TYPE_BATCH_UNPRODUCTIVE
        ]
        self.assertEqual(emits, [])


if __name__ == "__main__":
    unittest.main()

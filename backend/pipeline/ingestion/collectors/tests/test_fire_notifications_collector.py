from __future__ import annotations

import asyncio
import base64
import collections
import datetime
import os
import unittest
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemFailure,
)
from backend.pipeline.ingestion.collectors.fire_notifications import collector
from backend.pipeline.ingestion.models import AudioMimeType, CollectorFailure
from backend.pipeline.storage.feed_store import FeedStatusReason, SourceType


def _require_item_failure(value: ItemFailure | None) -> ItemFailure:
    """Return a typed item failure for tests that intentionally expect one."""
    if value is None:
        msg = "Expected ItemFailure, got None"
        raise AssertionError(msg)
    return value


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

        result = await collector._download_audio(
            self.session, "http://url", self.shutdown
        )
        self.assertEqual(result.audio_bytes, b"audio_data")
        self.assertIsNone(result.failure)

    async def test_non_retryable_4xx(self) -> None:
        resp = MagicMock(status_code=404)
        self.session.get = AsyncMock(return_value=resp)

        result = await collector._download_audio(
            self.session, "http://url", self.shutdown
        )
        self.assertIsNone(result.audio_bytes)
        failure = _require_item_failure(result.failure)
        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(failure.reason, "item_download_failed")

    async def test_auth_status_returns_item_failure(self) -> None:
        resp = MagicMock(status_code=403)
        self.session.get = AsyncMock(return_value=resp)

        result = await collector._download_audio(
            self.session, "http://url", self.shutdown
        )

        self.assertIsNone(result.audio_bytes)
        failure = _require_item_failure(result.failure)
        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
        )
        self.assertEqual(failure.reason, "item_http_403")

    async def test_rate_limit_status_returns_item_failure(self) -> None:
        resp = MagicMock(status_code=429)
        self.session.get = AsyncMock(return_value=resp)

        result = await collector._download_audio(
            self.session, "http://url", self.shutdown
        )

        self.assertIsNone(result.audio_bytes)
        failure = _require_item_failure(result.failure)
        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_RATE_LIMITED,
        )
        self.assertEqual(failure.reason, "item_http_429")

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_5xx_retry_success(self, mock_sleep: AsyncMock) -> None:
        mock_sleep.return_value = False
        resp500 = MagicMock(status_code=500)
        resp200 = MagicMock(status_code=200, content=b"data")

        self.session.get = AsyncMock(side_effect=[resp500, resp200])

        result = await collector._download_audio(
            self.session, "http://url", self.shutdown
        )
        self.assertEqual(result.audio_bytes, b"data")
        self.assertIsNone(result.failure)
        self.assertEqual(self.session.get.call_count, 2)

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_5xx_max_retries_fail(self, mock_sleep: MagicMock) -> None:
        mock_sleep.return_value = False
        resp500 = MagicMock(status_code=500)
        self.session.get = AsyncMock(return_value=resp500)

        result = await collector._download_audio(
            self.session, "http://url", self.shutdown
        )
        self.assertIsNone(result.audio_bytes)
        failure = _require_item_failure(result.failure)
        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(failure.reason, "item_download_failed")
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
    async def test_failed_download_promotes_and_does_not_mark_uuid_seen(
        self, mock_download: AsyncMock
    ) -> None:
        mock_download.return_value = collector._DownloadResult(
            failure=ItemFailure(
                FeedStatusReason.SOURCE_UNREACHABLE,
                "item_download_failed",
            )
        )
        files = [
            {
                "type": "file",
                "name": "CHAN 2026-05-20 12-00-00.mp3",
                "uuid": "uuid1",
                "size": 1000,
            },
        ]

        with self.assertRaises(CollectorFailure) as ctx:
            async for _ in collector._process_file_list(
                files,
                self.session,
                self.shutdown,
                "session-id",
                self.feed,  # type: ignore
                self.processed_uuids,
                "CHAN",
                "http://mock-s3-bucket",
            ):
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(str(ctx.exception), "item_download_failed")
        self.assertEqual(len(self.processed_uuids), 0)
        self.assertNotIn("uuid1", self.processed_uuids)

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._download_audio",
        new_callable=AsyncMock,
    )
    async def test_mixed_file_failures_promote_collector_error(
        self, mock_download: AsyncMock
    ) -> None:
        mock_download.side_effect = [
            collector._DownloadResult(
                failure=ItemFailure(
                    FeedStatusReason.SOURCE_UNREACHABLE,
                    "item_download_failed",
                )
            ),
            collector._DownloadResult(
                failure=ItemFailure(
                    FeedStatusReason.SOURCE_RATE_LIMITED,
                    "item_http_429",
                )
            ),
        ]
        files = [
            {
                "type": "file",
                "name": "CHAN 2026-05-20 12-00-00.mp3",
                "uuid": "uuid1",
            },
            {
                "type": "file",
                "name": "CHAN 2026-05-20 12-00-01.mp3",
                "uuid": "uuid2",
            },
        ]

        with self.assertRaises(CollectorFailure) as ctx:
            async for _ in collector._process_file_list(
                files,
                self.session,
                self.shutdown,
                "session-id",
                self.feed,  # type: ignore
                self.processed_uuids,
                "CHAN",
                "http://mock-s3-bucket",
            ):
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(str(ctx.exception), "mixed_item_failures")
        self.assertEqual(len(self.processed_uuids), 0)

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._download_audio",
        new_callable=AsyncMock,
    )
    async def test_one_file_success_prevents_feed_level_promotion(
        self, mock_download: AsyncMock
    ) -> None:
        mock_download.side_effect = [
            collector._DownloadResult(
                failure=ItemFailure(
                    FeedStatusReason.SOURCE_UNREACHABLE,
                    "item_download_failed",
                )
            ),
            collector._DownloadResult(audio_bytes=b"mp3_bytes"),
        ]
        files = [
            {
                "type": "file",
                "name": "CHAN 2026-05-20 12-00-00.mp3",
                "uuid": "uuid1",
            },
            {
                "type": "file",
                "name": "CHAN 2026-05-20 12-00-01.mp3",
                "uuid": "uuid2",
            },
        ]

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
            ):
                chunks.append(chunk)

        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0].audio_bytes, b"mp3_bytes")
        self.assertEqual(list(self.processed_uuids), ["uuid2"])

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._download_audio",
        new_callable=AsyncMock,
    )
    async def test_all_duration_probe_failures_promote_collector_error(
        self, mock_download: AsyncMock
    ) -> None:
        mock_download.return_value = collector._DownloadResult(
            audio_bytes=b"mp3_bytes"
        )
        files = [
            {
                "type": "file",
                "name": "CHAN 2026-05-20 12-00-00.mp3",
                "uuid": "uuid1",
            },
        ]

        with (
            patch.object(collector, "logger"),
            patch(
                "backend.pipeline.ingestion.collectors.fire_notifications.collector.get_audio_duration",
                side_effect=RuntimeError("ffprobe failed"),
            ),
            self.assertRaises(CollectorFailure) as ctx,
        ):
            async for _ in collector._process_file_list(
                files,
                self.session,
                self.shutdown,
                "session-id",
                self.feed,  # type: ignore
                self.processed_uuids,
                "CHAN",
                "http://mock-s3-bucket",
            ):
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(str(ctx.exception), "duration_probe_failed")
        self.assertEqual(len(self.processed_uuids), 0)

    async def test_no_eligible_files_does_not_raise(self) -> None:
        files = [
            {"type": "dir", "name": "folders"},
            {"type": "file", "name": "not-audio.txt", "uuid": "uuid1"},
        ]

        chunks = [
            c
            async for c in collector._process_file_list(
                files,
                self.session,
                self.shutdown,
                "session-id",
                self.feed,  # type: ignore
                self.processed_uuids,
                "CHAN",
                "http://mock-s3-bucket",
            )
        ]

        self.assertEqual(chunks, [])
        self.assertEqual(len(self.processed_uuids), 0)


@patch.dict(
    os.environ,
    {
        "FIRE_NOTIFICATIONS_S3_BASE": "http://mock-s3-bucket",
        "FIRE_NOTIFICATIONS_USER": "test-user",
        "FIRE_NOTIFICATIONS_PASSWORD": "test-password",
    },
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

        with self.assertRaises(CollectorFailure) as ctx:
            async for _ in collector_generator:
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(str(ctx.exception), "source_unreachable")
        self.assertEqual(mock_session.get.call_count, 10)

    async def test_missing_source_feed_id_raises_typed_failure(self) -> None:
        self.feed["source_feed_id"] = None

        with self.assertRaises(CollectorFailure) as ctx:
            async for _ in collector.fire_notifications_collector(
                self.feed,  # type: ignore
                self.shutdown,
                "http://base",
                self.resources,
            ):
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        )
        self.assertEqual(str(ctx.exception), "missing_source_feed_id")

    async def test_missing_s3_base_raises_typed_configuration_failure(
        self,
    ) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(CollectorFailure) as ctx:
                async for _ in collector.fire_notifications_collector(
                    self.feed,  # type: ignore
                    self.shutdown,
                    "http://base",
                    self.resources,
                ):
                    pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        )
        self.assertEqual(
            str(ctx.exception), "missing_fire_notifications_s3_base"
        )

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._MAX_CONSECUTIVE_FAILURES",
        1,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.AsyncSession",
    )
    async def test_poll_http_statuses_raise_typed_failures(
        self,
        mock_session_cls: MagicMock,
    ) -> None:
        cases = [
            (
                403,
                FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
                "fn_api_http_403",
            ),
            (
                404,
                FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
                "fn_api_http_404",
            ),
            (429, FeedStatusReason.SOURCE_RATE_LIMITED, "fn_api_http_429"),
            (503, FeedStatusReason.SOURCE_UNREACHABLE, "fn_api_http_503"),
        ]

        for status, expected_status_reason, expected_reason in cases:
            with self.subTest(status=status):
                mock_session = mock_session_cls.return_value
                mock_session.close = AsyncMock()
                mock_session.get = AsyncMock(
                    return_value=MagicMock(status_code=status)
                )

                with (
                    patch.object(collector, "logger"),
                    self.assertRaises(CollectorFailure) as ctx,
                ):
                    async for _ in collector.fire_notifications_collector(
                        self.feed,  # type: ignore
                        self.shutdown,
                        "http://base",
                        self.resources,
                    ):
                        pass

                self.assertIs(
                    ctx.exception.status_reason, expected_status_reason
                )
                self.assertEqual(str(ctx.exception), expected_reason)

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
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.AsyncSession",
    )
    async def test_polling_passes_authorization_header(
        self, mock_session_cls: MagicMock, mock_sleep: AsyncMock
    ) -> None:
        mock_sleep.return_value = True  # Trigger immediate exit from loop
        mock_session = mock_session_cls.return_value
        mock_session.close = AsyncMock()

        resp_ok = MagicMock(status_code=200)
        resp_ok.json.return_value = {"files": []}
        mock_session.get = AsyncMock(return_value=resp_ok)

        collector_generator = collector.fire_notifications_collector(
            self.feed,  # type: ignore
            self.shutdown,
            "http://base",
            self.resources,
        )

        async for _ in collector_generator:
            pass

        expected_auth = base64.b64encode(b"test-user:test-password").decode()
        mock_session.get.assert_called_once_with(
            "http://base/CHAN",
            headers={"Authorization": f"Basic {expected_auth}"},
            timeout=10.0,
        )


if __name__ == "__main__":
    unittest.main()

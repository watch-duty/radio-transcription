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
    ItemBatchOutcome,
    ItemFailure,
)
from backend.pipeline.ingestion.collectors.fire_notifications import collector
from backend.pipeline.ingestion.collectors.tests.conftest import (
    _default_resources,
)
from backend.pipeline.ingestion.models import (
    AudioMimeType,
    FeedFailure,
    SourceObservation,
)
from backend.pipeline.storage.feed_store import FeedStatusReason, SourceType


def _require_item_failure(value: ItemFailure | bytes | None) -> ItemFailure:
    """Return a typed item failure for tests that intentionally expect one."""
    if not isinstance(value, ItemFailure):
        msg = f"Expected ItemFailure, got {value!r}"
        raise TypeError(msg)
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

        data = await collector._download_audio(
            self.session, "http://url", self.shutdown
        )
        self.assertEqual(data, b"audio_data")

    async def test_non_retryable_4xx(self) -> None:
        resp = MagicMock(status_code=404)
        self.session.get = AsyncMock(return_value=resp)

        result = await collector._download_audio(
            self.session, "http://url", self.shutdown
        )
        failure = _require_item_failure(result)
        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(failure.reason, "item_http_404")

    async def test_non_retryable_3xx_returns_item_failure_without_retry(
        self,
    ) -> None:
        resp = MagicMock(status_code=302)
        self.session.get = AsyncMock(return_value=resp)

        result = await collector._download_audio(
            self.session, "http://url", self.shutdown
        )

        failure = _require_item_failure(result)
        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(failure.reason, "item_http_302")
        self.session.get.assert_awaited_once()

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.control_flow.sleep_or_cancel",
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
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.control_flow.sleep_or_cancel",
        new_callable=AsyncMock,
    )
    async def test_retryable_4xx_retry_success(
        self, mock_sleep: AsyncMock
    ) -> None:
        mock_sleep.return_value = False
        for status in (408, 429):
            with self.subTest(status=status):
                mock_sleep.reset_mock(return_value=True, side_effect=True)
                mock_sleep.return_value = False
                resp_retryable = MagicMock(status_code=status)
                resp200 = MagicMock(status_code=200, content=b"data")
                self.session.get = AsyncMock(
                    side_effect=[resp_retryable, resp200]
                )

                data = await collector._download_audio(
                    self.session, "http://url", self.shutdown
                )

                self.assertEqual(data, b"data")
                self.assertEqual(self.session.get.call_count, 2)
                mock_sleep.assert_awaited_once()

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.control_flow.sleep_or_cancel",
        new_callable=AsyncMock,
    )
    async def test_5xx_max_retries_fail(self, mock_sleep: MagicMock) -> None:
        mock_sleep.return_value = False
        resp500 = MagicMock(status_code=500)
        self.session.get = AsyncMock(return_value=resp500)

        result = await collector._download_audio(
            self.session, "http://url", self.shutdown
        )
        failure = _require_item_failure(result)
        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(failure.reason, "item_http_500")
        self.assertEqual(
            self.session.get.call_count, collector._DOWNLOAD_MAX_RETRIES
        )

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.control_flow.sleep_or_cancel",
        new_callable=AsyncMock,
    )
    async def test_network_errors_exhausted_return_item_failure(
        self, mock_sleep: MagicMock
    ) -> None:
        mock_sleep.return_value = False
        self.session.get = AsyncMock(side_effect=TimeoutError)

        result = await collector._download_audio(
            self.session, "http://url", self.shutdown
        )

        failure = _require_item_failure(result)
        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(failure.reason, "item_download_failed: TimeoutError")
        self.assertEqual(
            self.session.get.call_count, collector._DOWNLOAD_MAX_RETRIES
        )

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.control_flow.sleep_or_cancel",
        new_callable=AsyncMock,
    )
    async def test_503_retries_exhausted_return_item_failure(
        self, mock_sleep: MagicMock
    ) -> None:
        mock_sleep.return_value = False
        resp503 = MagicMock(status_code=503)
        self.session.get = AsyncMock(return_value=resp503)

        result = await collector._download_audio(
            self.session, "http://url", self.shutdown
        )

        failure = _require_item_failure(result)
        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(failure.reason, "item_http_503")
        self.assertEqual(
            self.session.get.call_count, collector._DOWNLOAD_MAX_RETRIES
        )

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.control_flow.sleep_or_cancel",
        new_callable=AsyncMock,
    )
    async def test_final_transport_error_wins_over_stale_status(
        self, mock_sleep: MagicMock
    ) -> None:
        mock_sleep.return_value = False
        resp503 = MagicMock(status_code=503)
        self.session.get = AsyncMock(
            side_effect=[
                resp503,
                TimeoutError("mid attempt"),
                TimeoutError("final attempt"),
            ]
        )

        result = await collector._download_audio(
            self.session, "http://url", self.shutdown
        )

        failure = _require_item_failure(result)
        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(
            failure.reason,
            "item_download_failed: TimeoutError: final attempt",
        )

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.control_flow.sleep_or_cancel",
        new_callable=AsyncMock,
    )
    async def test_shutdown_during_retry_raises_cancelled_error(
        self, mock_sleep: MagicMock
    ) -> None:
        mock_sleep.side_effect = asyncio.CancelledError
        resp503 = MagicMock(status_code=503)
        self.session.get = AsyncMock(return_value=resp503)

        with self.assertRaises(asyncio.CancelledError):
            await collector._download_audio(
                self.session, "http://url", self.shutdown
            )

        self.assertEqual(self.session.get.call_count, 1)


class TestPollStatusClassification(unittest.TestCase):
    def test_poll_4xx_maps_to_configuration_invalid(self) -> None:
        for status in (400, 404):
            with self.subTest(status=status):
                classification = collector._classify_poll_status(status)

                self.assertIs(
                    classification.status_reason,
                    FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
                )
                self.assertEqual(
                    classification.reason,
                    f"fn_api_http_{status}",
                )

    def test_poll_auth_and_rate_limit_keep_exact_meanings(self) -> None:
        cases = {
            401: FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
            403: FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
            429: FeedStatusReason.SOURCE_RATE_LIMITED,
        }
        for status, reason in cases.items():
            with self.subTest(status=status):
                classification = collector._classify_poll_status(status)

                self.assertIs(classification.status_reason, reason)
                self.assertEqual(
                    classification.reason,
                    f"fn_api_http_{status}",
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
                ItemBatchOutcome(),
            ):
                chunks.append(chunk)

        self.assertEqual(len(chunks), 2)
        self.assertEqual(mock_duration.call_count, 2)
        self.assertEqual(chunks[0].session_id, "session-id")
        self.assertEqual(chunks[0].audio_bytes, b"mp3_bytes")
        self.assertEqual(chunks[0].mime_type, AudioMimeType.MPEG)
        self.assertEqual(chunks[0].resume_position, chunks[0].chunk_start_time)
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
                ItemBatchOutcome(),
            ):
                chunks.append(chunk)

        self.assertEqual(len(chunks), 1)
        self.assertEqual(mock_duration.call_count, 1)
        self.assertEqual(
            chunks[0].chunk_start_time,
            datetime.datetime(2026, 5, 20, 12, 0, 1, tzinfo=datetime.UTC),
        )
        self.assertEqual(chunks[0].resume_position, chunks[0].chunk_start_time)

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._download_audio",
        new_callable=AsyncMock,
    )
    async def test_failed_download_does_not_mark_uuid_seen(
        self, mock_download: AsyncMock
    ) -> None:
        mock_download.return_value = ItemFailure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "item_download_failed",
        )
        files = [
            {
                "type": "file",
                "name": "CHAN 2026-05-20 12-00-00.mp3",
                "uuid": "uuid1",
                "size": 1000,
            },
        ]

        chunks = []
        with self.assertRaises(FeedFailure) as ctx:
            async for chunk in collector._process_file_list(
                files,
                self.session,
                self.shutdown,
                "session-id",
                self.feed,  # type: ignore
                self.processed_uuids,
                "CHAN",
                "http://mock-s3-bucket",
                ItemBatchOutcome(),
            ):
                chunks.append(chunk)

        self.assertEqual(len(chunks), 0)
        self.assertEqual(len(self.processed_uuids), 0)
        self.assertNotIn("uuid1", self.processed_uuids)
        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(ctx.exception.reason, "item_download_failed")

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._download_audio",
        new_callable=AsyncMock,
    )
    async def test_classified_download_failure_promotes_and_does_not_mark_uuid_seen(
        self, mock_download: AsyncMock
    ) -> None:
        mock_download.return_value = ItemFailure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "item_http_503",
        )
        files = [
            {
                "type": "file",
                "name": "CHAN 2026-05-20 12-00-00.mp3",
                "uuid": "uuid1",
                "size": 1000,
            },
        ]

        with self.assertRaises(FeedFailure) as ctx:
            async for _ in collector._process_file_list(
                files,
                self.session,
                self.shutdown,
                "session-id",
                self.feed,  # type: ignore
                self.processed_uuids,
                "CHAN",
                "http://mock-s3-bucket",
                ItemBatchOutcome(),
            ):
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(str(ctx.exception), "item_http_503")
        self.assertEqual(len(self.processed_uuids), 0)

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._download_audio",
        new_callable=AsyncMock,
    )
    async def test_mixed_classified_download_failures_promote_collector_error(
        self, mock_download: AsyncMock
    ) -> None:
        mock_download.side_effect = [
            ItemFailure(
                FeedStatusReason.SOURCE_UNREACHABLE,
                "item_http_503",
            ),
            ItemFailure(
                FeedStatusReason.SOURCE_RATE_LIMITED,
                "item_http_429",
            ),
        ]
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

        with self.assertRaises(FeedFailure) as ctx:
            async for _ in collector._process_file_list(
                files,
                self.session,
                self.shutdown,
                "session-id",
                self.feed,  # type: ignore
                self.processed_uuids,
                "CHAN",
                "http://mock-s3-bucket",
                ItemBatchOutcome(),
            ):
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(str(ctx.exception), "mixed_item_failures")

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._download_audio",
        new_callable=AsyncMock,
    )
    async def test_partial_item_success_suppresses_item_failure_promotion(
        self, mock_download: AsyncMock
    ) -> None:
        mock_download.side_effect = [
            ItemFailure(
                FeedStatusReason.SOURCE_UNREACHABLE,
                "item_http_503",
            ),
            b"mp3_bytes",
        ]
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
                ItemBatchOutcome(),
            ):
                chunks.append(chunk)

        self.assertEqual(len(chunks), 1)
        self.assertEqual(list(self.processed_uuids), ["uuid2"])

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.telemetry.emit_call_download_failed",
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._download_audio",
        new_callable=AsyncMock,
    )
    async def test_shutdown_suppresses_call_download_failed_emit(
        self,
        mock_download: AsyncMock,
        mock_emit: MagicMock,
    ) -> None:
        async def _fail_after_shutdown(*_args: object) -> ItemFailure:
            self.shutdown.set()
            return ItemFailure(
                FeedStatusReason.SOURCE_UNREACHABLE,
                "item_download_failed",
            )

        mock_download.side_effect = _fail_after_shutdown
        files = [
            {
                "type": "file",
                "name": "CHAN 2026-05-20 12-00-00.mp3",
                "uuid": "uuid1",
                "size": 1000,
            },
        ]

        chunks = [
            chunk
            async for chunk in collector._process_file_list(
                files,
                self.session,
                self.shutdown,
                "session-id",
                self.feed,  # type: ignore
                self.processed_uuids,
                "CHAN",
                "http://mock-s3-bucket",
                ItemBatchOutcome(),
            )
        ]

        self.assertEqual(chunks, [])
        mock_emit.assert_not_called()

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.telemetry.emit_call_download_failed",
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._download_audio",
        new_callable=AsyncMock,
    )
    async def test_duration_probe_failed_promotes_without_marking_uuid_or_slo(
        self,
        mock_download: AsyncMock,
        mock_emit: MagicMock,
    ) -> None:
        mock_download.return_value = b"mp3_bytes"
        files = [
            {
                "type": "file",
                "name": "CHAN 2026-05-20 12-00-00.mp3",
                "uuid": "uuid1",
                "size": 1000,
            },
        ]

        with patch(
            "backend.pipeline.ingestion.collectors.fire_notifications.collector.get_audio_duration",
            side_effect=ValueError("bad mp3"),
        ):
            with self.assertRaises(FeedFailure) as ctx:
                async for _ in collector._process_file_list(
                    files,
                    self.session,
                    self.shutdown,
                    "session-id",
                    self.feed,  # type: ignore
                    self.processed_uuids,
                    "CHAN",
                    "http://mock-s3-bucket",
                    ItemBatchOutcome(),
                ):
                    pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(str(ctx.exception), "duration_probe_failed")
        self.assertNotIn("uuid1", self.processed_uuids)
        mock_emit.assert_not_called()


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
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.control_flow.sleep_or_cancel",
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

        with self.assertRaises(FeedFailure) as ctx:
            async for _ in collector_generator:
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(
            str(ctx.exception),
            "source_unreachable: Exception: Connection failure",
        )
        self.assertEqual(mock_session.get.call_count, 10)

    async def test_missing_source_feed_id_raises_typed_failure(self) -> None:
        self.feed["source_feed_id"] = None

        with self.assertRaises(FeedFailure) as ctx:
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
            with self.assertRaises(FeedFailure) as ctx:
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
                400,
                FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
                "fn_api_http_400",
            ),
            (
                401,
                FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
                "fn_api_http_401",
            ),
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

                with self.assertRaises(FeedFailure) as ctx:
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
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.AsyncSession",
    )
    async def test_poll_malformed_json_raises_payload_failure(
        self,
        mock_session_cls: MagicMock,
    ) -> None:
        mock_session = mock_session_cls.return_value
        mock_session.close = AsyncMock()
        resp_ok = MagicMock(status_code=200)
        resp_ok.json.side_effect = ValueError("bad json")
        mock_session.get = AsyncMock(return_value=resp_ok)

        with self.assertRaises(FeedFailure) as ctx:
            async for _ in collector.fire_notifications_collector(
                self.feed,  # type: ignore
                self.shutdown,
                "http://base",
                self.resources,
            ):
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(
            str(ctx.exception),
            "fn_api_payload_malformed: ValueError: bad json",
        )

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.control_flow.sleep_or_cancel",
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

        events = []
        async for event in collector_generator:
            events.append(event)

        self.assertEqual(events, [SourceObservation()])
        self.assertEqual(mock_session.get.call_count, 11)

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.control_flow.sleep_or_cancel",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.AsyncSession",
    )
    async def test_collector_owns_session_and_closes_it(
        self, mock_session_cls: MagicMock, mock_sleep: AsyncMock
    ) -> None:
        mock_session = mock_session_cls.return_value
        mock_session.close = AsyncMock()

        async def _stop_after_poll_sleep(*_args: object) -> None:
            self.shutdown.set()

        mock_sleep.side_effect = _stop_after_poll_sleep

        resp_ok = MagicMock(status_code=200)
        resp_ok.json.return_value = {}
        mock_session.get = AsyncMock(return_value=resp_ok)

        async for _ in collector.fire_notifications_collector(
            self.feed,  # type: ignore
            self.shutdown,
            "http://base",
            _default_resources(),
        ):
            pass

        mock_session_cls.assert_called_once_with()
        mock_session.close.assert_awaited_once()

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.control_flow.sleep_or_cancel",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.AsyncSession",
    )
    async def test_successful_poll_missing_files_yields_source_observation(
        self, mock_session_cls: MagicMock, mock_sleep: AsyncMock
    ) -> None:
        mock_session = mock_session_cls.return_value
        mock_session.close = AsyncMock()

        async def _stop_after_poll_sleep(*_args: object) -> None:
            self.shutdown.set()

        mock_sleep.side_effect = _stop_after_poll_sleep

        resp_ok = MagicMock(status_code=200)
        resp_ok.json.return_value = {}
        mock_session.get = AsyncMock(return_value=resp_ok)

        events = []
        async for event in collector.fire_notifications_collector(
            self.feed,  # type: ignore
            self.shutdown,
            "http://base",
            self.resources,
        ):
            events.append(event)

        self.assertEqual(events, [SourceObservation()])

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.AsyncSession",
    )
    async def test_present_non_list_files_raises_malformed_failure(
        self, mock_session_cls: MagicMock
    ) -> None:
        mock_session = mock_session_cls.return_value
        mock_session.close = AsyncMock()

        resp_ok = MagicMock(status_code=200)
        resp_ok.json.return_value = {"files": {"bad": "shape"}}
        mock_session.get = AsyncMock(return_value=resp_ok)

        with self.assertRaises(FeedFailure) as ctx:
            async for _ in collector.fire_notifications_collector(
                self.feed,  # type: ignore
                self.shutdown,
                "http://base",
                self.resources,
            ):
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(str(ctx.exception), "fn_api_payload_malformed")
        mock_session.close.assert_awaited_once()

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.control_flow.sleep_or_cancel",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._download_audio",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.AsyncSession",
    )
    async def test_all_seen_files_yield_source_observation(
        self,
        mock_session_cls: MagicMock,
        mock_download: AsyncMock,
        mock_sleep: AsyncMock,
    ) -> None:
        mock_download.return_value = b"mp3"
        mock_session = mock_session_cls.return_value
        mock_session.close = AsyncMock()

        first_resp = MagicMock(status_code=200)
        first_resp.json.return_value = {
            "files": [
                {
                    "type": "file",
                    "name": "CHAN 2026-05-20 12-00-00.mp3",
                    "uuid": "uuid1",
                    "size": 1000,
                }
            ]
        }
        second_resp = MagicMock(status_code=200)
        second_resp.json.return_value = {
            "files": [
                {
                    "type": "file",
                    "name": "CHAN 2026-05-20 12-00-00.mp3",
                    "uuid": "uuid1",
                    "size": 1000,
                }
            ]
        }
        mock_session.get = AsyncMock(side_effect=[first_resp, second_resp])

        sleep_calls = 0

        async def sleep_side_effect(*args, **kwargs) -> bool:
            nonlocal sleep_calls
            sleep_calls += 1
            if sleep_calls >= 2:
                self.shutdown.set()
                return True
            return False

        mock_sleep.side_effect = sleep_side_effect

        events = []
        with patch(
            "backend.pipeline.ingestion.collectors.fire_notifications.collector.get_audio_duration",
            return_value=1000,
        ):
            async for event in collector.fire_notifications_collector(
                self.feed,  # type: ignore
                self.shutdown,
                "http://base",
                self.resources,
            ):
                events.append(event)

        self.assertEqual(len(events), 2)
        self.assertIsInstance(events[0], collector.CapturedChunk)
        self.assertEqual(events[1], SourceObservation())
        mock_download.assert_awaited_once()

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.control_flow.sleep_or_cancel",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.AsyncSession",
    )
    async def test_polling_passes_authorization_header(
        self, mock_session_cls: MagicMock, mock_sleep: AsyncMock
    ) -> None:
        mock_session = mock_session_cls.return_value
        mock_session.close = AsyncMock()

        async def _stop_after_poll_sleep(*_args: object) -> None:
            self.shutdown.set()

        mock_sleep.side_effect = _stop_after_poll_sleep

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

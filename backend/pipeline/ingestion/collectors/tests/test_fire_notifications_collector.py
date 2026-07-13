from __future__ import annotations

import asyncio
import collections
import datetime
import json
import os
import subprocess
import unittest
import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock, call, patch
from zoneinfo import ZoneInfo

from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemBatchOutcome,
    ItemFailure,
)
from backend.pipeline.ingestion.collectors.fire_notifications import collector
from backend.pipeline.ingestion.collectors.fire_notifications.client import (
    FireNotificationsClient,
    FireNotificationsFile,
    FireNotificationsRestClient,
)
from backend.pipeline.ingestion.collectors.tests.conftest import (
    _default_resources,
)
from backend.pipeline.ingestion.models import (
    AudioMimeType,
    CapturedChunk,
    FeedFailure,
    SourceObservation,
)
from backend.pipeline.storage.feed_store import (
    FeedStatusReason,
    LeasedFeed,
    SourceType,
)


def _require_item_failure(value: ItemFailure | bytes | None) -> ItemFailure:
    assert isinstance(value, ItemFailure)
    return value


def _mock_response(
    status: int, content: bytes = b"", json_data: Any = None
) -> MagicMock:
    resp = AsyncMock()
    resp.status = status
    if json_data is not None:
        content = json.dumps(json_data).encode()
        resp.json = AsyncMock(return_value=json_data)
        resp.headers = {"Content-Type": "application/json"}
    else:
        resp.headers = {}
    resp.read = AsyncMock(return_value=content)
    resp.get_encoding = MagicMock(return_value="utf-8")
    resp.request_info = MagicMock()
    resp.history = ()

    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=resp)
    cm.__aexit__ = AsyncMock(return_value=False)
    return cm


class TestClientDownloadAudio(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.session = MagicMock()
        self.shutdown = asyncio.Event()
        self.client = FireNotificationsRestClient(
            session=self.session,
            url_base="http://mock-api",
            s3_base_url="http://mock-s3-bucket",
            user="test-user",
            password="test-password",
            timezone=ZoneInfo("UTC"),
        )

    async def test_success(self) -> None:
        self.session.get = MagicMock(
            return_value=_mock_response(200, b"audio_data")
        )

        data = await self.client.download_audio("uuid1.mp3", self.shutdown)
        self.assertEqual(data, b"audio_data")

    async def test_fetch_file_list_deduplicates(self) -> None:
        payload = {
            "files": [
                {
                    "name": "SAN-JOSE-DISP 2026-06-15 17-45-43.mp3",
                    "uuid": "f66bcfae-c1bf-4abc-8def-1234567890ab",
                    "type": "file",
                    "size": 1000,
                },
                {
                    "name": "SAN-JOSE-DISP 2026-06-15 17-45-43.mp3",
                    "uuid": "6a21ba3b-c1bf-4abc-8def-1234567890ab",
                    "type": "file",
                    "size": 2000,
                },
                {
                    "name": "SAN-JOSE-DISP 2026-06-15 17-50-00.mp3",
                    "uuid": "3bc21da2-c1bf-4abc-8def-1234567890ab",
                    "type": "file",
                    "size": 1500,
                },
            ]
        }
        self.session.get = MagicMock(
            return_value=_mock_response(200, json_data=payload)
        )

        files = await self.client.fetch_file_list(
            source_feed_id="SAN-JOSE-DISP",
            feed_id="feed-id",
            shutdown_event=self.shutdown,
        )

        # It should only return 2 files (uuid1 and uuid3), with uuid2 filtered out as a duplicate name.
        self.assertEqual(len(files), 2)
        self.assertEqual(files[0].uuid, "f66bcfae-c1bf-4abc-8def-1234567890ab")
        self.assertEqual(
            files[0].filename, "SAN-JOSE-DISP 2026-06-15 17-45-43.mp3"
        )
        self.assertEqual(files[1].uuid, "3bc21da2-c1bf-4abc-8def-1234567890ab")
        self.assertEqual(
            files[1].filename, "SAN-JOSE-DISP 2026-06-15 17-50-00.mp3"
        )

    async def test_non_retryable_4xx(self) -> None:
        self.session.get = MagicMock(return_value=_mock_response(404))

        result = await self.client.download_audio("uuid1.mp3", self.shutdown)
        failure = _require_item_failure(result)
        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(failure.reason, "item_http_404")

    async def test_non_retryable_3xx_returns_item_failure_without_retry(
        self,
    ) -> None:
        self.session.get = MagicMock(return_value=_mock_response(302))

        result = await self.client.download_audio("uuid1.mp3", self.shutdown)

        failure = _require_item_failure(result)
        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(failure.reason, "item_http_302")
        self.session.get.assert_called_once()

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.client.control_flow.sleep_or_cancel",
        new_callable=AsyncMock,
    )
    async def test_5xx_retry_success(self, mock_sleep: AsyncMock) -> None:
        mock_sleep.return_value = False
        self.session.get = MagicMock(
            side_effect=[
                _mock_response(500),
                _mock_response(200, b"data"),
            ]
        )

        data = await self.client.download_audio("uuid1.mp3", self.shutdown)
        self.assertEqual(data, b"data")
        self.assertEqual(self.session.get.call_count, 2)

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.client.control_flow.sleep_or_cancel",
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
                self.session.get = MagicMock(
                    side_effect=[
                        _mock_response(status),
                        _mock_response(200, b"data"),
                    ]
                )

                data = await self.client.download_audio(
                    "uuid1.mp3", self.shutdown
                )

                self.assertEqual(data, b"data")
                self.assertEqual(self.session.get.call_count, 2)
                mock_sleep.assert_awaited_once()

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.client.control_flow.sleep_or_cancel",
        new_callable=AsyncMock,
    )
    async def test_5xx_max_retries_fail(self, mock_sleep: MagicMock) -> None:
        mock_sleep.return_value = False
        self.session.get = MagicMock(return_value=_mock_response(500))

        result = await self.client.download_audio("uuid1.mp3", self.shutdown)
        failure = _require_item_failure(result)
        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(failure.reason, "item_http_500")
        self.assertEqual(self.session.get.call_count, 3)

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.client.control_flow.sleep_or_cancel",
        new_callable=AsyncMock,
    )
    async def test_network_errors_exhausted_return_item_failure(
        self, mock_sleep: MagicMock
    ) -> None:
        mock_sleep.return_value = False
        self.session.get = MagicMock(side_effect=TimeoutError)

        result = await self.client.download_audio("uuid1.mp3", self.shutdown)

        failure = _require_item_failure(result)
        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(failure.reason, "item_download_failed: TimeoutError")
        self.assertEqual(self.session.get.call_count, 3)

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.client.control_flow.sleep_or_cancel",
        new_callable=AsyncMock,
    )
    async def test_503_retries_exhausted_return_item_failure(
        self, mock_sleep: MagicMock
    ) -> None:
        mock_sleep.return_value = False
        self.session.get = MagicMock(return_value=_mock_response(503))

        result = await self.client.download_audio("uuid1.mp3", self.shutdown)

        failure = _require_item_failure(result)
        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(failure.reason, "item_http_503")
        self.assertEqual(self.session.get.call_count, 3)

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.client.control_flow.sleep_or_cancel",
        new_callable=AsyncMock,
    )
    async def test_final_transport_error_wins_over_stale_status(
        self, mock_sleep: MagicMock
    ) -> None:
        mock_sleep.return_value = False
        self.session.get = MagicMock(
            side_effect=[
                _mock_response(503),
                TimeoutError("mid attempt"),
                TimeoutError("final attempt"),
            ]
        )

        result = await self.client.download_audio("uuid1.mp3", self.shutdown)

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
        "backend.pipeline.ingestion.collectors.fire_notifications.client.control_flow.sleep_or_cancel",
        new_callable=AsyncMock,
    )
    async def test_shutdown_during_retry_raises_cancelled_error(
        self, mock_sleep: MagicMock
    ) -> None:
        mock_sleep.side_effect = asyncio.CancelledError
        self.session.get = MagicMock(return_value=_mock_response(503))

        with self.assertRaises(asyncio.CancelledError):
            await self.client.download_audio("uuid1.mp3", self.shutdown)

        self.assertEqual(self.session.get.call_count, 1)


class TestProcessFileList(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.client = MagicMock(spec=FireNotificationsClient)
        self.shutdown = asyncio.Event()
        self.feed: dict[str, Any] = {
            "id": "feed-id",
            "source_type": SourceType.FIRE_NOTIFICATIONS,
            "name": "CHAN-feed",
        }
        self.processed_uuids = collections.deque(maxlen=1000)

    async def test_process_files(self) -> None:
        self.client.download_audio.return_value = b"mp3_bytes"
        files = [
            FireNotificationsFile(
                uuid="uuid1",
                filename="CHAN 2026-05-20 12-00-00.mp3",
                start_time=datetime.datetime(
                    2026, 5, 20, 12, 0, 0, tzinfo=datetime.UTC
                ),
                size=1000,
            ),
            FireNotificationsFile(
                uuid="uuid2",
                filename="CHAN 2026-05-20 12-00-01.mp3",
                start_time=datetime.datetime(
                    2026, 5, 20, 12, 0, 1, tzinfo=datetime.UTC
                ),
                size=1000,
            ),
        ]

        chunks = []
        with patch(
            "backend.pipeline.ingestion.collectors.fire_notifications.collector.probe_audio_metadata",
            return_value=(30000, AudioMimeType.MPEG),
        ) as mock_duration:
            async for chunk in collector._process_file_list(
                files,
                self.client,
                self.shutdown,
                "session-id",
                self.feed,  # type: ignore
                self.processed_uuids,
                "CHAN",
                ItemBatchOutcome(),
            ):
                chunks.append(chunk)

        self.assertEqual(len(chunks), 2)
        self.assertEqual(mock_duration.call_count, 2)
        mock_duration.assert_has_calls(
            [
                call(b"mp3_bytes"),
                call(b"mp3_bytes"),
            ]
        )
        chunk0 = chunks[0]
        assert isinstance(chunk0, CapturedChunk)
        self.assertEqual(chunk0.session_id, "session-id")
        self.assertEqual(chunk0.audio_bytes, b"mp3_bytes")
        self.assertEqual(chunk0.mime_type, AudioMimeType.MPEG)
        self.assertEqual(chunk0.resume_position, chunk0.chunk_start_time)
        self.assertEqual(len(self.processed_uuids), 2)
        self.assertIn("uuid1", self.processed_uuids)
        self.assertIn("uuid2", self.processed_uuids)

    async def test_process_files_m4a(self) -> None:
        self.client.download_audio.return_value = b"m4a_bytes"
        files = [
            FireNotificationsFile(
                uuid="uuid1",
                filename="CHAN 2026-05-20 12-00-00.mp3",
                start_time=datetime.datetime(
                    2026, 5, 20, 12, 0, 0, tzinfo=datetime.UTC
                ),
                size=1000,
            ),
        ]

        chunks = []
        with patch(
            "backend.pipeline.ingestion.collectors.fire_notifications.collector.probe_audio_metadata",
            return_value=(15000, AudioMimeType.MP4),
        ) as mock_duration:
            async for chunk in collector._process_file_list(
                files,
                self.client,
                self.shutdown,
                "session-id",
                self.feed,  # type: ignore
                self.processed_uuids,
                "CHAN",
                ItemBatchOutcome(),
            ):
                chunks.append(chunk)

        self.assertEqual(len(chunks), 1)
        mock_duration.assert_called_once_with(b"m4a_bytes")
        chunk0 = chunks[0]
        assert isinstance(chunk0, CapturedChunk)
        self.assertEqual(chunk0.session_id, "session-id")
        self.assertEqual(chunk0.audio_bytes, b"m4a_bytes")
        self.assertEqual(chunk0.mime_type, AudioMimeType.MP4)
        self.assertEqual(chunk0.resume_position, chunk0.chunk_start_time)
        self.assertEqual(len(self.processed_uuids), 1)
        self.assertIn("uuid1", self.processed_uuids)

    async def test_process_files_with_duplicates(self) -> None:
        self.client.download_audio.return_value = b"mp3_bytes"
        files = [
            FireNotificationsFile(
                uuid="uuid1",
                filename="CHAN 2026-05-20 12-00-00.mp3",
                start_time=datetime.datetime(
                    2026, 5, 20, 12, 0, 0, tzinfo=datetime.UTC
                ),
                size=1000,
            ),
            FireNotificationsFile(
                uuid="uuid2",
                filename="CHAN 2026-05-20 12-00-00.mp3",
                start_time=datetime.datetime(
                    2026, 5, 20, 12, 0, 0, tzinfo=datetime.UTC
                ),
                size=1000,
            ),
        ]

        chunks = []
        with patch(
            "backend.pipeline.ingestion.collectors.fire_notifications.collector.probe_audio_metadata",
            return_value=(30000, AudioMimeType.MPEG),
        ) as mock_duration:
            async for chunk in collector._process_file_list(
                files,
                self.client,
                self.shutdown,
                "session-id",
                self.feed,  # type: ignore
                self.processed_uuids,
                "CHAN",
                ItemBatchOutcome(),
            ):
                chunks.append(chunk)

        # Because the second file has the same start time, it should be skipped
        self.assertEqual(len(chunks), 1)
        self.assertEqual(mock_duration.call_count, 1)
        chunk0 = chunks[0]
        assert isinstance(chunk0, CapturedChunk)
        self.assertEqual(chunk0.session_id, "session-id")
        self.assertEqual(chunk0.audio_bytes, b"mp3_bytes")
        self.assertEqual(chunk0.mime_type, AudioMimeType.MPEG)
        self.assertEqual(chunk0.resume_position, chunk0.chunk_start_time)
        self.assertEqual(len(self.processed_uuids), 1)
        self.assertIn("uuid1", self.processed_uuids)
        self.assertNotIn("uuid2", self.processed_uuids)

    async def test_process_files_with_last_bookmark_time(
        self,
    ) -> None:
        self.client.download_audio.return_value = b"mp3_bytes"
        self.feed["last_bookmark_time"] = datetime.datetime(
            2026, 5, 20, 12, 0, 0, tzinfo=datetime.UTC
        )
        files = [
            FireNotificationsFile(
                uuid="uuid1",
                filename="CHAN 2026-05-20 12-00-00.mp3",
                start_time=datetime.datetime(
                    2026, 5, 20, 12, 0, 0, tzinfo=datetime.UTC
                ),
                size=1000,
            ),
            FireNotificationsFile(
                uuid="uuid2",
                filename="CHAN 2026-05-20 12-00-01.mp3",
                start_time=datetime.datetime(
                    2026, 5, 20, 12, 0, 1, tzinfo=datetime.UTC
                ),
                size=1000,
            ),
        ]

        chunks = []
        with patch(
            "backend.pipeline.ingestion.collectors.fire_notifications.collector.probe_audio_metadata",
            return_value=(30000, AudioMimeType.MPEG),
        ) as mock_duration:
            async for chunk in collector._process_file_list(
                files,
                self.client,
                self.shutdown,
                "session-id",
                self.feed,  # type: ignore
                self.processed_uuids,
                "CHAN",
                ItemBatchOutcome(),
            ):
                chunks.append(chunk)

        self.assertEqual(len(chunks), 1)
        self.assertEqual(mock_duration.call_count, 1)
        chunk0 = chunks[0]
        assert isinstance(chunk0, CapturedChunk)
        self.assertEqual(
            chunk0.chunk_start_time,
            datetime.datetime(2026, 5, 20, 12, 0, 1, tzinfo=datetime.UTC),
        )
        self.assertEqual(chunk0.resume_position, chunk0.chunk_start_time)

    async def test_failed_download_does_not_mark_uuid_seen(
        self,
    ) -> None:
        self.client.download_audio.return_value = ItemFailure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "item_download_failed",
        )
        files = [
            FireNotificationsFile(
                uuid="uuid1",
                filename="CHAN 2026-05-20 12-00-00.mp3",
                start_time=datetime.datetime(
                    2026, 5, 20, 12, 0, 0, tzinfo=datetime.UTC
                ),
                size=1000,
            ),
        ]

        chunks = []
        with self.assertRaises(FeedFailure) as ctx:
            async for chunk in collector._process_file_list(
                files,
                self.client,
                self.shutdown,
                "session-id",
                self.feed,  # type: ignore
                self.processed_uuids,
                "CHAN",
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

    async def test_classified_download_failure_promotes_and_does_not_mark_uuid_seen(
        self,
    ) -> None:
        self.client.download_audio.return_value = ItemFailure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "item_http_503",
        )
        files = [
            FireNotificationsFile(
                uuid="uuid1",
                filename="CHAN 2026-05-20 12-00-00.mp3",
                start_time=datetime.datetime(
                    2026, 5, 20, 12, 0, 0, tzinfo=datetime.UTC
                ),
                size=1000,
            ),
        ]

        with self.assertRaises(FeedFailure) as ctx:
            async for _ in collector._process_file_list(
                files,
                self.client,
                self.shutdown,
                "session-id",
                self.feed,  # type: ignore
                self.processed_uuids,
                "CHAN",
                ItemBatchOutcome(),
            ):
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(str(ctx.exception), "item_http_503")
        self.assertEqual(len(self.processed_uuids), 0)

    async def test_mixed_classified_download_failures_promote_collector_error(
        self,
    ) -> None:
        self.client.download_audio.side_effect = [
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
            FireNotificationsFile(
                uuid="uuid1",
                filename="CHAN 2026-05-20 12-00-00.mp3",
                start_time=datetime.datetime(
                    2026, 5, 20, 12, 0, 0, tzinfo=datetime.UTC
                ),
                size=1000,
            ),
            FireNotificationsFile(
                uuid="uuid2",
                filename="CHAN 2026-05-20 12-00-01.mp3",
                start_time=datetime.datetime(
                    2026, 5, 20, 12, 0, 1, tzinfo=datetime.UTC
                ),
                size=1000,
            ),
        ]

        with self.assertRaises(FeedFailure) as ctx:
            async for _ in collector._process_file_list(
                files,
                self.client,
                self.shutdown,
                "session-id",
                self.feed,  # type: ignore
                self.processed_uuids,
                "CHAN",
                ItemBatchOutcome(),
            ):
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(str(ctx.exception), "mixed_item_failures")

    async def test_partial_item_success_suppresses_item_failure_promotion(
        self,
    ) -> None:
        self.client.download_audio.side_effect = [
            ItemFailure(
                FeedStatusReason.SOURCE_UNREACHABLE,
                "item_http_503",
            ),
            b"mp3_bytes",
        ]
        files = [
            FireNotificationsFile(
                uuid="uuid1",
                filename="CHAN 2026-05-20 12-00-00.mp3",
                start_time=datetime.datetime(
                    2026, 5, 20, 12, 0, 0, tzinfo=datetime.UTC
                ),
                size=1000,
            ),
            FireNotificationsFile(
                uuid="uuid2",
                filename="CHAN 2026-05-20 12-00-01.mp3",
                start_time=datetime.datetime(
                    2026, 5, 20, 12, 0, 1, tzinfo=datetime.UTC
                ),
                size=1000,
            ),
        ]

        chunks = []
        with patch(
            "backend.pipeline.ingestion.collectors.fire_notifications.collector.probe_audio_metadata",
            return_value=(30000, AudioMimeType.MPEG),
        ):
            async for chunk in collector._process_file_list(
                files,
                self.client,
                self.shutdown,
                "session-id",
                self.feed,  # type: ignore
                self.processed_uuids,
                "CHAN",
                ItemBatchOutcome(),
            ):
                chunks.append(chunk)

        self.assertEqual(len(chunks), 1)
        self.assertEqual(list(self.processed_uuids), ["uuid2"])

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.telemetry.emit_call_download_failed",
    )
    async def test_shutdown_suppresses_call_download_failed_emit(
        self,
        mock_emit: MagicMock,
    ) -> None:
        async def _fail_after_shutdown(*_args: object) -> ItemFailure:
            self.shutdown.set()
            return ItemFailure(
                FeedStatusReason.SOURCE_UNREACHABLE,
                "item_download_failed",
            )

        self.client.download_audio.side_effect = _fail_after_shutdown
        files = [
            FireNotificationsFile(
                uuid="uuid1",
                filename="CHAN 2026-05-20 12-00-00.mp3",
                start_time=datetime.datetime(
                    2026, 5, 20, 12, 0, 0, tzinfo=datetime.UTC
                ),
                size=1000,
            ),
        ]

        chunks = [
            chunk
            async for chunk in collector._process_file_list(
                files,
                self.client,
                self.shutdown,
                "session-id",
                self.feed,  # type: ignore
                self.processed_uuids,
                "CHAN",
                ItemBatchOutcome(),
            )
        ]

        self.assertEqual(chunks, [])
        mock_emit.assert_not_called()

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.telemetry.emit_call_download_failed",
    )
    async def test_duration_probe_failed_promotes_without_marking_uuid_or_slo(
        self,
        mock_emit: MagicMock,
    ) -> None:
        self.client.download_audio.return_value = b"mp3_bytes"
        files = [
            FireNotificationsFile(
                uuid="uuid1",
                filename="CHAN 2026-05-20 12-00-00.mp3",
                start_time=datetime.datetime(
                    2026, 5, 20, 12, 0, 0, tzinfo=datetime.UTC
                ),
                size=1000,
            ),
        ]

        expected_reason = (
            "ffprobe exited with code 1; "
            "Invalid data found when processing input"
        )
        with (
            patch(
                "backend.pipeline.ingestion.collectors.fire_notifications.collector.probe_audio_metadata",
                side_effect=subprocess.CalledProcessError(
                    1,
                    ["ffprobe"],
                    stderr=b"Invalid data found when processing input\n",
                ),
            ),
            self.assertLogs(
                "backend.pipeline.ingestion.collectors.fire_notifications.collector",
                level="WARNING",
            ) as logs,
        ):
            events = []
            with self.assertRaises(FeedFailure) as ctx:
                async for event in collector._process_file_list(
                    files,
                    self.client,
                    self.shutdown,
                    "session-id",
                    self.feed,  # type: ignore
                    self.processed_uuids,
                    "CHAN",
                    ItemBatchOutcome(),
                ):
                    events.append(event)

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
        )
        self.assertEqual(str(ctx.exception), expected_reason)
        log_text = "\n".join(logs.output)
        self.assertIn(expected_reason, log_text)
        self.assertIn("uuid1", log_text)
        self.assertIn("CHAN 2026-05-20 12-00-00.mp3", log_text)
        self.assertIn("listed_size=1000", log_text)
        self.assertIn("downloaded_bytes=9", log_text)
        self.assertIn("Permanently skipping corrupt audio file.", log_text)
        self.assertIn("CHAN-feed", log_text)
        self.assertNotIn("Traceback", log_text)
        self.assertNotIn("uuid1", self.processed_uuids)
        mock_emit.assert_not_called()

        # Verify that a SourceObservation with the correct start time was yielded
        self.assertEqual(len(events), 1)
        self.assertIsInstance(events[0], SourceObservation)
        self.assertEqual(
            events[0].resume_position,
            datetime.datetime(2026, 5, 20, 12, 0, 0, tzinfo=datetime.UTC),
        )

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.telemetry.emit_call_download_failed",
    )
    async def test_duration_generic_failure_is_retryable(
        self,
        mock_emit: MagicMock,
    ) -> None:
        self.client.download_audio.return_value = b"mp3_bytes"
        files = [
            FireNotificationsFile(
                uuid="uuid1",
                filename="CHAN 2026-05-20 12-00-00.mp3",
                start_time=datetime.datetime(
                    2026, 5, 20, 12, 0, 0, tzinfo=datetime.UTC
                ),
                size=1000,
            ),
        ]

        expected_reason = "ValueError: bad mp3"
        with (
            patch(
                "backend.pipeline.ingestion.collectors.fire_notifications.collector.probe_audio_metadata",
                side_effect=ValueError("bad mp3"),
            ),
            self.assertLogs(
                "backend.pipeline.ingestion.collectors.fire_notifications.collector",
                level="WARNING",
            ) as logs,
        ):
            events = []
            with self.assertRaises(FeedFailure) as ctx:
                async for event in collector._process_file_list(
                    files,
                    self.client,
                    self.shutdown,
                    "session-id",
                    self.feed,  # type: ignore
                    self.processed_uuids,
                    "CHAN",
                    ItemBatchOutcome(),
                ):
                    events.append(event)

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(str(ctx.exception), expected_reason)
        log_text = "\n".join(logs.output)
        self.assertIn(expected_reason, log_text)
        self.assertIn("This failure is retryable.", log_text)
        self.assertNotIn("Permanently skipping corrupt audio file.", log_text)
        self.assertIn("CHAN-feed", log_text)
        self.assertNotIn("Traceback", log_text)
        self.assertNotIn("uuid1", self.processed_uuids)
        mock_emit.assert_not_called()

        # Verify that no events were yielded (since the failure is retryable and bookmark is not advanced)
        self.assertEqual(events, [])

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.telemetry.emit_call_download_failed",
    )
    async def test_duration_timeout_failure_is_retryable(
        self,
        mock_emit: MagicMock,
    ) -> None:
        self.client.download_audio.return_value = b"mp3_bytes"
        files = [
            FireNotificationsFile(
                uuid="uuid1",
                filename="CHAN 2026-05-20 12-00-00.mp3",
                start_time=datetime.datetime(
                    2026, 5, 20, 12, 0, 0, tzinfo=datetime.UTC
                ),
                size=1000,
            ),
        ]

        expected_reason = "ffprobe timed out after 10s"
        with (
            patch(
                "backend.pipeline.ingestion.collectors.fire_notifications.collector.probe_audio_metadata",
                side_effect=subprocess.TimeoutExpired(
                    cmd=["ffprobe"], timeout=10.0
                ),
            ),
            self.assertLogs(
                "backend.pipeline.ingestion.collectors.fire_notifications.collector",
                level="WARNING",
            ) as logs,
        ):
            events = []
            with self.assertRaises(FeedFailure) as ctx:
                async for event in collector._process_file_list(
                    files,
                    self.client,
                    self.shutdown,
                    "session-id",
                    self.feed,  # type: ignore
                    self.processed_uuids,
                    "CHAN",
                    ItemBatchOutcome(),
                ):
                    events.append(event)

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(str(ctx.exception), expected_reason)
        log_text = "\n".join(logs.output)
        self.assertIn(expected_reason, log_text)
        self.assertIn("This failure is retryable.", log_text)
        self.assertNotIn("Permanently skipping corrupt audio file.", log_text)
        self.assertIn("CHAN-feed", log_text)
        self.assertNotIn("Traceback", log_text)
        self.assertNotIn("uuid1", self.processed_uuids)
        mock_emit.assert_not_called()

        # Verify that no events were yielded
        self.assertEqual(events, [])

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.telemetry.emit_call_download_failed",
    )
    async def test_duration_probe_failed_with_size_none_does_not_crash(
        self,
        mock_emit: MagicMock,
    ) -> None:
        self.client.download_audio.return_value = b"mp3_bytes"
        files = [
            FireNotificationsFile(
                uuid="uuid1",
                filename="CHAN 2026-05-20 12-00-00.mp3",
                start_time=datetime.datetime(
                    2026, 5, 20, 12, 0, 0, tzinfo=datetime.UTC
                ),
                size=None,
            ),
        ]

        expected_reason = (
            "ffprobe exited with code 1; "
            "Invalid data found when processing input"
        )
        with (
            patch(
                "backend.pipeline.ingestion.collectors.fire_notifications.collector.probe_audio_metadata",
                side_effect=subprocess.CalledProcessError(
                    1,
                    ["ffprobe"],
                    stderr=b"Invalid data found when processing input\n",
                ),
            ),
            self.assertLogs(
                "backend.pipeline.ingestion.collectors.fire_notifications.collector",
                level="WARNING",
            ) as logs,
        ):
            events = []
            with self.assertRaises(FeedFailure) as ctx:
                async for event in collector._process_file_list(
                    files,
                    self.client,
                    self.shutdown,
                    "session-id",
                    self.feed,  # type: ignore
                    self.processed_uuids,
                    "CHAN",
                    ItemBatchOutcome(),
                ):
                    events.append(event)

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
        )
        self.assertEqual(str(ctx.exception), expected_reason)
        log_text = "\n".join(logs.output)
        self.assertIn("listed_size=unknown", log_text)
        self.assertIn("Permanently skipping corrupt audio file.", log_text)
        self.assertNotIn("Traceback", log_text)
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
        self.resources = _default_resources()

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.control_flow.sleep_or_cancel",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.client.FireNotificationsRestClient.fetch_file_list",
        new_callable=AsyncMock,
    )
    async def test_max_consecutive_failures_raises_source_unreachable(
        self, mock_fetch: AsyncMock, mock_sleep: AsyncMock
    ) -> None:
        mock_sleep.return_value = False  # Sleep normally
        mock_fetch.side_effect = Exception("Connection failure")

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
        self.assertEqual(mock_fetch.call_count, 10)

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
            FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID,
        )
        self.assertEqual(
            str(ctx.exception), "missing_fire_notifications_s3_base"
        )

    async def test_missing_auth_env_raises_typed_configuration_failure(
        self,
    ) -> None:
        with patch.dict(
            os.environ,
            {"FIRE_NOTIFICATIONS_S3_BASE": "http://mock-s3-bucket"},
            clear=True,
        ):
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
            FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID,
        )
        self.assertEqual(
            str(ctx.exception), "missing_fire_notifications_auth_config"
        )

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector._MAX_CONSECUTIVE_FAILURES",
        1,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.client.FireNotificationsRestClient.fetch_file_list",
        new_callable=AsyncMock,
    )
    async def test_poll_http_statuses_raise_typed_failures(
        self,
        mock_fetch: AsyncMock,
    ) -> None:
        cases = [
            (
                400,
                FeedStatusReason.SYSTEM_SOURCE_CONFIGURATION_INVALID,
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
                FeedStatusReason.SYSTEM_SOURCE_CONFIGURATION_INVALID,
                "fn_api_http_404",
            ),
            (429, FeedStatusReason.SOURCE_RATE_LIMITED, "fn_api_http_429"),
            (503, FeedStatusReason.SOURCE_UNREACHABLE, "fn_api_http_503"),
        ]

        for status, expected_status_reason, expected_reason in cases:
            with self.subTest(status=status):
                mock_fetch.side_effect = FeedFailure(
                    expected_status_reason, expected_reason
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
        "backend.pipeline.ingestion.collectors.fire_notifications.client.FireNotificationsRestClient.fetch_file_list",
        new_callable=AsyncMock,
    )
    async def test_poll_malformed_json_raises_payload_failure(
        self,
        mock_fetch: AsyncMock,
    ) -> None:
        mock_fetch.side_effect = FeedFailure(
            FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
            "fn_api_payload_malformed: ValueError: bad json",
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
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
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
        "backend.pipeline.ingestion.collectors.fire_notifications.client.FireNotificationsRestClient.fetch_file_list",
        new_callable=AsyncMock,
    )
    async def test_successful_poll_resets_consecutive_failures(
        self, mock_fetch: AsyncMock, mock_sleep: AsyncMock
    ) -> None:
        mock_sleep.return_value = False

        resp_fail = Exception("Connection failure")
        resp_ok = []

        # 9 failures, 1 success, 2 failures, then we trigger shutdown to exit gracefully.
        mock_fetch.side_effect = [resp_fail] * 9 + [resp_ok] + [resp_fail] * 2

        async def sleep_side_effect(event, duration):
            if mock_sleep.call_count >= 13:
                self.shutdown.set()
            return False

        mock_sleep.side_effect = sleep_side_effect

        events = []
        async for event in collector.fire_notifications_collector(
            self.feed,  # type: ignore
            self.shutdown,
            "http://base",
            self.resources,
        ):
            events.append(event)

        # 9 failed polls, 1 successful empty poll (yields observation), 2 failed polls.
        # It shouldn't raise since failures reset at 10th poll.
        self.assertEqual(events, [SourceObservation()])
        self.assertEqual(mock_fetch.call_count, 12)

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.control_flow.sleep_or_cancel",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.client.FireNotificationsRestClient.fetch_file_list",
        new_callable=AsyncMock,
    )
    async def test_successful_poll_missing_files_yields_source_observation(
        self, mock_fetch: AsyncMock, mock_sleep: AsyncMock
    ) -> None:
        sleep_count = 0

        async def _stop_after_poll_sleep(*_args: object) -> None:
            nonlocal sleep_count
            sleep_count += 1
            if sleep_count >= 2:
                self.shutdown.set()

        mock_sleep.side_effect = _stop_after_poll_sleep
        mock_fetch.return_value = []

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
        "backend.pipeline.ingestion.collectors.fire_notifications.client.FireNotificationsRestClient.fetch_file_list",
        new_callable=AsyncMock,
    )
    async def test_present_non_list_files_raises_malformed_failure(
        self, mock_fetch: AsyncMock
    ) -> None:
        mock_fetch.side_effect = FeedFailure(
            FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
            "fn_api_payload_malformed",
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
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
        )
        self.assertEqual(str(ctx.exception), "fn_api_payload_malformed")

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.control_flow.sleep_or_cancel",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.client.FireNotificationsRestClient.download_audio",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.client.FireNotificationsRestClient.fetch_file_list",
        new_callable=AsyncMock,
    )
    async def test_repeated_seen_files_yield_source_observation(
        self,
        mock_fetch: AsyncMock,
        mock_download: AsyncMock,
        mock_sleep: AsyncMock,
    ) -> None:
        mock_download.return_value = b"mp3"
        repeated_file = FireNotificationsFile(
            uuid="uuid1",
            filename="CHAN 2026-05-20 12-00-00.mp3",
            start_time=datetime.datetime(
                2026, 5, 20, 12, 0, 0, tzinfo=datetime.UTC
            ),
            size=1000,
        )
        mock_fetch.side_effect = [[repeated_file], [repeated_file]]

        sleep_calls = 0

        async def sleep_side_effect(*args, **kwargs) -> bool:
            nonlocal sleep_calls
            sleep_calls += 1
            if sleep_calls >= 3:
                self.shutdown.set()
                return True
            return False

        mock_sleep.side_effect = sleep_side_effect

        events = []
        with patch(
            "backend.pipeline.ingestion.collectors.fire_notifications.collector.probe_audio_metadata",
            return_value=(1000, AudioMimeType.MPEG),
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
        "backend.pipeline.ingestion.collectors.fire_notifications.client.FireNotificationsRestClient.download_audio",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.client.FireNotificationsRestClient.fetch_file_list",
        new_callable=AsyncMock,
    )
    async def test_all_seen_files_yield_source_observation(
        self,
        mock_fetch: AsyncMock,
        mock_download: AsyncMock,
        mock_sleep: MagicMock,
    ) -> None:
        mock_download.return_value = b"mp3"

        first_payload = [
            FireNotificationsFile(
                uuid="uuid1",
                filename="CHAN 2026-05-20 12-00-00.mp3",
                start_time=datetime.datetime(
                    2026, 5, 20, 12, 0, 0, tzinfo=datetime.UTC
                ),
                size=1000,
            )
        ]
        second_payload = [
            FireNotificationsFile(
                uuid="uuid1",
                filename="CHAN 2026-05-20 12-00-00.mp3",
                start_time=datetime.datetime(
                    2026, 5, 20, 12, 0, 0, tzinfo=datetime.UTC
                ),
                size=1000,
            )
        ]
        mock_fetch.side_effect = [first_payload, second_payload]

        sleep_calls = 0

        async def sleep_side_effect(*args, **kwargs) -> bool:
            nonlocal sleep_calls
            sleep_calls += 1
            if sleep_calls >= 3:
                self.shutdown.set()
                return True
            return False

        mock_sleep.side_effect = sleep_side_effect

        events = []
        with patch(
            "backend.pipeline.ingestion.collectors.fire_notifications.collector.probe_audio_metadata",
            return_value=(1000, AudioMimeType.MPEG),
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
        "backend.pipeline.ingestion.collectors.fire_notifications.client.FireNotificationsRestClient.fetch_file_list",
        new_callable=AsyncMock,
    )
    async def test_polling_passes_authorization_header(
        self, mock_fetch: MagicMock, mock_sleep: AsyncMock
    ) -> None:
        sleep_count = 0

        async def _stop_after_poll_sleep(*_args: object) -> None:
            nonlocal sleep_count
            sleep_count += 1
            if sleep_count >= 2:
                self.shutdown.set()

        mock_sleep.side_effect = _stop_after_poll_sleep
        mock_fetch.return_value = []

        collector_generator = collector.fire_notifications_collector(
            self.feed,  # type: ignore
            self.shutdown,
            "http://base",
            self.resources,
        )

        async for _ in collector_generator:
            pass

        mock_fetch.assert_called_once_with(
            "CHAN",
            "feed-id",
            self.shutdown,
        )

    @patch.dict(
        "os.environ",
        {
            "FIRE_NOTIFICATIONS_S3_BASE": "http://mock-s3",
            "FIRE_NOTIFICATIONS_USER": "test-user",
            "FIRE_NOTIFICATIONS_PASSWORD": "test-password",
        },
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.probe_audio_metadata",
        return_value=(15000, AudioMimeType.MPEG),
    )
    async def test_bookmark_progression(self, mock_duration: MagicMock) -> None:
        feed = LeasedFeed(
            id=uuid.uuid4(),
            name="test-fn-feed",
            source_type=SourceType.FIRE_NOTIFICATIONS,
            last_processed_filename=None,
            last_bookmark_time=datetime.datetime(
                2026, 6, 15, 17, 30, tzinfo=datetime.UTC
            ),
            fencing_token=0,
            failure_count=0,
            status_reason=None,
            source_feed_id="RECORDINGS/SAN-JOSE-DISP",
        )

        shutdown_event = asyncio.Event()

        # Mock responses
        payload_ok = [
            FireNotificationsFile(
                uuid="uuid1",
                filename="SAN-JOSE-DISP 2026-06-15 17-37-43.mp3",
                start_time=datetime.datetime(
                    2026, 6, 15, 17, 37, 43, tzinfo=datetime.UTC
                ),
                size=1024,
            )
        ]

        # Keep track of sleep calls.
        sleep_count = 0

        async def mock_sleep_or_cancel(
            event: asyncio.Event, delay: float
        ) -> None:
            nonlocal sleep_count
            sleep_count += 1
            if sleep_count >= 3:
                event.set()

        # Patch fetch_file_list, download_audio and sleep_or_cancel
        with (
            patch(
                "backend.pipeline.ingestion.collectors.fire_notifications.client.FireNotificationsRestClient.fetch_file_list",
                return_value=payload_ok,
            ),
            patch(
                "backend.pipeline.ingestion.collectors.fire_notifications.client.FireNotificationsRestClient.download_audio",
                return_value=b"fake audio bytes",
            ),
            patch(
                "backend.pipeline.ingestion.collectors.control_flow.sleep_or_cancel",
                side_effect=mock_sleep_or_cancel,
            ),
        ):
            collector_iter = collector.fire_notifications_collector(
                feed,
                shutdown_event,
                "http://mock-api/",
                _default_resources(),
            )

            events = []
            async for event in collector_iter:
                events.append(event)

            # Iteration 1:
            # - Bookmark starts at 17:30
            # - Audio file at 17:37:43 is newer -> yields CapturedChunk
            # Iteration 2:
            # - Bookmark has progressed to 17:37:43
            # - Audio file at 17:37:43 is <= bookmark -> skipped, yields SourceObservation

            self.assertEqual(len(events), 2)
            chunk = events[0]
            self.assertIsInstance(chunk, CapturedChunk)
            assert isinstance(chunk, CapturedChunk)
            self.assertEqual(
                chunk.chunk_start_time,
                datetime.datetime(2026, 6, 15, 17, 37, 43, tzinfo=datetime.UTC),
            )
            self.assertIsInstance(events[1], SourceObservation)

            # Verify bookmark was advanced in the feed dictionary
            self.assertEqual(
                feed["last_bookmark_time"],
                datetime.datetime(2026, 6, 15, 17, 37, 43, tzinfo=datetime.UTC),
            )

    @patch.dict(
        "os.environ",
        {
            "FIRE_NOTIFICATIONS_S3_BASE": "http://mock-s3",
            "FIRE_NOTIFICATIONS_USER": "test-user",
            "FIRE_NOTIFICATIONS_PASSWORD": "test-password",
        },
    )
    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.probe_audio_metadata",
        return_value=(15000, AudioMimeType.MPEG),
    )
    async def test_bookmark_progression_with_duplicates(
        self, mock_duration: MagicMock
    ) -> None:
        feed = LeasedFeed(
            id=uuid.uuid4(),
            name="test-fn-feed",
            source_type=SourceType.FIRE_NOTIFICATIONS,
            last_processed_filename=None,
            last_bookmark_time=datetime.datetime(
                2026, 6, 15, 17, 30, tzinfo=datetime.UTC
            ),
            fencing_token=0,
            failure_count=0,
            status_reason=None,
            source_feed_id="RECORDINGS/SAN-JOSE-DISP",
        )

        shutdown_event = asyncio.Event()

        # Mock responses
        payload_1 = [
            FireNotificationsFile(
                uuid="uuid1",
                filename="SAN-JOSE-DISP 2026-06-15 17-37-43.mp3",
                start_time=datetime.datetime(
                    2026, 6, 15, 17, 37, 43, tzinfo=datetime.UTC
                ),
                size=1024,
            )
        ]
        payload_2 = [
            FireNotificationsFile(
                uuid="uuid2",
                filename="SAN-JOSE-DISP 2026-06-15 17-37-43.mp3",
                start_time=datetime.datetime(
                    2026, 6, 15, 17, 37, 43, tzinfo=datetime.UTC
                ),
                size=1024,
            )
        ]

        # Keep track of sleep calls.
        sleep_count = 0

        async def mock_sleep_or_cancel(
            event: asyncio.Event, delay: float
        ) -> None:
            nonlocal sleep_count
            sleep_count += 1
            if sleep_count >= 3:
                event.set()

        # Patch fetch_file_list, download_audio and sleep_or_cancel
        with (
            patch(
                "backend.pipeline.ingestion.collectors.fire_notifications.client.FireNotificationsRestClient.fetch_file_list",
                side_effect=[payload_1, payload_2, []],
            ),
            patch(
                "backend.pipeline.ingestion.collectors.fire_notifications.client.FireNotificationsRestClient.download_audio",
                return_value=b"fake audio bytes",
            ),
            patch(
                "backend.pipeline.ingestion.collectors.control_flow.sleep_or_cancel",
                side_effect=mock_sleep_or_cancel,
            ),
        ):
            collector_iter = collector.fire_notifications_collector(
                feed,
                shutdown_event,
                "http://mock-api/",
                _default_resources(),
            )

            events = []
            async for event in collector_iter:
                events.append(event)

            # Iteration 1:
            # - Bookmark starts at 17:30
            # - Audio file at 17:37:43 is newer -> yields CapturedChunk (uuid1)
            # Iteration 2:
            # - Bookmark has progressed to 17:37:43
            # - Audio file (uuid2) at 17:37:43 is <= bookmark -> skipped, yields SourceObservation

            self.assertEqual(len(events), 2)
            chunk = events[0]
            self.assertIsInstance(chunk, CapturedChunk)
            assert isinstance(chunk, CapturedChunk)
            self.assertEqual(
                chunk.chunk_start_time,
                datetime.datetime(2026, 6, 15, 17, 37, 43, tzinfo=datetime.UTC),
            )
            self.assertEqual(
                chunk.external_audio_segment_id,
                "uuid1|SAN-JOSE-DISP 2026-06-15 17-37-43.mp3",
            )
            self.assertIsInstance(events[1], SourceObservation)


class TestTimezoneResolution(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.session = MagicMock()
        self.client = FireNotificationsRestClient(
            session=self.session,
            url_base="http://mock-api",
            s3_base_url="http://mock-s3-bucket",
            user="test-user",
            password="test-password",
            timezone=ZoneInfo("UTC"),
        )

    def test_timezone_override_from_tags(self) -> None:
        # America/New_York (EDT) is UTC-4 in June.
        # 17:45:43 local -> 21:45:43 UTC
        client = FireNotificationsRestClient(
            session=self.session,
            url_base="http://mock-api",
            s3_base_url="http://mock-s3-bucket",
            user="test-user",
            password="test-password",
            timezone=ZoneInfo("America/New_York"),
        )
        dt = client._parse_filename_timestamp(
            "SAN-JOSE-DISP 2026-06-15 17-45-43.mp3",
        )
        self.assertEqual(
            dt,
            datetime.datetime(2026, 6, 15, 21, 45, 43, tzinfo=datetime.UTC),
        )

    def test_timezone_override_fallback_to_utc_if_no_tags(self) -> None:
        # Default is UTC (self.client has timezone = None)
        dt = self.client._parse_filename_timestamp(
            "SAN-JOSE-DISP 2026-06-15 17-45-43.mp3"
        )
        self.assertEqual(
            dt,
            datetime.datetime(2026, 6, 15, 17, 45, 43, tzinfo=datetime.UTC),
        )

    def test_collector_get_channel_timezone_valid(self) -> None:
        tz = collector._get_channel_timezone("America/New_York")
        self.assertEqual(tz, ZoneInfo("America/New_York"))

    def test_collector_get_channel_timezone_invalid_fallback_to_utc(
        self,
    ) -> None:
        tz = collector._get_channel_timezone("Invalid/Timezone_Name")
        self.assertEqual(tz, ZoneInfo("UTC"))

    def test_collector_get_channel_timezone_none_fallback_to_utc(self) -> None:
        tz = collector._get_channel_timezone(None)
        self.assertEqual(tz, ZoneInfo("UTC"))


if __name__ == "__main__":
    unittest.main()

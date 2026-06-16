from __future__ import annotations

import asyncio
import datetime
import unittest
import uuid
from typing import Any
from unittest import mock

from curl_cffi.requests import AsyncSession

from backend.pipeline.ingestion.collectors.fire_notifications.collector import (
    fire_notifications_collector,
)
from backend.pipeline.ingestion.models import (
    CapturedChunk,
    CaptureResources,
    SourceObservation,
)
from backend.pipeline.storage.feed_store import LeasedFeed, SourceType


class TestFireNotificationsCollector(unittest.IsolatedAsyncioTestCase):
    @mock.patch.dict(
        "os.environ",
        {
            "FIRE_NOTIFICATIONS_S3_BASE": "http://mock-s3",
            "FIRE_NOTIFICATIONS_USER": "test-user",
            "FIRE_NOTIFICATIONS_PASSWORD": "test-password",
        },
    )
    @mock.patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.collector.get_audio_duration",
        return_value=15000,
    )
    async def test_bookmark_progression(
        self, mock_duration: mock.MagicMock
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
        poll_resp = mock.MagicMock()
        poll_resp.status_code = 200
        poll_resp.json.return_value = {
            "files": [
                {
                    "type": "file",
                    "name": "SAN-JOSE-DISP 2026-06-15 17-37-43.mp3",
                    "uuid": "uuid1",
                    "size": 1024,
                }
            ],
            "directories": [],
        }

        download_resp = mock.MagicMock()
        download_resp.status_code = 200
        download_resp.content = b"fake audio bytes"

        # Mock session.get side effect
        async def mock_get(url: str, *args: Any, **kwargs: Any) -> Any:
            if "mock-api" in url:
                return poll_resp
            return download_resp

        mock_session = mock.AsyncMock(spec=AsyncSession)
        mock_session.get.side_effect = mock_get

        # Keep track of sleep calls. On the first sleep call, we shut down.
        sleep_count = 0

        async def mock_sleep_or_cancel(
            event: asyncio.Event, delay: float
        ) -> None:
            nonlocal sleep_count
            sleep_count += 1
            if sleep_count >= 2:
                event.set()

        # Patch AsyncSession and sleep_or_cancel
        with (
            mock.patch(
                "backend.pipeline.ingestion.collectors.fire_notifications.collector.AsyncSession",
                return_value=mock_session,
            ),
            mock.patch(
                "backend.pipeline.ingestion.collectors.control_flow.sleep_or_cancel",
                side_effect=mock_sleep_or_cancel,
            ),
        ):
            collector_iter = fire_notifications_collector(
                feed,
                shutdown_event,
                "http://mock-api/",
                CaptureResources(http_session=mock.AsyncMock()),
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


if __name__ == "__main__":
    unittest.main()

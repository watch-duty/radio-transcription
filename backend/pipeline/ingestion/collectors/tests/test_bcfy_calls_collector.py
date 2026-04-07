from __future__ import annotations

import asyncio
import datetime
import unittest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

from backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector import (
    capture_bcfy_calls,
)
from backend.pipeline.ingestion.models import CapturedChunk
from backend.pipeline.storage.feed_store import LeasedFeed, SourceType

_TEST_FEED = LeasedFeed(
    id=uuid.UUID("12345678-1234-5678-1234-567812345678"),
    name="test-bcfy-calls",
    source_type=SourceType.BCFY_CALLS,
    last_processed_filename=None,
    last_bookmark_time=None,
    fencing_token=1,
    source_feed_id="12345",
)


class TestBcfyCallsCollector(unittest.IsolatedAsyncioTestCase):
    @patch.dict(
        "os.environ",
        {
            "GOOGLE_CLOUD_PROJECT": "test-project",
            "BROADCASTIFY_JWT_SECRET_ID": "test-secret",
        },
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector.secretmanager.SecretManagerServiceClient"
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector.aiohttp.ClientSession"
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector.convert_to_flac"
    )
    async def test_yields_captured_chunk(
        self, mock_convert, mock_session, mock_secret_client
    ) -> None:
        # Mock Secret Manager
        mock_client_instance = mock_secret_client.return_value
        mock_response = MagicMock()
        mock_response.payload.data.decode.return_value = "fake-jwt"
        mock_client_instance.access_secret_version.return_value = mock_response

        # Mock aiohttp
        mock_session_instance = AsyncMock()
        mock_session.return_value.__aenter__.return_value = (
            mock_session_instance
        )

        # Mock API response
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json.return_value = [
            {
                "url": "https://calls.broadcastify.com/test.mp3",
                "start_ts": 1774544351,
                "end_ts": 1774544353,
            }
        ]

        # Mock audio download response
        mock_audio_resp = AsyncMock()
        mock_audio_resp.status = 200
        mock_audio_resp.read.return_value = b"fake-mp3-bytes"

        mock_session_instance.get = MagicMock()
        mock_cm1 = MagicMock()
        mock_cm1.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm2 = MagicMock()
        mock_cm2.__aenter__ = AsyncMock(return_value=mock_audio_resp)
        mock_session_instance.get.side_effect = [mock_cm1, mock_cm2]

        # Mock convert_to_flac
        mock_convert.return_value = b"fake-flac-bytes"

        shutdown = asyncio.Event()

        with patch(
            "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep:
            mock_sleep.side_effect = [None, asyncio.CancelledError]

            results = []
            try:
                async for chunk in capture_bcfy_calls(_TEST_FEED, shutdown, ""):
                    results.append(chunk)
                    shutdown.set()
                    break
            except asyncio.CancelledError:
                pass

        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], CapturedChunk)
        self.assertEqual(results[0].audio_bytes, b"fake-flac-bytes")
        self.assertEqual(
            results[0].chunk_start_time,
            datetime.datetime.fromtimestamp(1774544351, datetime.UTC),
        )
        self.assertEqual(
            results[0].chunk_end_time,
            datetime.datetime.fromtimestamp(1774544353, datetime.UTC),
        )

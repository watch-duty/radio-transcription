from __future__ import annotations

import asyncio
import datetime
import os
import unittest
import uuid
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp

from backend.pipeline.ingestion.collectors.bcfy_calls import (
    bcfy_calls_collector,
)
from backend.pipeline.storage.feed_store import LeasedFeed, SourceType


class TestSleepOrShutdown(unittest.IsolatedAsyncioTestCase):
    async def test_timeout(self) -> None:
        shutdown = asyncio.Event()
        res = await bcfy_calls_collector._sleep_or_shutdown(shutdown, 0.001)
        self.assertFalse(res)

    async def test_shutdown_set(self) -> None:
        shutdown = asyncio.Event()
        shutdown.set()
        res = await bcfy_calls_collector._sleep_or_shutdown(shutdown, 10.0)
        self.assertTrue(res)


class TestGetJwtToken(unittest.TestCase):
    @patch.dict(
        os.environ,
        {"GOOGLE_CLOUD_PROJECT": "proj", "BROADCASTIFY_JWT_SECRET_ID": "sec"},
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector.secretmanager.SecretManagerServiceClient"
    )
    def test_success(self, mock_smc: MagicMock) -> None:
        mock_client = MagicMock()
        mock_smc.return_value = mock_client
        mock_resp = MagicMock()
        mock_resp.payload.data.decode.return_value = " mytoken  "
        mock_client.access_secret_version.return_value = mock_resp

        token = bcfy_calls_collector._get_jwt_token()

        self.assertEqual(token, "mytoken")
        mock_client.access_secret_version.assert_called_once_with(
            request={"name": "projects/proj/secrets/sec/versions/latest"}
        )

    @patch.dict(os.environ, {}, clear=True)
    def test_missing_env(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "must be set"):
            bcfy_calls_collector._get_jwt_token()

    @patch.dict(
        os.environ,
        {"GOOGLE_CLOUD_PROJECT": "proj", "BROADCASTIFY_JWT_SECRET_ID": "sec"},
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector.secretmanager.SecretManagerServiceClient"
    )
    def test_gcp_error(self, mock_smc: MagicMock) -> None:
        mock_client = MagicMock()
        mock_smc.return_value = mock_client
        mock_client.access_secret_version.side_effect = Exception("API error")
        with self.assertRaisesRegex(RuntimeError, "Failed to access secret"):
            bcfy_calls_collector._get_jwt_token()


class TestRaiseForFatalStatus(unittest.TestCase):
    def test_fatal_statuses(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "Rate limited"):
            bcfy_calls_collector._raise_for_fatal_status(429, "fid", "sid")
        with self.assertRaisesRegex(RuntimeError, "Auth failure"):
            bcfy_calls_collector._raise_for_fatal_status(401, "fid", "sid")
        with self.assertRaisesRegex(RuntimeError, "Auth failure"):
            bcfy_calls_collector._raise_for_fatal_status(403, "fid", "sid")
        with self.assertRaisesRegex(RuntimeError, "Feed not found"):
            bcfy_calls_collector._raise_for_fatal_status(404, "fid", "sid")

    def test_non_fatal(self) -> None:
        bcfy_calls_collector._raise_for_fatal_status(200, "fid", "sid")
        bcfy_calls_collector._raise_for_fatal_status(500, "fid", "sid")


class TestFetchCallsBatch(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.session = MagicMock()
        self.shutdown = asyncio.Event()

    async def test_shutdown_is_set(self) -> None:
        self.shutdown.set()
        res = await bcfy_calls_collector._fetch_calls_batch(
            self.session, "url", {}, {}, "fid", "sid", self.shutdown
        )
        self.assertIsNone(res)

    async def test_success_list(self) -> None:
        resp = AsyncMock(status=200)
        resp.json.return_value = [{"call": 1}]
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=resp)
        cm.__aexit__ = AsyncMock(return_value=False)
        self.session.get.return_value = cm

        res = await bcfy_calls_collector._fetch_calls_batch(
            self.session, "url", {}, {}, "fid", "sid", self.shutdown
        )
        self.assertEqual(res, [{"call": 1}])

    async def test_success_dict(self) -> None:
        resp = AsyncMock(status=200)
        resp.json.return_value = {"call": 1}
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=resp)
        cm.__aexit__ = AsyncMock(return_value=False)
        self.session.get.return_value = cm

        res = await bcfy_calls_collector._fetch_calls_batch(
            self.session, "url", {}, {}, "fid", "sid", self.shutdown
        )
        self.assertEqual(res, [{"call": 1}])

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_5xx_retry_success(self, mock_sleep: AsyncMock) -> None:
        mock_sleep.return_value = False
        resp500 = AsyncMock(status=500)
        resp200 = AsyncMock(status=200)
        resp200.json.return_value = [{"call": 1}]

        self.session.get.side_effect = [
            MagicMock(__aenter__=AsyncMock(return_value=resp500)),
            MagicMock(__aenter__=AsyncMock(return_value=resp200)),
        ]

        res = await bcfy_calls_collector._fetch_calls_batch(
            self.session, "url", {}, {}, "fid", "sid", self.shutdown
        )
        self.assertEqual(res, [{"call": 1}])
        self.assertEqual(self.session.get.call_count, 2)
        mock_sleep.assert_called_once()

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_5xx_max_retries_fail(self, mock_sleep: AsyncMock) -> None:
        mock_sleep.return_value = False
        resp500 = AsyncMock(status=500)
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=resp500)
        cm.__aexit__ = AsyncMock(return_value=False)
        self.session.get.return_value = cm

        res = await bcfy_calls_collector._fetch_calls_batch(
            self.session, "url", {}, {}, "fid", "sid", self.shutdown
        )
        self.assertIsNone(res)
        self.assertEqual(
            self.session.get.call_count,
            bcfy_calls_collector._MAX_5XX_RETRIES + 1,
        )

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_5xx_retry_interrupted_by_shutdown(
        self, mock_sleep: AsyncMock
    ) -> None:
        mock_sleep.return_value = True
        resp500 = AsyncMock(status=500)
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=resp500)
        cm.__aexit__ = AsyncMock(return_value=False)
        self.session.get.return_value = cm

        res = await bcfy_calls_collector._fetch_calls_batch(
            self.session, "url", {}, {}, "fid", "sid", self.shutdown
        )
        self.assertIsNone(res)
        self.assertEqual(self.session.get.call_count, 1)

    async def test_other_non_200_status(self) -> None:
        resp = AsyncMock(status=400)
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=resp)
        cm.__aexit__ = AsyncMock(return_value=False)
        self.session.get.return_value = cm

        res = await bcfy_calls_collector._fetch_calls_batch(
            self.session, "url", {}, {}, "fid", "sid", self.shutdown
        )
        self.assertIsNone(res)


class TestDownloadAndConvertAudio(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.session = MagicMock()

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector.asyncio.to_thread",
        new_callable=AsyncMock,
    )
    async def test_success(self, mock_to_thread: AsyncMock) -> None:
        resp = AsyncMock(status=200)
        resp.read.return_value = b"mp3"
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=resp)
        cm.__aexit__ = AsyncMock(return_value=False)
        self.session.get.return_value = cm
        mock_to_thread.return_value = b"flac"

        res = await bcfy_calls_collector._download_and_convert_audio(
            self.session, "http://mp3"
        )
        self.assertEqual(res, b"flac")
        mock_to_thread.assert_called_once_with(
            bcfy_calls_collector.convert_to_flac, b"mp3", "mp3"
        )

    async def test_non_200_status(self) -> None:
        resp = AsyncMock(status=404)
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=resp)
        cm.__aexit__ = AsyncMock(return_value=False)
        self.session.get.return_value = cm

        res = await bcfy_calls_collector._download_and_convert_audio(
            self.session, "http://mp3"
        )
        self.assertIsNone(res)

    async def test_http_exception(self) -> None:
        self.session.get.side_effect = aiohttp.ClientError()

        res = await bcfy_calls_collector._download_and_convert_audio(
            self.session, "http://mp3"
        )
        self.assertIsNone(res)


class TestCaptureBcfyCalls(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.shutdown = asyncio.Event()
        self.feed = {
            "id": uuid.uuid4(),
            "name": "test-feed",
            "source_type": SourceType.BCFY_CALLS,
            "last_processed_filename": None,
            "last_bookmark_time": datetime.datetime(
                2026, 1, 1, tzinfo=datetime.UTC
            ),
            "fencing_token": 1,
            "source_feed_id": "sid123",
        }
        self.leased_feed = cast("LeasedFeed", self.feed)
        self.url_base = "http://base"

    async def test_missing_source_feed_id(self) -> None:
        self.feed["source_feed_id"] = None
        with self.assertRaisesRegex(ValueError, "missing source_feed_id"):
            async for _ in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed, self.shutdown, self.url_base
            ):
                pass

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._get_jwt_token"
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._fetch_calls_batch",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._download_and_convert_audio",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_success_and_pagination(
        self,
        mock_sleep: AsyncMock,
        mock_dl: AsyncMock,
        mock_fetch: AsyncMock,
        mock_jwt: MagicMock,
    ) -> None:
        mock_jwt.return_value = "token"
        mock_dl.return_value = b"flac"
        mock_fetch.return_value = [
            {
                "url": "http://1",
                "start_ts": 1000,
                "end_ts": 2000,
                "lastPos": 9999,
            }
        ]

        async def sleep_side_effect(*args, **kwargs) -> bool:
            self.shutdown.set()
            return True

        mock_sleep.side_effect = sleep_side_effect

        chunks = [
            c
            async for c in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed, self.shutdown, self.url_base
            )
        ]

        self.assertEqual(len(chunks), 1)
        chunk = chunks[0]
        self.assertEqual(chunk.audio_bytes, b"flac")
        self.assertEqual(chunk.chunk_start_time.timestamp(), 1000)
        self.assertEqual(chunk.chunk_end_time.timestamp(), 2000)

        mock_fetch.assert_called_once()
        params = mock_fetch.call_args[0][3]
        last_bookmark_time = cast(
            "datetime.datetime", self.feed["last_bookmark_time"]
        )
        self.assertEqual(params["pos"], int(last_bookmark_time.timestamp()))

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._get_jwt_token"
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._fetch_calls_batch",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._download_and_convert_audio",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_duplicate_urls_and_missing_ts(
        self,
        mock_sleep: AsyncMock,
        mock_dl: AsyncMock,
        mock_fetch: AsyncMock,
        mock_jwt: MagicMock,
    ) -> None:
        mock_jwt.return_value = "token"
        mock_dl.return_value = b"flac"

        mock_fetch.side_effect = [
            [
                {"url": "http://dup", "start_ts": None, "end_ts": None},
                {"url": "http://dup", "start_ts": 1, "end_ts": 2},
            ]
        ]

        async def sleep_side_effect(*args, **kwargs) -> bool:
            self.shutdown.set()
            return True

        mock_sleep.side_effect = sleep_side_effect

        chunks = [
            c
            async for c in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed, self.shutdown, self.url_base
            )
        ]

        self.assertEqual(len(chunks), 1)
        self.assertEqual(mock_dl.call_count, 1)

        now = datetime.datetime.now(datetime.UTC).timestamp()
        self.assertAlmostEqual(
            chunks[0].chunk_start_time.timestamp(), now, places=1
        )
        self.assertAlmostEqual(
            chunks[0].chunk_end_time.timestamp(), now, places=1
        )

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._get_jwt_token"
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._fetch_calls_batch",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._download_and_convert_audio",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_audio_processing_exceptions(
        self,
        mock_sleep: AsyncMock,
        mock_dl: AsyncMock,
        mock_fetch: AsyncMock,
        mock_jwt: MagicMock,
    ) -> None:
        mock_jwt.return_value = "token"

        mock_fetch.return_value = [
            {"url": "http://1"},
            {"url": "http://2"},
            {"url": "http://3", "start_ts": 10},
        ]

        mock_dl.side_effect = [Exception("error"), None, b"flac"]

        async def sleep_side_effect(*args, **kwargs) -> bool:
            self.shutdown.set()
            return True

        mock_sleep.side_effect = sleep_side_effect

        chunks = [
            c
            async for c in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed, self.shutdown, self.url_base
            )
        ]

        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0].audio_bytes, b"flac")
        self.assertEqual(mock_dl.call_count, 3)

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._get_jwt_token"
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._fetch_calls_batch",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_runtime_error_bubbles_up(
        self, mock_sleep: AsyncMock, mock_fetch: AsyncMock, mock_jwt: MagicMock
    ) -> None:
        mock_jwt.return_value = "token"
        mock_fetch.side_effect = RuntimeError("Fatal API Error")

        with self.assertRaisesRegex(RuntimeError, "Fatal API Error"):
            async for _ in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed, self.shutdown, self.url_base
            ):
                pass

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._get_jwt_token"
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._fetch_calls_batch",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_generic_exception_caught(
        self, mock_sleep: AsyncMock, mock_fetch: AsyncMock, mock_jwt: MagicMock
    ) -> None:
        mock_jwt.return_value = "token"
        mock_fetch.side_effect = ValueError("Some weird error")

        async def sleep_side_effect(*args, **kwargs) -> bool:
            self.shutdown.set()
            return True

        mock_sleep.side_effect = sleep_side_effect

        chunks = [
            c
            async for c in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed, self.shutdown, self.url_base
            )
        ]

        self.assertEqual(len(chunks), 0)
        mock_sleep.assert_called_once()

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._get_jwt_token"
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._fetch_calls_batch",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._download_and_convert_audio",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_shutdown_during_result_processing(
        self,
        mock_sleep: AsyncMock,
        mock_dl: AsyncMock,
        mock_fetch: AsyncMock,
        mock_jwt: MagicMock,
    ) -> None:
        mock_jwt.return_value = "token"
        mock_fetch.return_value = [{"url": "http://1"}, {"url": "http://2"}]

        async def dl_side_effect(session, url) -> bytes:
            self.shutdown.set()
            return b"flac"

        mock_dl.side_effect = dl_side_effect

        chunks = [
            c
            async for c in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed, self.shutdown, self.url_base
            )
        ]

        self.assertEqual(len(chunks), 1)
        self.assertEqual(mock_dl.call_count, 1)

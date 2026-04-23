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
        with self.assertRaisesRegex(
            bcfy_calls_collector.AuthError, "Auth failure"
        ):
            bcfy_calls_collector._raise_for_fatal_status(401, "fid", "sid")
        with self.assertRaisesRegex(
            bcfy_calls_collector.AuthError, "Auth failure"
        ):
            bcfy_calls_collector._raise_for_fatal_status(403, "fid", "sid")
        with self.assertRaisesRegex(RuntimeError, "Feed not found"):
            bcfy_calls_collector._raise_for_fatal_status(404, "fid", "sid")

    def test_non_fatal(self) -> None:
        bcfy_calls_collector._raise_for_fatal_status(200, "fid", "sid")
        bcfy_calls_collector._raise_for_fatal_status(500, "fid", "sid")


class TestFetchCalls(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.session = MagicMock()
        self.shutdown = asyncio.Event()

    async def test_shutdown_is_set(self) -> None:
        self.shutdown.set()
        res = await bcfy_calls_collector._fetch_calls(
            self.session, "url", {}, {}, "fid", "sid", self.shutdown
        )
        self.assertIsNone(res)

    async def test_success_list(self) -> None:
        resp = AsyncMock(status=200)
        resp.json.return_value = {"calls": [{"call": 1}]}
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=resp)
        cm.__aexit__ = AsyncMock(return_value=False)
        self.session.get.return_value = cm

        res = await bcfy_calls_collector._fetch_calls(
            self.session, "url", {}, {}, "fid", "sid", self.shutdown
        )
        self.assertEqual(res, {"calls": [{"call": 1}]})

    async def test_success_dict(self) -> None:
        resp = AsyncMock(status=200)
        resp.json.return_value = {"call": 1}
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=resp)
        cm.__aexit__ = AsyncMock(return_value=False)
        self.session.get.return_value = cm

        res = await bcfy_calls_collector._fetch_calls(
            self.session, "url", {}, {}, "fid", "sid", self.shutdown
        )
        self.assertEqual(res, {"call": 1})

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_5xx_retry_success(self, mock_sleep: AsyncMock) -> None:
        mock_sleep.return_value = False
        resp500 = AsyncMock(status=500)
        resp200 = AsyncMock(status=200)
        resp200.json.return_value = {"calls": [{"call": 1}]}

        self.session.get.side_effect = [
            MagicMock(
                __aenter__=AsyncMock(return_value=resp500),
                __aexit__=AsyncMock(return_value=False),
            ),
            MagicMock(
                __aenter__=AsyncMock(return_value=resp200),
                __aexit__=AsyncMock(return_value=False),
            ),
        ]

        res = await bcfy_calls_collector._fetch_calls(
            self.session, "url", {}, {}, "fid", "sid", self.shutdown
        )
        self.assertEqual(res, {"calls": [{"call": 1}]})
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

        res = await bcfy_calls_collector._fetch_calls(
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

        res = await bcfy_calls_collector._fetch_calls(
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

        res = await bcfy_calls_collector._fetch_calls(
            self.session, "url", {}, {}, "fid", "sid", self.shutdown
        )
        self.assertIsNone(res)


class TestGetAudioFormat(unittest.TestCase):
    def test_mp3_url(self) -> None:
        res = bcfy_calls_collector._get_audio_format(
            "http://example.com/audio.mp3"
        )
        self.assertEqual(res, "mp3")

    def test_m4a_url(self) -> None:
        res = bcfy_calls_collector._get_audio_format(
            "http://example.com/audio.m4a"
        )
        self.assertEqual(res, "m4a")

    def test_uppercase_extension(self) -> None:
        res = bcfy_calls_collector._get_audio_format(
            "http://example.com/audio.MP3"
        )
        self.assertEqual(res, "mp3")

    def test_no_extension_defaults_to_mp3(self) -> None:
        res = bcfy_calls_collector._get_audio_format("http://example.com/audio")
        self.assertEqual(res, "mp3")

    def test_url_without_dot_defaults_to_mp3(self) -> None:
        res = bcfy_calls_collector._get_audio_format("http://mp3")
        self.assertEqual(res, "mp3")

    def test_unknown_extension_defaults_to_mp3(self) -> None:
        res = bcfy_calls_collector._get_audio_format(
            "http://example.com/audio.php"
        )
        self.assertEqual(res, "mp3")


class TestDownloadAudio(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.session = MagicMock()
        self.shutdown = asyncio.Event()

    async def test_success(self) -> None:
        resp = AsyncMock(status=200)
        resp.read.return_value = b"mp3"
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=resp)
        cm.__aexit__ = AsyncMock(return_value=False)
        self.session.get.return_value = cm

        res = await bcfy_calls_collector._download_audio(
            self.session, "http://example.com/audio.mp3", self.shutdown
        )
        self.assertEqual(res, b"mp3")

    async def test_success_m4a(self) -> None:
        resp = AsyncMock(status=200)
        resp.read.return_value = b"m4a"
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=resp)
        cm.__aexit__ = AsyncMock(return_value=False)
        self.session.get.return_value = cm

        res = await bcfy_calls_collector._download_audio(
            self.session, "http://example.com/audio.m4a", self.shutdown
        )
        self.assertEqual(res, b"m4a")

    async def test_non_200_status(self) -> None:
        resp = AsyncMock(status=404)
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=resp)
        cm.__aexit__ = AsyncMock(return_value=False)
        self.session.get.return_value = cm

        res = await bcfy_calls_collector._download_audio(
            self.session, "http://mp3", self.shutdown
        )
        self.assertIsNone(res)

    async def test_http_exception(self) -> None:
        self.session.get.side_effect = aiohttp.ClientError()

        res = await bcfy_calls_collector._download_audio(
            self.session, "http://mp3", self.shutdown
        )
        self.assertIsNone(res)

    async def test_429_raises(self) -> None:
        resp = AsyncMock(status=429)
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=resp)
        cm.__aexit__ = AsyncMock(return_value=False)
        self.session.get.return_value = cm

        with self.assertRaisesRegex(RuntimeError, "rate limit"):
            await bcfy_calls_collector._download_audio(
                self.session, "http://mp3", self.shutdown
            )

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_5xx_retry_success(self, mock_sleep: AsyncMock) -> None:
        mock_sleep.return_value = False
        resp500 = AsyncMock(status=500)
        resp200 = AsyncMock(status=200)
        resp200.read.return_value = b"mp3"

        self.session.get.side_effect = [
            MagicMock(
                __aenter__=AsyncMock(return_value=resp500),
                __aexit__=AsyncMock(return_value=False),
            ),
            MagicMock(
                __aenter__=AsyncMock(return_value=resp200),
                __aexit__=AsyncMock(return_value=False),
            ),
        ]

        res = await bcfy_calls_collector._download_audio(
            self.session, "http://mp3", self.shutdown
        )

        self.assertEqual(res, b"mp3")
        self.assertEqual(self.session.get.call_count, 2)
        mock_sleep.assert_called_once()

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_5xx_max_retries_returns_none(
        self, mock_sleep: AsyncMock
    ) -> None:
        mock_sleep.return_value = False
        resp503 = AsyncMock(status=503)
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=resp503)
        cm.__aexit__ = AsyncMock(return_value=False)
        self.session.get.return_value = cm

        res = await bcfy_calls_collector._download_audio(
            self.session, "http://mp3", self.shutdown
        )
        self.assertIsNone(res)
        self.assertEqual(
            self.session.get.call_count,
            bcfy_calls_collector._AUDIO_FILE_DOWNLOAD_MAX_RETRIES + 1,
        )

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_5xx_retry_interrupted_by_shutdown(
        self, mock_sleep: AsyncMock
    ) -> None:
        mock_sleep.return_value = True
        resp502 = AsyncMock(status=502)
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=resp502)
        cm.__aexit__ = AsyncMock(return_value=False)
        self.session.get.return_value = cm

        res = await bcfy_calls_collector._download_audio(
            self.session, "http://mp3", self.shutdown
        )
        self.assertIsNone(res)
        self.assertEqual(self.session.get.call_count, 1)
        mock_sleep.assert_called_once()


class TestExtractCallsFromResponse(unittest.TestCase):
    def test_none_input(self) -> None:
        res = bcfy_calls_collector._extract_calls_from_response(None)
        self.assertEqual(res, [])

    def test_non_dict_input(self) -> None:
        res = bcfy_calls_collector._extract_calls_from_response(
            [{"url": "http://1"}]  # type: ignore
        )
        self.assertEqual(res, [])

    def test_missing_calls_key(self) -> None:
        res = bcfy_calls_collector._extract_calls_from_response(
            {"lastPos": 123}
        )
        self.assertEqual(res, [])

    def test_calls_value_is_not_list(self) -> None:
        res = bcfy_calls_collector._extract_calls_from_response(
            {"calls": "not-a-list"}
        )
        self.assertEqual(res, [])

    def test_empty_calls_list(self) -> None:
        res = bcfy_calls_collector._extract_calls_from_response({"calls": []})
        self.assertEqual(res, [])

    def test_valid_calls_list(self) -> None:
        calls = [{"url": "http://1"}, {"url": "http://2"}]
        res = bcfy_calls_collector._extract_calls_from_response(
            {"calls": calls}
        )
        self.assertEqual(res, calls)


class TestCreateChunkFromCall(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.session = MagicMock()
        self.shutdown = asyncio.Event()

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._download_audio",
        new_callable=AsyncMock,
    )
    async def test_success_with_timestamps(self, mock_dl: AsyncMock) -> None:
        mock_dl.return_value = b"flac"
        result = {"url": "http://1", "start_ts": 1000, "end_ts": 2000}

        chunk = await bcfy_calls_collector._create_chunk_from_call(
            self.session,
            result,
            "http://1",
            self.shutdown,
            "test-session",
            datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        )

        assert chunk is not None
        self.assertIsNotNone(chunk)
        self.assertEqual(chunk.audio_bytes, b"flac")
        self.assertEqual(chunk.chunk_start_time.timestamp(), 1000)
        self.assertEqual(chunk.chunk_end_time.timestamp(), 2000)

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._download_audio",
        new_callable=AsyncMock,
    )
    async def test_success_missing_timestamps_uses_now(
        self, mock_dl: AsyncMock
    ) -> None:
        mock_dl.return_value = b"flac"
        result = {"url": "http://1"}
        fixed_now = datetime.datetime(2026, 4, 9, 0, 0, 0, tzinfo=datetime.UTC)

        with patch(
            "backend.pipeline.ingestion.collectors.bcfy_calls"
            ".bcfy_calls_collector.datetime.datetime",
            wraps=datetime.datetime,
        ) as mock_datetime:
            mock_datetime.now.return_value = fixed_now
            chunk = await bcfy_calls_collector._create_chunk_from_call(
                self.session,
                result,
                "http://1",
                self.shutdown,
                "test-session",
                datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
            )

        assert chunk is not None
        self.assertIsNotNone(chunk)
        self.assertEqual(chunk.chunk_start_time, fixed_now)
        self.assertEqual(chunk.chunk_end_time, fixed_now)

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._download_audio",
        new_callable=AsyncMock,
    )
    async def test_download_returns_none_returns_none(
        self, mock_dl: AsyncMock
    ) -> None:
        mock_dl.return_value = None
        result = {"url": "http://1", "start_ts": 1000, "end_ts": 2000}

        chunk = await bcfy_calls_collector._create_chunk_from_call(
            self.session,
            result,
            "http://1",
            self.shutdown,
            "test-session",
            datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        )

        self.assertIsNone(chunk)

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._download_audio",
        new_callable=AsyncMock,
    )
    async def test_runtime_error_reraised(self, mock_dl: AsyncMock) -> None:
        mock_dl.side_effect = RuntimeError("CDN rate limit")
        result = {"url": "http://1"}

        with self.assertRaisesRegex(RuntimeError, "CDN rate limit"):
            await bcfy_calls_collector._create_chunk_from_call(
                self.session,
                result,
                "http://1",
                self.shutdown,
                "test-session",
                datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
            )

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._download_audio",
        new_callable=AsyncMock,
    )
    async def test_unexpected_exception_returns_none(
        self, mock_dl: AsyncMock
    ) -> None:
        mock_dl.side_effect = ValueError("unexpected")
        result = {"url": "http://1"}

        chunk = await bcfy_calls_collector._create_chunk_from_call(
            self.session,
            result,
            "http://1",
            self.shutdown,
            "test-session",
            datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        )

        self.assertIsNone(chunk)

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._download_audio",
        new_callable=AsyncMock,
    )
    async def test_receipt_time_preserved_through_chunk(
        self, mock_dl: AsyncMock
    ) -> None:
        """RCPT-04: receipt_time passed through to the yielded CapturedChunk."""
        mock_dl.return_value = b"flac"
        result = {"url": "http://1", "start_ts": 1000, "end_ts": 2000}
        rt = datetime.datetime(2026, 4, 22, 12, 0, 0, tzinfo=datetime.UTC)

        chunk = await bcfy_calls_collector._create_chunk_from_call(
            self.session,
            result,
            "http://1",
            self.shutdown,
            "test-session",
            rt,
        )

        assert chunk is not None
        self.assertEqual(chunk.receipt_time, rt)


class TestHandleLoopFailure(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.shutdown = asyncio.Event()

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_increments_failure_count(
        self, mock_sleep: AsyncMock
    ) -> None:
        mock_sleep.return_value = False
        result = await bcfy_calls_collector._handle_loop_failure(
            "fid", 0, self.shutdown
        )
        self.assertEqual(result, 1)

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_sleeps_with_poll_interval(
        self, mock_sleep: AsyncMock
    ) -> None:
        mock_sleep.return_value = False
        await bcfy_calls_collector._handle_loop_failure("fid", 0, self.shutdown)
        mock_sleep.assert_called_once_with(
            self.shutdown, bcfy_calls_collector._POLL_INTERVAL_SEC
        )

    async def test_raises_on_max_consecutive_failures(self) -> None:
        threshold = bcfy_calls_collector._MAX_CONSECUTIVE_FAILURES
        with self.assertRaisesRegex(RuntimeError, "consecutive failures"):
            await bcfy_calls_collector._handle_loop_failure(
                "fid", threshold - 1, self.shutdown
            )

    async def test_does_not_raise_below_max(self) -> None:
        threshold = bcfy_calls_collector._MAX_CONSECUTIVE_FAILURES
        # threshold - 2 increments to threshold - 1, which is still below max
        with patch(
            "backend.pipeline.ingestion.collectors.bcfy_calls"
            ".bcfy_calls_collector._sleep_or_shutdown",
            new_callable=AsyncMock,
        ) as mock_sleep:
            mock_sleep.return_value = False
            result = await bcfy_calls_collector._handle_loop_failure(
                "fid", threshold - 2, self.shutdown
            )
        self.assertEqual(result, threshold - 1)


class TestCaptureBcfyCalls(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.shutdown = asyncio.Event()
        self.feed = {
            "id": uuid.uuid4(),
            "name": "test-feed",
            "external_id": "ext-id",
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
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._fetch_calls",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._download_audio",
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

        fetch_calls = 0

        async def fetch_side_effect(*args, **kwargs):
            nonlocal fetch_calls
            fetch_calls += 1
            if fetch_calls == 1:
                return {
                    "calls": [
                        {
                            "url": "http://1",
                            "start_ts": 1000,
                            "end_ts": 2000,
                        }
                    ],
                    "lastPos": 9999,
                }
            self.shutdown.set()
            return {"calls": []}

        mock_fetch.side_effect = fetch_side_effect
        mock_sleep.return_value = False

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

        self.assertEqual(mock_fetch.call_count, 2)
        first_params = mock_fetch.call_args_list[0][0][3]
        second_params = mock_fetch.call_args_list[1][0][3]
        last_bookmark_time = cast(
            "datetime.datetime", self.feed["last_bookmark_time"]
        )
        self.assertEqual(
            first_params["pos"], int(last_bookmark_time.timestamp())
        )
        self.assertEqual(second_params["pos"], 9999)

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._get_jwt_token"
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._fetch_calls",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._download_audio",
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
            {
                "calls": [
                    {"url": "http://dup", "start_ts": None, "end_ts": None},
                    {"url": "http://dup", "start_ts": 1, "end_ts": 2},
                ]
            }
        ]

        async def sleep_side_effect(*args, **kwargs) -> bool:
            self.shutdown.set()
            return True

        mock_sleep.side_effect = sleep_side_effect

        fixed_now = datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)

        with patch(
            "backend.pipeline.ingestion.collectors.bcfy_calls"
            ".bcfy_calls_collector.datetime.datetime",
            wraps=datetime.datetime,
        ) as mock_datetime:
            mock_datetime.now.return_value = fixed_now
            chunks = [
                c
                async for c in bcfy_calls_collector.capture_bcfy_calls(
                    self.leased_feed, self.shutdown, self.url_base
                )
            ]

        self.assertEqual(len(chunks), 1)
        self.assertEqual(mock_dl.call_count, 1)
        self.assertEqual(chunks[0].chunk_start_time, fixed_now)
        self.assertEqual(chunks[0].chunk_end_time, fixed_now)

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._get_jwt_token"
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._fetch_calls",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._download_audio",
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

        mock_fetch.return_value = {
            "calls": [
                {"url": "http://1"},
                {"url": "http://2"},
                {"url": "http://3", "start_ts": 10},
            ]
        }

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
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._fetch_calls",
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
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._fetch_calls",
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
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._fetch_calls",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_auth_error_refreshes_token(
        self, mock_sleep: AsyncMock, mock_fetch: AsyncMock, mock_jwt: MagicMock
    ) -> None:
        mock_jwt.side_effect = ["old_token", "new_token"]
        mock_fetch.side_effect = [
            bcfy_calls_collector.AuthError("Auth failure"),
            {"calls": []},
        ]

        sleep_calls = 0

        async def sleep_side_effect(*args, **kwargs) -> bool:
            nonlocal sleep_calls
            sleep_calls += 1
            if sleep_calls == 1:
                return False
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
        self.assertEqual(mock_jwt.call_count, 2)
        self.assertEqual(mock_fetch.call_count, 2)

        second_fetch_args = mock_fetch.call_args_list[1]
        headers = second_fetch_args[0][2]
        self.assertEqual(headers["Authorization"], "Bearer new_token")

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._get_jwt_token"
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._fetch_calls",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._download_audio",
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
        mock_fetch.return_value = {
            "calls": [{"url": "http://1"}, {"url": "http://2"}]
        }

        async def dl_side_effect(session, url, shutdown) -> bytes:
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

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._get_jwt_token"
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._fetch_calls",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_max_consecutive_failures_generic_exception(
        self, mock_sleep: AsyncMock, mock_fetch: AsyncMock, mock_jwt: MagicMock
    ) -> None:
        mock_jwt.return_value = "token"
        mock_fetch.side_effect = ValueError("Persistent network glitch")
        mock_sleep.return_value = (
            False  # Simulate sleeping normally without shutdown
        )

        with self.assertRaisesRegex(
            RuntimeError, "exceeded 10 consecutive failures"
        ):
            async for _ in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed, self.shutdown, self.url_base
            ):
                pass

        self.assertEqual(mock_fetch.call_count, 10)

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._get_jwt_token"
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._fetch_calls",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_max_consecutive_failures_auth_error(
        self, mock_sleep: AsyncMock, mock_fetch: AsyncMock, mock_jwt: MagicMock
    ) -> None:
        mock_jwt.return_value = "token"
        mock_fetch.side_effect = bcfy_calls_collector.AuthError("Auth failure")
        mock_sleep.return_value = (
            False  # Simulate sleeping normally without shutdown
        )

        with self.assertRaisesRegex(
            RuntimeError, "exceeded 10 consecutive failures"
        ):
            async for _ in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed, self.shutdown, self.url_base
            ):
                pass

        self.assertEqual(mock_fetch.call_count, 10)

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._get_jwt_token"
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._fetch_calls",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._download_audio",
        new_callable=AsyncMock,
    )
    async def test_download_audio_runtime_error_reraised(
        self, mock_dl: AsyncMock, mock_fetch: AsyncMock, mock_jwt: MagicMock
    ) -> None:
        mock_jwt.return_value = "token"
        mock_fetch.return_value = {"calls": [{"url": "http://1"}]}
        mock_dl.side_effect = RuntimeError("CDN rate limit")

        with self.assertRaisesRegex(RuntimeError, "CDN rate limit"):
            async for _ in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed, self.shutdown, self.url_base
            ):
                pass

        self.assertEqual(mock_dl.call_count, 1)

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._get_jwt_token"
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._fetch_calls",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._download_audio",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_session_id_set_and_consistent(
        self,
        mock_sleep: AsyncMock,
        mock_dl: AsyncMock,
        mock_fetch: AsyncMock,
        mock_jwt: MagicMock,
    ) -> None:
        """All chunks from one capture_bcfy_calls call share the same session_id."""
        mock_jwt.return_value = "token"
        mock_dl.return_value = b"flac"

        fetch_calls = 0

        async def fetch_side_effect(*args, **kwargs):
            nonlocal fetch_calls
            fetch_calls += 1
            if fetch_calls == 1:
                return {
                    "calls": [
                        {"url": "http://1", "start_ts": 1000, "end_ts": 2000},
                        {"url": "http://2", "start_ts": 3000, "end_ts": 4000},
                    ],
                    "lastPos": 9999,
                }
            self.shutdown.set()
            return {"calls": []}

        mock_fetch.side_effect = fetch_side_effect
        mock_sleep.return_value = False

        chunks = [
            c
            async for c in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed, self.shutdown, self.url_base
            )
        ]

        self.assertEqual(len(chunks), 2)
        for chunk in chunks:
            self.assertIsNotNone(chunk.session_id)
        self.assertEqual(chunks[0].session_id, chunks[1].session_id)


class TestCaptureBcfyCallsReceiptTimeStamp(unittest.IsolatedAsyncioTestCase):
    """RCPT-04: capture_bcfy_calls stamps receipt_time per-call iteration."""

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._get_jwt_token",
        return_value="tok",
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._fetch_calls",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._download_audio",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector.datetime"
    )
    async def test_stamps_receipt_time_on_yielded_chunk(
        self,
        mock_datetime: MagicMock,
        mock_download: AsyncMock,
        mock_fetch: AsyncMock,
        _mock_jwt: MagicMock,
    ) -> None:
        fixed_time = datetime.datetime(
            2026, 4, 22, 12, 0, 0, tzinfo=datetime.UTC
        )
        mock_datetime.datetime.now.return_value = fixed_time
        mock_datetime.UTC = datetime.UTC
        mock_datetime.datetime.fromtimestamp = datetime.datetime.fromtimestamp
        mock_fetch.return_value = {
            "calls": [{"url": "http://a.mp3", "start_ts": 1000, "end_ts": 2000}]
        }
        mock_download.return_value = b"flac"

        feed = LeasedFeed(
            id=uuid.UUID("12345678-1234-5678-1234-567812345678"),
            name="test-bcfy-calls",
            external_id="ext-id",
            source_type=SourceType.BCFY_CALLS,
            last_processed_filename=None,
            last_bookmark_time=None,
            fencing_token=1,
            source_feed_id="sid",
        )
        shutdown = asyncio.Event()

        results = []
        async for chunk in bcfy_calls_collector.capture_bcfy_calls(
            cast("LeasedFeed", feed), shutdown, "https://api.example/"
        ):
            results.append(chunk)
            shutdown.set()

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].receipt_time, fixed_time)


class TestBcfyCallsCallDownloadFailedEmit(unittest.IsolatedAsyncioTestCase):
    """LOG-02: bcfy_calls emits call_download_failed at _create_chunk_from_call caller."""

    def setUp(self) -> None:
        self.feed_uuid = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        self.feed: dict[str, object] = {
            "id": self.feed_uuid,
            "name": "test-bcfy",
            "external_id": "ext-id",
            "source_type": SourceType.BCFY_CALLS,
            "last_processed_filename": None,
            "last_bookmark_time": None,
            "fencing_token": 1,
            "source_feed_id": "sid",
        }
        self.leased_feed = cast("LeasedFeed", self.feed)

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._get_jwt_token"
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._fetch_calls",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._create_chunk_from_call",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_emits_call_download_failed_on_terminal_failure(
        self,
        mock_sleep: AsyncMock,
        mock_create: AsyncMock,
        mock_fetch: AsyncMock,
        mock_jwt: MagicMock,
    ) -> None:
        """_create_chunk_from_call returns None → emit WARNING log."""
        mock_jwt.return_value = "tok"
        mock_create.return_value = None  # simulate download failure

        shutdown = asyncio.Event()

        fetch_calls = 0

        async def _fetch_side_effect(*args, **kwargs):
            nonlocal fetch_calls
            fetch_calls += 1
            if fetch_calls == 1:
                return {
                    "calls": [
                        {
                            "url": "https://x/c.mp3",
                            "start_ts": 1_700_000_000,
                            "end_ts": 1_700_000_010,
                        }
                    ],
                    "lastPos": 1_700_000_010,
                }
            shutdown.set()
            return {"calls": []}

        mock_fetch.side_effect = _fetch_side_effect
        mock_sleep.return_value = False

        with self.assertLogs(
            "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector",
            level="WARNING",
        ) as cm:
            async for _ in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed, shutdown, "https://api.bcfy/"
            ):
                pass

        emits = [
            r for r in cm.records if r.getMessage() == "Call download failed"
        ]
        self.assertEqual(len(emits), 1)
        rec = emits[0]
        self.assertEqual(rec.json_fields["event_type"], "call_download_failed")
        self.assertEqual(rec.json_fields["feed_id"], str(self.feed_uuid))
        self.assertEqual(
            rec.json_fields["source_type"], self.feed["source_type"]
        )
        # Golden match
        import json as _json  # noqa: PLC0415
        import pathlib as _pathlib  # noqa: PLC0415

        golden = _json.loads(
            (
                _pathlib.Path(__file__).resolve().parents[2]  # noqa: ASYNC240 -- sync file read in test is fine
                / "tests"
                / "golden"
                / "call_download_failed.json"
            ).read_text()
        )
        self.assertEqual(
            set(rec.json_fields.keys()), set(golden["expected_keys"])
        )

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._get_jwt_token"
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._fetch_calls",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._create_chunk_from_call",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_no_emit_on_successful_chunk_creation(
        self,
        mock_sleep: AsyncMock,
        mock_create: AsyncMock,
        mock_fetch: AsyncMock,
        mock_jwt: MagicMock,
    ) -> None:
        """_create_chunk_from_call returns a CapturedChunk → no emit."""
        mock_jwt.return_value = "tok"
        now = datetime.datetime.now(datetime.UTC)
        chunk_ok = bcfy_calls_collector.CapturedChunk(
            audio_bytes=b"x",
            chunk_start_time=now,
            chunk_end_time=now + datetime.timedelta(seconds=10),
            session_id="sid",
            receipt_time=now,
        )
        mock_create.return_value = chunk_ok

        shutdown = asyncio.Event()
        mock_fetch.return_value = {
            "calls": [
                {
                    "url": "https://x/c.mp3",
                    "start_ts": 1_700_000_000,
                    "end_ts": 1_700_000_010,
                }
            ],
            "lastPos": 1_700_000_010,
        }

        async def _sleep_side_effect(*args, **kwargs) -> bool:
            shutdown.set()
            return True

        mock_sleep.side_effect = _sleep_side_effect

        with self.assertLogs(
            "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector",
            level="WARNING",
        ) as cm:
            # Placeholder WARNING so assertLogs captures something regardless of emit.
            bcfy_calls_collector.logger.warning("_test_placeholder_")
            async for _ in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed, shutdown, "https://api.bcfy/"
            ):
                shutdown.set()

        emits = [
            r for r in cm.records if r.getMessage() == "Call download failed"
        ]
        self.assertEqual(emits, [])

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._get_jwt_token"
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._fetch_calls",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._create_chunk_from_call",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_no_emit_during_shutdown(
        self,
        mock_sleep: AsyncMock,
        mock_create: AsyncMock,
        mock_fetch: AsyncMock,
        mock_jwt: MagicMock,
    ) -> None:
        """Shutdown set while chunk creation fails → no emit."""
        mock_jwt.return_value = "tok"

        shutdown = asyncio.Event()

        async def _create_then_shut(*a, **kw):
            # shutdown set BEFORE create returns None to suppress emit
            shutdown.set()

        mock_create.side_effect = _create_then_shut
        mock_fetch.return_value = {
            "calls": [
                {
                    "url": "https://x/c.mp3",
                    "start_ts": 1_700_000_000,
                    "end_ts": 1_700_000_010,
                }
            ],
            "lastPos": 1_700_000_010,
        }
        mock_sleep.return_value = True

        with self.assertLogs(
            "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector",
            level="WARNING",
        ) as cm:
            # Placeholder WARNING so assertLogs captures something regardless of emit.
            bcfy_calls_collector.logger.warning("_test_placeholder_")
            async for _ in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed, shutdown, "https://api.bcfy/"
            ):
                pass

        emits = [
            r for r in cm.records if r.getMessage() == "Call download failed"
        ]
        self.assertEqual(emits, [])

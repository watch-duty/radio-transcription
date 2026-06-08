from __future__ import annotations

import asyncio
import datetime
import os
import threading
import time
import unittest
import uuid
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp

from backend.pipeline.ingestion.collectors.bcfy_calls import (
    bcfy_calls_collector,
)
from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemFailure,
)
from backend.pipeline.ingestion.collectors.tests.conftest import (
    _default_resources,
)
from backend.pipeline.ingestion.models import (
    AudioMimeType,
    FeedFailure,
    SourceObservation,
)
from backend.pipeline.storage.feed_store import (
    FeedStatusReason,
    LeasedFeed,
    SourceType,
)


def _require_item_failure(value: object) -> ItemFailure:
    """Return a typed item failure for tests that intentionally expect one."""
    if not isinstance(value, ItemFailure):
        msg = f"Expected ItemFailure, got {value!r}"
        raise TypeError(msg)
    return value


def _fetch_payload(
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Build the fetch payload expected by capture_bcfy_calls."""
    return payload


def _call_chunk(
    chunk: bcfy_calls_collector.CapturedChunk | None,
) -> bcfy_calls_collector._CallChunkResult:
    """Build the typed call result expected by capture_bcfy_calls."""
    return bcfy_calls_collector._CallChunkResult(chunk=chunk)


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
    def setUp(self) -> None:
        bcfy_calls_collector._reset_jwt_cache_for_tests()

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
        with self.assertRaises(FeedFailure) as ctx:
            bcfy_calls_collector._get_jwt_token()
        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        )
        self.assertEqual(str(ctx.exception), "calls_jwt_config_missing")

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
        with self.assertRaises(FeedFailure) as ctx:
            bcfy_calls_collector._get_jwt_token()
        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
        )
        self.assertEqual(str(ctx.exception), "calls_jwt_secret_access_failed")


class TestSharedJwtToken(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        bcfy_calls_collector._reset_jwt_cache_for_tests()

    async def asyncTearDown(self) -> None:
        bcfy_calls_collector._reset_jwt_cache_for_tests()

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._get_jwt_token"
    )
    async def test_concurrent_callers_share_one_fetch(
        self, mock_jwt: MagicMock
    ) -> None:
        def _slow_fetch() -> str:
            time.sleep(0.01)
            return "token"

        mock_jwt.side_effect = _slow_fetch

        tokens = await asyncio.gather(
            *[bcfy_calls_collector._get_shared_jwt_token() for _ in range(50)]
        )

        self.assertEqual(set(tokens), {"token"})
        self.assertEqual(mock_jwt.call_count, 1)

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._get_jwt_token"
    )
    async def test_cache_hit_does_not_fetch_again(
        self, mock_jwt: MagicMock
    ) -> None:
        mock_jwt.return_value = "token"

        first = await bcfy_calls_collector._get_shared_jwt_token()
        second = await bcfy_calls_collector._get_shared_jwt_token()

        self.assertEqual(first, "token")
        self.assertEqual(second, "token")
        self.assertEqual(mock_jwt.call_count, 1)

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._get_jwt_token"
    )
    async def test_concurrent_force_refresh_shares_one_fetch(
        self, mock_jwt: MagicMock
    ) -> None:
        mock_jwt.side_effect = ["old-token", "new-token"]
        old_token = await bcfy_calls_collector._get_shared_jwt_token()

        tokens = await asyncio.gather(
            *[
                bcfy_calls_collector._get_shared_jwt_token(
                    force_refresh=True,
                    stale_token=old_token,
                )
                for _ in range(50)
            ]
        )

        self.assertEqual(set(tokens), {"new-token"})
        self.assertEqual(mock_jwt.call_count, 2)

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._get_jwt_token"
    )
    async def test_stale_token_refresh_reuses_newer_cache(
        self, mock_jwt: MagicMock
    ) -> None:
        mock_jwt.side_effect = ["old-token", "new-token"]
        old_token = await bcfy_calls_collector._get_shared_jwt_token()
        refreshed = await bcfy_calls_collector._get_shared_jwt_token(
            force_refresh=True,
            stale_token=old_token,
        )
        reused = await bcfy_calls_collector._get_shared_jwt_token(
            force_refresh=True,
            stale_token=old_token,
        )

        self.assertEqual(refreshed, "new-token")
        self.assertEqual(reused, "new-token")
        self.assertEqual(mock_jwt.call_count, 2)

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._get_jwt_token"
    )
    async def test_failed_refresh_clears_in_flight_task(
        self, mock_jwt: MagicMock
    ) -> None:
        mock_jwt.side_effect = [
            RuntimeError("temporary secret failure"),
            "token",
        ]

        with self.assertRaisesRegex(RuntimeError, "temporary secret failure"):
            await bcfy_calls_collector._get_shared_jwt_token()

        token = await bcfy_calls_collector._get_shared_jwt_token()

        self.assertEqual(token, "token")
        self.assertEqual(mock_jwt.call_count, 2)

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._get_jwt_token"
    )
    async def test_cancelled_waiter_does_not_duplicate_fetch(
        self, mock_jwt: MagicMock
    ) -> None:
        started = asyncio.Event()
        proceed = threading.Event()
        loop = asyncio.get_running_loop()

        def _slow_fetch() -> str:
            loop.call_soon_threadsafe(started.set)
            self.assertTrue(proceed.wait(timeout=2.0))
            return "token"

        mock_jwt.side_effect = _slow_fetch

        task = asyncio.create_task(bcfy_calls_collector._get_shared_jwt_token())
        await started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        proceed.set()
        token = await bcfy_calls_collector._get_shared_jwt_token()

        self.assertEqual(token, "token")
        self.assertEqual(mock_jwt.call_count, 1)

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._get_jwt_token"
    )
    async def test_loop_change_drops_stale_refresh_task(
        self, mock_jwt: MagicMock
    ) -> None:
        old_loop = asyncio.new_event_loop()
        try:
            stale_task = cast("asyncio.Task[str]", old_loop.create_future())
            stale_task.cancel()
            bcfy_calls_collector._jwt_state.lock = asyncio.Lock()
            bcfy_calls_collector._jwt_state.lock_loop = old_loop
            bcfy_calls_collector._jwt_state.refresh_task = stale_task
        finally:
            old_loop.close()

        mock_jwt.return_value = "token"

        token = await bcfy_calls_collector._get_shared_jwt_token()

        self.assertEqual(token, "token")
        self.assertEqual(mock_jwt.call_count, 1)
        self.assertIsNone(bcfy_calls_collector._jwt_state.refresh_task)


class TestRaiseForFatalStatus(unittest.TestCase):
    def test_fatal_statuses(self) -> None:
        with self.assertRaises(FeedFailure) as auth_401:
            bcfy_calls_collector._raise_for_fatal_status(401, "fid", "sid")
        self.assertIs(
            auth_401.exception.status_reason,
            FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
        )
        self.assertEqual(str(auth_401.exception), "calls_api_http_401")

        with self.assertRaises(FeedFailure) as auth_403:
            bcfy_calls_collector._raise_for_fatal_status(403, "fid", "sid")
        self.assertIs(
            auth_403.exception.status_reason,
            FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
        )
        self.assertEqual(str(auth_403.exception), "calls_api_http_403")

        with self.assertRaises(FeedFailure) as missing:
            bcfy_calls_collector._raise_for_fatal_status(404, "fid", "sid")
        self.assertIs(
            missing.exception.status_reason,
            FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        )
        self.assertEqual(str(missing.exception), "calls_api_http_404")

    def test_non_fatal(self) -> None:
        bcfy_calls_collector._raise_for_fatal_status(200, "fid", "sid")
        bcfy_calls_collector._raise_for_fatal_status(429, "fid", "sid")
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
        failure = _require_item_failure(res)
        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(failure.reason, "calls_api_http_500")
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
        failure = _require_item_failure(res)
        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(failure.reason, "calls_api_http_400")

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_429_max_retries_returns_rate_limited_failure(
        self, mock_sleep: AsyncMock
    ) -> None:
        mock_sleep.return_value = False
        resp429 = AsyncMock(status=429)
        resp429.headers = {"Retry-After": "1"}
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=resp429)
        cm.__aexit__ = AsyncMock(return_value=False)
        self.session.get.return_value = cm

        res = await bcfy_calls_collector._fetch_calls(
            self.session, "url", {}, {}, "fid", "sid", self.shutdown
        )

        failure = _require_item_failure(res)
        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_RATE_LIMITED,
        )
        self.assertEqual(failure.reason, "calls_api_http_429")
        self.assertEqual(
            self.session.get.call_count,
            bcfy_calls_collector._MAX_5XX_RETRIES + 1,
        )


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
        failure = _require_item_failure(res)
        self.assertIs(
            failure.status_reason, FeedStatusReason.SOURCE_UNREACHABLE
        )
        self.assertEqual(failure.reason, "item_http_404")

    async def test_http_exception(self) -> None:
        self.session.get.side_effect = aiohttp.ClientError()

        res = await bcfy_calls_collector._download_audio(
            self.session, "http://mp3", self.shutdown
        )
        failure = _require_item_failure(res)
        self.assertIs(
            failure.status_reason, FeedStatusReason.SOURCE_UNREACHABLE
        )
        self.assertEqual(failure.reason, "audio_download_failed")

    async def test_429_retries_then_returns_rate_limited_failure(self) -> None:
        resp = AsyncMock(status=429)
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=resp)
        cm.__aexit__ = AsyncMock(return_value=False)
        self.session.get.return_value = cm

        with patch(
            "backend.pipeline.ingestion.collectors.bcfy_calls"
            ".bcfy_calls_collector._sleep_or_shutdown",
            new_callable=AsyncMock,
        ) as mock_sleep:
            mock_sleep.return_value = False
            res = await bcfy_calls_collector._download_audio(
                self.session, "http://mp3", self.shutdown
            )

        failure = _require_item_failure(res)
        self.assertIs(
            failure.status_reason, FeedStatusReason.SOURCE_RATE_LIMITED
        )
        self.assertEqual(failure.reason, "item_http_429")

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
        failure = _require_item_failure(res)
        self.assertIs(
            failure.status_reason, FeedStatusReason.SOURCE_UNREACHABLE
        )
        self.assertEqual(failure.reason, "item_http_503")
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

        result = await bcfy_calls_collector._create_chunk_from_call(
            self.session,
            result,
            "http://1",
            self.shutdown,
            "test-session",
            datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        )

        chunk = result.chunk
        assert chunk is not None
        self.assertIsNotNone(chunk)
        self.assertIsNone(result.failure)
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
            result = await bcfy_calls_collector._create_chunk_from_call(
                self.session,
                result,
                "http://1",
                self.shutdown,
                "test-session",
                datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
            )

        chunk = result.chunk
        assert chunk is not None
        self.assertIsNotNone(chunk)
        self.assertIsNone(result.failure)
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

        result = await bcfy_calls_collector._create_chunk_from_call(
            self.session,
            result,
            "http://1",
            self.shutdown,
            "test-session",
            datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        )

        self.assertIsNone(result.chunk)
        self.assertIsNone(result.failure)

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._download_audio",
        new_callable=AsyncMock,
    )
    async def test_runtime_error_returns_unreachable_failure(
        self, mock_dl: AsyncMock
    ) -> None:
        mock_dl.side_effect = RuntimeError("CDN rate limit")
        result = {"url": "http://1"}

        result = await bcfy_calls_collector._create_chunk_from_call(
            self.session,
            result,
            "http://1",
            self.shutdown,
            "test-session",
            datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        )

        self.assertIsNone(result.chunk)
        failure = _require_item_failure(result.failure)
        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(failure.reason, "audio_download_failed")

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

        result = await bcfy_calls_collector._create_chunk_from_call(
            self.session,
            result,
            "http://1",
            self.shutdown,
            "test-session",
            datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        )

        self.assertIsNone(result.chunk)
        failure = _require_item_failure(result.failure)
        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )

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

        result = await bcfy_calls_collector._create_chunk_from_call(
            self.session,
            result,
            "http://1",
            self.shutdown,
            "test-session",
            rt,
        )

        chunk = result.chunk
        assert chunk is not None
        self.assertEqual(chunk.receipt_time, rt)

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._download_audio",
        new_callable=AsyncMock,
    )
    async def test_create_chunk_captures_mime_type(
        self, mock_dl: AsyncMock
    ) -> None:
        mock_dl.return_value = b"mpeg_bytes"

        async def mock_dl_side_effect(
            session, audio_url, shutdown_event, out_headers=None
        ):
            if out_headers is not None:
                out_headers["Content-Type"] = "audio/mpeg"
            return b"mpeg_bytes"

        mock_dl.side_effect = mock_dl_side_effect
        result = {"url": "http://1", "start_ts": 1000, "end_ts": 2000}
        rt = datetime.datetime(2026, 4, 22, 12, 0, 0, tzinfo=datetime.UTC)

        result = await bcfy_calls_collector._create_chunk_from_call(
            self.session,
            result,
            "http://1",
            self.shutdown,
            "test-session",
            rt,
        )

        chunk = result.chunk
        assert chunk is not None
        self.assertEqual(chunk.mime_type, AudioMimeType.MPEG)


class TestHandleLoopFailure(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.shutdown = asyncio.Event()
        self.failure = ItemFailure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "calls_api_http_503",
        )

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
            "fid", 0, self.shutdown, self.failure
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
        await bcfy_calls_collector._handle_loop_failure(
            "fid", 0, self.shutdown, self.failure
        )
        mock_sleep.assert_called_once_with(
            self.shutdown, bcfy_calls_collector._POLL_INTERVAL_SEC
        )

    async def test_raises_on_max_consecutive_failures(self) -> None:
        threshold = bcfy_calls_collector._MAX_CONSECUTIVE_FAILURES
        with self.assertRaises(FeedFailure) as ctx:
            await bcfy_calls_collector._handle_loop_failure(
                "fid", threshold - 1, self.shutdown, self.failure
            )
        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(str(ctx.exception), "calls_api_http_503")

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
                "fid", threshold - 2, self.shutdown, self.failure
            )
        self.assertEqual(result, threshold - 1)


class TestCaptureBcfyCalls(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        bcfy_calls_collector._reset_jwt_cache_for_tests()
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
            "failure_count": 0,
            "status_reason": None,
            "source_feed_id": "sid123",
        }
        self.leased_feed = cast("LeasedFeed", self.feed)
        self.url_base = "http://base"

    async def test_missing_source_feed_id(self) -> None:
        self.feed["source_feed_id"] = None
        with self.assertRaises(FeedFailure) as ctx:
            async for _ in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed,
                self.shutdown,
                self.url_base,
                _default_resources(),
            ):
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        )
        self.assertEqual(str(ctx.exception), "missing_source_feed_id")

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
                return _fetch_payload(
                    {
                        "calls": [
                            {
                                "url": "http://1",
                                "start_ts": 1000,
                                "end_ts": 2000,
                            }
                        ],
                        "lastPos": 9999,
                    }
                )
            self.shutdown.set()
            return _fetch_payload({"calls": []})

        mock_fetch.side_effect = fetch_side_effect
        mock_sleep.return_value = False

        events = [
            c
            async for c in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed,
                self.shutdown,
                self.url_base,
                _default_resources(),
            )
        ]
        chunks = [
            e
            for e in events
            if isinstance(e, bcfy_calls_collector.CapturedChunk)
        ]
        observations = [
            e for e in events if isinstance(e, SourceObservation)
        ]

        self.assertEqual(len(chunks), 1)
        self.assertEqual(len(observations), 1)
        self.assertIsNone(observations[0].resume_position)
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
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_empty_success_yields_source_observation(
        self, mock_sleep: AsyncMock, mock_fetch: AsyncMock, mock_jwt: MagicMock
    ) -> None:
        mock_jwt.return_value = "token"

        async def _fetch_then_stop(*args, **kwargs):
            self.shutdown.set()
            return _fetch_payload({"calls": [], "lastPos": 1_700_000_010})

        mock_fetch.side_effect = _fetch_then_stop
        mock_sleep.return_value = True

        events = [
            e
            async for e in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed,
                self.shutdown,
                self.url_base,
                _default_resources(),
            )
        ]

        self.assertEqual(
            events,
            [
                SourceObservation(
                    resume_position=datetime.datetime.fromtimestamp(
                        1_700_000_010,
                        datetime.UTC,
                    )
                )
            ],
        )

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
            _fetch_payload(
                {
                    "calls": [
                        {"url": "http://dup", "start_ts": None, "end_ts": None},
                        {"url": "http://dup", "start_ts": 1, "end_ts": 2},
                    ]
                }
            )
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
                    self.leased_feed,
                    self.shutdown,
                    self.url_base,
                    _default_resources(),
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

        mock_fetch.return_value = _fetch_payload(
            {
                "calls": [
                    {"url": "http://1"},
                    {"url": "http://2"},
                    {"url": "http://3", "start_ts": 10},
                ]
            }
        )

        mock_dl.side_effect = [Exception("error"), None, b"flac"]

        async def sleep_side_effect(*args, **kwargs) -> bool:
            self.shutdown.set()
            return True

        mock_sleep.side_effect = sleep_side_effect

        chunks = [
            c
            async for c in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed,
                self.shutdown,
                self.url_base,
                _default_resources(),
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
    async def test_runtime_error_retries_as_source_unreachable(
        self, mock_sleep: AsyncMock, mock_fetch: AsyncMock, mock_jwt: MagicMock
    ) -> None:
        mock_jwt.return_value = "token"
        mock_fetch.side_effect = RuntimeError("Fatal API Error")
        mock_sleep.return_value = False

        with self.assertRaises(FeedFailure) as ctx:
            async for _ in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed,
                self.shutdown,
                self.url_base,
                _default_resources(),
            ):
                pass
        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(str(ctx.exception), "source_unreachable")
        self.assertEqual(
            mock_fetch.call_count,
            bcfy_calls_collector._MAX_CONSECUTIVE_FAILURES,
        )

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
                self.leased_feed,
                self.shutdown,
                self.url_base,
                _default_resources(),
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
            _fetch_payload({"calls": []}),
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

        events = [
            c
            async for c in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed,
                self.shutdown,
                self.url_base,
                _default_resources(),
            )
        ]

        self.assertEqual(events, [SourceObservation()])
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
        "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_concurrent_startup_uses_one_jwt_fetch(
        self, mock_sleep: AsyncMock, mock_fetch: AsyncMock, mock_jwt: MagicMock
    ) -> None:
        def _slow_fetch() -> str:
            time.sleep(0.01)
            return "token"

        async def _fetch_then_shutdown(*args, **kwargs):
            shutdown = args[6]
            shutdown.set()
            return _fetch_payload({"calls": []})

        mock_jwt.side_effect = _slow_fetch
        mock_fetch.side_effect = _fetch_then_shutdown
        mock_sleep.return_value = True

        async def _consume(feed_index: int) -> None:
            feed = dict(self.feed)
            feed["id"] = uuid.uuid4()
            feed["source_feed_id"] = f"sid{feed_index}"
            shutdown = asyncio.Event()
            async for _ in bcfy_calls_collector.capture_bcfy_calls(
                cast("LeasedFeed", feed),
                shutdown,
                self.url_base,
                _default_resources(),
            ):
                pass

        await asyncio.gather(*[_consume(i) for i in range(50)])

        self.assertEqual(mock_jwt.call_count, 1)
        self.assertEqual(mock_fetch.call_count, 50)

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
    async def test_auth_refresh_reuses_newer_cached_token(
        self, mock_sleep: AsyncMock, mock_fetch: AsyncMock, mock_jwt: MagicMock
    ) -> None:
        mock_jwt.side_effect = ["old-token", "new-token"]
        old_token = await bcfy_calls_collector._get_shared_jwt_token()

        async def _fetch_side_effect(*args, **kwargs):
            headers = args[2]
            if headers["Authorization"] == f"Bearer {old_token}":
                msg = "Auth failure"
                raise bcfy_calls_collector.AuthError(msg)
            self.shutdown.set()
            return _fetch_payload({"calls": []})

        await bcfy_calls_collector._get_shared_jwt_token(
            force_refresh=True,
            stale_token=old_token,
        )
        mock_fetch.side_effect = _fetch_side_effect
        mock_sleep.return_value = False

        events = [
            c
            async for c in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed,
                self.shutdown,
                self.url_base,
                _default_resources(),
            )
        ]

        self.assertEqual(events, [SourceObservation()])
        self.assertEqual(mock_jwt.call_count, 2)
        self.assertEqual(mock_fetch.call_count, 1)
        headers = mock_fetch.call_args_list[0][0][2]
        self.assertEqual(headers["Authorization"], "Bearer new-token")

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
        mock_fetch.return_value = _fetch_payload(
            {"calls": [{"url": "http://1"}, {"url": "http://2"}]}
        )

        async def dl_side_effect(*args, **kwargs) -> bytes:
            self.shutdown.set()
            return b"flac"

        mock_dl.side_effect = dl_side_effect

        chunks = [
            c
            async for c in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed,
                self.shutdown,
                self.url_base,
                _default_resources(),
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

        with self.assertRaises(FeedFailure) as ctx:
            async for _ in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed,
                self.shutdown,
                self.url_base,
                _default_resources(),
            ):
                pass
        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(str(ctx.exception), "source_unreachable")
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
    async def test_auth_refresh_secret_failures_retry_before_terminal_error(
        self, mock_sleep: AsyncMock, mock_fetch: AsyncMock, mock_jwt: MagicMock
    ) -> None:
        # AuthError triggers a token refresh; repeated Secret Manager failures
        # are retried while keeping the lease before surfacing a JWT-specific
        # terminal reason.
        mock_jwt.side_effect = [
            "token",
            *[Exception("secret unavailable")] * 10,
        ]
        mock_fetch.side_effect = bcfy_calls_collector.AuthError("Auth failure")
        mock_sleep.return_value = False

        with self.assertRaises(FeedFailure) as ctx:
            async for _ in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed,
                self.shutdown,
                self.url_base,
                _default_resources(),
            ):
                pass
        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
        )
        self.assertEqual(str(ctx.exception), "calls_jwt_secret_access_failed")
        self.assertEqual(mock_fetch.call_count, 1)
        self.assertEqual(mock_jwt.call_count, 11)

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
    async def test_persistent_fetch_rate_limit_raises_typed_failure(
        self, mock_sleep: AsyncMock, mock_fetch: AsyncMock, mock_jwt: MagicMock
    ) -> None:
        mock_jwt.return_value = "token"
        mock_sleep.return_value = False
        mock_fetch.return_value = ItemFailure(
            FeedStatusReason.SOURCE_RATE_LIMITED,
            "calls_api_http_429",
        )

        with self.assertRaises(FeedFailure) as ctx:
            async for _ in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed,
                self.shutdown,
                self.url_base,
                _default_resources(),
            ):
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SOURCE_RATE_LIMITED,
        )
        self.assertEqual(str(ctx.exception), "calls_api_http_429")
        self.assertEqual(
            mock_fetch.call_count,
            bcfy_calls_collector._MAX_CONSECUTIVE_FAILURES,
        )

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
    async def test_persistent_fetch_unreachable_raises_typed_failure(
        self, mock_sleep: AsyncMock, mock_fetch: AsyncMock, mock_jwt: MagicMock
    ) -> None:
        mock_jwt.return_value = "token"
        mock_sleep.return_value = False
        mock_fetch.return_value = ItemFailure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "calls_api_http_503",
        )

        with self.assertRaises(FeedFailure) as ctx:
            async for _ in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed,
                self.shutdown,
                self.url_base,
                _default_resources(),
            ):
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(str(ctx.exception), "calls_api_http_503")

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
    async def test_all_page_items_same_failure_promotes_that_reason(
        self,
        mock_create: AsyncMock,
        mock_fetch: AsyncMock,
        mock_jwt: MagicMock,
    ) -> None:
        mock_jwt.return_value = "token"
        mock_fetch.return_value = {
            "calls": [{"url": "http://1"}, {"url": "http://2"}]
        }
        mock_create.return_value = bcfy_calls_collector._CallChunkResult(
            failure=ItemFailure(
                FeedStatusReason.SOURCE_UNREACHABLE,
                "audio_download_failed",
            )
        )

        with self.assertRaises(FeedFailure) as ctx:
            async for _ in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed,
                self.shutdown,
                self.url_base,
                _default_resources(),
            ):
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(str(ctx.exception), "audio_download_failed")

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
    async def test_all_page_items_mixed_failures_promote_collector_error(
        self,
        mock_create: AsyncMock,
        mock_fetch: AsyncMock,
        mock_jwt: MagicMock,
    ) -> None:
        mock_jwt.return_value = "token"
        mock_fetch.return_value = {
            "calls": [{"url": "http://1"}, {"url": "http://2"}]
        }
        mock_create.side_effect = [
            bcfy_calls_collector._CallChunkResult(
                failure=ItemFailure(
                    FeedStatusReason.SOURCE_UNREACHABLE,
                    "audio_download_failed",
                )
            ),
            bcfy_calls_collector._CallChunkResult(
                failure=ItemFailure(
                    FeedStatusReason.SOURCE_RATE_LIMITED,
                    "item_http_429",
                )
            ),
        ]

        with self.assertRaises(FeedFailure) as ctx:
            async for _ in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed,
                self.shutdown,
                self.url_base,
                _default_resources(),
            ):
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(str(ctx.exception), "mixed_item_failures")

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
    async def test_one_page_item_success_prevents_feed_level_promotion(
        self,
        mock_sleep: AsyncMock,
        mock_create: AsyncMock,
        mock_fetch: AsyncMock,
        mock_jwt: MagicMock,
    ) -> None:
        mock_jwt.return_value = "token"
        now = datetime.datetime.now(datetime.UTC)
        chunk_ok = bcfy_calls_collector.CapturedChunk(
            audio_bytes=b"x",
            chunk_start_time=now,
            chunk_end_time=now,
            session_id="sid",
            receipt_time=now,
        )
        mock_fetch.return_value = {
            "calls": [{"url": "http://1"}, {"url": "http://2"}]
        }
        mock_create.side_effect = [
            bcfy_calls_collector._CallChunkResult(
                failure=ItemFailure(
                    FeedStatusReason.SOURCE_UNREACHABLE,
                    "audio_download_failed",
                )
            ),
            bcfy_calls_collector._CallChunkResult(chunk=chunk_ok),
        ]

        async def _sleep_then_stop(*args, **kwargs) -> bool:
            self.shutdown.set()
            return True

        mock_sleep.side_effect = _sleep_then_stop

        chunks = [
            c
            async for c in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed,
                self.shutdown,
                self.url_base,
                _default_resources(),
            )
        ]

        self.assertEqual(chunks, [chunk_ok])

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
    async def test_download_audio_runtime_error_promotes_item_failure(
        self, mock_dl: AsyncMock, mock_fetch: AsyncMock, mock_jwt: MagicMock
    ) -> None:
        mock_jwt.return_value = "token"
        mock_fetch.return_value = _fetch_payload(
            {"calls": [{"url": "http://1"}]}
        )
        mock_dl.side_effect = RuntimeError("CDN rate limit")

        with self.assertRaises(FeedFailure) as ctx:
            async for _ in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed,
                self.shutdown,
                self.url_base,
                _default_resources(),
            ):
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(str(ctx.exception), "audio_download_failed")
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
                return _fetch_payload(
                    {
                        "calls": [
                            {
                                "url": "http://1",
                                "start_ts": 1000,
                                "end_ts": 2000,
                            },
                            {
                                "url": "http://2",
                                "start_ts": 3000,
                                "end_ts": 4000,
                            },
                        ],
                        "lastPos": 9999,
                    }
                )
            self.shutdown.set()
            return _fetch_payload({"calls": []})

        mock_fetch.side_effect = fetch_side_effect
        mock_sleep.return_value = False

        events = [
            c
            async for c in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed,
                self.shutdown,
                self.url_base,
                _default_resources(),
            )
        ]
        chunks = [
            e
            for e in events
            if isinstance(e, bcfy_calls_collector.CapturedChunk)
        ]

        self.assertEqual(len(chunks), 2)
        for chunk in chunks:
            self.assertIsNotNone(chunk.session_id)
        self.assertEqual(chunks[0].session_id, chunks[1].session_id)


class TestCaptureBcfyCallsReceiptTimeStamp(unittest.IsolatedAsyncioTestCase):
    """RCPT-04: capture_bcfy_calls stamps receipt_time per-call iteration."""

    def setUp(self) -> None:
        bcfy_calls_collector._reset_jwt_cache_for_tests()

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
        mock_fetch.return_value = _fetch_payload(
            {
                "calls": [
                    {
                        "url": "http://a.mp3",
                        "start_ts": 1000,
                        "end_ts": 2000,
                    }
                ]
            }
        )
        mock_download.return_value = b"flac"

        feed = LeasedFeed(
            id=uuid.UUID("12345678-1234-5678-1234-567812345678"),
            name="test-bcfy-calls",
            source_type=SourceType.BCFY_CALLS,
            last_processed_filename=None,
            last_bookmark_time=None,
            fencing_token=1,
            failure_count=0,
            status_reason=None,
            source_feed_id="sid",
        )
        shutdown = asyncio.Event()

        results = []
        async for chunk in bcfy_calls_collector.capture_bcfy_calls(
            feed,
            shutdown,
            "https://api.example/",
            _default_resources(),
        ):
            results.append(chunk)
            shutdown.set()

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].receipt_time, fixed_time)


class TestBcfyCallsCallDownloadFailedEmit(unittest.IsolatedAsyncioTestCase):
    """LOG-02: bcfy_calls emits call_download_failed at _create_chunk_from_call caller."""

    def setUp(self) -> None:
        bcfy_calls_collector._reset_jwt_cache_for_tests()
        self.feed_uuid = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        self.feed: dict[str, object] = {
            "id": self.feed_uuid,
            "name": "test-bcfy",
            "source_type": SourceType.BCFY_CALLS,
            "last_processed_filename": None,
            "last_bookmark_time": None,
            "fencing_token": 1,
            "failure_count": 0,
            "status_reason": None,
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
        mock_create.return_value = bcfy_calls_collector._CallChunkResult()

        shutdown = asyncio.Event()

        fetch_calls = 0

        async def _fetch_side_effect(*args, **kwargs):
            nonlocal fetch_calls
            fetch_calls += 1
            if fetch_calls == 1:
                return _fetch_payload(
                    {
                        "calls": [
                            {
                                "url": "https://x/c.mp3",
                                "start_ts": 1_700_000_000,
                                "end_ts": 1_700_000_010,
                            }
                        ],
                        "lastPos": 1_700_000_010,
                    }
                )
            shutdown.set()
            return _fetch_payload({"calls": []})

        mock_fetch.side_effect = _fetch_side_effect
        mock_sleep.return_value = False

        with self.assertLogs(
            "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector",
            level="WARNING",
        ) as cm:
            async for _ in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed,
                shutdown,
                "https://api.bcfy/",
                _default_resources(),
            ):
                pass

        emits = [
            r for r in cm.records if r.getMessage() == "Call download failed"
        ]
        self.assertEqual(len(emits), 1)
        rec = cast("Any", emits[0])
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
            set(rec.json_fields.keys()),
            set(golden["expected_keys"]),
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
        mock_create.return_value = _call_chunk(chunk_ok)

        shutdown = asyncio.Event()
        mock_fetch.return_value = _fetch_payload(
            {
                "calls": [
                    {
                        "url": "https://x/c.mp3",
                        "start_ts": 1_700_000_000,
                        "end_ts": 1_700_000_010,
                    }
                ],
                "lastPos": 1_700_000_010,
            }
        )

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
                self.leased_feed,
                shutdown,
                "https://api.bcfy/",
                _default_resources(),
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
            return bcfy_calls_collector._CallChunkResult()

        mock_create.side_effect = _create_then_shut
        mock_fetch.return_value = _fetch_payload(
            {
                "calls": [
                    {
                        "url": "https://x/c.mp3",
                        "start_ts": 1_700_000_000,
                        "end_ts": 1_700_000_010,
                    }
                ],
                "lastPos": 1_700_000_010,
            }
        )
        mock_sleep.return_value = True

        with self.assertLogs(
            "backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector",
            level="WARNING",
        ) as cm:
            # Placeholder WARNING so assertLogs captures something regardless of emit.
            bcfy_calls_collector.logger.warning("_test_placeholder_")
            async for _ in bcfy_calls_collector.capture_bcfy_calls(
                self.leased_feed,
                shutdown,
                "https://api.bcfy/",
                _default_resources(),
            ):
                pass

        emits = [
            r for r in cm.records if r.getMessage() == "Call download failed"
        ]
        self.assertEqual(emits, [])


class TestBcfyCallsHttp01(unittest.IsolatedAsyncioTestCase):
    """HTTP-01: capture_bcfy_calls must NOT construct aiohttp.ClientSession.

    The runtime-owned session is passed in via CaptureResources.http_session.
    Per D-04/D-05 (Phase 3 CONTEXT), the collector reuses the runtime session
    instead of opening a new one per poll. This is the Pitfall 3 fix from
    research/PITFALLS.md.
    """

    def setUp(self) -> None:
        bcfy_calls_collector._reset_jwt_cache_for_tests()
        self.feed_uuid = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        self.feed: dict[str, object] = {
            "id": self.feed_uuid,
            "name": "test-bcfy-http01",
            "source_type": SourceType.BCFY_CALLS,
            "last_processed_filename": None,
            "last_bookmark_time": None,
            "fencing_token": 1,
            "failure_count": 0,
            "status_reason": None,
            "source_feed_id": "sid",
        }
        self.leased_feed = cast("LeasedFeed", self.feed)

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._get_jwt_token"
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._fetch_calls",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector.aiohttp.ClientSession"
    )
    async def test_no_per_poll_session_construction(
        self,
        mock_session_ctor: MagicMock,
        mock_sleep: AsyncMock,
        mock_fetch: AsyncMock,
        mock_jwt: MagicMock,
    ) -> None:
        """capture_bcfy_calls must reuse resources.http_session, never construct."""
        mock_jwt.return_value = "token"
        shutdown = asyncio.Event()

        async def _fetch_then_shut(*args, **kwargs):
            shutdown.set()
            return _fetch_payload({"calls": []})

        mock_fetch.side_effect = _fetch_then_shut
        mock_sleep.return_value = True

        # Build a CaptureResources whose http_session is a distinct mock
        # we can verify was forwarded to _fetch_calls.
        # NB: do NOT pass spec=aiohttp.ClientSession here — the @patch
        # decorator above replaces aiohttp.ClientSession with a MagicMock
        # during this test, and AsyncMock rejects a Mock-typed spec
        # ("Cannot spec a Mock object"). The identity-only assertion below
        # does not require a spec.
        runtime_session = AsyncMock()
        resources = bcfy_calls_collector.CaptureResources(
            http_session=runtime_session,
        )

        async for _ in bcfy_calls_collector.capture_bcfy_calls(
            self.leased_feed,
            shutdown,
            "https://api.bcfy/",
            resources,
        ):
            pass

        # HTTP-01 assertion: the bcfy_calls module never constructed
        # an aiohttp.ClientSession. The runtime-owned session is
        # consumed via resources.http_session.
        self.assertEqual(
            mock_session_ctor.call_count,
            0,
            "HTTP-01: capture_bcfy_calls constructed a new "
            "aiohttp.ClientSession; expected 0 constructions "
            "(must reuse resources.http_session per D-04).",
        )

        # Sanity: the runtime-owned session was forwarded to _fetch_calls
        # (positional arg 0) — proves the wiring is correct, not just absent.
        self.assertGreaterEqual(mock_fetch.call_count, 1)
        forwarded_session = mock_fetch.call_args_list[0][0][0]
        self.assertIs(
            forwarded_session,
            runtime_session,
            "HTTP-01: _fetch_calls did not receive resources.http_session; "
            "runtime-owned session is not being forwarded.",
        )


class TestCreateChunkFromCallResumePosition(unittest.IsolatedAsyncioTestCase):
    """resume_position cursor derivation in _create_chunk_from_call.

    The bcfy_calls duplicate-ingestion fix: the feed resume cursor must be
    the call's own API index time `ts`, never its audio `end_ts`. The API
    filters `ts > pos` and `end_ts < ts`, so an end_ts-valued cursor
    re-fetches the boundary call on every lease handoff.
    """

    def setUp(self) -> None:
        self.session = MagicMock()
        self.shutdown = asyncio.Event()

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._download_audio",
        new_callable=AsyncMock,
    )
    async def test_resume_position_is_call_ts_not_end_ts(
        self, mock_dl: AsyncMock
    ) -> None:
        """resume_position = fromtimestamp(ts), distinct from chunk_end_time."""
        mock_dl.return_value = b"flac"
        # `ts` deliberately a few seconds past `end_ts`, as the live API does.
        result = {
            "url": "http://1",
            "start_ts": 1000,
            "end_ts": 2000,
            "ts": 2002,
        }

        result = await bcfy_calls_collector._create_chunk_from_call(
            self.session,
            result,
            "http://1",
            self.shutdown,
            "test-session",
            datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        )

        chunk = result.chunk
        assert chunk is not None
        self.assertEqual(
            chunk.resume_position,
            datetime.datetime.fromtimestamp(2002, datetime.UTC),
        )
        self.assertEqual(
            chunk.chunk_end_time,
            datetime.datetime.fromtimestamp(2000, datetime.UTC),
        )
        # The resume cursor is the API index time, NOT the audio end time.
        self.assertNotEqual(chunk.resume_position, chunk.chunk_end_time)

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._download_audio",
        new_callable=AsyncMock,
    )
    async def test_missing_ts_sets_none_and_logs_warning(
        self, mock_dl: AsyncMock
    ) -> None:
        """A call with no `ts` → resume_position is None AND a warning logged.

        A missing pagination key signals API contract drift — the runtime's
        `or` fallback to chunk_end_time keeps the chunk ingested (dup-biased,
        never lost), and the warning surfaces the drift.
        """
        mock_dl.return_value = b"flac"
        result = {"url": "http://1", "start_ts": 1000, "end_ts": 2000}

        with self.assertLogs(
            "backend.pipeline.ingestion.collectors.bcfy_calls"
            ".bcfy_calls_collector",
            level="WARNING",
        ) as cm:
            result = await bcfy_calls_collector._create_chunk_from_call(
                self.session,
                result,
                "http://1",
                self.shutdown,
                "test-session",
                datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
            )

        chunk = result.chunk
        assert chunk is not None
        self.assertIsNone(chunk.resume_position)
        missing = [
            r
            for r in cm.records
            if getattr(r, "json_fields", {}).get("event_type")
            == "bcfy_calls_missing_ts"
        ]
        self.assertEqual(len(missing), 1)


class TestCaptureBcfyCallsResumePosition(unittest.IsolatedAsyncioTestCase):
    """capture_bcfy_calls page-sort and cross-lease resume behavior."""

    def setUp(self) -> None:
        bcfy_calls_collector._reset_jwt_cache_for_tests()
        self.shutdown = asyncio.Event()
        self.feed: dict[str, Any] = {
            "id": uuid.uuid4(),
            "name": "test-feed",
            "source_type": SourceType.BCFY_CALLS,
            "last_processed_filename": None,
            "last_bookmark_time": None,
            "fencing_token": 1,
            "failure_count": 0,
            "status_reason": None,
            "source_feed_id": "sid123",
        }
        self.url_base = "http://base"

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._get_jwt_token"
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
        ".bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_page_sorted_by_ts_before_processing(
        self,
        mock_sleep: AsyncMock,
        mock_dl: AsyncMock,
        mock_fetch: AsyncMock,
        mock_jwt: MagicMock,
    ) -> None:
        """A page out of `ts` order is sorted ascending before yielding.

        The per-call cursor must advance monotonically; the sort bounds
        data-loss on a mid-page crash to the accepted tie case.
        """
        mock_jwt.return_value = "token"
        mock_dl.return_value = b"flac"
        # Page deliberately NOT in `ts` order (c, a, b).
        mock_fetch.return_value = _fetch_payload(
            {
                "calls": [
                    {
                        "url": "http://c",
                        "start_ts": 3000,
                        "end_ts": 3009,
                        "ts": 3011,
                    },
                    {
                        "url": "http://a",
                        "start_ts": 1000,
                        "end_ts": 1009,
                        "ts": 1011,
                    },
                    {
                        "url": "http://b",
                        "start_ts": 2000,
                        "end_ts": 2009,
                        "ts": 2011,
                    },
                ],
            }
        )

        async def sleep_side_effect(*args, **kwargs) -> bool:
            self.shutdown.set()
            return True

        mock_sleep.side_effect = sleep_side_effect

        chunks = [
            c
            async for c in bcfy_calls_collector.capture_bcfy_calls(
                cast("LeasedFeed", self.feed),
                self.shutdown,
                self.url_base,
                _default_resources(),
            )
        ]

        self.assertEqual(len(chunks), 3)
        positions = [c.resume_position for c in chunks]
        self.assertEqual(
            positions,
            [
                datetime.datetime.fromtimestamp(t, datetime.UTC)
                for t in (1011, 2011, 3011)
            ],
        )

    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector._get_jwt_token"
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
        ".bcfy_calls_collector._sleep_or_shutdown",
        new_callable=AsyncMock,
    )
    async def test_cross_lease_resume_refetches_nothing(
        self,
        mock_sleep: AsyncMock,
        mock_dl: AsyncMock,
        mock_fetch: AsyncMock,
        mock_jwt: MagicMock,
    ) -> None:
        """A second lease resuming at the persisted `ts` cursor re-fetches nothing.

        Simulates the runtime's role: lease-1 commits a page, the last
        call's resume_position is persisted as feeds.last_bookmark_time,
        lease-2 reads it back as `pos`. With the API's strict `ts > pos`
        filter the prior page is fully excluded. The original bug
        (persisting `end_ts < ts`) would instead re-return the boundary
        call — this test fails if the cursor regresses to end_ts.
        """
        mock_jwt.return_value = "token"
        mock_dl.return_value = b"flac"

        # A fixed corpus; `ts` runs a few seconds past each `end_ts`.
        corpus = [
            {"url": "http://a", "start_ts": 1000, "end_ts": 1009, "ts": 1011},
            {"url": "http://b", "start_ts": 2000, "end_ts": 2009, "ts": 2012},
            {"url": "http://c", "start_ts": 3000, "end_ts": 3009, "ts": 3013},
        ]

        def _visible(pos: int | None) -> list[dict[str, Any]]:
            # Broadcastify Calls API filter: strict `ts > pos`.
            # cast: corpus dicts mix str/int values, so c["ts"] widens
            # to `str | int`; the `ts` field is always an int.
            return [
                c for c in corpus if pos is None or cast("int", c["ts"]) > pos
            ]

        # --- Lease 1: cold start, commits the whole corpus. ---
        self.feed["last_bookmark_time"] = None
        lease1_shutdown = asyncio.Event()
        fetch_n = 0

        async def lease1_fetch(session, url, headers, params, *args, **kwargs):
            nonlocal fetch_n
            fetch_n += 1
            if fetch_n == 1:
                visible = _visible(params.get("pos"))
                return _fetch_payload(
                    {
                        "calls": visible,
                        "lastPos": max(c["ts"] for c in visible),
                    }
                )
            lease1_shutdown.set()
            return _fetch_payload({"calls": []})

        mock_fetch.side_effect = lease1_fetch
        mock_sleep.return_value = False

        lease1_events = [
            c
            async for c in bcfy_calls_collector.capture_bcfy_calls(
                cast("LeasedFeed", self.feed),
                lease1_shutdown,
                self.url_base,
                _default_resources(),
            )
        ]
        lease1_chunks = [
            e
            for e in lease1_events
            if isinstance(e, bcfy_calls_collector.CapturedChunk)
        ]

        self.assertEqual(len(lease1_chunks), 3)
        committed_cursor = lease1_chunks[-1].resume_position
        assert committed_cursor is not None
        # The persisted cursor is the call's `ts`, NOT its audio end time.
        self.assertEqual(
            committed_cursor,
            datetime.datetime.fromtimestamp(3013, datetime.UTC),
        )
        self.assertNotEqual(committed_cursor, lease1_chunks[-1].chunk_end_time)

        # --- Lease 2: resume from the persisted cursor. ---
        self.feed["last_bookmark_time"] = committed_cursor
        lease2_shutdown = asyncio.Event()
        lease2_pos_seen: list[int | None] = []

        async def lease2_fetch(session, url, headers, params, *args, **kwargs):
            lease2_pos_seen.append(params.get("pos"))
            lease2_shutdown.set()
            return _fetch_payload({"calls": _visible(params.get("pos"))})

        mock_fetch.side_effect = lease2_fetch

        lease2_events = [
            c
            async for c in bcfy_calls_collector.capture_bcfy_calls(
                cast("LeasedFeed", self.feed),
                lease2_shutdown,
                self.url_base,
                _default_resources(),
            )
        ]
        lease2_chunks = [
            e
            for e in lease2_events
            if isinstance(e, bcfy_calls_collector.CapturedChunk)
        ]

        # Lease 2 issued `pos` = the persisted `ts` and got an empty page.
        self.assertEqual(lease2_pos_seen[0], 3013)
        self.assertEqual(lease2_chunks, [])

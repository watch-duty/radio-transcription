from __future__ import annotations

import asyncio
import typing
import unittest
from unittest import mock

import aiohttp

from backend.pipeline.ingestion.collectors import aiohttp_requests
from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemFailure,
)
from backend.pipeline.ingestion.collectors.failure_classifiers import (
    http_status,
)
from backend.pipeline.ingestion.models import FeedFailure
from backend.pipeline.storage import feed_store


class _Response:
    def __init__(
        self,
        status: int,
        *,
        payload: object = None,
        content: bytes = b"",
        headers: dict[str, str] | None = None,
        json_error: Exception | None = None,
    ) -> None:
        self.status = status
        self._payload = payload
        self._content = content
        self.headers = headers or {}
        self._json_error = json_error

    async def __aenter__(self) -> typing.Self:
        return self

    async def __aexit__(self, *args: object) -> bool:
        return False

    async def json(self) -> object:
        if self._json_error is not None:
            raise self._json_error
        return self._payload

    async def read(self) -> bytes:
        return self._content


def _validate_dict(payload: object) -> dict[str, object]:
    if not isinstance(payload, dict):
        msg = "invalid"
        raise TypeError(msg)
    return payload


class TestFetchJsonWithRetries(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.session = mock.MagicMock()
        self.shutdown = asyncio.Event()

    async def test_success_validates_payload(self) -> None:
        self.session.get.return_value = _Response(200, payload={"ok": True})

        result = await aiohttp_requests.fetch_json_with_retries(
            self.session,
            "https://api.example.invalid",
            self.shutdown,
            timeout_sec=1.0,
            max_attempts=3,
            log_label="test api",
            reason_prefix="test_api_http",
            status_policy=http_status.DEFAULT_HTTP_STATUS_POLICY,
            validate_payload=_validate_dict,
            invalid_payload_status_reason=(
                feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR
            ),
            invalid_payload_reason="test_api_response_invalid",
            transport_status_reason=(
                feed_store.FeedStatusReason.SOURCE_UNREACHABLE
            ),
            transport_reason="test_api_transport_failed",
        )

        self.assertEqual(result, {"ok": True})

    async def test_retry_after_then_success(self) -> None:
        sleep = mock.AsyncMock(return_value=False)
        self.session.get.side_effect = [
            _Response(429, headers={"Retry-After": "7"}),
            _Response(200, payload={"ok": True}),
        ]

        result = await aiohttp_requests.fetch_json_with_retries(
            self.session,
            "https://api.example.invalid",
            self.shutdown,
            timeout_sec=1.0,
            max_attempts=3,
            log_label="test api",
            reason_prefix="test_api_http",
            status_policy=http_status.DEFAULT_HTTP_STATUS_POLICY,
            validate_payload=_validate_dict,
            invalid_payload_status_reason=(
                feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR
            ),
            invalid_payload_reason="test_api_response_invalid",
            transport_status_reason=(
                feed_store.FeedStatusReason.SOURCE_UNREACHABLE
            ),
            transport_reason="test_api_transport_failed",
            sleep_func=sleep,
        )

        self.assertEqual(result, {"ok": True})
        sleep.assert_awaited_once_with(self.shutdown, 7.0)

    async def test_invalid_payload_exhaustion_raises_feed_failure(self) -> None:
        sleep = mock.AsyncMock(return_value=False)
        self.session.get.return_value = _Response(200, payload=[])

        with self.assertRaises(FeedFailure) as context:
            await aiohttp_requests.fetch_json_with_retries(
                self.session,
                "https://api.example.invalid",
                self.shutdown,
                timeout_sec=1.0,
                max_attempts=2,
                log_label="test api",
                reason_prefix="test_api_http",
                status_policy=http_status.DEFAULT_HTTP_STATUS_POLICY,
                validate_payload=_validate_dict,
                invalid_payload_status_reason=(
                    feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR
                ),
                invalid_payload_reason="test_api_response_invalid",
                transport_status_reason=(
                    feed_store.FeedStatusReason.SOURCE_UNREACHABLE
                ),
                transport_reason="test_api_transport_failed",
                sleep_func=sleep,
            )

        self.assertIs(
            context.exception.status_reason,
            feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(str(context.exception), "test_api_response_invalid")

    async def test_transport_exhaustion_raises_feed_failure(self) -> None:
        sleep = mock.AsyncMock(return_value=False)
        self.session.get.side_effect = aiohttp.ClientError()

        with self.assertRaises(FeedFailure) as context:
            await aiohttp_requests.fetch_json_with_retries(
                self.session,
                "https://api.example.invalid",
                self.shutdown,
                timeout_sec=1.0,
                max_attempts=2,
                log_label="test api",
                reason_prefix="test_api_http",
                status_policy=http_status.DEFAULT_HTTP_STATUS_POLICY,
                validate_payload=_validate_dict,
                invalid_payload_status_reason=(
                    feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR
                ),
                invalid_payload_reason="test_api_response_invalid",
                transport_status_reason=(
                    feed_store.FeedStatusReason.SOURCE_UNREACHABLE
                ),
                transport_reason="test_api_transport_failed",
                sleep_func=sleep,
            )

        self.assertIs(
            context.exception.status_reason,
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(str(context.exception), "test_api_transport_failed")


class TestDownloadItemMedia(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.session = mock.MagicMock()
        self.shutdown = asyncio.Event()

    async def test_success_returns_content_and_headers(self) -> None:
        self.session.get.return_value = _Response(
            200,
            content=b"audio",
            headers={"Content-Type": "audio/mpeg"},
        )

        result = await aiohttp_requests.download_item_media(
            self.session,
            "https://media.example.invalid/audio.mp3",
            self.shutdown,
            timeout_sec=1.0,
            max_attempts=3,
            log_label="test item",
        )

        self.assertIsInstance(result, aiohttp_requests.DownloadedItem)
        downloaded = typing.cast("aiohttp_requests.DownloadedItem", result)
        self.assertEqual(downloaded.content, b"audio")
        self.assertEqual(downloaded.headers["Content-Type"], "audio/mpeg")

    async def test_ambiguous_4xx_returns_collector_error_item_failure(
        self,
    ) -> None:
        self.session.get.return_value = _Response(404)

        result = await aiohttp_requests.download_item_media(
            self.session,
            "https://media.example.invalid/audio.mp3",
            self.shutdown,
            timeout_sec=1.0,
            max_attempts=3,
            log_label="test item",
        )

        self.assertIsInstance(result, ItemFailure)
        failure = typing.cast("ItemFailure", result)
        self.assertIs(
            failure.status_reason,
            feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(failure.reason, "item_http_404")

    async def test_retryable_status_then_success(self) -> None:
        sleep = mock.AsyncMock(return_value=False)
        self.session.get.side_effect = [
            _Response(503),
            _Response(200, content=b"audio"),
        ]

        result = await aiohttp_requests.download_item_media(
            self.session,
            "https://media.example.invalid/audio.mp3",
            self.shutdown,
            timeout_sec=1.0,
            max_attempts=3,
            log_label="test item",
            sleep_func=sleep,
        )

        self.assertIsInstance(result, aiohttp_requests.DownloadedItem)
        downloaded = typing.cast("aiohttp_requests.DownloadedItem", result)
        self.assertEqual(downloaded.content, b"audio")
        sleep.assert_awaited_once()

    async def test_transport_exhaustion_returns_item_failure(self) -> None:
        sleep = mock.AsyncMock(return_value=False)
        self.session.get.side_effect = aiohttp.ClientError()

        result = await aiohttp_requests.download_item_media(
            self.session,
            "https://media.example.invalid/audio.mp3",
            self.shutdown,
            timeout_sec=1.0,
            max_attempts=2,
            log_label="test item",
            sleep_func=sleep,
        )

        self.assertIsInstance(result, ItemFailure)
        failure = typing.cast("ItemFailure", result)
        self.assertIs(
            failure.status_reason,
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(failure.reason, "item_download_failed")

    async def test_shutdown_during_retry_returns_none(self) -> None:
        sleep = mock.AsyncMock(return_value=True)
        self.session.get.return_value = _Response(503)

        result = await aiohttp_requests.download_item_media(
            self.session,
            "https://media.example.invalid/audio.mp3",
            self.shutdown,
            timeout_sec=1.0,
            max_attempts=3,
            log_label="test item",
            sleep_func=sleep,
        )

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()

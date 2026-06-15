from __future__ import annotations

import asyncio
import typing
import unittest
from unittest import mock

import aiohttp

from backend.pipeline.ingestion import failure_policy
from backend.pipeline.ingestion.collectors import aiohttp_requests, control_flow
from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemFailure,
)
from backend.pipeline.ingestion.failure_classifiers import (
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
    return typing.cast("dict[str, object]", payload)


def _retry_config(
    *,
    max_attempts: int = 3,
    sleep_func: aiohttp_requests.SleepFunc | None = None,
) -> aiohttp_requests.RetryConfig:
    return aiohttp_requests.RetryConfig(
        timeout_sec=1.0,
        max_attempts=max_attempts,
        jitter_max_sec=0.0,
        sleep_func=sleep_func or control_flow.sleep_or_cancel,
    )


class _JsonEvidenceKwargs(typing.TypedDict):
    failure_scope: failure_policy.FailureScope
    endpoint_kind: failure_policy.EndpointKind


def _json_evidence_kwargs() -> _JsonEvidenceKwargs:
    return {
        "failure_scope": failure_policy.FailureScope.FEED,
        "endpoint_kind": failure_policy.EndpointKind.CALLS_API,
    }


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
            retry_config=_retry_config(),
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
            **_json_evidence_kwargs(),
        )

        self.assertEqual(result, {"ok": True})

    async def test_retry_after_then_success(self) -> None:
        sleep = mock.AsyncMock(return_value=False)
        self.session.get.side_effect = [
            _Response(429, headers={"Retry-After": " 7 "}),
            _Response(200, payload={"ok": True}),
        ]

        result = await aiohttp_requests.fetch_json_with_retries(
            self.session,
            "https://api.example.invalid",
            self.shutdown,
            retry_config=_retry_config(sleep_func=sleep),
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
            **_json_evidence_kwargs(),
        )

        self.assertEqual(result, {"ok": True})
        sleep.assert_awaited_once_with(self.shutdown, 7.0)

    async def test_invalid_payload_raises_feed_failure_without_retry(
        self,
    ) -> None:
        sleep = mock.AsyncMock(return_value=False)
        self.session.get.return_value = _Response(200, payload=[])

        with self.assertRaises(FeedFailure) as context:
            await aiohttp_requests.fetch_json_with_retries(
                self.session,
                "https://api.example.invalid",
                self.shutdown,
                retry_config=_retry_config(
                    max_attempts=3,
                    sleep_func=sleep,
                ),
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
                **_json_evidence_kwargs(),
            )

        self.assertIs(
            context.exception.status_reason,
            feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(
            str(context.exception),
            "test_api_response_invalid: TypeError: invalid",
        )
        self.assertIsInstance(context.exception.__cause__, TypeError)
        self.assertEqual(self.session.get.call_count, 1)
        sleep.assert_not_awaited()

    async def test_transport_exhaustion_raises_feed_failure(self) -> None:
        sleep = mock.AsyncMock(return_value=False)
        self.session.get.side_effect = aiohttp.ClientError(
            "socket down\nretry failed"
        )

        with self.assertRaises(FeedFailure) as context:
            await aiohttp_requests.fetch_json_with_retries(
                self.session,
                "https://api.example.invalid",
                self.shutdown,
                retry_config=_retry_config(
                    max_attempts=2,
                    sleep_func=sleep,
                ),
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
                **_json_evidence_kwargs(),
            )

        self.assertIs(
            context.exception.status_reason,
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(
            str(context.exception),
            "test_api_transport_failed: ClientError: socket down retry failed",
        )
        self.assertIsInstance(context.exception.__cause__, aiohttp.ClientError)

    async def test_invalid_retry_config_raises_value_error(self) -> None:
        with self.assertRaises(ValueError):
            await aiohttp_requests.fetch_json_with_retries(
                self.session,
                "https://api.example.invalid",
                self.shutdown,
                retry_config=_retry_config(max_attempts=0),
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
                **_json_evidence_kwargs(),
            )

    async def test_shutdown_is_set_raises_cancelled_error(self) -> None:
        self.shutdown.set()

        with self.assertRaises(asyncio.CancelledError):
            await aiohttp_requests.fetch_json_with_retries(
                self.session,
                "https://api.example.invalid",
                self.shutdown,
                retry_config=_retry_config(),
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
                **_json_evidence_kwargs(),
            )

        self.session.get.assert_not_called()


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
            retry_config=_retry_config(),
            log_label="test item",
        )

        self.assertIsInstance(result, aiohttp_requests.DownloadedItem)
        downloaded = typing.cast("aiohttp_requests.DownloadedItem", result)
        self.assertEqual(downloaded.content, b"audio")
        self.assertEqual(downloaded.headers["content-type"], "audio/mpeg")
        self.assertNotIn("Content-Type", downloaded.headers)

    async def test_ambiguous_4xx_returns_collector_error_item_failure(
        self,
    ) -> None:
        self.session.get.return_value = _Response(404)

        result = await aiohttp_requests.download_item_media(
            self.session,
            "https://media.example.invalid/audio.mp3",
            self.shutdown,
            retry_config=_retry_config(),
            log_label="test item",
        )

        self.assertIsInstance(result, ItemFailure)
        failure = typing.cast("ItemFailure", result)
        self.assertIs(
            failure.status_reason,
            feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(failure.reason, "item_http_404")

    async def test_empty_success_body_returns_item_failure(self) -> None:
        self.session.get.return_value = _Response(200, content=b"")

        result = await aiohttp_requests.download_item_media(
            self.session,
            "https://media.example.invalid/audio.mp3",
            self.shutdown,
            retry_config=_retry_config(),
            log_label="test item",
        )

        self.assertIsInstance(result, ItemFailure)
        failure = typing.cast("ItemFailure", result)
        self.assertIs(
            failure.status_reason,
            feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(failure.reason, "item_download_failed")

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
            retry_config=_retry_config(sleep_func=sleep),
            log_label="test item",
        )

        self.assertIsInstance(result, aiohttp_requests.DownloadedItem)
        downloaded = typing.cast("aiohttp_requests.DownloadedItem", result)
        self.assertEqual(downloaded.content, b"audio")
        sleep.assert_awaited_once()

    async def test_transport_exhaustion_returns_item_failure(self) -> None:
        sleep = mock.AsyncMock(return_value=False)
        self.session.get.side_effect = aiohttp.ClientError(
            "socket down\nretry failed"
        )

        result = await aiohttp_requests.download_item_media(
            self.session,
            "https://media.example.invalid/audio.mp3",
            self.shutdown,
            retry_config=_retry_config(max_attempts=2, sleep_func=sleep),
            log_label="test item",
        )

        self.assertIsInstance(result, ItemFailure)
        failure = typing.cast("ItemFailure", result)
        self.assertIs(
            failure.status_reason,
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(
            failure.reason,
            "item_download_failed: ClientError: socket down retry failed",
        )

    async def test_shutdown_during_retry_raises_cancelled_error(self) -> None:
        sleep = mock.AsyncMock(side_effect=asyncio.CancelledError)
        self.session.get.return_value = _Response(503)

        with self.assertRaises(asyncio.CancelledError):
            await aiohttp_requests.download_item_media(
                self.session,
                "https://media.example.invalid/audio.mp3",
                self.shutdown,
                retry_config=_retry_config(sleep_func=sleep),
                log_label="test item",
            )

        sleep.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()

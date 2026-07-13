from __future__ import annotations

import asyncio
import dataclasses
import json
import typing
import unittest
from unittest import mock

import aiohttp

from backend.pipeline.ingestion.collectors import aiohttp_requests, control_flow
from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemFailure,
)
from backend.pipeline.ingestion.failure_classifiers import (
    http_status,
)
from backend.pipeline.ingestion.models import FeedFailure
from backend.pipeline.storage import feed_store

_MISSING = object()


class _Response:
    def __init__(
        self,
        status: int,
        *,
        payload: object = _MISSING,
        content: bytes | None = None,
        headers: dict[str, str] | None = None,
        json_error: Exception | None = None,
        encoding: str = "utf-8",
        read_error: BaseException | None = None,
    ) -> None:
        self.status = status
        self._payload = payload
        self._content = (
            json.dumps(payload).encode()
            if content is None and payload is not _MISSING
            else content or b""
        )
        self.headers = dict(headers or {})
        if payload is not _MISSING:
            self.headers.setdefault("Content-Type", "application/json")
        self._json_error = json_error
        self._encoding = encoding
        self._read_error = read_error
        self.request_info = mock.MagicMock()
        self.history: tuple[object, ...] = ()
        self.read_count = 0

    async def __aenter__(self) -> typing.Self:
        return self

    async def __aexit__(self, *args: object) -> bool:
        return False

    async def json(self) -> object:
        if self._json_error is not None:
            raise self._json_error
        return self._payload

    async def read(self) -> bytes:
        self.read_count += 1
        if self._read_error is not None:
            raise self._read_error
        return self._content

    def get_encoding(self) -> str:
        return self._encoding


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


async def _fetch_json_evidence(
    session: object,
    shutdown: asyncio.Event,
    *,
    max_attempts: int = 3,
    sleep_func: aiohttp_requests.SleepFunc | None = None,
    attempt_observer: aiohttp_requests.HttpAttemptObserver | None = None,
) -> aiohttp_requests.JsonFetchEvidence[dict[str, object]]:
    return await aiohttp_requests.fetch_json_with_retries_evidence(
        typing.cast("aiohttp.ClientSession", session),
        "https://api.example.invalid",
        shutdown,
        retry_config=_retry_config(
            max_attempts=max_attempts,
            sleep_func=sleep_func,
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
        attempt_observer=attempt_observer,
    )


class TestFetchJsonWithRetries(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.session = mock.MagicMock()
        self.shutdown = asyncio.Event()

    async def test_success_validates_payload(self) -> None:
        response = _Response(200, payload={"ok": True})
        self.session.get.return_value = response

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
        )

        self.assertEqual(result, {"ok": True})
        self.assertEqual(response.read_count, 1)

    async def test_evidence_counts_one_multibyte_accepted_body_read(
        self,
    ) -> None:
        body = '{"message":"café"}'.encode()
        response = _Response(
            200,
            content=body,
            headers={
                "Content-Type": "application/vnd.calls+json; charset=utf-8"
            },
        )
        self.session.get.return_value = response

        evidence = await _fetch_json_evidence(self.session, self.shutdown)

        self.assertEqual(evidence.payload, {"message": "café"})
        self.assertEqual(evidence.http_attempt_count, 1)
        self.assertEqual(evidence.response_byte_count, len(body))
        self.assertEqual(response.read_count, 1)
        self.assertNotIn("café", repr(evidence))
        self.assertNotIn("message", repr(evidence))
        with self.assertRaises(dataclasses.FrozenInstanceError):
            evidence.http_attempt_count = 2  # type: ignore[misc]  # ty: ignore[invalid-assignment]

    async def test_evidence_preserves_content_type_and_encoding_policy(
        self,
    ) -> None:
        body = '{"message":"café"}'.encode("utf-16")
        response = _Response(
            200,
            content=body,
            headers={"Content-Type": "application/json; charset=utf-16"},
            encoding="utf-16",
        )
        self.session.get.return_value = response

        evidence = await _fetch_json_evidence(self.session, self.shutdown)

        self.assertEqual(evidence.payload, {"message": "café"})
        self.assertEqual(evidence.response_byte_count, len(body))

        self.session.get.return_value = _Response(
            200,
            content=b'{"ok":true}',
            headers={"Content-Type": "text/plain"},
        )
        with self.assertRaises(FeedFailure):
            await _fetch_json_evidence(self.session, self.shutdown)

    async def test_error_body_is_drained_but_excluded_from_evidence(
        self,
    ) -> None:
        error_body = b"secret response body signature=sensitive"
        success_body = '{"message":"火災"}'.encode()
        error_response = _Response(
            503,
            content=error_body,
            headers={"Retry-After": "0"},
        )
        success_response = _Response(
            200,
            content=success_body,
            headers={"Content-Type": "application/json"},
        )
        sleep = mock.AsyncMock(return_value=False)
        self.session.get.side_effect = [error_response, success_response]

        with self.assertLogs(aiohttp_requests.logger, level="WARNING") as logs:
            evidence = await _fetch_json_evidence(
                self.session,
                self.shutdown,
                sleep_func=sleep,
            )

        self.assertEqual(evidence.http_attempt_count, 2)
        self.assertEqual(evidence.response_byte_count, len(success_body))
        self.assertEqual(error_response.read_count, 1)
        self.assertEqual(success_response.read_count, 1)
        self.assertNotIn(error_body.decode(), "\n".join(logs.output))
        self.assertNotIn("signature=sensitive", "\n".join(logs.output))

    async def test_error_body_drain_failure_preserves_http_status(
        self,
    ) -> None:
        response = _Response(
            401,
            read_error=aiohttp.ClientPayloadError("sensitive response URL"),
        )
        attempts: list[tuple[object, int]] = []
        self.session.get.return_value = response

        with self.assertRaises(FeedFailure) as context:
            await _fetch_json_evidence(
                self.session,
                self.shutdown,
                max_attempts=1,
                attempt_observer=lambda kind, ordinal: attempts.append(
                    (kind, ordinal)
                ),
            )

        self.assertIs(
            context.exception.status_reason,
            feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
        )
        self.assertEqual(str(context.exception), "test_api_http_401")
        self.assertEqual(response.read_count, 1)
        self.assertEqual(
            attempts,
            [(aiohttp_requests.HttpAttemptKind.JSON, 1)],
        )

    async def test_retry_after_survives_error_body_drain_failure(
        self,
    ) -> None:
        sleep = mock.AsyncMock(return_value=False)
        error_response = _Response(
            429,
            headers={"Retry-After": "7"},
            read_error=TimeoutError("sensitive response URL"),
        )
        success_response = _Response(200, payload={"ok": True})
        self.session.get.side_effect = [error_response, success_response]

        evidence = await _fetch_json_evidence(
            self.session,
            self.shutdown,
            sleep_func=sleep,
        )

        self.assertEqual(evidence.http_attempt_count, 2)
        self.assertEqual(error_response.read_count, 1)
        sleep.assert_awaited_once_with(self.shutdown, 7.0)

    async def test_malformed_200_has_no_success_evidence(self) -> None:
        response = _Response(
            200,
            content=b'{"secret":"signed-body"',
            headers={"Content-Type": "application/json"},
        )
        attempts: list[tuple[object, int]] = []
        self.session.get.return_value = response

        with self.assertRaises(FeedFailure):
            await _fetch_json_evidence(
                self.session,
                self.shutdown,
                attempt_observer=lambda kind, ordinal: attempts.append(
                    (kind, ordinal)
                ),
            )

        self.assertEqual(
            attempts,
            [(aiohttp_requests.HttpAttemptKind.JSON, 1)],
        )
        self.assertEqual(response.read_count, 1)

    async def test_attempt_observer_is_exact_and_failure_is_contained(
        self,
    ) -> None:
        self.session.get.side_effect = [
            aiohttp.ClientError("unreachable"),
            _Response(200, payload={"ok": True}),
        ]
        attempts: list[tuple[object, int]] = []

        def observe(kind: object, ordinal: int) -> None:
            attempts.append((kind, ordinal))
            msg = "observer secret"
            raise RuntimeError(msg)

        evidence = await _fetch_json_evidence(
            self.session,
            self.shutdown,
            sleep_func=mock.AsyncMock(return_value=False),
            attempt_observer=observe,
        )

        self.assertEqual(evidence.http_attempt_count, 2)
        self.assertEqual(
            attempts,
            [
                (aiohttp_requests.HttpAttemptKind.JSON, 1),
                (aiohttp_requests.HttpAttemptKind.JSON, 2),
            ],
        )

        def canceling_observer(kind: object, ordinal: int) -> None:
            del kind, ordinal
            raise asyncio.CancelledError

        self.session.get.side_effect = None
        self.session.get.return_value = _Response(200, payload={"ok": True})
        contained = await _fetch_json_evidence(
            self.session,
            self.shutdown,
            attempt_observer=canceling_observer,
        )
        self.assertEqual(contained.payload, {"ok": True})

    async def test_read_cancellation_propagates_after_attempt_observation(
        self,
    ) -> None:
        response = _Response(200, payload={"ok": True})
        response.read = mock.AsyncMock(  # ty: ignore[invalid-assignment]
            side_effect=asyncio.CancelledError
        )
        attempts: list[tuple[object, int]] = []
        self.session.get.return_value = response

        with self.assertRaises(asyncio.CancelledError):
            await _fetch_json_evidence(
                self.session,
                self.shutdown,
                attempt_observer=lambda kind, ordinal: attempts.append(
                    (kind, ordinal)
                ),
            )

        self.assertEqual(
            attempts,
            [(aiohttp_requests.HttpAttemptKind.JSON, 1)],
        )

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
            )

        self.session.get.assert_not_called()

    def test_evidence_rejects_negative_counts(self) -> None:
        with self.assertRaises(ValueError):
            aiohttp_requests.JsonFetchEvidence(
                payload={"secret": "hidden"},
                http_attempt_count=-1,
                response_byte_count=0,
            )
        with self.assertRaises(ValueError):
            aiohttp_requests.JsonFetchEvidence(
                payload={"secret": "hidden"},
                http_attempt_count=0,
                response_byte_count=-1,
            )


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

    async def test_error_body_drain_failure_preserves_item_status(
        self,
    ) -> None:
        response = _Response(
            404,
            read_error=aiohttp.ClientPayloadError("sensitive signed URL"),
        )
        self.session.get.return_value = response

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
        self.assertEqual(response.read_count, 1)

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
        retry_response = _Response(503, content=b"signed error body")
        success_response = _Response(200, content=b"audio")
        self.session.get.side_effect = [retry_response, success_response]
        attempts: list[tuple[object, int]] = []

        result = await aiohttp_requests.download_item_media(
            self.session,
            "https://media.example.invalid/audio.mp3",
            self.shutdown,
            retry_config=_retry_config(sleep_func=sleep),
            log_label="test item",
            attempt_observer=lambda kind, ordinal: attempts.append(
                (kind, ordinal)
            ),
        )

        self.assertIsInstance(result, aiohttp_requests.DownloadedItem)
        downloaded = typing.cast("aiohttp_requests.DownloadedItem", result)
        self.assertEqual(downloaded.content, b"audio")
        sleep.assert_awaited_once()
        self.assertEqual(retry_response.read_count, 1)
        self.assertEqual(success_response.read_count, 1)
        self.assertEqual(
            attempts,
            [
                (aiohttp_requests.HttpAttemptKind.MEDIA, 1),
                (aiohttp_requests.HttpAttemptKind.MEDIA, 2),
            ],
        )

    async def test_transport_exhaustion_returns_item_failure(self) -> None:
        sleep = mock.AsyncMock(return_value=False)
        self.session.get.side_effect = aiohttp.ClientError(
            "socket down\nretry failed"
        )

        attempts: list[tuple[object, int]] = []
        result = await aiohttp_requests.download_item_media(
            self.session,
            "https://media.example.invalid/audio.mp3",
            self.shutdown,
            retry_config=_retry_config(max_attempts=2, sleep_func=sleep),
            log_label="test item",
            attempt_observer=lambda kind, ordinal: attempts.append(
                (kind, ordinal)
            ),
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
        self.assertEqual(
            attempts,
            [
                (aiohttp_requests.HttpAttemptKind.MEDIA, 1),
                (aiohttp_requests.HttpAttemptKind.MEDIA, 2),
            ],
        )

    async def test_media_observer_exception_cannot_change_success(self) -> None:
        self.session.get.return_value = _Response(200, content=b"audio")

        def broken_observer(kind: object, ordinal: int) -> None:
            del kind, ordinal
            msg = "observer payload"
            raise ValueError(msg)

        result = await aiohttp_requests.download_item_media(
            self.session,
            "https://media.example.invalid/audio.mp3",
            self.shutdown,
            retry_config=_retry_config(),
            log_label="test item",
            attempt_observer=broken_observer,
        )

        self.assertIsInstance(result, aiohttp_requests.DownloadedItem)

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

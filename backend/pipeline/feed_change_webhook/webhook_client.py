"""Destination webhook client for Feed Change Notifications."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

import httpx
import tenacity

if TYPE_CHECKING:
    from collections.abc import Mapping

logger = logging.getLogger(__name__)

_MAX_ATTEMPTS = 3
_REQUEST_TIMEOUT_SECONDS = 15.0
_CONNECTION_LIMIT = 80
_MAX_LOGGED_RESPONSE_BODY_CHARS = 1024
_RETRYABLE_STATUS_CODES = {408, 429}


class AsyncHTTPClient(Protocol):
    async def post(
        self,
        url: str,
        *,
        content: bytes,
        headers: Mapping[str, str],
    ) -> httpx.Response: ...

    async def aclose(self) -> None: ...


@dataclass(frozen=True)
class WebhookResult:
    status_code: int
    response_body: str
    attempts: int


class WebhookDeliveryError(RuntimeError):
    """Raised when a Feed Change Notification cannot be delivered."""

    def __init__(
        self,
        *,
        status_code: int | None,
        response_body: str,
        retryable: bool,
        attempts: int,
    ) -> None:
        self.status_code = status_code
        self.response_body = response_body
        self.retryable = retryable
        self.attempts = attempts
        detail = (
            f"status_code={status_code}, retryable={retryable}, "
            f"attempts={attempts}"
        )
        super().__init__(f"Feed change webhook delivery failed ({detail})")


class WebhookClient:
    def __init__(
        self,
        *,
        webhook_url: str,
        api_key: str,
        http: AsyncHTTPClient | None = None,
        wait_strategy: Any | None = None,
    ) -> None:
        self._webhook_url = webhook_url
        self._api_key = api_key
        self._http = (
            http
            if http is not None
            else httpx.AsyncClient(
                transport=httpx.AsyncHTTPTransport(retries=0),
                limits=httpx.Limits(
                    max_connections=_CONNECTION_LIMIT,
                    max_keepalive_connections=_CONNECTION_LIMIT,
                ),
                follow_redirects=True,
                timeout=httpx.Timeout(_REQUEST_TIMEOUT_SECONDS),
            )
        )
        self._wait_strategy = wait_strategy or tenacity.wait_exponential(
            multiplier=0.5,
            min=0.5,
            max=2.0,
        )

    async def send(
        self,
        payload: Mapping[str, Any],
    ) -> WebhookResult:
        request_body = json.dumps(
            payload,
            separators=(",", ":"),
        ).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "X-Api-Key": self._api_key,
        }

        try:
            async for attempt in tenacity.AsyncRetrying(
                retry=tenacity.retry_if_exception(_is_retryable_exception),
                stop=tenacity.stop_after_attempt(_MAX_ATTEMPTS),
                wait=self._wait_strategy,
                reraise=True,
            ):
                attempt_number = attempt.retry_state.attempt_number
                with attempt:
                    return await self._send_attempt(
                        payload,
                        request_body=request_body,
                        headers=headers,
                        attempt=attempt_number,
                    )
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            raise WebhookDeliveryError(
                status_code=status_code,
                response_body=_decode_response_body(exc.response.content),
                retryable=_is_retryable_status(status_code),
                attempts=_MAX_ATTEMPTS,
            ) from exc
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            raise WebhookDeliveryError(
                status_code=None,
                response_body=_decode_response_body(str(exc)),
                retryable=True,
                attempts=_MAX_ATTEMPTS,
            ) from exc

        msg = "unreachable webhook retry state"
        raise AssertionError(msg)

    async def close(self) -> None:
        """Release pooled HTTP connections owned by this client."""
        await self._http.aclose()

    async def _send_attempt(
        self,
        payload: Mapping[str, Any],
        *,
        request_body: bytes,
        headers: Mapping[str, str],
        attempt: int,
    ) -> WebhookResult:
        try:
            response = await self._http.post(
                self._webhook_url,
                content=request_body,
                headers=headers,
            )
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            self._log_delivery_failure(
                payload,
                status_code=None,
                response_body=_decode_response_body(str(exc)),
                retryable=True,
                attempts=attempt,
                log_level=logging.WARNING,
            )
            raise

        status_code = response.status_code
        response_body = _decode_response_body(response.content)
        if 200 <= status_code < 300:
            logger.info(
                "Feed Change Notification delivered to webhook",
                extra={
                    "json_fields": _log_fields(
                        payload,
                        status_code=status_code,
                        attempts=attempt,
                    )
                },
            )
            return WebhookResult(
                status_code=status_code,
                response_body=response_body,
                attempts=attempt,
            )

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            retryable = _is_retryable_status(status_code)
            self._log_delivery_failure(
                payload,
                status_code=status_code,
                response_body=response_body,
                retryable=retryable,
                attempts=attempt,
                log_level=logging.WARNING if retryable else logging.ERROR,
            )
            if retryable:
                raise
            raise WebhookDeliveryError(
                status_code=status_code,
                response_body=response_body,
                retryable=False,
                attempts=attempt,
            ) from exc

        self._log_delivery_failure(
            payload,
            status_code=status_code,
            response_body=response_body,
            retryable=False,
            attempts=attempt,
            log_level=logging.ERROR,
        )
        raise WebhookDeliveryError(
            status_code=status_code,
            response_body=response_body,
            retryable=False,
            attempts=attempt,
        )

    def _log_delivery_failure(
        self,
        payload: Mapping[str, Any],
        *,
        status_code: int | None,
        response_body: str,
        retryable: bool,
        attempts: int,
        log_level: int,
    ) -> None:
        logger.log(
            log_level,
            "Feed Change Notification delivery to webhook failed",
            extra={
                "json_fields": _log_fields(
                    payload,
                    status_code=status_code,
                    attempts=attempts,
                    retryable=retryable,
                    response_body=response_body,
                )
            },
        )


def _is_retryable_status(status_code: int) -> bool:
    return status_code in _RETRYABLE_STATUS_CODES or status_code >= 500


def _is_retryable_exception(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return _is_retryable_status(exc.response.status_code)
    return isinstance(exc, (httpx.TimeoutException, httpx.TransportError))


def _decode_response_body(data: object) -> str:
    if isinstance(data, bytes):
        body = data.decode("utf-8", errors="replace")
    else:
        body = str(data)
    return body[:_MAX_LOGGED_RESPONSE_BODY_CHARS]


def _log_fields(
    payload: Mapping[str, Any],
    *,
    status_code: int | None,
    attempts: int,
    retryable: bool | None = None,
    response_body: str | None = None,
) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "relay_event": "feed_change_webhook_delivery",
        "event_id": payload.get("event_id"),
        "feed_id": payload.get("feed_id"),
        "feed_revision": payload.get("feed_revision"),
        "webhook_status_code": status_code,
        "attempts": attempts,
    }
    if retryable is not None:
        fields["retryable"] = retryable
    if response_body is not None:
        fields["webhook_response_body"] = response_body
    return fields

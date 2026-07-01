"""Destination webhook client for Feed Change Notifications."""

from __future__ import annotations

import json
import logging
import random
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from urllib3 import PoolManager, Retry, Timeout
from urllib3 import exceptions as urllib3_exceptions

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

logger = logging.getLogger(__name__)

_MAX_ATTEMPTS = 2
_REQUEST_TIMEOUT_SECONDS = 15.0
_CONNECTION_POOL_MAXSIZE = 5
_MAX_LOGGED_RESPONSE_BODY_CHARS = 1024
_RETRY_JITTER_MIN_SECONDS = 0.25
_RETRY_JITTER_MAX_SECONDS = 0.5
_RETRYABLE_STATUS_CODES = {408, 429}
_URLLIB3_RETRY_POLICY = Retry(
    total=None,
    connect=False,
    read=False,
    status=False,
    other=False,
    redirect=3,
)
_TRANSIENT_EXCEPTIONS = (
    urllib3_exceptions.ConnectTimeoutError,
    urllib3_exceptions.MaxRetryError,
    urllib3_exceptions.NewConnectionError,
    urllib3_exceptions.ProtocolError,
    urllib3_exceptions.ReadTimeoutError,
    urllib3_exceptions.TimeoutError,
)


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
        http: Any | None = None,
        sleep_func: Callable[[float], None] = time.sleep,
        jitter_func: Callable[[float, float], float] = random.uniform,
    ) -> None:
        self._webhook_url = webhook_url
        self._api_key = api_key
        self._http = (
            http
            if http is not None
            else PoolManager(
                retries=False,
                maxsize=_CONNECTION_POOL_MAXSIZE,
            )
        )
        self._sleep = sleep_func
        self._jitter = jitter_func

    def send(
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

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                response = self._http.request(
                    "POST",
                    self._webhook_url,
                    body=request_body,
                    headers=headers,
                    timeout=Timeout(total=_REQUEST_TIMEOUT_SECONDS),
                    retries=_URLLIB3_RETRY_POLICY,
                )
            except _TRANSIENT_EXCEPTIONS as exc:
                response_body = str(exc)
                self._log_delivery_failure(
                    payload,
                    status_code=None,
                    response_body=response_body,
                    retryable=True,
                    attempts=attempt,
                    log_level=logging.WARNING,
                )
                if attempt < _MAX_ATTEMPTS:
                    self._sleep(self._retry_sleep_seconds())
                    continue
                raise WebhookDeliveryError(
                    status_code=None,
                    response_body=response_body,
                    retryable=True,
                    attempts=attempt,
                ) from exc
            except urllib3_exceptions.HTTPError as exc:
                response_body = str(exc)
                self._log_delivery_failure(
                    payload,
                    status_code=None,
                    response_body=response_body,
                    retryable=False,
                    attempts=attempt,
                    log_level=logging.ERROR,
                )
                raise WebhookDeliveryError(
                    status_code=None,
                    response_body=response_body,
                    retryable=False,
                    attempts=attempt,
                ) from exc

            status_code = int(response.status)
            response_body = _decode_response_body(response.data)
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

            retryable = _is_retryable_status(status_code)
            self._log_delivery_failure(
                payload,
                status_code=status_code,
                response_body=response_body,
                retryable=retryable,
                attempts=attempt,
                log_level=logging.WARNING if retryable else logging.ERROR,
            )
            if retryable and attempt < _MAX_ATTEMPTS:
                self._sleep(self._retry_sleep_seconds())
                continue

            raise WebhookDeliveryError(
                status_code=status_code,
                response_body=response_body,
                retryable=retryable,
                attempts=attempt,
            )

        msg = "unreachable webhook retry state"
        raise AssertionError(msg)

    def close(self) -> None:
        """Release pooled HTTP connections owned by this client."""
        self._http.clear()

    def _retry_sleep_seconds(self) -> float:
        return self._jitter(
            _RETRY_JITTER_MIN_SECONDS,
            _RETRY_JITTER_MAX_SECONDS,
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

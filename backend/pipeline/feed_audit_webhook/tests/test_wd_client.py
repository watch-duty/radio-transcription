from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

import pytest
from urllib3.exceptions import ReadTimeoutError

from backend.pipeline.feed_audit_webhook import wd_client
from backend.pipeline.feed_audit_webhook.wd_client import (
    WatchDutyWebhookClient,
    WatchDutyWebhookError,
)


@dataclass
class _Response:
    status: int
    data: bytes = b""


class _FakeHTTP:
    def __init__(self, responses: list[object]) -> None:
        self._responses = list(responses)
        self.requests: list[tuple[tuple[object, ...], dict[str, Any]]] = []

    def request(self, *args: object, **kwargs: Any) -> object:
        self.requests.append((args, kwargs))
        response = self._responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


def _payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "event_id": "event-1",
        "feed_id": "feed-1",
        "feed_revision": 7,
        "before_values": {"status": "active"},
        "after_values": {"status": "failing"},
    }
    payload.update(overrides)
    return payload


def _client(
    responses: list[object],
    *,
    api_key: str = "secret-api-key",
) -> tuple[WatchDutyWebhookClient, _FakeHTTP, list[float]]:
    http = _FakeHTTP(responses)
    sleeps: list[float] = []
    client = WatchDutyWebhookClient(
        webhook_url="https://backend.watchduty.test/api/v1/echo/radio_transcription/internal/audit/webhook/",
        api_key=api_key,
        http=http,
        sleep_func=sleeps.append,
        jitter_func=lambda _minimum, _maximum: 0.25,
    )
    return client, http, sleeps


def test_send_succeeds_on_2xx_with_expected_headers_and_body() -> None:
    client, http, sleeps = _client([_Response(status=204)])

    result = client.send(_payload())

    assert result.status_code == 204
    assert result.attempts == 1
    assert sleeps == []
    assert len(http.requests) == 1
    args, kwargs = http.requests[0]
    assert args[:2] == (
        "POST",
        "https://backend.watchduty.test/api/v1/echo/radio_transcription/internal/audit/webhook/",
    )
    assert kwargs["headers"] == {
        "Content-Type": "application/json",
        "X-Api-Key": "secret-api-key",
    }
    assert json.loads(kwargs["body"]) == _payload()
    assert kwargs["retries"] is False


@pytest.mark.parametrize("status_code", [408, 429, 500, 502, 503])
def test_send_retries_transient_statuses_once(status_code: int) -> None:
    client, http, sleeps = _client(
        [
            _Response(status=status_code, data=b"try again"),
            _Response(status=204),
        ]
    )

    result = client.send(_payload())

    assert result.status_code == 204
    assert result.attempts == 2
    assert len(http.requests) == 2
    assert sleeps == [0.25]


def test_send_retries_timeout_once() -> None:
    client, http, sleeps = _client(
        [
            ReadTimeoutError(None, "https://backend.watchduty.test", "timeout"),
            _Response(status=204),
        ]
    )

    result = client.send(_payload())

    assert result.status_code == 204
    assert len(http.requests) == 2
    assert sleeps == [0.25]


@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 409, 422])
def test_send_does_not_retry_non_transient_4xx(status_code: int) -> None:
    client, http, sleeps = _client(
        [_Response(status=status_code, data=b"permanent")]
    )

    with pytest.raises(WatchDutyWebhookError) as exc_info:
        client.send(_payload())

    assert exc_info.value.status_code == status_code
    assert exc_info.value.retryable is False
    assert exc_info.value.attempts == 1
    assert len(http.requests) == 1
    assert sleeps == []


def test_send_raises_after_two_transient_attempts() -> None:
    client, http, sleeps = _client(
        [
            _Response(status=500, data=b"first failure"),
            _Response(status=500, data=b"second failure"),
        ]
    )

    with pytest.raises(WatchDutyWebhookError) as exc_info:
        client.send(_payload())

    assert exc_info.value.status_code == 500
    assert exc_info.value.response_body == "second failure"
    assert exc_info.value.retryable is True
    assert exc_info.value.attempts == 2
    assert len(http.requests) == 2
    assert sleeps == [0.25]


def test_failure_logs_response_body_without_api_key(
    caplog: pytest.LogCaptureFixture,
) -> None:
    api_key = "very-secret-key"
    client, _http, _sleeps = _client(
        [_Response(status=401, data=b"not authorized")],
        api_key=api_key,
    )

    with caplog.at_level(logging.ERROR, logger=wd_client.__name__):
        with pytest.raises(WatchDutyWebhookError):
            client.send(_payload())

    assert api_key not in caplog.text
    fields = [
        getattr(record, "json_fields", {})
        for record in caplog.records
        if record.name == wd_client.__name__
    ]
    assert any(
        field.get("wd_response_body") == "not authorized" for field in fields
    )
    assert all("before_values" not in field for field in fields)
    assert all("after_values" not in field for field in fields)

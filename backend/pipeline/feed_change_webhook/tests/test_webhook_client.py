from __future__ import annotations

import json
import logging
from typing import Any

import httpx
import pytest
import tenacity

from backend.pipeline.feed_change_webhook import webhook_client
from backend.pipeline.feed_change_webhook.webhook_client import (
    WebhookClient,
    WebhookDeliveryError,
)

pytestmark = pytest.mark.asyncio

_WEBHOOK_URL = "https://webhook.example.test/feed-change"


class _FakeHTTP:
    def __init__(self, responses: list[object]) -> None:
        self._responses = list(responses)
        self.requests: list[tuple[tuple[object, ...], dict[str, Any]]] = []
        self.closed = False

    async def post(self, *args: object, **kwargs: Any) -> object:
        self.requests.append((args, kwargs))
        response = self._responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response

    async def aclose(self) -> None:
        self.closed = True


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


def _response(status_code: int, data: bytes = b"") -> httpx.Response:
    return httpx.Response(
        status_code,
        content=data,
        request=httpx.Request("POST", _WEBHOOK_URL),
    )


def _client(
    responses: list[object],
    *,
    api_key: str = "secret-api-key",
) -> tuple[WebhookClient, _FakeHTTP, list[float]]:
    http = _FakeHTTP(responses)
    sleeps: list[float] = []
    client = WebhookClient(
        webhook_url=_WEBHOOK_URL,
        api_key=api_key,
        http=http,
        wait_strategy=tenacity.wait_none(),
    )
    return client, http, sleeps


async def test_send_succeeds_on_2xx_with_expected_headers_and_body() -> None:
    client, http, sleeps = _client([_response(status_code=204)])

    result = await client.send(_payload())

    assert result.status_code == 204
    assert result.attempts == 1
    assert sleeps == []
    assert len(http.requests) == 1
    args, kwargs = http.requests[0]
    assert args == (_WEBHOOK_URL,)
    assert kwargs["headers"] == {
        "Content-Type": "application/json",
        "X-Api-Key": "secret-api-key",
    }
    assert json.loads(kwargs["content"]) == _payload()


@pytest.mark.parametrize("status_code", [408, 429, 500, 502, 503])
async def test_send_retries_transient_statuses(status_code: int) -> None:
    client, http, sleeps = _client(
        [
            _response(status_code=status_code, data=b"try again"),
            _response(status_code=500, data=b"try again again"),
            _response(status_code=204),
        ]
    )

    result = await client.send(_payload())

    assert result.status_code == 204
    assert result.attempts == 3
    assert len(http.requests) == 3
    assert sleeps == []


async def test_send_retries_timeout() -> None:
    client, http, sleeps = _client(
        [
            httpx.ReadTimeout("timeout"),
            _response(status_code=500, data=b"try again"),
            _response(status_code=204),
        ]
    )

    result = await client.send(_payload())

    assert result.status_code == 204
    assert len(http.requests) == 3
    assert sleeps == []


async def test_send_retries_transport_error() -> None:
    client, http, sleeps = _client(
        [
            httpx.RemoteProtocolError("connection reset by peer"),
            _response(status_code=204),
        ]
    )

    result = await client.send(_payload())

    assert result.status_code == 204
    assert len(http.requests) == 2
    assert sleeps == []


@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 409, 422])
async def test_send_does_not_retry_non_transient_4xx(
    status_code: int,
) -> None:
    client, http, sleeps = _client(
        [_response(status_code=status_code, data=b"permanent")]
    )

    with pytest.raises(WebhookDeliveryError) as exc_info:
        await client.send(_payload())

    assert exc_info.value.status_code == status_code
    assert exc_info.value.retryable is False
    assert exc_info.value.attempts == 1
    assert len(http.requests) == 1
    assert sleeps == []


async def test_send_does_not_treat_final_redirect_as_success() -> None:
    client, http, sleeps = _client(
        [_response(status_code=302, data=b"redirect")]
    )

    with pytest.raises(WebhookDeliveryError) as exc_info:
        await client.send(_payload())

    assert exc_info.value.status_code == 302
    assert exc_info.value.retryable is False
    assert exc_info.value.attempts == 1
    assert len(http.requests) == 1
    assert sleeps == []


async def test_send_raises_after_three_transient_attempts() -> None:
    client, http, sleeps = _client(
        [
            _response(status_code=500, data=b"first failure"),
            _response(status_code=500, data=b"second failure"),
            _response(status_code=500, data=b"third failure"),
        ]
    )

    with pytest.raises(WebhookDeliveryError) as exc_info:
        await client.send(_payload())

    assert exc_info.value.status_code == 500
    assert exc_info.value.response_body == "third failure"
    assert exc_info.value.retryable is True
    assert exc_info.value.attempts == 3
    assert len(http.requests) == 3
    assert sleeps == []


async def test_send_raises_after_three_transport_attempts() -> None:
    client, http, _sleeps = _client(
        [
            httpx.ReadTimeout("first timeout"),
            httpx.RemoteProtocolError("connection reset by peer"),
            httpx.TransportError("network unavailable"),
        ]
    )

    with pytest.raises(WebhookDeliveryError) as exc_info:
        await client.send(_payload())

    assert exc_info.value.status_code is None
    assert exc_info.value.response_body == "network unavailable"
    assert exc_info.value.retryable is True
    assert exc_info.value.attempts == 3
    assert len(http.requests) == 3


async def test_failure_logs_response_body_without_api_key(
    caplog: pytest.LogCaptureFixture,
) -> None:
    api_key = "very-secret-key"
    client, _http, _sleeps = _client(
        [_response(status_code=401, data=b"not authorized")],
        api_key=api_key,
    )

    with caplog.at_level(logging.ERROR, logger=webhook_client.__name__):
        with pytest.raises(WebhookDeliveryError):
            await client.send(_payload())

    assert api_key not in caplog.text
    fields = [
        getattr(record, "json_fields", {})
        for record in caplog.records
        if record.name == webhook_client.__name__
    ]
    assert any(
        field.get("webhook_response_body") == "not authorized"
        for field in fields
    )
    assert all("before_values" not in field for field in fields)
    assert all("after_values" not in field for field in fields)


async def test_failure_logs_truncated_response_body(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client, _http, _sleeps = _client(
        [
            _response(status_code=502, data=b"x" * 2048),
            _response(status_code=502, data=b"y" * 2048),
            _response(status_code=502, data=b"z" * 2048),
        ],
    )

    with caplog.at_level(logging.WARNING, logger=webhook_client.__name__):
        with pytest.raises(WebhookDeliveryError):
            await client.send(_payload())

    bodies = [
        getattr(record, "json_fields", {}).get("webhook_response_body")
        for record in caplog.records
        if record.name == webhook_client.__name__
    ]
    assert bodies
    assert all(isinstance(body, str) and len(body) == 1024 for body in bodies)


async def test_close_closes_async_http_client() -> None:
    client, http, _sleeps = _client([])

    await client.close()

    assert http.closed is True


async def test_default_client_uses_async_http_limits_and_redirects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_kwargs: dict[str, Any] = {}

    class _CapturingAsyncClient:
        def __init__(self, **kwargs: Any) -> None:
            captured_kwargs.update(kwargs)

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(
        webhook_client.httpx,
        "AsyncClient",
        _CapturingAsyncClient,
    )

    WebhookClient(webhook_url=_WEBHOOK_URL, api_key="secret-api-key")

    assert captured_kwargs["follow_redirects"] is True
    limits = captured_kwargs["limits"]
    assert limits.max_connections == 80
    assert limits.max_keepalive_connections == 80
    assert captured_kwargs["timeout"].as_dict()["connect"] == 15.0
    assert isinstance(captured_kwargs["transport"], httpx.AsyncHTTPTransport)

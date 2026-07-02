from __future__ import annotations

import base64
import inspect
import json
import logging
from typing import TYPE_CHECKING, Any

from fastapi.testclient import TestClient

from backend.pipeline.feed_change_webhook import main as main_module
from backend.pipeline.feed_change_webhook.main import create_app
from backend.pipeline.feed_change_webhook.settings import (
    FeedChangeWebhookSettings,
)
from backend.pipeline.feed_change_webhook.webhook_client import (
    WebhookDeliveryError,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    import pytest

_SENSITIVE_LOG_MARKERS = (
    '"before_values":',
    '"after_values":',
    "test-api-key",
)


class _FakeWebhookClient:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.payloads: list[Mapping[str, Any]] = []
        self.closed = False

    async def send(self, payload: Mapping[str, Any]) -> None:
        self.payloads.append(payload)
        if self.error is not None:
            raise self.error

    async def close(self) -> None:
        self.closed = True


def _settings() -> FeedChangeWebhookSettings:
    return FeedChangeWebhookSettings(
        webhook_url="https://webhook.example.test/feed-change",
        webhook_api_key="test-api-key",
    )


def _payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "event_type": "radio_transcription.feed_change_notification",
        "schema_version": 1,
        "event_id": "audit-event-1",
        "action": "feed.failure_reported",
        "occurred_at": "2026-06-26T22:00:00Z",
        "actor_id": "service_account:gcp:collector@example.iam.gserviceaccount.com",
        "feed_id": "feed-1",
        "feed_revision": 12,
        "before_values": {"status": "active"},
        "after_values": {"status": "failing"},
    }
    payload.update(overrides)
    return payload


def _envelope(payload: object) -> dict[str, object]:
    data = base64.b64encode(
        json.dumps({"jsonPayload": payload}).encode()
    ).decode()
    return {"message": {"data": data}}


def _json_fields(caplog: pytest.LogCaptureFixture) -> list[dict[str, object]]:
    return [
        getattr(record, "json_fields", {})
        for record in caplog.records
        if record.name == main_module.__name__
    ]


def _assert_no_sensitive_log_values(
    caplog: pytest.LogCaptureFixture,
    fields: list[dict[str, object]],
) -> None:
    rendered_fields = json.dumps(fields, sort_keys=True)
    for marker in _SENSITIVE_LOG_MARKERS:
        assert marker not in rendered_fields
        assert marker not in caplog.text


def test_valid_message_and_webhook_success_returns_204() -> None:
    payload = _payload()
    webhook_client = _FakeWebhookClient()
    app = create_app(settings=_settings(), webhook_client=webhook_client)

    with TestClient(app) as client:
        response = client.post(
            "/pubsub/feed-change-notifications",
            json=_envelope(payload),
        )

    assert response.status_code == 204
    assert webhook_client.payloads == [payload]
    assert webhook_client.closed is True


def test_webhook_transient_failure_returns_non_2xx() -> None:
    webhook_client = _FakeWebhookClient(
        WebhookDeliveryError(
            status_code=500,
            response_body="server error",
            retryable=True,
            attempts=2,
        )
    )
    app = create_app(settings=_settings(), webhook_client=webhook_client)

    with TestClient(app) as client:
        response = client.post(
            "/pubsub/feed-change-notifications",
            json=_envelope(_payload()),
        )

    assert response.status_code == 502
    assert len(webhook_client.payloads) == 1


def test_webhook_auth_failure_returns_non_2xx() -> None:
    webhook_client = _FakeWebhookClient(
        WebhookDeliveryError(
            status_code=401,
            response_body="unauthorized",
            retryable=False,
            attempts=1,
        )
    )
    app = create_app(settings=_settings(), webhook_client=webhook_client)

    with TestClient(app) as client:
        response = client.post(
            "/pubsub/feed-change-notifications",
            json=_envelope(_payload()),
        )

    assert response.status_code == 502
    assert len(webhook_client.payloads) == 1


def test_malformed_pubsub_message_returns_204_without_calling_webhook(
    caplog: pytest.LogCaptureFixture,
) -> None:
    webhook_client = _FakeWebhookClient()
    app = create_app(settings=_settings(), webhook_client=webhook_client)

    with caplog.at_level(logging.WARNING, logger=main_module.__name__):
        with TestClient(app) as client:
            response = client.post("/pubsub/feed-change-notifications", json={})

    assert response.status_code == 204
    assert webhook_client.payloads == []
    fields = _json_fields(caplog)
    assert any(
        field.get("relay_event") == "feed_change_webhook_invalid_pubsub_message"
        for field in fields
    )
    assert any(field.get("reason") for field in fields)
    assert any(field.get("path") == "message" for field in fields)
    _assert_no_sensitive_log_values(caplog, fields)


def test_unparseable_request_body_returns_204_without_calling_webhook(
    caplog: pytest.LogCaptureFixture,
) -> None:
    webhook_client = _FakeWebhookClient()
    app = create_app(settings=_settings(), webhook_client=webhook_client)

    with caplog.at_level(logging.WARNING, logger=main_module.__name__):
        with TestClient(app) as client:
            response = client.post(
                "/pubsub/feed-change-notifications",
                content="{",
                headers={"Content-Type": "application/json"},
            )

    assert response.status_code == 204
    assert webhook_client.payloads == []
    fields = _json_fields(caplog)
    assert any(
        field.get("reason") == "Pub/Sub request body is not JSON"
        for field in fields
    )
    assert any(field.get("path") == "body" for field in fields)
    _assert_no_sensitive_log_values(caplog, fields)


def test_non_object_request_body_returns_204_without_calling_webhook(
    caplog: pytest.LogCaptureFixture,
) -> None:
    webhook_client = _FakeWebhookClient()
    app = create_app(settings=_settings(), webhook_client=webhook_client)

    with caplog.at_level(logging.WARNING, logger=main_module.__name__):
        with TestClient(app) as client:
            response = client.post(
                "/pubsub/feed-change-notifications",
                json=[],
            )

    assert response.status_code == 204
    assert webhook_client.payloads == []
    fields = _json_fields(caplog)
    assert any(
        field.get("reason") == "Pub/Sub request envelope must be an object"
        for field in fields
    )
    assert any(field.get("path") == "envelope" for field in fields)
    _assert_no_sensitive_log_values(caplog, fields)


def test_invalid_payload_returns_204_without_calling_webhook(
    caplog: pytest.LogCaptureFixture,
) -> None:
    webhook_client = _FakeWebhookClient()
    app = create_app(settings=_settings(), webhook_client=webhook_client)

    with caplog.at_level(logging.WARNING, logger=main_module.__name__):
        with TestClient(app) as client:
            response = client.post(
                "/pubsub/feed-change-notifications",
                json=_envelope(_payload(after_values=[])),
            )

    assert response.status_code == 204
    assert webhook_client.payloads == []
    fields = _json_fields(caplog)
    assert any(
        field.get("reason")
        == "Feed Change Notification payload validation failed"
        for field in fields
    )
    assert any(
        field.get("path") == "jsonPayload.after_values" for field in fields
    )
    _assert_no_sensitive_log_values(caplog, fields)


def test_missing_webhook_client_returns_non_2xx_with_structured_config_log(
    caplog: pytest.LogCaptureFixture,
) -> None:
    webhook_client = _FakeWebhookClient()
    app = create_app(settings=_settings(), webhook_client=webhook_client)

    with TestClient(app) as client:
        app.state.webhook_client = None
        with caplog.at_level(logging.WARNING, logger=main_module.__name__):
            response = client.post(
                "/pubsub/feed-change-notifications",
                json=_envelope(_payload()),
            )

    assert response.status_code == 503
    assert webhook_client.payloads == []
    fields = _json_fields(caplog)
    assert any(
        field.get("relay_event") == "feed_change_webhook_client_not_initialized"
        for field in fields
    )
    _assert_no_sensitive_log_values(caplog, fields)


def test_unexpected_webhook_client_error_returns_non_2xx_with_structured_log(
    caplog: pytest.LogCaptureFixture,
) -> None:
    webhook_client = _FakeWebhookClient(RuntimeError("transport unavailable"))
    app = create_app(settings=_settings(), webhook_client=webhook_client)

    with caplog.at_level(logging.ERROR, logger=main_module.__name__):
        with TestClient(app) as client:
            response = client.post(
                "/pubsub/feed-change-notifications",
                json=_envelope(_payload()),
            )

    assert response.status_code == 502
    assert len(webhook_client.payloads) == 1
    fields = _json_fields(caplog)
    assert any(
        field.get("relay_event")
        == "feed_change_webhook_unhandled_delivery_error"
        for field in fields
    )
    _assert_no_sensitive_log_values(caplog, fields)


def test_relay_route_does_not_use_thread_pool_delivery() -> None:
    source = inspect.getsource(main_module.create_app)

    assert "asyncio.to_thread" not in source

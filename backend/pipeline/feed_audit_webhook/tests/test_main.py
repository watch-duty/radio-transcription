from __future__ import annotations

import base64
import json
from typing import TYPE_CHECKING, Any

from fastapi.testclient import TestClient

from backend.pipeline.feed_audit_webhook.main import create_app
from backend.pipeline.feed_audit_webhook.settings import (
    FeedAuditWebhookSettings,
)
from backend.pipeline.feed_audit_webhook.wd_client import WatchDutyWebhookError

if TYPE_CHECKING:
    from collections.abc import Mapping


class _FakeWDClient:
    def __init__(self, error: WatchDutyWebhookError | None = None) -> None:
        self.error = error
        self.payloads: list[Mapping[str, Any]] = []

    def send(self, payload: Mapping[str, Any]) -> None:
        self.payloads.append(payload)
        if self.error is not None:
            raise self.error


def _settings() -> FeedAuditWebhookSettings:
    return FeedAuditWebhookSettings(
        wd_backend_base_url="https://backend.watchduty.test",
        wd_backend_api_key="test-api-key",
    )


def _payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "event_type": "radio_transcription.feed_audit_notification",
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


def test_valid_message_and_wd_success_returns_204() -> None:
    payload = _payload()
    wd_client = _FakeWDClient()
    app = create_app(settings=_settings(), wd_client=wd_client)

    with TestClient(app) as client:
        response = client.post(
            "/pubsub/feed-audit-notifications",
            json=_envelope(payload),
        )

    assert response.status_code == 204
    assert wd_client.payloads == [payload]


def test_wd_transient_failure_returns_non_2xx() -> None:
    wd_client = _FakeWDClient(
        WatchDutyWebhookError(
            status_code=500,
            response_body="server error",
            retryable=True,
            attempts=2,
        )
    )
    app = create_app(settings=_settings(), wd_client=wd_client)

    with TestClient(app) as client:
        response = client.post(
            "/pubsub/feed-audit-notifications",
            json=_envelope(_payload()),
        )

    assert response.status_code == 502
    assert len(wd_client.payloads) == 1


def test_wd_auth_failure_returns_non_2xx() -> None:
    wd_client = _FakeWDClient(
        WatchDutyWebhookError(
            status_code=401,
            response_body="unauthorized",
            retryable=False,
            attempts=1,
        )
    )
    app = create_app(settings=_settings(), wd_client=wd_client)

    with TestClient(app) as client:
        response = client.post(
            "/pubsub/feed-audit-notifications",
            json=_envelope(_payload()),
        )

    assert response.status_code == 502
    assert len(wd_client.payloads) == 1


def test_malformed_pubsub_message_returns_non_2xx_without_calling_wd() -> None:
    wd_client = _FakeWDClient()
    app = create_app(settings=_settings(), wd_client=wd_client)

    with TestClient(app) as client:
        response = client.post("/pubsub/feed-audit-notifications", json={})

    assert response.status_code == 400
    assert wd_client.payloads == []

from __future__ import annotations

import base64
import json
from typing import Any

import pytest
from fastapi.testclient import TestClient

from backend.pipeline.feed_audit_webhook.main import create_app
from backend.pipeline.feed_audit_webhook.pubsub import (
    InvalidPubSubMessage,
    extract_feed_audit_payload,
)
from backend.pipeline.feed_audit_webhook.settings import (
    FeedAuditWebhookSettings,
)


def _feed_audit_payload(**overrides: object) -> dict[str, object]:
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


def _pubsub_envelope(log_entry: object) -> dict[str, object]:
    data = base64.b64encode(json.dumps(log_entry).encode()).decode()
    return {
        "message": {
            "data": data,
            "messageId": "message-1",
            "publishTime": "2026-06-26T22:00:01Z",
        },
        "subscription": "projects/project/subscriptions/subscription",
    }


def _invalid_json_envelope() -> dict[str, object]:
    return {"message": {"data": base64.b64encode(b"{").decode()}}


def _test_settings() -> FeedAuditWebhookSettings:
    return FeedAuditWebhookSettings(
        wd_backend_base_url="https://backend.watchduty.test",
        wd_backend_api_key="test-key",
    )


def test_extract_feed_audit_payload_returns_json_payload_copy() -> None:
    payload = _feed_audit_payload()
    extracted = extract_feed_audit_payload(
        _pubsub_envelope({"jsonPayload": payload})
    )

    assert extracted == payload
    assert extracted is not payload


@pytest.mark.parametrize(
    "envelope",
    [
        {},
        {"message": {}},
        {"message": {"data": "not base64"}},
        _invalid_json_envelope(),
        _pubsub_envelope([]),
        _pubsub_envelope({}),
        _pubsub_envelope({"jsonPayload": []}),
        _feed_audit_payload(),
    ],
)
def test_extract_feed_audit_payload_rejects_malformed_envelopes(
    envelope: dict[str, Any],
) -> None:
    with pytest.raises(InvalidPubSubMessage):
        extract_feed_audit_payload(envelope)


@pytest.mark.parametrize(
    "payload",
    [
        {
            key: value
            for key, value in _feed_audit_payload().items()
            if key != "feed_id"
        },
        _feed_audit_payload(event_type="other.event"),
        _feed_audit_payload(schema_version=2),
        _feed_audit_payload(before_values=[]),
        _feed_audit_payload(after_values=[]),
    ],
)
def test_extract_feed_audit_payload_rejects_unsupported_payloads(
    payload: dict[str, object],
) -> None:
    with pytest.raises(InvalidPubSubMessage):
        extract_feed_audit_payload(_pubsub_envelope({"jsonPayload": payload}))


def test_extract_feed_audit_payload_preserves_extra_fields() -> None:
    payload = _feed_audit_payload(extra_context={"source": "test"})

    extracted = extract_feed_audit_payload(
        _pubsub_envelope({"jsonPayload": payload})
    )

    assert extracted == payload
    assert extracted["extra_context"] == {"source": "test"}


def test_endpoint_returns_bad_request_for_malformed_pubsub_message() -> None:
    app = create_app(
        settings=_test_settings(), delivery_handler=lambda _payload: None
    )

    with TestClient(app) as client:
        response = client.post("/pubsub/feed-audit-notifications", json={})

    assert response.status_code == 400


def test_endpoint_passes_valid_payload_to_downstream_handler() -> None:
    payload = _feed_audit_payload()
    seen: list[dict[str, Any]] = []
    app = create_app(settings=_test_settings(), delivery_handler=seen.append)

    with TestClient(app) as client:
        response = client.post(
            "/pubsub/feed-audit-notifications",
            json=_pubsub_envelope({"jsonPayload": payload}),
        )

    assert response.status_code == 204
    assert seen == [payload]

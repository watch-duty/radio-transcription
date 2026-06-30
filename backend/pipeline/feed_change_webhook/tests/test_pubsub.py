from __future__ import annotations

import base64
import json
from typing import TYPE_CHECKING, Any

import pytest
from fastapi.testclient import TestClient

from backend.pipeline.feed_change_webhook.main import create_app
from backend.pipeline.feed_change_webhook.pubsub import (
    InvalidPubSubMessage,
    extract_feed_change_payload,
)
from backend.pipeline.feed_change_webhook.settings import (
    FeedChangeWebhookSettings,
)

if TYPE_CHECKING:
    from collections.abc import Mapping


def _feed_change_payload(**overrides: object) -> dict[str, object]:
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


def _test_settings() -> FeedChangeWebhookSettings:
    return FeedChangeWebhookSettings(
        wd_backend_base_url="https://backend.watchduty.test",
        wd_backend_api_key="test-key",
    )


def test_extract_feed_change_payload_returns_json_payload_copy() -> None:
    payload = _feed_change_payload()
    extracted = extract_feed_change_payload(
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
        _feed_change_payload(),
    ],
)
def test_extract_feed_change_payload_rejects_malformed_envelopes(
    envelope: dict[str, Any],
) -> None:
    with pytest.raises(InvalidPubSubMessage) as exc_info:
        extract_feed_change_payload(envelope)

    assert exc_info.value.reason
    assert exc_info.value.path


@pytest.mark.parametrize(
    "payload",
    [
        {
            key: value
            for key, value in _feed_change_payload().items()
            if key != "feed_id"
        },
        _feed_change_payload(event_type="other.event"),
        _feed_change_payload(schema_version=2),
        _feed_change_payload(before_values=[]),
        _feed_change_payload(after_values=[]),
    ],
)
def test_extract_feed_change_payload_rejects_unsupported_payloads(
    payload: dict[str, object],
) -> None:
    with pytest.raises(InvalidPubSubMessage) as exc_info:
        extract_feed_change_payload(_pubsub_envelope({"jsonPayload": payload}))

    assert (
        exc_info.value.reason
        == "Feed Change Notification payload validation failed"
    )
    assert exc_info.value.path.startswith("jsonPayload")


def test_extract_feed_change_payload_reports_validation_path() -> None:
    payload = _feed_change_payload(after_values=[])

    with pytest.raises(InvalidPubSubMessage) as exc_info:
        extract_feed_change_payload(_pubsub_envelope({"jsonPayload": payload}))

    assert exc_info.value.path == "jsonPayload.after_values"


def test_extract_feed_change_payload_preserves_extra_fields() -> None:
    payload = _feed_change_payload(extra_context={"source": "test"})

    extracted = extract_feed_change_payload(
        _pubsub_envelope({"jsonPayload": payload})
    )

    assert extracted == payload
    assert extracted["extra_context"] == {"source": "test"}


def test_endpoint_ack_drops_malformed_pubsub_message() -> None:
    app = create_app(settings=_test_settings(), wd_client=_FakeWDClient())

    with TestClient(app) as client:
        response = client.post("/pubsub/feed-change-notifications", json={})

    assert response.status_code == 204


def test_endpoint_passes_valid_payload_to_downstream_handler() -> None:
    payload = _feed_change_payload()
    wd_client = _FakeWDClient()
    app = create_app(settings=_test_settings(), wd_client=wd_client)

    with TestClient(app) as client:
        response = client.post(
            "/pubsub/feed-change-notifications",
            json=_pubsub_envelope({"jsonPayload": payload}),
        )

    assert response.status_code == 204
    assert wd_client.payloads == [payload]


class _FakeWDClient:
    def __init__(self) -> None:
        self.payloads: list[Mapping[str, Any]] = []

    def send(self, payload: Mapping[str, Any]) -> None:
        self.payloads.append(payload)

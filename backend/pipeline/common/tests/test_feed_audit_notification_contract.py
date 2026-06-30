from __future__ import annotations

import pytest
from pydantic import ValidationError

from backend.pipeline.common.feed_audit_notification_contract import (
    FEED_AUDIT_NOTIFICATION_EVENT_TYPE,
    FEED_AUDIT_NOTIFICATION_SCHEMA_VERSION,
    FeedAuditNotificationPayload,
)

_EXPECTED_FIELDS = {
    "event_type",
    "schema_version",
    "event_id",
    "action",
    "occurred_at",
    "actor_id",
    "feed_id",
    "feed_revision",
    "before_values",
    "after_values",
}


def _payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "event_type": FEED_AUDIT_NOTIFICATION_EVENT_TYPE,
        "schema_version": FEED_AUDIT_NOTIFICATION_SCHEMA_VERSION,
        "event_id": "audit-event-1",
        "action": "feed.failure_reported",
        "occurred_at": "2026-06-26T22:00:00Z",
        "actor_id": (
            "service_account:gcp:collector@example.iam.gserviceaccount.com"
        ),
        "feed_id": "feed-1",
        "feed_revision": 12,
        "before_values": {"status": "active"},
        "after_values": {"status": "failing"},
    }
    payload.update(overrides)
    return payload


def test_payload_contract_fields_are_the_notification_top_level_fields() -> (
    None
):
    assert set(FeedAuditNotificationPayload.model_fields) == _EXPECTED_FIELDS


def test_valid_payload_round_trips_as_json_compatible_dict() -> None:
    payload = _payload()

    model = FeedAuditNotificationPayload.model_validate(payload)

    assert model.model_dump(mode="json") == payload


def test_extra_top_level_fields_are_preserved() -> None:
    payload = _payload(extra_context={"source": "test"})

    model = FeedAuditNotificationPayload.model_validate(payload)

    assert model.model_dump(mode="json")["extra_context"] == {"source": "test"}


@pytest.mark.parametrize(
    "overrides",
    [
        {"event_type": "other.event"},
        {"schema_version": 2},
        {"before_values": []},
        {"after_values": []},
    ],
)
def test_invalid_contract_values_raise_validation_error(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        FeedAuditNotificationPayload.model_validate(_payload(**overrides))


def test_missing_required_field_raises_validation_error() -> None:
    payload = _payload()
    del payload["feed_id"]

    with pytest.raises(ValidationError):
        FeedAuditNotificationPayload.model_validate(payload)

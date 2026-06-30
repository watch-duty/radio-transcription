"""Shared Feed Change Notification contract."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator

FEED_CHANGE_NOTIFICATION_EVENT_TYPE = (
    "radio_transcription.feed_change_notification"
)
FEED_CHANGE_NOTIFICATION_SCHEMA_VERSION = 1


class FeedChangeNotificationPayload(BaseModel):
    """Shallow v1 payload contract for Feed Change Notification delivery."""

    model_config = ConfigDict(extra="allow", strict=True)

    event_type: str
    schema_version: int
    event_id: str
    action: str
    occurred_at: str
    actor_id: str
    feed_id: str
    feed_revision: int
    before_values: dict[str, Any]
    after_values: dict[str, Any]

    @field_validator("event_type")
    @classmethod
    def _validate_event_type(cls, value: str) -> str:
        if value != FEED_CHANGE_NOTIFICATION_EVENT_TYPE:
            msg = "unsupported Feed Change Notification event_type"
            raise ValueError(msg)
        return value

    @field_validator("schema_version")
    @classmethod
    def _validate_schema_version(cls, value: int) -> int:
        if value != FEED_CHANGE_NOTIFICATION_SCHEMA_VERSION:
            msg = "unsupported Feed Change Notification schema_version"
            raise ValueError(msg)
        return value

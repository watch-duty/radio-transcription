"""Shared Feed Change Notification contract."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

FEED_CHANGE_NOTIFICATION_EVENT_TYPE = (
    "radio_transcription.feed_change_notification"
)
FEED_CHANGE_NOTIFICATION_SCHEMA_VERSION = 1


class FeedChangeNotificationPayload(BaseModel):
    """Shallow v1 payload contract for Feed Change Notification delivery."""

    model_config = ConfigDict(extra="allow", strict=True)

    event_type: Literal["radio_transcription.feed_change_notification"]
    schema_version: Literal[1]
    event_id: str
    action: str
    occurred_at: str
    actor_id: str
    feed_id: str
    feed_revision: int
    before_values: dict[str, Any]
    after_values: dict[str, Any]

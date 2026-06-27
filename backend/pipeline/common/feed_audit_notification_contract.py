"""Shared Feed Audit Notification contract constants."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

FEED_AUDIT_NOTIFICATION_EVENT_TYPE = (
    "radio_transcription.feed_audit_notification"
)
FEED_AUDIT_NOTIFICATION_SCHEMA_VERSION = 1
FEED_AUDIT_NOTIFICATION_REQUIRED_FIELDS = frozenset(
    {
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
)


def missing_feed_audit_notification_fields(
    payload: Mapping[str, Any],
) -> set[str]:
    return set(FEED_AUDIT_NOTIFICATION_REQUIRED_FIELDS - payload.keys())


def is_valid_feed_audit_notification_payload(
    payload: Mapping[str, Any],
) -> bool:
    return (
        not missing_feed_audit_notification_fields(payload)
        and payload.get("event_type") == FEED_AUDIT_NOTIFICATION_EVENT_TYPE
        and payload.get("schema_version")
        == FEED_AUDIT_NOTIFICATION_SCHEMA_VERSION
        and isinstance(payload.get("before_values"), Mapping)
        and isinstance(payload.get("after_values"), Mapping)
    )

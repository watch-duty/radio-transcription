"""Shared Feed Audit Notification logging helper."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any

logger = logging.getLogger(__name__)

_EVENT_TYPE = "radio_transcription.feed_audit_notification"
_SCHEMA_VERSION = 1
_REQUIRED_FIELDS = frozenset(
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


def emit_feed_audit_notification(
    feed_audit_event: object | None,
) -> None:
    """Emit a Feed Audit Notification structured log. Never raises."""
    if feed_audit_event is None:
        return

    try:
        payload = _normalize_feed_audit_event(feed_audit_event)
        if payload is None:
            return

        logger.info(
            "Feed audit notification emitted",
            extra={"json_fields": payload},
        )
    except Exception:  # noqa: S110
        pass


def _normalize_feed_audit_event(
    feed_audit_event: object,
) -> dict[str, Any] | None:
    if isinstance(feed_audit_event, str):
        feed_audit_event = json.loads(feed_audit_event)

    if not isinstance(feed_audit_event, Mapping):
        return None

    payload = dict(feed_audit_event)
    if not _is_valid_feed_audit_payload(payload):
        return None

    return payload


def _is_valid_feed_audit_payload(
    payload: Mapping[str, Any],
) -> bool:
    return (
        payload.keys() >= _REQUIRED_FIELDS
        and payload.get("event_type") == _EVENT_TYPE
        and payload.get("schema_version") == _SCHEMA_VERSION
    )

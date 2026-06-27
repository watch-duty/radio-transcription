"""Shared Feed Audit Notification logging helper."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any

from backend.pipeline.common.feed_audit_notification_contract import (
    is_valid_feed_audit_notification_payload,
)

logger = logging.getLogger(__name__)


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
    except Exception:
        try:
            logger.exception("Failed to emit feed audit notification")
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
    if not is_valid_feed_audit_notification_payload(payload):
        return None

    return payload

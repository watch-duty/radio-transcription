"""Shared Feed Audit Notification logging helper."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any

logger = logging.getLogger(__name__)

_EMIT_FAILURE_LOG_FIELDS = {
    "event": "feed_audit_notification_emit_failed",
    "failure_class": "producer_emit_error",
}


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
            logger.exception(
                "Feed audit notification emission failed",
                extra={"json_fields": _EMIT_FAILURE_LOG_FIELDS},
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

    return dict(feed_audit_event)

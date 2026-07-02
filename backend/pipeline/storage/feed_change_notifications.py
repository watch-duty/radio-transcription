"""Shared Feed Change Notification logging helper."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any, cast

logger = logging.getLogger(__name__)

_EMIT_FAILURE_LOG_FIELDS = {
    "event": "feed_change_notification_emit_failed",
    "failure_class": "producer_emit_error",
}
_DROP_LOG_FIELDS = {
    "event": "feed_change_notification_dropped",
}


def emit_feed_change_notification(
    feed_audit_event: object | None,
) -> None:
    """Emit a best-effort Feed Change Notification log. Never raises."""
    if feed_audit_event is None:
        return

    try:
        payload = _normalize_feed_audit_event(feed_audit_event)
        if payload is None:
            return

        logger.info(
            "Feed change notification emitted",
            extra={"json_fields": payload},
        )
    except Exception:
        try:
            logger.exception(
                "Feed change notification emission failed",
                extra={"json_fields": _EMIT_FAILURE_LOG_FIELDS},
            )
        except Exception:  # noqa: S110
            pass


def _normalize_feed_audit_event(
    feed_audit_event: object,
) -> dict[str, Any] | None:
    if isinstance(feed_audit_event, str):
        try:
            feed_audit_event = json.loads(feed_audit_event)
        except json.JSONDecodeError:
            _log_dropped_payload(
                reason="invalid_json_payload",
                payload_type=type(feed_audit_event).__name__,
            )
            return None

    if not isinstance(feed_audit_event, Mapping):
        _log_dropped_payload(
            reason="non_mapping_payload",
            payload_type=type(feed_audit_event).__name__,
        )
        return None

    payload = cast("Mapping[str, Any]", feed_audit_event)
    return dict(payload)


def _log_dropped_payload(*, reason: str, payload_type: str) -> None:
    logger.warning(
        "Feed change notification payload dropped",
        extra={
            "json_fields": {
                **_DROP_LOG_FIELDS,
                "reason": reason,
                "payload_type": payload_type,
            }
        },
    )

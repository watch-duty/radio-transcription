"""Pub/Sub push parsing for Feed Audit Notification log entries."""

from __future__ import annotations

import base64
import binascii
import json
from collections.abc import Mapping
from typing import Any, cast

from backend.pipeline.common.feed_audit_notification_contract import (
    FEED_AUDIT_NOTIFICATION_EVENT_TYPE,
    FEED_AUDIT_NOTIFICATION_SCHEMA_VERSION,
    is_valid_feed_audit_notification_payload,
    missing_feed_audit_notification_fields,
)


class InvalidPubSubMessage(ValueError):
    """Raised when a Pub/Sub push request cannot be relayed."""


def extract_feed_audit_payload(envelope: Mapping[str, Any]) -> dict[str, Any]:
    """Extract and validate a Feed Audit Notification from a Pub/Sub envelope."""
    message = _require_mapping(envelope.get("message"), "message")
    data = message.get("data")
    if not isinstance(data, str) or not data:
        msg = "Pub/Sub message.data is required"
        raise InvalidPubSubMessage(msg)

    try:
        decoded = base64.b64decode(data, validate=True)
    except binascii.Error as exc:
        msg = "Pub/Sub message.data is not valid base64"
        raise InvalidPubSubMessage(msg) from exc

    try:
        log_entry = json.loads(decoded.decode("utf-8"))
    except UnicodeDecodeError as exc:
        msg = "Pub/Sub message.data is not UTF-8"
        raise InvalidPubSubMessage(msg) from exc
    except json.JSONDecodeError as exc:
        msg = "Pub/Sub message.data is not JSON"
        raise InvalidPubSubMessage(msg) from exc

    log_entry_mapping = _require_mapping(log_entry, "Cloud Logging LogEntry")
    json_payload = _require_mapping(
        log_entry_mapping.get("jsonPayload"), "jsonPayload"
    )
    payload = dict(json_payload)
    _validate_feed_audit_payload(payload)
    return payload


def _validate_feed_audit_payload(payload: Mapping[str, Any]) -> None:
    missing_fields = missing_feed_audit_notification_fields(payload)
    if missing_fields:
        msg = "Feed Audit Notification is missing required fields"
        raise InvalidPubSubMessage(msg)

    if payload.get("event_type") != FEED_AUDIT_NOTIFICATION_EVENT_TYPE:
        msg = "Feed Audit Notification has unsupported event_type"
        raise InvalidPubSubMessage(msg)

    if payload.get("schema_version") != FEED_AUDIT_NOTIFICATION_SCHEMA_VERSION:
        msg = "Feed Audit Notification has unsupported schema_version"
        raise InvalidPubSubMessage(msg)

    if not is_valid_feed_audit_notification_payload(payload):
        msg = "Feed Audit Notification has invalid snapshot fields"
        raise InvalidPubSubMessage(msg)


def _require_mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        msg = f"{label} must be an object"
        raise InvalidPubSubMessage(msg)
    return cast("Mapping[str, Any]", value)

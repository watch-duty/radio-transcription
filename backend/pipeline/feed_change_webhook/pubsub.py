"""Pub/Sub push parsing for Feed Change Notification log entries."""

from __future__ import annotations

import base64
import binascii
import json
from collections.abc import Mapping
from typing import Any, cast

from pydantic import ValidationError

from backend.pipeline.common.feed_change_notifications_contract import (
    FeedChangeNotificationPayload,
)


class InvalidPubSubMessage(ValueError):
    """Raised when a Pub/Sub push request cannot be relayed."""

    def __init__(self, reason: str, *, path: str = "envelope") -> None:
        super().__init__(reason)
        self.reason = reason
        self.path = path


def extract_feed_change_payload(envelope: Mapping[str, Any]) -> dict[str, Any]:
    """Extract and validate a Feed Change Notification.

    The input is the Pub/Sub push envelope emitted from Cloud Logging.
    """
    message = _require_mapping(envelope.get("message"), "message")
    data = message.get("data")
    if not isinstance(data, str) or not data:
        msg = "Pub/Sub message.data is required"
        raise InvalidPubSubMessage(msg, path="message.data")

    try:
        decoded = base64.b64decode(data, validate=True)
    except binascii.Error as exc:
        msg = "Pub/Sub message.data is not valid base64"
        raise InvalidPubSubMessage(msg, path="message.data") from exc

    try:
        log_entry = json.loads(decoded.decode("utf-8"))
    except UnicodeDecodeError as exc:
        msg = "Pub/Sub message.data is not UTF-8"
        raise InvalidPubSubMessage(msg, path="message.data") from exc
    except json.JSONDecodeError as exc:
        msg = "Pub/Sub message.data is not JSON"
        raise InvalidPubSubMessage(msg, path="message.data") from exc

    log_entry_mapping = _require_mapping(log_entry, "Cloud Logging LogEntry")
    json_payload = _require_mapping(
        log_entry_mapping.get("jsonPayload"), "jsonPayload"
    )
    return _validate_feed_change_payload(json_payload).model_dump(mode="json")


def _validate_feed_change_payload(
    payload: Mapping[str, Any],
) -> FeedChangeNotificationPayload:
    try:
        return FeedChangeNotificationPayload.model_validate(dict(payload))
    except ValidationError as exc:
        msg = "Feed Change Notification payload validation failed"
        raise InvalidPubSubMessage(
            msg,
            path=_validation_error_path(exc),
        ) from exc


def _require_mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        msg = f"{label} must be an object"
        raise InvalidPubSubMessage(msg, path=label)
    return cast("Mapping[str, Any]", value)


def _validation_error_path(exc: ValidationError) -> str:
    errors = exc.errors()
    if not errors:
        return "jsonPayload"

    location = errors[0].get("loc", ())
    if not location:
        return "jsonPayload"

    return "jsonPayload." + ".".join(str(value) for value in location)

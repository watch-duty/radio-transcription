"""Render Pub/Sub publish failure evidence for pipeline quarantine reasons."""

from __future__ import annotations

from google.cloud.pubsub_v1.publisher import exceptions as pubsub_exceptions

from backend.pipeline.ingestion import quarantine_reason


def publish_failure_reason(exc: Exception) -> str:
    """Build operator diagnostic text for a failed Pub/Sub publish."""
    if isinstance(exc, pubsub_exceptions.PublishToPausedOrderingKeyException):
        return (
            "Pub/Sub publish paused for ordering key: "
            f"{quarantine_reason.exception_text(exc)}"
        )

    text = str(exc)
    if "schema validation" in text.lower():
        parts = ["Pub/Sub schema validation failed"]
        if "INVALID_BINARY_PROTO_MESSAGE" in text:
            parts.append("INVALID_BINARY_PROTO_MESSAGE")
        parts.append(text)
        return "; ".join(parts)

    return f"Pub/Sub publish failed: {quarantine_reason.exception_text(exc)}"

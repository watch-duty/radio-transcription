"""Tests for Pub/Sub failure diagnostic rendering."""

from __future__ import annotations

from google.cloud.pubsub_v1.publisher import exceptions as pubsub_exceptions

from backend.pipeline.ingestion.failure_classifiers import pubsub


def test_publish_failure_reason_for_paused_ordering_key() -> None:
    reason = pubsub.publish_failure_reason(
        pubsub_exceptions.PublishToPausedOrderingKeyException("feed-42")
    )

    assert reason.startswith("Pub/Sub publish paused for ordering key:")
    assert "PublishToPausedOrderingKeyException" in reason


def test_publish_failure_reason_for_invalid_binary_schema_error() -> None:
    reason = pubsub.publish_failure_reason(
        RuntimeError("schema validation failed: INVALID_BINARY_PROTO_MESSAGE")
    )

    assert reason == (
        "Pub/Sub schema validation failed; "
        "INVALID_BINARY_PROTO_MESSAGE; "
        "schema validation failed: INVALID_BINARY_PROTO_MESSAGE"
    )


def test_publish_failure_reason_for_schema_error_without_binary_marker() -> None:
    reason = pubsub.publish_failure_reason(
        RuntimeError("schema validation failed: missing field")
    )

    assert reason == (
        "Pub/Sub schema validation failed; "
        "schema validation failed: missing field"
    )


def test_publish_failure_reason_for_generic_exception() -> None:
    reason = pubsub.publish_failure_reason(RuntimeError("pubsub boom"))

    assert reason == "Pub/Sub publish failed: RuntimeError: pubsub boom"

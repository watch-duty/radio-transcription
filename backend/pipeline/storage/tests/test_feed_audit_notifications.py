from __future__ import annotations

import inspect
import json
import logging
import unittest
from typing import Any, cast
from unittest import mock

from backend.pipeline.common.feed_audit_notification_contract import (
    FeedAuditNotificationPayload,
)
from backend.pipeline.storage import feed_audit_notifications

_EXPECTED_FEED_AUDIT_KEYS = set(
    FeedAuditNotificationPayload.model_fields,
)


def _feed_audit_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "event_type": "radio_transcription.feed_audit_notification",
        "schema_version": 1,
        "event_id": "audit-event-1",
        "action": "feed.failed",
        "occurred_at": "2026-06-26T22:00:00Z",
        "actor_id": "collector:test-worker",
        "feed_id": "feed-1",
        "feed_revision": 12,
        "before_values": {"status": "active"},
        "after_values": {"status": "failed"},
    }
    payload.update(overrides)
    return payload


class TestEmitFeedAuditNotification(unittest.TestCase):
    """Tests for feed_audit_notifications.emit_feed_audit_notification."""

    def test_emits_structured_log(self) -> None:
        payload = _feed_audit_payload()

        with self.assertLogs(
            "backend.pipeline.storage.feed_audit_notifications",
            level=logging.INFO,
        ) as cm:
            feed_audit_notifications.emit_feed_audit_notification(payload)

        self.assertEqual(len(cm.records), 1)
        record = cast("Any", cm.records[0])
        self.assertIn("Feed audit notification emitted", record.getMessage())
        self.assertIsInstance(record.json_fields, dict)
        self.assertEqual(set(record.json_fields), _EXPECTED_FEED_AUDIT_KEYS)
        self.assertNotIn("feed_audit_event", record.json_fields)
        self.assertEqual(record.json_fields, payload)

    def test_parses_string_payload(self) -> None:
        payload = _feed_audit_payload()

        with self.assertLogs(
            "backend.pipeline.storage.feed_audit_notifications",
            level=logging.INFO,
        ) as cm:
            feed_audit_notifications.emit_feed_audit_notification(
                json.dumps(payload)
            )

        self.assertEqual(len(cm.records), 1)
        record = cast("Any", cm.records[0])
        self.assertIsInstance(record.json_fields, dict)
        self.assertEqual(record.json_fields, payload)

    def test_noops_for_none(self) -> None:
        with mock.patch.object(
            feed_audit_notifications.logger,
            "info",
        ) as mock_info:
            feed_audit_notifications.emit_feed_audit_notification(None)

        mock_info.assert_not_called()

    def test_noops_for_non_mapping_payloads(self) -> None:
        values: list[object] = [
            ["not", "a", "mapping"],
        ]

        for value in values:
            with self.subTest(value=value):
                with mock.patch.object(
                    feed_audit_notifications.logger,
                    "info",
                ) as mock_info:
                    feed_audit_notifications.emit_feed_audit_notification(value)

                mock_info.assert_not_called()

    def test_logs_operational_error_for_invalid_json_string(self) -> None:
        with self.assertLogs(
            "backend.pipeline.storage.feed_audit_notifications",
            level=logging.ERROR,
        ) as cm:
            feed_audit_notifications.emit_feed_audit_notification("{")

        self.assertEqual(len(cm.records), 1)
        record = cast("Any", cm.records[0])
        self.assertIn(
            "Feed audit notification emission failed", record.getMessage()
        )
        self.assertEqual(
            record.json_fields,
            {
                "event": "feed_audit_notification_emit_failed",
                "failure_class": "producer_emit_error",
            },
        )
        self.assertNotIn("before_values", json.dumps(record.json_fields))
        self.assertNotIn("after_values", json.dumps(record.json_fields))

    def test_schema_invalid_mapping_payload_still_emits(self) -> None:
        payload = {
            key: value
            for key, value in _feed_audit_payload(event_type="wrong").items()
            if key != "feed_id"
        }

        with self.assertLogs(
            "backend.pipeline.storage.feed_audit_notifications",
            level=logging.INFO,
        ) as cm:
            feed_audit_notifications.emit_feed_audit_notification(payload)

        self.assertEqual(len(cm.records), 1)
        record = cast("Any", cm.records[0])
        self.assertEqual(record.json_fields, payload)

    def test_extra_fields_pass_through(self) -> None:
        payload = _feed_audit_payload(extra_context={"source": "test"})

        with self.assertLogs(
            "backend.pipeline.storage.feed_audit_notifications",
            level=logging.INFO,
        ) as cm:
            feed_audit_notifications.emit_feed_audit_notification(payload)

        self.assertEqual(len(cm.records), 1)
        record = cast("Any", cm.records[0])
        self.assertEqual(
            record.json_fields["extra_context"], {"source": "test"}
        )

    def test_never_raises_when_logging_fails(self) -> None:
        with mock.patch.object(
            feed_audit_notifications.logger,
            "info",
            side_effect=RuntimeError("logging broken"),
        ):
            feed_audit_notifications.emit_feed_audit_notification(
                _feed_audit_payload()
            )

    def test_helper_has_no_delivery_client_coupling(self) -> None:
        source = inspect.getsource(feed_audit_notifications)

        forbidden_strings = (
            "google.cloud",
            "pubsub",
            "webhook",
            "requests",
            "httpx",
            "CloudLoggingHandler",
            "log_struct",
        )
        for value in forbidden_strings:
            with self.subTest(value=value):
                self.assertNotIn(value, source)

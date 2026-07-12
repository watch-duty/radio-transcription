from __future__ import annotations

import logging
import unittest
from typing import Any, cast
from unittest import mock

from backend.pipeline.ingestion import quarantine_telemetry

_CONTEXT = {
    "profile": "legacy",
    "profile_digest": "profile-digest",
    "domain_id": "feed",
    "authority_kind": "feed",
}


class TestEmitQuarantineEvent(unittest.IsolatedAsyncioTestCase):
    """Tests for quarantine_telemetry.emit_quarantine_event."""

    def tearDown(self) -> None:
        # Reset module state between tests.
        quarantine_telemetry._client = None

    async def test_emits_structured_log(self) -> None:
        """ERROR log is emitted with the correct extra fields."""
        quarantine_telemetry.configure(None)
        reason = "ConnectionError: Cannot connect to host openmhz.com:443"

        with self.assertLogs(
            "backend.pipeline.ingestion.quarantine_telemetry",
            level=logging.ERROR,
        ) as cm:
            await quarantine_telemetry.emit_quarantine_event(
                feed_id="abc-123",
                feed_name="Test Feed",
                source_type="bcfy_feeds",
                reason=reason,
                status_reason="system_unexpected_error",
                **_CONTEXT,
            )

        self.assertEqual(len(cm.records), 1)
        record = cast("Any", cm.records[0])
        # D-11: emit uses extra={"json_fields": {...}} — LogRecord stores the
        # wrapped dict as getattr(record, "json_fields"). The CloudLoggingHandler flattens
        # it identically to flat extras in production (Cloud Logging payload
        # shape unchanged), but in-repo assertions read the wrapped key.
        self.assertEqual(record.json_fields["event_type"], "feed_quarantined")
        self.assertEqual(record.json_fields["feed_id"], "abc-123")
        self.assertEqual(record.json_fields["feed_name"], "Test Feed")
        self.assertEqual(record.json_fields["reason"], reason)
        self.assertEqual(
            record.json_fields["status_reason"],
            "system_unexpected_error",
        )
        self.assertEqual(record.json_fields["source_type"], "bcfy_feeds")
        for key, value in _CONTEXT.items():
            self.assertEqual(record.json_fields[key], value)
        # Reason is also interpolated into the message so the Logs Explorer
        # summary row shows it without expanding the structured payload.
        self.assertEqual(record.getMessage(), f"Feed quarantined: {reason}")

    async def test_calls_write_time_series_when_configured(self) -> None:
        """Metric is written when a project ID is configured."""
        mock_client = mock.AsyncMock()
        mock_client.project_id = "test-project"
        quarantine_telemetry.configure("test-project")
        quarantine_telemetry._client = mock_client

        await quarantine_telemetry.emit_quarantine_event(
            feed_id="abc-123",
            feed_name="Test Feed",
            source_type="bcfy_feeds",
            reason="r",
            status_reason="source_offline",
            **_CONTEXT,
        )

        expected_labels = {
            **_CONTEXT,
            "source_type": "bcfy_feeds",
        }

        mock_client.write_time_series.assert_awaited_once_with(
            metric_type="custom.googleapis.com/feeds/quarantine_events",
            labels=expected_labels,
            value=1,
            resource_labels={"project_id": "test-project"},
        )
        status_reason_key = "status_reason"
        self.assertNotIn(status_reason_key, expected_labels)
        for forbidden in ("feed_id", "feed_name", "reason"):
            self.assertNotIn(forbidden, expected_labels)

    async def test_skips_metric_when_not_configured(self) -> None:
        """No metric call when project ID is None."""
        quarantine_telemetry.configure(None)

        # Should not raise and should not attempt any GCP call.
        await quarantine_telemetry.emit_quarantine_event(
            feed_id="abc",
            feed_name="F",
            source_type="s",
            reason="r",
            status_reason="system_unexpected_error",
            **_CONTEXT,
        )

        self.assertIsNone(quarantine_telemetry._client)

    async def test_never_raises_on_api_error(self) -> None:
        """API errors are caught — emit_quarantine_event never raises."""
        mock_client = mock.AsyncMock()
        mock_client.write_time_series.side_effect = RuntimeError("boom")
        quarantine_telemetry.configure("p")
        quarantine_telemetry._client = mock_client

        # Must not raise.
        await quarantine_telemetry.emit_quarantine_event(
            feed_id="abc",
            feed_name="F",
            source_type="s",
            reason="r",
            status_reason="system_unexpected_error",
            **_CONTEXT,
        )

    async def test_never_raises_even_if_logging_fails(self) -> None:
        """Even a broken logger cannot make emit_quarantine_event raise."""
        quarantine_telemetry.configure(None)

        with mock.patch.object(
            quarantine_telemetry.logger,
            "error",
            side_effect=RuntimeError("logging broken"),
        ):
            # Must not raise.
            await quarantine_telemetry.emit_quarantine_event(
                feed_id="abc",
                feed_name="F",
                source_type="s",
                reason="r",
                status_reason="system_unexpected_error",
                **_CONTEXT,
            )

    async def test_log_emitted_even_when_metric_fails(self) -> None:
        """Structured log is emitted before the metric call."""
        mock_client = mock.AsyncMock()
        mock_client.write_time_series.side_effect = RuntimeError("boom")
        quarantine_telemetry.configure("p")
        quarantine_telemetry._client = mock_client

        with self.assertLogs(
            "backend.pipeline.ingestion.quarantine_telemetry",
            level=logging.ERROR,
        ) as cm:
            await quarantine_telemetry.emit_quarantine_event(
                feed_id="abc",
                feed_name="F",
                source_type="s",
                reason="r",
                status_reason="system_unexpected_error",
                **_CONTEXT,
            )

        # The ERROR log was emitted before the metric call failed, and it
        # carries the reason so on-callers reading the log entry know the
        # structured payload is intact even when metric emission breaks.
        error_records = [r for r in cm.records if r.levelno == logging.ERROR]
        self.assertEqual(len(error_records), 1)
        record = cast("Any", error_records[0])
        self.assertEqual(record.json_fields["reason"], "r")
        self.assertEqual(
            record.json_fields["status_reason"],
            "system_unexpected_error",
        )


class TestConfigure(unittest.TestCase):
    """Tests for quarantine_telemetry.configure."""

    def tearDown(self) -> None:
        quarantine_telemetry._client = None

    @mock.patch(
        "backend.pipeline.ingestion.quarantine_telemetry.MonitoringClient"
    )
    def test_creates_client_with_project_id(
        self,
        mock_cls: mock.MagicMock,
    ) -> None:
        """configure() creates a MonitoringClient when project ID is set."""
        quarantine_telemetry.configure("my-project")

        mock_cls.assert_called_once_with("my-project")
        self.assertIs(quarantine_telemetry._client, mock_cls.return_value)

    def test_none_disables_client(self) -> None:
        """configure(None) sets _client to None."""
        quarantine_telemetry.configure(None)

        self.assertIsNone(quarantine_telemetry._client)


class TestFeedQuarantinedGoldenFile(unittest.IsolatedAsyncioTestCase):
    """D-13: feed_quarantined emit's json_fields key-set matches the golden file.

    A PR that adds, removes, or renames a key in the emit's json_fields dict
    without updating tests/golden/feed_quarantined.json fails this test.
    Key-set equality only — no value comparison per D-12.
    """

    def tearDown(self) -> None:
        quarantine_telemetry._client = None

    async def test_json_fields_keys_match_golden(self) -> None:
        import json  # noqa: PLC0415 -- keep import local to this test
        import pathlib  # noqa: PLC0415

        golden_path = (
            pathlib.Path(__file__).parent / "golden" / "feed_quarantined.json"
        )
        with golden_path.open(encoding="utf-8") as fh:
            golden = json.load(fh)

        quarantine_telemetry.configure(None)
        with self.assertLogs(
            "backend.pipeline.ingestion.quarantine_telemetry",
            level=logging.ERROR,
        ) as cm:
            await quarantine_telemetry.emit_quarantine_event(
                feed_id="abc-123",
                feed_name="Test Feed",
                source_type="bcfy_feeds",
                reason="r",
                status_reason="system_unexpected_error",
                **_CONTEXT,
            )

        self.assertEqual(len(cm.records), 1)
        record = cast("Any", cm.records[0])
        self.assertEqual(
            set(record.json_fields.keys()),
            set(golden["expected_keys"]),
        )

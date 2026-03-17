"""Unit tests for the telemetry metrics exporters."""

import unittest
from unittest.mock import MagicMock, patch

from google.api_core.exceptions import GoogleAPIError

from backend.pipeline.transcription.enums import MetricsExporterType
from backend.pipeline.transcription.telemetry import (
    GcpMonitoringConfig,
    GcpMonitoringExporter,
    MultiExporter,
    get_metrics_exporter,
)


class TestMetricsExporters(unittest.TestCase):
    """Tests for the metrics exporters."""

    def test_gcp_monitoring_config_parsing(self) -> None:
        """Test that the GCP monitoring config parses valid JSON and rejects invalid JSON."""
        config = GcpMonitoringConfig.from_json('{"some_unknown_key": "val"}')
        self.assertIsInstance(config, GcpMonitoringConfig)

        # Invalid JSON
        with self.assertRaises(ValueError):
            GcpMonitoringConfig.from_json("invalid-json")

        # Empty string
        config_empty = GcpMonitoringConfig.from_json("")
        self.assertIsInstance(config_empty, GcpMonitoringConfig)

    @patch("backend.pipeline.transcription.telemetry.monitoring_v3.MetricServiceClient")
    def test_gcp_exporter_setup_and_record(self, mock_client_class: MagicMock) -> None:
        mock_client_inst = MagicMock()
        mock_client_class.return_value = mock_client_inst

        exporter = GcpMonitoringExporter("test-project", "{}")
        exporter.record_transcription_time(feed_id="f1", duration_ms=100)
        exporter.setup()

        exporter.record_transcription_time(feed_id="f1", duration_ms=100)
        series = mock_client_inst.create_time_series.call_args.kwargs["time_series"][0]
        self.assertEqual(
            series.metric.type,
            "custom.googleapis.com/radio_transcription/transcription_time",
        )

        exporter.record_stitching_time(feed_id="f1", duration_ms=20)
        series = mock_client_inst.create_time_series.call_args.kwargs["time_series"][0]
        self.assertEqual(
            series.metric.type,
            "custom.googleapis.com/radio_transcription/stitching_time",
        )

    @patch("backend.pipeline.transcription.telemetry.monitoring_v3.MetricServiceClient")
    def test_gcp_exporter_handles_exception(self, mock_client_class: MagicMock) -> None:
        """Test that GCP exporter ignores and logs exceptions instead of crashing."""
        mock_client_inst = MagicMock()
        mock_client_inst.create_time_series.side_effect = GoogleAPIError(
            "Network failure"
        )
        mock_client_class.return_value = mock_client_inst

        exporter = GcpMonitoringExporter("test-project", "{}")
        exporter.setup()

        # Exception should be caught and logged, not raised
        exporter.record_transcription_time(feed_id="f1", duration_ms=100)
        self.assertEqual(mock_client_inst.create_time_series.call_count, 1)

    def test_multi_exporter(self) -> None:
        mock_exp1 = MagicMock()
        mock_exp2 = MagicMock()

        multi = MultiExporter([mock_exp1, mock_exp2])
        multi.setup()

        multi.record_transcription_time(feed_id="f1", duration_ms=250)
        mock_exp1.record_transcription_time.assert_called_once_with(
            feed_id="f1", duration_ms=250
        )
        mock_exp2.record_transcription_time.assert_called_once_with(
            feed_id="f1", duration_ms=250
        )

        multi.record_stitching_time(feed_id="f1", duration_ms=50)
        mock_exp1.record_stitching_time.assert_called_once_with(
            feed_id="f1", duration_ms=50
        )

    @patch("backend.pipeline.transcription.telemetry.GcpMonitoringExporter")
    def test_get_metrics_exporter(self, mock_gcp_exporter_class: MagicMock) -> None:
        """Test the exporter factory handles GCP and empty list configurations."""
        # Test NONE or empty
        exporter_none = get_metrics_exporter([MetricsExporterType.NONE], "proj", "{}")
        self.assertIsInstance(exporter_none, MultiExporter)
        self.assertEqual(len(exporter_none.exporters), 0)  # type: ignore

        exporter_empty = get_metrics_exporter([], "proj", "{}")
        self.assertIsInstance(exporter_empty, MultiExporter)
        self.assertEqual(len(exporter_empty.exporters), 0)  # type: ignore

        # Test GCP mapping
        mock_gcp_inst = MagicMock()
        mock_gcp_exporter_class.return_value = mock_gcp_inst

        exporter_gcp = get_metrics_exporter([MetricsExporterType.GCP], "proj", "{}")
        self.assertIsInstance(exporter_gcp, MultiExporter)
        self.assertEqual(len(exporter_gcp.exporters), 1)  # type: ignore
        self.assertEqual(exporter_gcp.exporters[0], mock_gcp_inst)  # type: ignore
        mock_gcp_exporter_class.assert_called_once_with("proj", "{}")

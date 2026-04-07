"""Unit tests for the telemetry metrics exporters."""

import unittest
from unittest.mock import MagicMock, patch

from backend.pipeline.transcription.enums import MetricsExporterType
from backend.pipeline.transcription.telemetry import (
    GcpMonitoringConfig,
    GcpMonitoringExporter,
    MultiExporter,
    get_metrics_exporter,
)


class TestMetricsExporters(unittest.TestCase):
    def test_gcp_monitoring_config_parsing(self) -> None:
        """Verifies that GcpMonitoringConfig can robustly parse completely empty strings and handles invalid JSON input by actively throwing a ValueError."""
        config = GcpMonitoringConfig.from_json('{"some_unknown_key": "val"}')
        self.assertIsInstance(config, GcpMonitoringConfig)

        # Invalid JSON
        with self.assertRaises(ValueError):
            GcpMonitoringConfig.from_json("invalid-json")

        # Empty string
        config_empty = GcpMonitoringConfig.from_json("")
        self.assertIsInstance(config_empty, GcpMonitoringConfig)

    @patch("backend.pipeline.transcription.telemetry.Metrics.distribution")
    def test_gcp_exporter_setup_and_record(
        self, mock_distribution: MagicMock
    ) -> None:
        """Verifies the GcpMonitoringExporter correctly yields native Beam Distribution metrics specifically for transcription and stitching."""
        mock_dist_inst = MagicMock()
        mock_distribution.return_value = mock_dist_inst

        exporter = GcpMonitoringExporter("test-project", "{}")
        exporter.record_transcription_time(feed_id="f1", duration_ms=100)
        exporter.setup()

        exporter.record_transcription_time(feed_id="f1", duration_ms=100)
        mock_distribution.assert_any_call(
            "custom.googleapis.com/radio_transcription", "transcription_time"
        )
        mock_dist_inst.update.assert_any_call(100)

        exporter.record_stitching_time(feed_id="f1", duration_ms=20)
        mock_distribution.assert_any_call(
            "custom.googleapis.com/radio_transcription", "stitching_time"
        )
        mock_dist_inst.update.assert_any_call(20)

    def test_multi_exporter(self) -> None:
        """Verifies that MultiExporter successfully and uniformly delegates method execution across all configured internal component metrics exporters."""
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
    def test_get_metrics_exporter(
        self, mock_gcp_exporter_class: MagicMock
    ) -> None:
        """Verifies that get_metrics_exporter outputs an empty composite exporter when inactive, but includes GcpMonitoringExporter if specifically toggled."""
        # Test NONE or empty
        exporter_none = get_metrics_exporter(
            [MetricsExporterType.NONE], "proj", "{}"
        )
        self.assertIsInstance(exporter_none, MultiExporter)
        self.assertEqual(len(exporter_none.exporters), 0)  # type: ignore

        exporter_empty = get_metrics_exporter([], "proj", "{}")
        self.assertIsInstance(exporter_empty, MultiExporter)
        self.assertEqual(len(exporter_empty.exporters), 0)  # type: ignore

        # Test GCP mapping
        mock_gcp_inst = MagicMock()
        mock_gcp_exporter_class.return_value = mock_gcp_inst

        exporter_gcp = get_metrics_exporter(
            [MetricsExporterType.GCP], "proj", "{}"
        )
        self.assertIsInstance(exporter_gcp, MultiExporter)
        self.assertEqual(len(exporter_gcp.exporters), 1)  # type: ignore
        self.assertEqual(exporter_gcp.exporters[0], mock_gcp_inst)  # type: ignore
        mock_gcp_exporter_class.assert_called_once_with("proj", "{}")

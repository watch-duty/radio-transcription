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

        # Calling record before setup does nothing (client is None)
        exporter.record_transcription_time(feed_id="f1", duration_ms=100)
        mock_client_inst.create_time_series.assert_not_called()

        exporter.setup()

        # Calling record after setup sends the metric
        exporter.record_transcription_time(feed_id="f1", duration_ms=100)

        self.assertEqual(mock_client_inst.create_time_series.call_count, 1)
        kwargs = mock_client_inst.create_time_series.call_args.kwargs
        self.assertEqual(kwargs["name"], "projects/test-project")

        series = kwargs["time_series"][0]
        self.assertEqual(
            series.metric.type,
            "custom.googleapis.com/radio_transcription/transcription_time",
        )
        self.assertEqual(series.metric.labels["feed_id"], "f1")
        self.assertEqual(series.points[0].value.int64_value, 100)

    @patch("backend.pipeline.transcription.telemetry.monitoring_v3.MetricServiceClient")
    def test_gcp_exporter_handles_exception(self, mock_client_class: MagicMock) -> None:
        mock_client_inst = MagicMock()
        mock_client_inst.create_time_series.side_effect = Exception("Network failure")
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

        mock_exp1.setup.assert_called_once()
        mock_exp2.setup.assert_called_once()

        multi.record_transcription_time(feed_id="f1", duration_ms=250)

        mock_exp1.record_transcription_time.assert_called_once_with(
            feed_id="f1", duration_ms=250
        )
        mock_exp2.record_transcription_time.assert_called_once_with(
            feed_id="f1", duration_ms=250
        )

    @patch("backend.pipeline.transcription.telemetry.GcpMonitoringExporter")
    def test_get_metrics_exporter(self, mock_gcp_exporter_class: MagicMock) -> None:
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

import abc
import logging
import time
from collections.abc import Sequence
from dataclasses import dataclass

from google.api_core.exceptions import GoogleAPIError
from google.cloud import monitoring_v3

from backend.pipeline.transcription.enums import MetricsExporterType
from backend.pipeline.transcription.utils import JsonConfigMixin

logger = logging.getLogger(__name__)


class MetricsExporter(abc.ABC):
    """
    Abstract interface for pushing custom pipeline metrics to external dashboards.
    """

    @abc.abstractmethod
    def setup(self) -> None:
        pass

    @abc.abstractmethod
    def record_transcription_time(self, *, feed_id: str, duration_ms: int) -> None:
        pass


@dataclass(frozen=True)
class GcpMonitoringConfig(JsonConfigMixin):
    """Strongly typed configuration for the GCP Monitoring Exporter."""

    metric_prefix: str = "custom.googleapis.com/radio_transcription"
    duration_metric_name: str = "transcription_time"


class GcpMonitoringExporter(MetricsExporter):
    """
    Exports metrics to Google Cloud Monitoring (Stackdriver).
    """

    def __init__(self, project_id: str, config_json: str) -> None:
        self.project_id = project_id
        self.config_json = config_json
        self.client: monitoring_v3.MetricServiceClient | None = None
        self.config: GcpMonitoringConfig | None = None

    def setup(self) -> None:
        self.client = monitoring_v3.MetricServiceClient()
        self.config = GcpMonitoringConfig.from_json(self.config_json)

    def record_transcription_time(self, *, feed_id: str, duration_ms: int) -> None:
        if not self.client or not self.config:
            return

        project_name = f"projects/{self.project_id}"
        series = monitoring_v3.TimeSeries()
        series.metric.type = (
            f"{self.config.metric_prefix}/{self.config.duration_metric_name}"
        )
        series.metric.labels["feed_id"] = feed_id

        point = monitoring_v3.Point()
        point.value.int64_value = duration_ms
        now = time.time()
        point.interval = monitoring_v3.TimeInterval(
            end_time={"seconds": int(now), "nanos": int((now - int(now)) * 10**9)}
        )

        series.points = [point]
        try:
            self.client.create_time_series(name=project_name, time_series=[series])
        except GoogleAPIError as e:
            logger.warning("Failed to export GCP metric: %s", e)


class MultiExporter(MetricsExporter):
    """
    Broadcasts metrics to multiple configured exporters.
    """

    def __init__(self, exporters: Sequence[MetricsExporter]) -> None:
        self.exporters = exporters

    def setup(self) -> None:
        for exporter in self.exporters:
            exporter.setup()

    def record_transcription_time(self, *, feed_id: str, duration_ms: int) -> None:
        for exporter in self.exporters:
            exporter.record_transcription_time(feed_id=feed_id, duration_ms=duration_ms)


def get_metrics_exporter(
    exporter_types: list[MetricsExporterType], project_id: str, config_json: str
) -> MetricsExporter:
    if not exporter_types or MetricsExporterType.NONE in exporter_types:
        return MultiExporter([])

    exporters: list[MetricsExporter] = []
    if MetricsExporterType.GCP in exporter_types:
        exporters.append(GcpMonitoringExporter(project_id, config_json))

    return MultiExporter(exporters)

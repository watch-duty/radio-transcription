"""Application-level metrics exporters (e.g. DataDog) for pipeline observability."""

import abc
import logging
from collections.abc import Sequence

from apache_beam.metrics import Metrics

from backend.pipeline.transcription.constants import (
    GCP_DURATION_METRIC_NAME,
    GCP_METRIC_PREFIX,
    GCP_STITCHING_METRIC_NAME,
)
from backend.pipeline.transcription.enums import MetricsExporterType
from backend.pipeline.transcription.utils import ConfigBase

logger = logging.getLogger(__name__)


class MetricsExporter(abc.ABC):
    """Abstract interface for pushing custom pipeline metrics to external dashboards."""

    @abc.abstractmethod
    def setup(self) -> None:
        """Instantiates unpicklable clients (e.g. gRPC stubs) lazily on the worker node."""

    @abc.abstractmethod
    def record_transcription_time(
        self, *, feed_id: str, duration_ms: int
    ) -> None:
        """Records the transcription time telemetry."""

    @abc.abstractmethod
    def record_stitching_time(self, *, feed_id: str, duration_ms: int) -> None:
        """Records the stitching time telemetry."""


class GcpMonitoringConfig(ConfigBase):
    """Strongly typed configuration for the GCP Monitoring Exporter."""

    metric_prefix: str = GCP_METRIC_PREFIX
    duration_metric_name: str = GCP_DURATION_METRIC_NAME
    stitching_metric_name: str = GCP_STITCHING_METRIC_NAME


class GcpMonitoringExporter(MetricsExporter):
    """Exports metrics using Apache Beam's native built-in Distribution Metrics API."""

    def __init__(self, project_id: str, config_json: str) -> None:
        """Initializes the exporter with the configuration JSON."""
        self.project_id = project_id
        self.config_json = config_json
        self.config: GcpMonitoringConfig | None = None

    def setup(self) -> None:
        """Parses configuration lazily on the executing worker."""
        self.config = GcpMonitoringConfig.from_json(self.config_json)

    def record_transcription_time(
        self, *, feed_id: str, duration_ms: int
    ) -> None:
        """Records the transcription time telemetry as a distribution."""
        if self.config:
            Metrics.distribution(
                self.config.metric_prefix, self.config.duration_metric_name
            ).update(duration_ms)

    def record_stitching_time(self, *, feed_id: str, duration_ms: int) -> None:
        """Records the stitching time telemetry as a distribution."""
        if self.config:
            Metrics.distribution(
                self.config.metric_prefix, self.config.stitching_metric_name
            ).update(duration_ms)


class MultiExporter(MetricsExporter):
    """Broadcasts metrics to multiple configured exporters."""

    def __init__(self, exporters: Sequence[MetricsExporter]) -> None:
        """Initializes the broadcaster with a list of downstream exporters to fan-out to."""
        self.exporters = exporters

    def setup(self) -> None:
        """Cascades the setup lifecycle call to all registered downstream exporters."""
        for exporter in self.exporters:
            exporter.setup()

    def record_transcription_time(
        self, *, feed_id: str, duration_ms: int
    ) -> None:
        """Records the transcription time telemetry."""
        for exporter in self.exporters:
            exporter.record_transcription_time(
                feed_id=feed_id, duration_ms=duration_ms
            )

    def record_stitching_time(self, *, feed_id: str, duration_ms: int) -> None:
        """Records the stitching time telemetry."""
        for exporter in self.exporters:
            exporter.record_stitching_time(
                feed_id=feed_id, duration_ms=duration_ms
            )


def get_metrics_exporter(
    exporter_types: list[MetricsExporterType], project_id: str, config_json: str
) -> MetricsExporter:
    """A factory method instantiating the requested MetricsExporter implementation based on the enum type."""
    if not exporter_types or MetricsExporterType.NONE in exporter_types:
        return MultiExporter([])

    exporters: list[MetricsExporter] = []
    if MetricsExporterType.GCP in exporter_types:
        exporters.append(GcpMonitoringExporter(project_id, config_json))

    return MultiExporter(exporters)

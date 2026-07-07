from __future__ import annotations

import time

from google.cloud import monitoring_v3

from backend.pipeline.common.constants import NANOS_PER_SECOND


class MonitoringClient:
    """Lazily initialized async Google Cloud Monitoring client.

    Thread safety: ``_get_client`` is not thread-safe.  This is fine
    because all callers run on the single-threaded asyncio event loop.
    Do not call from the OS heartbeat thread.
    """

    def __init__(self, project_id: str) -> None:
        self.project_id = project_id
        self._client: monitoring_v3.MetricServiceAsyncClient | None = None

    def _get_client(self) -> monitoring_v3.MetricServiceAsyncClient:
        """Return a shared async client, creating one lazily."""
        if self._client is None:
            self._client = monitoring_v3.MetricServiceAsyncClient()
        return self._client

    async def write_time_series(
        self,
        metric_type: str,
        labels: dict[str, str],
        value: int,
        *,
        resource_type: str = "global",
        resource_labels: dict[str, str],
    ) -> None:
        """Write a single GAUGE INT64 data point to Cloud Monitoring.

        Args:
            metric_type: e.g. ``custom.googleapis.com/feeds/quarantine_events``.
            labels: metric labels (callers enforce allowlist;
                cardinality-sensitive).
            value: int64 GAUGE value.
            resource_type: monitored-resource type. Defaults to ``"global"``.
                Pass ``"gce_instance"`` for per-VM metrics.
            resource_labels: resource labels (required). Caller owns the full
                label dict — the client does not inject any defaults.
        """
        series = monitoring_v3.TimeSeries()
        series.metric.type = metric_type
        series.metric.labels.update(labels)
        series.resource.type = resource_type
        series.resource.labels.update(resource_labels)

        now = time.time()
        point = monitoring_v3.Point()
        point.value.int64_value = value
        point.interval = monitoring_v3.TimeInterval(
            end_time={
                "seconds": int(now),
                "nanos": int((now - int(now)) * NANOS_PER_SECOND),
            }
        )
        series.points = [point]

        await self._get_client().create_time_series(
            name=f"projects/{self.project_id}",
            time_series=[series],
        )

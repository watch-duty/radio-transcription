from __future__ import annotations

import time

from google.cloud import monitoring_v3

_NANOS_PER_SECOND = 1_000_000_000


class MonitoringClient:
    """Lazily initialized async Google Cloud Monitoring client."""

    def __init__(self, project_id: str) -> None:
        self._project_id = project_id
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
    ) -> None:
        """Write a single GAUGE INT64 data point to Cloud Monitoring."""
        series = monitoring_v3.TimeSeries()
        series.metric.type = metric_type
        series.metric.labels.update(labels)
        series.resource.type = "global"

        now = time.time()
        point = monitoring_v3.Point()
        point.value.int64_value = value
        point.interval = monitoring_v3.TimeInterval(
            end_time={
                "seconds": int(now),
                "nanos": int((now - int(now)) * _NANOS_PER_SECOND),
            }
        )
        series.points = [point]

        await self._get_client().create_time_series(
            name=f"projects/{self._project_id}",
            time_series=[series],
        )

from __future__ import annotations

import unittest
from unittest import mock

from google.api_core.exceptions import GoogleAPIError
from google.cloud import monitoring_v3

from backend.pipeline.common.clients import monitoring_client


class TestMonitoringClient(unittest.IsolatedAsyncioTestCase):
    """Tests for lazy init and write behavior of MonitoringClient."""

    @mock.patch(
        "backend.pipeline.common.clients.monitoring_client.monitoring_v3"
    )
    async def test_client_not_created_at_init(
        self,
        mock_monitoring: mock.MagicMock,
    ) -> None:
        """MetricServiceAsyncClient is not created at construction."""
        client = monitoring_client.MonitoringClient("test-project")

        mock_monitoring.MetricServiceAsyncClient.assert_not_called()
        self.assertIsNone(client._client)

    @mock.patch(
        "backend.pipeline.common.clients.monitoring_client.monitoring_v3"
    )
    async def test_write_creates_client_lazily(
        self,
        mock_monitoring: mock.MagicMock,
    ) -> None:
        """First write_time_series call creates the async client."""
        mock_async_client = mock.AsyncMock()
        mock_monitoring.MetricServiceAsyncClient.return_value = (
            mock_async_client
        )

        client = monitoring_client.MonitoringClient("test-project")
        await client.write_time_series(
            metric_type="custom.googleapis.com/test",
            labels={"k": "v"},
            value=1,
        )

        mock_monitoring.MetricServiceAsyncClient.assert_called_once()

    @mock.patch(
        "backend.pipeline.common.clients.monitoring_client.monitoring_v3"
    )
    async def test_write_passes_correct_args(
        self,
        mock_monitoring: mock.MagicMock,
    ) -> None:
        """create_time_series receives the correct project and series."""
        mock_async_client = mock.AsyncMock()
        mock_monitoring.MetricServiceAsyncClient.return_value = (
            mock_async_client
        )
        # Let the protobuf-like objects be real MagicMocks.
        mock_monitoring.TimeSeries.return_value = mock.MagicMock()
        mock_monitoring.Point.return_value = mock.MagicMock()
        mock_monitoring.TimeInterval.return_value = mock.MagicMock()

        client = monitoring_client.MonitoringClient("my-project")
        await client.write_time_series(
            metric_type="custom.googleapis.com/feeds/quarantine_events",
            labels={"feed_id": "abc", "feed_name": "X"},
            value=42,
        )

        mock_async_client.create_time_series.assert_awaited_once()
        call_kwargs = mock_async_client.create_time_series.call_args[1]
        self.assertEqual(call_kwargs["name"], "projects/my-project")
        self.assertEqual(len(call_kwargs["time_series"]), 1)

    @mock.patch(
        "backend.pipeline.common.clients.monitoring_client.monitoring_v3"
    )
    async def test_client_reused_across_calls(
        self,
        mock_monitoring: mock.MagicMock,
    ) -> None:
        """The async client is created once and reused."""
        mock_async_client = mock.AsyncMock()
        mock_monitoring.MetricServiceAsyncClient.return_value = (
            mock_async_client
        )
        mock_monitoring.TimeSeries.return_value = mock.MagicMock()
        mock_monitoring.Point.return_value = mock.MagicMock()
        mock_monitoring.TimeInterval.return_value = mock.MagicMock()

        client = monitoring_client.MonitoringClient("p")
        await client.write_time_series("t", {}, 1)
        await client.write_time_series("t", {}, 2)

        mock_monitoring.MetricServiceAsyncClient.assert_called_once()

    @mock.patch(
        "backend.pipeline.common.clients.monitoring_client.monitoring_v3"
    )
    async def test_errors_propagate(
        self,
        mock_monitoring: mock.MagicMock,
    ) -> None:
        """API errors are not caught — they propagate to the caller."""
        mock_async_client = mock.AsyncMock()
        mock_async_client.create_time_series.side_effect = GoogleAPIError(
            "quota exceeded"
        )
        mock_monitoring.MetricServiceAsyncClient.return_value = (
            mock_async_client
        )
        mock_monitoring.TimeSeries.return_value = mock.MagicMock()
        mock_monitoring.Point.return_value = mock.MagicMock()
        mock_monitoring.TimeInterval.return_value = mock.MagicMock()

        client = monitoring_client.MonitoringClient("p")

        with self.assertRaises(GoogleAPIError):
            await client.write_time_series("t", {}, 1)

    @mock.patch(
        "backend.pipeline.common.clients.monitoring_client.monitoring_v3"
    )
    async def test_default_resource_is_global_with_project_id(
        self,
        mock_monitoring: mock.MagicMock,
    ) -> None:
        """Backward-compat: omitting the new kwargs writes resource.type='global' and resource.labels={'project_id': ...}.

        This test locks the shipped quarantine-caller behavior (no resource_type,
        no resource_labels passed) — the D-19 backward-compat contract.
        """
        mock_async_client = mock.AsyncMock()
        mock_monitoring.MetricServiceAsyncClient.return_value = (
            mock_async_client
        )
        # Use a real TimeSeries so the client populates resource.type / labels
        # for inspection.
        real_series = monitoring_v3.TimeSeries()
        mock_monitoring.TimeSeries.return_value = real_series
        # Point and TimeInterval must be real protos because the client
        # assigns `series.points = [point]` on the real TimeSeries, which
        # proto-plus type-checks against the actual Point message class.
        mock_monitoring.Point.side_effect = monitoring_v3.Point
        mock_monitoring.TimeInterval.side_effect = monitoring_v3.TimeInterval

        client = monitoring_client.MonitoringClient("my-project")
        await client.write_time_series(
            metric_type="custom.googleapis.com/test",
            labels={"k": "v"},
            value=1,
        )

        self.assertEqual(real_series.resource.type, "global")
        self.assertEqual(
            dict(real_series.resource.labels),
            {"project_id": "my-project"},
        )

    @mock.patch(
        "backend.pipeline.common.clients.monitoring_client.monitoring_v3"
    )
    async def test_gce_instance_resource_with_full_labels(
        self,
        mock_monitoring: mock.MagicMock,
    ) -> None:
        """Passing resource_type='gce_instance' + full resource_labels writes them verbatim.

        This test locks the D-19 new path used by metric_reporter.py (plan 03-02).
        """
        mock_async_client = mock.AsyncMock()
        mock_monitoring.MetricServiceAsyncClient.return_value = (
            mock_async_client
        )
        real_series = monitoring_v3.TimeSeries()
        mock_monitoring.TimeSeries.return_value = real_series
        # Point and TimeInterval must be real protos because the client
        # assigns `series.points = [point]` on the real TimeSeries, which
        # proto-plus type-checks against the actual Point message class.
        mock_monitoring.Point.side_effect = monitoring_v3.Point
        mock_monitoring.TimeInterval.side_effect = monitoring_v3.TimeInterval

        client = monitoring_client.MonitoringClient("my-project")
        await client.write_time_series(
            metric_type="custom.googleapis.com/ingestion/active_feed_count",
            labels={},
            value=42,
            resource_type="gce_instance",
            resource_labels={
                "project_id": "my-project",
                "instance_id": "1234567890",
                "zone": "us-central1-a",
            },
        )

        self.assertEqual(real_series.resource.type, "gce_instance")
        self.assertEqual(
            dict(real_series.resource.labels),
            {
                "project_id": "my-project",
                "instance_id": "1234567890",
                "zone": "us-central1-a",
            },
        )

    @mock.patch(
        "backend.pipeline.common.clients.monitoring_client.monitoring_v3"
    )
    async def test_partial_resource_labels_does_not_inject_project_id(
        self,
        mock_monitoring: mock.MagicMock,
    ) -> None:
        """When caller supplies resource_labels, we do NOT auto-inject project_id.

        Locks the D-19 invariant: caller owns the full label dict. Passing
        resource_labels={'instance_id': 'i1'} alone means the emitted labels
        are EXACTLY {'instance_id': 'i1'} — no implicit mutations.
        """
        mock_async_client = mock.AsyncMock()
        mock_monitoring.MetricServiceAsyncClient.return_value = (
            mock_async_client
        )
        real_series = monitoring_v3.TimeSeries()
        mock_monitoring.TimeSeries.return_value = real_series
        # Point and TimeInterval must be real protos because the client
        # assigns `series.points = [point]` on the real TimeSeries, which
        # proto-plus type-checks against the actual Point message class.
        mock_monitoring.Point.side_effect = monitoring_v3.Point
        mock_monitoring.TimeInterval.side_effect = monitoring_v3.TimeInterval

        client = monitoring_client.MonitoringClient("my-project")
        await client.write_time_series(
            metric_type="custom.googleapis.com/test",
            labels={},
            value=1,
            resource_type="gce_instance",
            resource_labels={"instance_id": "i1"},
        )

        self.assertEqual(real_series.resource.type, "gce_instance")
        self.assertEqual(
            dict(real_series.resource.labels),
            {"instance_id": "i1"},
        )

from __future__ import annotations

import unittest
from unittest import mock

from google.api_core.exceptions import GoogleAPIError

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

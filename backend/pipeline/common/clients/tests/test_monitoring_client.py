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
            resource_labels={"project_id": "test-project"},
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
            resource_labels={"project_id": "my-project"},
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
        await client.write_time_series(
            "t", {}, 1, resource_labels={"project_id": "p"}
        )
        await client.write_time_series(
            "t", {}, 2, resource_labels={"project_id": "p"}
        )

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
            await client.write_time_series(
                "t", {}, 1, resource_labels={"project_id": "p"}
            )

    @mock.patch(
        "backend.pipeline.common.clients.monitoring_client.monitoring_v3"
    )
    async def test_gce_instance_resource_with_full_labels(
        self,
        mock_monitoring: mock.MagicMock,
    ) -> None:
        """Passing resource_type='gce_instance' + full resource_labels writes them verbatim."""
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
            metric_type="custom.googleapis.com/test/gce_resource",
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
    async def test_caller_owned_labels_are_not_mutated(
        self,
        mock_monitoring: mock.MagicMock,
    ) -> None:
        """Caller owns the full resource.labels dict — the client does not
        add, remove, or mutate keys. Passing resource_labels={'instance_id': 'i1'}
        alone means the emitted labels are EXACTLY {'instance_id': 'i1'}.
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


class TestMonitoringClientDoubleGauge(unittest.IsolatedAsyncioTestCase):
    """Tests for the DOUBLE-typed write path used by the Publisher."""

    @mock.patch(
        "backend.pipeline.common.clients.monitoring_client.monitoring_v3"
    )
    async def test_double_write_sets_double_value(
        self,
        mock_monitoring: mock.MagicMock,
    ) -> None:
        """A DOUBLE write produces a Point with double_value populated."""
        mock_async_client = mock.AsyncMock()
        mock_monitoring.MetricServiceAsyncClient.return_value = (
            mock_async_client
        )
        real_series = monitoring_v3.TimeSeries()
        mock_monitoring.TimeSeries.return_value = real_series
        # Real Point + TimeInterval — see comment in TestMonitoringClient
        # tests above for why proto-plus type-checks require this.
        mock_monitoring.Point.side_effect = monitoring_v3.Point
        mock_monitoring.TimeInterval.side_effect = monitoring_v3.TimeInterval

        client = monitoring_client.MonitoringClient("test-project")
        await client.write_time_series_double(
            metric_type=(
                "custom.googleapis.com/feeds/oldest_unclaimed_age_seconds"
            ),
            labels={},
            value=42.5,
            resource_labels={"project_id": "test-project"},
        )

        mock_async_client.create_time_series.assert_awaited_once()
        call_kwargs = mock_async_client.create_time_series.call_args[1]
        self.assertEqual(call_kwargs["name"], "projects/test-project")
        self.assertEqual(len(call_kwargs["time_series"]), 1)
        # Verify the Point's value-oneof selected double_value (not int64_value)
        self.assertEqual(len(real_series.points), 1)
        point = real_series.points[0]
        self.assertEqual(point.value.double_value, 42.5)

    @mock.patch(
        "backend.pipeline.common.clients.monitoring_client.monitoring_v3"
    )
    async def test_int64_path_unchanged_regression(
        self,
        mock_monitoring: mock.MagicMock,
    ) -> None:
        """Existing INT64 write_time_series still sets int64_value, not double."""
        mock_async_client = mock.AsyncMock()
        mock_monitoring.MetricServiceAsyncClient.return_value = (
            mock_async_client
        )
        real_series = monitoring_v3.TimeSeries()
        mock_monitoring.TimeSeries.return_value = real_series
        mock_monitoring.Point.side_effect = monitoring_v3.Point
        mock_monitoring.TimeInterval.side_effect = monitoring_v3.TimeInterval

        client = monitoring_client.MonitoringClient("p")
        await client.write_time_series(
            metric_type="custom.googleapis.com/feeds/quarantine_events",
            labels={},
            value=7,
            resource_labels={"project_id": "p"},
        )

        self.assertEqual(len(real_series.points), 1)
        point = real_series.points[0]
        self.assertEqual(point.value.int64_value, 7)
        # double_value is the proto-default (0.0) when the int64 oneof slot
        # is selected — a sanity check that the INT64 path didn't switch to
        # double under us.
        self.assertEqual(point.value.double_value, 0.0)

    @mock.patch(
        "backend.pipeline.common.clients.monitoring_client.monitoring_v3"
    )
    async def test_zero_value_is_published(
        self,
        mock_monitoring: mock.MagicMock,
    ) -> None:
        """value=0.0 is a valid datapoint (COALESCE → 0.0 for empty unclaimed set)."""
        mock_async_client = mock.AsyncMock()
        mock_monitoring.MetricServiceAsyncClient.return_value = (
            mock_async_client
        )
        real_series = monitoring_v3.TimeSeries()
        mock_monitoring.TimeSeries.return_value = real_series
        mock_monitoring.Point.side_effect = monitoring_v3.Point
        mock_monitoring.TimeInterval.side_effect = monitoring_v3.TimeInterval

        client = monitoring_client.MonitoringClient("p")
        await client.write_time_series_double(
            metric_type="custom.googleapis.com/feeds/oldest_unclaimed_age_seconds",
            labels={},
            value=0.0,
            resource_labels={"project_id": "p"},
        )

        mock_async_client.create_time_series.assert_awaited_once()
        self.assertEqual(len(real_series.points), 1)
        self.assertEqual(real_series.points[0].value.double_value, 0.0)

    @mock.patch(
        "backend.pipeline.common.clients.monitoring_client.monitoring_v3"
    )
    async def test_resource_type_default_global_and_kw_only(
        self,
        mock_monitoring: mock.MagicMock,
    ) -> None:
        """resource_type defaults to 'global' and resource_labels is keyword-only."""
        mock_async_client = mock.AsyncMock()
        mock_monitoring.MetricServiceAsyncClient.return_value = (
            mock_async_client
        )
        real_series = monitoring_v3.TimeSeries()
        mock_monitoring.TimeSeries.return_value = real_series
        mock_monitoring.Point.side_effect = monitoring_v3.Point
        mock_monitoring.TimeInterval.side_effect = monitoring_v3.TimeInterval

        client = monitoring_client.MonitoringClient("my-project")
        await client.write_time_series_double(
            metric_type=(
                "custom.googleapis.com/feeds/oldest_unclaimed_age_seconds"
            ),
            labels={"k": "v"},
            value=1.5,
            resource_labels={"project_id": "my-project"},
        )

        # Default resource_type is "global"
        self.assertEqual(real_series.resource.type, "global")
        self.assertEqual(
            dict(real_series.resource.labels), {"project_id": "my-project"}
        )
        # Metric labels propagated
        self.assertEqual(dict(real_series.metric.labels), {"k": "v"})

        # Calling positionally without naming `resource_labels` must fail
        # (signature should mark it kw-only via `*` separator).
        with self.assertRaises(TypeError):
            await client.write_time_series_double(
                "custom.googleapis.com/x",
                {},
                1.0,
                "global",
                {"project_id": "p"},
            )

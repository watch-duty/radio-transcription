"""Tests for backend/pipeline/ingestion/metric_reporter.py.

Covers the 7-case test matrix from 03-CONTEXT.md <specifics>:
    1. Happy path — 3 ticks write TimeSeries with correct schema.
    2. Label-allowlist — emitted metric labels are empty; resource labels subset of allowlist union {project_id}.
    3. Exception matrix — 6 subtests across D-23's transient + permanent classes.
    4. Permanent-error dedup — first PermissionDenied ERROR, repeat DEBUG.
    5. Metadata probe failure — 3 subtests (ConnectionError, TimeoutError, 500).
    6. Shutdown interruption — sleep_fn returns True, reporter exits cleanly.
    7. Recovery log — first success after seen-error emits INFO.
"""

from __future__ import annotations

import logging
import unittest
from unittest import mock

import aiohttp
from google.api_core.exceptions import (
    Aborted,
    DeadlineExceeded,
    InvalidArgument,
    NotFound,
    PermissionDenied,
    ResourceExhausted,
    ServiceUnavailable,
)

from backend.pipeline.ingestion import metric_reporter
from backend.pipeline.ingestion.slo_contract import (
    METRIC_LABEL_ALLOWLIST,
    METRIC_TYPE_ACTIVE_FEED_COUNT,
    MONITORED_RESOURCE_TYPE,
)

_LOGGER_NAME = "backend.pipeline.ingestion.metric_reporter"
_RESOURCE_LABELS = {
    "project_id": "test-project",
    "instance_id": "1234567890",
    "zone": "us-central1-a",
}


def _reset_module_state() -> None:
    """Clear module-level singletons between tests."""
    metric_reporter._client = None
    metric_reporter._seen_error_classes.clear()


class _SleepFn:
    """Controllable async sleep_fn that fires N False's then True."""

    def __init__(self, true_after: int) -> None:
        self._calls = 0
        self._true_after = true_after

    async def __call__(self, seconds: float) -> bool:
        self._calls += 1
        return self._calls > self._true_after

    @property
    def calls(self) -> int:
        return self._calls


class TestReporterLoopHappyPath(unittest.IsolatedAsyncioTestCase):
    """Case 1: 3 ticks write TimeSeries with correct metric_type + resource_type + labels."""

    def tearDown(self) -> None:
        _reset_module_state()

    async def test_three_ticks_emit_three_writes(self) -> None:
        mock_client = mock.AsyncMock()
        metric_reporter._client = mock_client
        sleep_fn = _SleepFn(true_after=3)  # 3 False's then True

        await metric_reporter.reporter_loop(
            count_fn=lambda: 42,
            resource_labels=_RESOURCE_LABELS,
            interval_sec=0.01,
            sleep_fn=sleep_fn,
        )

        self.assertEqual(mock_client.write_time_series.await_count, 3)
        # Inspect kwargs of the last call — all 3 have identical kwargs shape.
        kwargs = mock_client.write_time_series.await_args.kwargs
        self.assertEqual(kwargs["metric_type"], METRIC_TYPE_ACTIVE_FEED_COUNT)
        self.assertEqual(kwargs["resource_type"], MONITORED_RESOURCE_TYPE)
        self.assertEqual(kwargs["value"], 42)
        self.assertEqual(kwargs["labels"], {})
        self.assertEqual(kwargs["resource_labels"], _RESOURCE_LABELS)


class TestReporterLoopLabelAllowlist(unittest.IsolatedAsyncioTestCase):
    """Case 2: metric labels empty; resource labels subset of allowlist union {project_id}."""

    def tearDown(self) -> None:
        _reset_module_state()

    async def test_metric_labels_empty_and_resource_labels_within_allowlist(
        self,
    ) -> None:
        mock_client = mock.AsyncMock()
        metric_reporter._client = mock_client
        sleep_fn = _SleepFn(true_after=1)

        await metric_reporter.reporter_loop(
            count_fn=lambda: 7,
            resource_labels=_RESOURCE_LABELS,
            interval_sec=0.01,
            sleep_fn=sleep_fn,
        )

        kwargs = mock_client.write_time_series.await_args.kwargs
        # Pitfall 1 — metric labels are the cardinality-sensitive surface.
        # They must be completely empty; feed_id/source_type must never appear.
        self.assertEqual(kwargs["labels"], {})
        # Resource labels may include {instance_id, zone} from the allowlist
        # plus {project_id} (required by Cloud Monitoring's gce_instance schema).
        resource_keys = set(kwargs["resource_labels"].keys())
        allowed = METRIC_LABEL_ALLOWLIST | {"project_id"}
        self.assertTrue(
            resource_keys <= allowed,
            f"resource label keys {resource_keys} must be subset of {allowed}",
        )


class TestReporterLoopExceptionMatrix(unittest.IsolatedAsyncioTestCase):
    """Case 3: reporter survives each modelled exception class and ticks again.

    D-23 exception bucketing — 6 classes split between transient (4) and
    permanent (2 tested in this test; dedup covered in Case 4).
    """

    def tearDown(self) -> None:
        _reset_module_state()

    async def _run_with_exception(
        self,
        exc_class: type[BaseException],
    ) -> mock.AsyncMock:
        """Run 2 ticks — first raises exc_class, second succeeds. Return mock."""
        mock_client = mock.AsyncMock()
        mock_client.write_time_series.side_effect = [exc_class("boom"), None]
        metric_reporter._client = mock_client
        sleep_fn = _SleepFn(true_after=2)

        await metric_reporter.reporter_loop(
            count_fn=lambda: 1,
            resource_labels=_RESOURCE_LABELS,
            interval_sec=0.01,
            sleep_fn=sleep_fn,
        )
        return mock_client

    async def test_each_modelled_exception_class(self) -> None:
        exception_classes = [
            ResourceExhausted,
            DeadlineExceeded,
            ServiceUnavailable,
            Aborted,
            PermissionDenied,
            InvalidArgument,
            NotFound,
            # Unknown Exception catch-all.
            RuntimeError,
        ]
        for exc_class in exception_classes:
            with self.subTest(exc_class=exc_class.__name__):
                _reset_module_state()
                mock_client = await self._run_with_exception(exc_class)
                # 2 ticks: exception on first, success on second — reporter
                # must have attempted both writes (no propagation out).
                self.assertEqual(
                    mock_client.write_time_series.await_count,
                    2,
                    f"{exc_class.__name__}: expected reporter to continue",
                )


class TestReporterLoopPermanentErrorDedup(unittest.IsolatedAsyncioTestCase):
    """Case 4: two PermissionDenied in a row — first logs ERROR, second logs DEBUG."""

    def tearDown(self) -> None:
        _reset_module_state()

    async def test_first_permission_denied_logs_error_then_debug(self) -> None:
        mock_client = mock.AsyncMock()
        mock_client.write_time_series.side_effect = [
            PermissionDenied("no perms"),
            PermissionDenied("still no perms"),
        ]
        metric_reporter._client = mock_client
        sleep_fn = _SleepFn(true_after=2)

        # DEBUG level captures both ERROR and DEBUG records.
        with self.assertLogs(_LOGGER_NAME, level=logging.DEBUG) as cm:
            await metric_reporter.reporter_loop(
                count_fn=lambda: 1,
                resource_labels=_RESOURCE_LABELS,
                interval_sec=0.01,
                sleep_fn=sleep_fn,
            )

        error_records = [r for r in cm.records if r.levelno == logging.ERROR]
        debug_records = [
            r
            for r in cm.records
            if r.levelno == logging.DEBUG
            and "recurring permanent error" in r.getMessage()
        ]
        self.assertEqual(
            len(error_records),
            1,
            "first PermissionDenied must log exactly one ERROR",
        )
        self.assertEqual(
            len(debug_records),
            1,
            "second PermissionDenied must log a DEBUG (dedup), not another ERROR",
        )


class TestResolveGceResourceLabelsSuccess(unittest.IsolatedAsyncioTestCase):
    """Case (5-partial): happy-path metadata probe returns the expected dict."""

    def tearDown(self) -> None:
        _reset_module_state()

    async def test_returns_project_instance_zone_on_200(self) -> None:
        settings = mock.MagicMock()
        settings.google_cloud_project = "test-project"

        # Build a sequence of 3 mock responses (one per endpoint).
        def _make_resp(text: str) -> mock.MagicMock:
            resp = mock.AsyncMock()
            resp.raise_for_status = mock.MagicMock()
            resp.text = mock.AsyncMock(return_value=text)
            resp.__aenter__ = mock.AsyncMock(return_value=resp)
            resp.__aexit__ = mock.AsyncMock(return_value=None)
            return resp

        responses = [
            _make_resp("test-project"),
            _make_resp("1234567890"),
            _make_resp("projects/NNNN/zones/us-central1-a"),
        ]

        session = mock.MagicMock()
        session.get = mock.MagicMock(side_effect=responses)
        session.__aenter__ = mock.AsyncMock(return_value=session)
        session.__aexit__ = mock.AsyncMock(return_value=None)

        with mock.patch(
            "backend.pipeline.ingestion.metric_reporter.aiohttp.ClientSession",
            return_value=session,
        ):
            labels = await metric_reporter.resolve_gce_resource_labels(settings)

        self.assertEqual(
            labels,
            {
                "project_id": "test-project",
                "instance_id": "1234567890",
                "zone": "us-central1-a",  # zone path prefix stripped
            },
        )


class TestResolveGceResourceLabelsFailure(unittest.IsolatedAsyncioTestCase):
    """Case 5: each failure mode → returns None + one WARNING log."""

    def tearDown(self) -> None:
        _reset_module_state()

    async def test_connection_error_returns_none(self) -> None:
        await self._assert_failure_returns_none(aiohttp.ClientConnectionError())

    async def test_timeout_error_returns_none(self) -> None:
        await self._assert_failure_returns_none(TimeoutError())

    async def test_http_500_returns_none(self) -> None:
        settings = mock.MagicMock(google_cloud_project="p")

        def _raise() -> None:
            raise aiohttp.ClientResponseError(
                request_info=mock.MagicMock(),
                history=(),
                status=500,
                message="Internal Server Error",
            )

        resp = mock.AsyncMock()
        resp.raise_for_status = mock.MagicMock(side_effect=_raise)
        resp.text = mock.AsyncMock(return_value="")
        resp.__aenter__ = mock.AsyncMock(return_value=resp)
        resp.__aexit__ = mock.AsyncMock(return_value=None)

        session = mock.MagicMock()
        session.get = mock.MagicMock(return_value=resp)
        session.__aenter__ = mock.AsyncMock(return_value=session)
        session.__aexit__ = mock.AsyncMock(return_value=None)

        with mock.patch(
            "backend.pipeline.ingestion.metric_reporter.aiohttp.ClientSession",
            return_value=session,
        ):
            with self.assertLogs(_LOGGER_NAME, level=logging.WARNING) as cm:
                result = await metric_reporter.resolve_gce_resource_labels(
                    settings,
                )

        self.assertIsNone(result)
        warning_records = [
            r for r in cm.records if r.levelno == logging.WARNING
        ]
        self.assertEqual(len(warning_records), 1)

    async def _assert_failure_returns_none(
        self,
        exc: BaseException,
    ) -> None:
        settings = mock.MagicMock(google_cloud_project="p")

        session = mock.MagicMock()
        session.get = mock.MagicMock(side_effect=exc)
        session.__aenter__ = mock.AsyncMock(return_value=session)
        session.__aexit__ = mock.AsyncMock(return_value=None)

        with mock.patch(
            "backend.pipeline.ingestion.metric_reporter.aiohttp.ClientSession",
            return_value=session,
        ):
            with self.assertLogs(_LOGGER_NAME, level=logging.WARNING) as cm:
                result = await metric_reporter.resolve_gce_resource_labels(
                    settings,
                )

        self.assertIsNone(result)
        warning_records = [
            r for r in cm.records if r.levelno == logging.WARNING
        ]
        self.assertEqual(len(warning_records), 1)


class TestReporterLoopShutdownInterruption(unittest.IsolatedAsyncioTestCase):
    """Case 6: sleep_fn returns True first → reporter exits without writing."""

    def tearDown(self) -> None:
        _reset_module_state()

    async def test_shutdown_before_first_write(self) -> None:
        mock_client = mock.AsyncMock()
        metric_reporter._client = mock_client
        # sleep_fn returns True on the first call.
        sleep_fn = _SleepFn(true_after=0)

        await metric_reporter.reporter_loop(
            count_fn=lambda: 1,
            resource_labels=_RESOURCE_LABELS,
            interval_sec=0.01,
            sleep_fn=sleep_fn,
        )

        mock_client.write_time_series.assert_not_awaited()
        self.assertEqual(sleep_fn.calls, 1)


class TestReporterLoopRecoveryInfo(unittest.IsolatedAsyncioTestCase):
    """Case 7: first success after non-empty _seen_error_classes logs INFO."""

    def tearDown(self) -> None:
        _reset_module_state()

    async def test_recovery_info_after_seen_error(self) -> None:
        # Seed a prior permanent-error observation.
        metric_reporter._seen_error_classes.add(PermissionDenied)

        mock_client = mock.AsyncMock()
        mock_client.write_time_series.return_value = None  # success
        metric_reporter._client = mock_client
        sleep_fn = _SleepFn(true_after=1)

        with self.assertLogs(_LOGGER_NAME, level=logging.INFO) as cm:
            await metric_reporter.reporter_loop(
                count_fn=lambda: 1,
                resource_labels=_RESOURCE_LABELS,
                interval_sec=0.01,
                sleep_fn=sleep_fn,
            )

        info_records = [
            r
            for r in cm.records
            if r.levelno == logging.INFO
            and "metric reporter recovered" in r.getMessage()
        ]
        self.assertEqual(len(info_records), 1)
        # _seen_error_classes is cleared after the recovery INFO fires.
        self.assertEqual(metric_reporter._seen_error_classes, set())


class TestConfigure(unittest.TestCase):
    """Sanity check on configure() mirrors quarantine_telemetry shape."""

    def tearDown(self) -> None:
        _reset_module_state()

    @mock.patch(
        "backend.pipeline.ingestion.metric_reporter.MonitoringClient",
    )
    def test_creates_client_with_project_id(
        self,
        mock_cls: mock.MagicMock,
    ) -> None:
        metric_reporter.configure("my-project")

        mock_cls.assert_called_once_with("my-project")
        self.assertIs(metric_reporter._client, mock_cls.return_value)

    def test_none_disables_client(self) -> None:
        metric_reporter.configure(None)

        self.assertIsNone(metric_reporter._client)

import logging
from unittest import TestCase, mock

from backend.pipeline.common.logging import TraceFilter, setup_logging


class TestLogging(TestCase):
    def setUp(self) -> None:
        setup_logging.cache_clear()

    @mock.patch("backend.pipeline.common.tracing_utils.set_tracer_provider")
    @mock.patch("backend.pipeline.common.tracing_utils.CloudTraceSpanExporter")
    @mock.patch("backend.pipeline.common.logging.cloud_logging")
    def test_setup_logging_gcp(
        self, mock_cloud_logging, mock_exporter, mock_set_provider
    ) -> None:
        # Mock cloud_logging to look like the real library
        mock_client_inst = mock.Mock()
        mock_cloud_logging.Client.return_value = mock_client_inst

        # Explicitly mock is_gcp_env to return True to trigger the GCP logging path
        with (
            mock.patch(
                "backend.pipeline.common.logging.is_gcp_env", return_value=True
            ),
            mock.patch(
                "backend.pipeline.common.tracing_utils.is_gcp_env",
                return_value=True,
            ),
        ):
            with mock.patch.dict(
                "os.environ", {"GOOGLE_CLOUD_PROJECT": "test-project"}
            ):
                # First call should initialize cloud logging and tracing
                setup_logging()
                mock_cloud_logging.Client.assert_called_once()
                mock_client_inst.setup_logging.assert_called_once()
                mock_exporter.assert_called_once_with(project_id="test-project")
                mock_set_provider.assert_called_once()

                # Verify TraceFilter was added
                root_logger = logging.getLogger()
                self.assertTrue(
                    any(isinstance(f, TraceFilter) for f in root_logger.filters)
                )

                # Second call should do nothing (idempotency due to cache)
                setup_logging()
                mock_cloud_logging.Client.assert_called_once()
                mock_client_inst.setup_logging.assert_called_once()
                mock_exporter.assert_called_once()
                mock_set_provider.assert_called_once()

    @mock.patch("logging.basicConfig")
    def test_setup_logging_local(self, mock_basic_config) -> None:
        # Mock is_gcp_env to return False to trigger local logging path
        with mock.patch(
            "backend.pipeline.common.logging.is_gcp_env", return_value=False
        ):
            # First call should initialize local logging
            setup_logging()
            mock_basic_config.assert_called_once()
            _args, kwargs = mock_basic_config.call_args
            self.assertEqual(kwargs["level"], logging.INFO)
            self.assertTrue(kwargs["force"])

            # Second call should do nothing (idempotency)
            setup_logging()
            mock_basic_config.assert_called_once()


class TestTraceFilter(TestCase):
    def test_filter_adds_trace_info_when_span_valid(self) -> None:
        with (
            mock.patch(
                "backend.pipeline.common.logging.get_trace_attributes",
                return_value={
                    "trace": "projects/test-project/traces/4bf92f3577b34da6a3ce929d0e0e4736",
                    "spanId": "00f067aa0ba902b7",
                },
            ),
            mock.patch.dict(
                "os.environ", {"GOOGLE_CLOUD_PROJECT": "test-project"}
            ),
        ):
            record = logging.LogRecord(
                "name", logging.INFO, "pathname", 1, "msg", (), None
            )
            filter_inst = TraceFilter()

            self.assertTrue(filter_inst.filter(record))
            self.assertEqual(
                record.trace,  # ty: ignore[unresolved-attribute]
                "projects/test-project/traces/4bf92f3577b34da6a3ce929d0e0e4736",
            )
            self.assertEqual(record.spanId, "00f067aa0ba902b7")  # ty: ignore[unresolved-attribute]

    def test_filter_sets_empty_strings_when_span_invalid(self) -> None:

        with mock.patch(
            "backend.pipeline.common.logging.get_trace_attributes",
            return_value={
                "trace": "",
                "spanId": "",
            },
        ):
            record = logging.LogRecord(
                "name", logging.INFO, "pathname", 1, "msg", (), None
            )
            filter_inst = TraceFilter()

            self.assertTrue(filter_inst.filter(record))
            self.assertEqual(record.trace, "")  # ty: ignore[unresolved-attribute]
            self.assertEqual(record.spanId, "")  # ty: ignore[unresolved-attribute]

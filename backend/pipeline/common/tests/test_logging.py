import logging
from unittest import TestCase, mock

from backend.pipeline.common import logging as pipeline_logging
from backend.pipeline.common.logging import setup_logging


class TestLogging(TestCase):
    def setUp(self) -> None:
        setup_logging.cache_clear()

    @mock.patch("backend.pipeline.common.logging.cloud_logging")
    def test_setup_logging_gcp(self, mock_cloud_logging) -> None:
        # Mock cloud_logging to look like the real library
        mock_client_inst = mock.Mock()
        mock_cloud_logging.Client.return_value = mock_client_inst

        # Explicitly mock is_gcp_env to return True to trigger the GCP logging path
        with mock.patch(
            "backend.pipeline.common.logging.is_gcp_env", return_value=True
        ):
            # First call should initialize cloud logging
            setup_logging()
            mock_cloud_logging.Client.assert_called_once()
            mock_client_inst.setup_logging.assert_called_once()

            # Second call should do nothing (idempotency)
            setup_logging()
            mock_cloud_logging.Client.assert_called_once()
            mock_client_inst.setup_logging.assert_called_once()

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


class TestTraceId(TestCase):
    def test_set_trace_id_explicit(self) -> None:
        trace_id = "test-trace-id"
        pipeline_logging.set_trace_id(trace_id)
        self.assertEqual(pipeline_logging.get_trace_id(), trace_id)

        record = logging.LogRecord(
            "test", logging.INFO, "test.py", 1, "msg", (), None
        )
        filter_inst = pipeline_logging.TraceIdFilter()
        filter_inst.filter(record)
        self.assertEqual(record.trace_id, trace_id)  # type: ignore

    def test_set_trace_id_generate(self) -> None:
        pipeline_logging.set_trace_id()
        self.assertTrue(pipeline_logging.get_trace_id())

        record = logging.LogRecord(
            "test", logging.INFO, "test.py", 1, "msg", (), None
        )
        filter_inst = pipeline_logging.TraceIdFilter()
        filter_inst.filter(record)
        self.assertEqual(record.trace_id, pipeline_logging.get_trace_id())  # type: ignore

    def test_clear_trace_id(self) -> None:
        pipeline_logging.set_trace_id("test")
        pipeline_logging.set_trace_id("")

        record = logging.LogRecord(
            "test", logging.INFO, "test.py", 1, "msg", (), None
        )
        filter_inst = pipeline_logging.TraceIdFilter()
        filter_inst.filter(record)
        self.assertEqual(record.trace_id, "")  # type: ignore

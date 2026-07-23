import io
import json
import logging
from unittest import TestCase, mock

from backend.pipeline.common.log_helper import (
    StructuredMessageFilter,
    TaskJsonFormatter,
    record_pipeline_stage,
    setup_logging,
)


class TestLogging(TestCase):
    def setUp(self) -> None:
        setup_logging.cache_clear()

    @mock.patch("backend.pipeline.common.log_helper.cloud_logging")
    def test_setup_logging_gcp(
        self,
        mock_cloud_logging,
    ) -> None:
        # Mock cloud_logging to look like the real library
        mock_client_inst = mock.Mock()
        mock_cloud_logging.Client.return_value = mock_client_inst

        # Explicitly mock is_gcp_env to return True to trigger the GCP logging path
        with mock.patch(
            "backend.pipeline.common.log_helper.is_gcp_env",
            return_value=True,
        ):
            with mock.patch.dict(
                "os.environ", {"GOOGLE_CLOUD_PROJECT": "test-project"}
            ):
                # First call should initialize cloud logging
                setup_logging()
                mock_cloud_logging.Client.assert_called_once()
                mock_client_inst.setup_logging.assert_called_once()

                # Second call should do nothing (idempotency due to cache)
                setup_logging()
                mock_cloud_logging.Client.assert_called_once()
                mock_client_inst.setup_logging.assert_called_once()

    @mock.patch("logging.basicConfig")
    def test_setup_logging_local(self, mock_basic_config) -> None:
        # Mock is_gcp_env to return False to trigger local logging path
        with mock.patch(
            "backend.pipeline.common.log_helper.is_gcp_env", return_value=False
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

    def test_structured_exception_hooks(self) -> None:
        import asyncio  # noqa: PLC0415, F401
        import sys  # noqa: PLC0415
        import threading  # noqa: PLC0415

        with mock.patch(
            "backend.pipeline.common.log_helper.is_gcp_env",
            return_value=False,
        ):
            setup_logging()

        # Test sys.excepthook
        with mock.patch("logging.getLogger") as mock_get_logger:
            mock_logger = mock.Mock()
            mock_get_logger.return_value = mock_logger
            exc = ValueError("test")
            sys.excepthook(type(exc), exc, None)
            mock_get_logger.assert_called_with("unhandled_exception")
            mock_logger.critical.assert_called_once()

        # Test threading.excepthook
        with mock.patch("logging.getLogger") as mock_get_logger:
            mock_logger = mock.Mock()
            mock_get_logger.return_value = mock_logger
            exc = ValueError("thread test")
            mock_thread = mock.Mock()
            mock_thread.name = "bg_thread"
            hook_args = threading.ExceptHookArgs(
                (type(exc), exc, None, mock_thread),
            )
            threading.excepthook(hook_args)
            mock_get_logger.assert_called_with("unhandled_exception.bg_thread")
            mock_logger.critical.assert_called_once()

    def test_record_pipeline_stage_structured_json(self) -> None:
        stream = io.StringIO()
        logger = logging.getLogger("pipeline.metrics")
        handler = logging.StreamHandler(stream)
        handler.setFormatter(TaskJsonFormatter())
        logger.addHandler(handler)
        self.addCleanup(logger.removeHandler, handler)
        logger.setLevel(logging.INFO)

        record_pipeline_stage("transcription_status", "fallback")

        output = stream.getvalue().strip()
        data = json.loads(output)
        self.assertEqual(data["logger"], "pipeline.metrics")
        self.assertEqual(data["event_type"], "pipeline_stage")
        self.assertEqual(data["stage"], "transcription_status")
        self.assertEqual(data["status"], "fallback")

    def test_task_json_formatter_flattens_json_fields(self) -> None:
        """Pins the contract that extra={"json_fields": {...}} lands as
        top-level jsonPayload keys, matching google-cloud-logging's
        CloudLoggingHandler behavior. GCP log-based metric filters key off
        top-level fields, so a regression back to nesting would silently
        break Cloud Monitoring dashboards again.
        """
        record = logging.LogRecord(
            "name", logging.INFO, "pathname", 1, "msg", (), None
        )
        record.json_fields = {"stage": "transcription", "status": "success"}
        formatter = TaskJsonFormatter()

        log_record = json.loads(formatter.format(record))

        self.assertEqual(log_record["stage"], "transcription")
        self.assertEqual(log_record["status"], "success")
        self.assertNotIn("json_fields", log_record)

    def test_structured_message_filter_flattens_json_fields(self) -> None:
        """StructuredMessageFilter (the Dataflow structured-propagation
        path) must flatten json_fields identically to TaskJsonFormatter.
        """
        record = logging.LogRecord(
            "name", logging.INFO, "pathname", 1, "msg", (), None
        )
        record.json_fields = {"stage": "transcription", "status": "success"}
        filt = StructuredMessageFilter()

        self.assertTrue(filt.filter(record))
        log_record = json.loads(record.msg)

        self.assertEqual(log_record["stage"], "transcription")
        self.assertEqual(log_record["status"], "success")
        self.assertNotIn("json_fields", log_record)

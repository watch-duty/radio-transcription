import json
import logging
import unittest
from unittest import mock

from backend.pipeline.common import log_helper
from backend.pipeline.common.log_helper import (
    StructuredMessageFilter,
    TaskJsonFormatter,
    get_logger,
)
from backend.pipeline.segmentation.logging import setup_logging


class TestTaskJsonFormatter(unittest.TestCase):
    def test_format_adds_trace_info_when_span_valid(self) -> None:
        with (
            mock.patch(
                "backend.pipeline.common.log_helper.get_trace_attributes",
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
            formatter = TaskJsonFormatter()

            log_str = formatter.format(record)
            log_record = json.loads(log_str)

            self.assertEqual(
                log_record["logging.googleapis.com/trace"],
                "projects/test-project/traces/4bf92f3577b34da6a3ce929d0e0e4736",
            )
            self.assertEqual(
                log_record["logging.googleapis.com/spanId"], "00f067aa0ba902b7"
            )

    def test_format_sets_no_trace_info_when_span_invalid(self) -> None:
        with mock.patch(
            "backend.pipeline.common.log_helper.get_trace_attributes",
            return_value={
                "trace": "",
                "spanId": "",
            },
        ):
            record = logging.LogRecord(
                "name", logging.INFO, "pathname", 1, "msg", (), None
            )
            formatter = TaskJsonFormatter()

            log_str = formatter.format(record)
            log_record = json.loads(log_str)

            self.assertNotIn("trace_id", log_record)
            self.assertNotIn("span_id", log_record)
            self.assertNotIn("trace", log_record)
            self.assertNotIn("spanId", log_record)


class TestStructuredPropagationLogging(unittest.TestCase):
    def test_structured_message_filter(self) -> None:
        record = logging.LogRecord(
            name="test_logger",
            level=logging.ERROR,
            pathname="test_file.py",
            lineno=42,
            msg="Error occurred: %s",
            args=("some_detail",),
            exc_info=None,
        )
        # Add attributes like LoggerAdapter would
        record.feed_id = "feed_123"
        record.session_id = "session_456"

        filt = StructuredMessageFilter()
        res = filt.filter(record)

        self.assertTrue(res)
        self.assertTrue(record.__dict__.get("structured_formatted", False))
        self.assertEqual(record.args, ())

        # Parse the JSON msg
        log_data = json.loads(record.msg)
        self.assertEqual(log_data["message"], "Error occurred: some_detail")
        self.assertEqual(log_data["severity"], "ERROR")
        self.assertEqual(log_data["logger"], "test_logger")
        self.assertEqual(log_data["feed_id"], "feed_123")
        self.assertEqual(log_data["session_id"], "session_456")

    def test_get_logger_with_structured_propagation(self) -> None:
        # Clean up logger if it exists to avoid side effects
        logger_name = "test_propagated_logger"
        logger = logging.getLogger(logger_name)
        logger.handlers.clear()
        logger.filters.clear()

        with mock.patch.object(
            log_helper._LoggingState, "structured_propagation", new=True
        ):
            configured_logger = get_logger(logger_name)
            self.assertTrue(configured_logger.propagate)
            self.assertEqual(len(configured_logger.handlers), 0)
            self.assertTrue(
                any(
                    isinstance(f, StructuredMessageFilter)
                    for f in configured_logger.filters
                )
            )

    def test_get_logger_without_structured_propagation(self) -> None:
        logger_name = "test_normal_logger"
        logger = logging.getLogger(logger_name)
        logger.handlers.clear()
        logger.filters.clear()

        with mock.patch.object(
            log_helper._LoggingState, "structured_propagation", new=False
        ):
            configured_logger = get_logger(logger_name)
            self.assertFalse(configured_logger.propagate)
            self.assertEqual(len(configured_logger.handlers), 1)
            handler = configured_logger.handlers[0]
            self.assertTrue(isinstance(handler, logging.StreamHandler))
            self.assertTrue(isinstance(handler.formatter, TaskJsonFormatter))


class TestSegmentationLogging(unittest.TestCase):
    def test_setup_logging_under_dataflow(self) -> None:
        # Mock argv to simulate running on Dataflow worker
        with (
            mock.patch(
                "sys.argv", ["main.py", "--logging_endpoint=localhost:12345"]
            ),
            mock.patch.object(
                log_helper._LoggingState, "structured_propagation", new=False
            ),
        ):
            setup_logging()
            self.assertTrue(log_helper._LoggingState.structured_propagation)

    def test_setup_logging_normal(self) -> None:
        # Mock argv to simulate normal/local execution
        with (
            mock.patch("sys.argv", ["main.py"]),
            mock.patch.object(
                log_helper._LoggingState, "structured_propagation", new=False
            ),
        ):
            setup_logging()
            self.assertFalse(log_helper._LoggingState.structured_propagation)


if __name__ == "__main__":
    unittest.main()

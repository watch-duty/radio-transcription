import json
import logging
import unittest
from unittest import mock

from backend.pipeline.common.log_helper import TaskJsonFormatter


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


if __name__ == "__main__":
    unittest.main()

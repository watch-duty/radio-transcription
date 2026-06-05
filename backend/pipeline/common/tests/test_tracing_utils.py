import unittest
from unittest.mock import MagicMock, patch

from opentelemetry.trace import get_current_span

from backend.pipeline.common.tracing_utils import (
    ContextPropagationValidator,
    extract_trace_context,
    get_current_traceparent,
    with_tracer_context,
)


class TestTracingUtils(unittest.TestCase):
    def test_with_tracer_context_propagates_trace_id(self) -> None:
        """Verifies that with_tracer_context creates a valid span and propagates trace ID."""
        traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

        span_before = get_current_span()
        self.assertFalse(span_before.get_span_context().is_valid)

        with with_tracer_context(traceparent, "test_span", __name__) as span:
            self.assertTrue(span.get_span_context().is_valid)

            # Verify current span is the one yielded
            self.assertEqual(get_current_span(), span)

            # Verify trace ID was propagated correctly
            span_ctx = span.get_span_context()
            self.assertEqual(
                format(span_ctx.trace_id, "032x"),
                "4bf92f3577b34da6a3ce929d0e0e4736",
            )

        # Verify context is restored on exit
        span_after = get_current_span()
        self.assertFalse(span_after.get_span_context().is_valid)

    def test_get_current_traceparent(self) -> None:
        """Verifies that get_current_traceparent returns a valid traceparent."""
        traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        with with_tracer_context(traceparent, "test_span", __name__):
            current_tp = get_current_traceparent()
            self.assertTrue(
                current_tp.startswith("00-4bf92f3577b34da6a3ce929d0e0e4736-")
            )

    @patch("backend.pipeline.common.tracing_utils.telemetry_logger")
    def test_context_propagation_validator(self, mock_logger) -> None:
        """Verifies that ContextPropagationValidator logs propagation failures for downstream root spans."""
        # 1. Create a validator for a downstream service
        validator = ContextPropagationValidator(
            service_name="test_downstream", is_ingestion=False
        )

        # 2. Mock a span that has no parent (root span)
        mock_span = MagicMock()
        mock_span.parent = None
        mock_span.name = "test_root_span"

        # 3. Trigger on_start
        validator.on_start(mock_span)
        mock_logger.error.assert_called_once_with(
            "Trace context propagation failure in service 'test_downstream'. "
            "Started new root span 'test_root_span' because no parent trace context was received."
        )

        mock_logger.reset_mock()

        # 4. Mock a span that HAS a parent (child span)
        mock_child_span = MagicMock()
        mock_child_span.parent = MagicMock()

        validator.on_start(mock_child_span)
        mock_logger.error.assert_not_called()

        # 5. Create a validator for an ingestion service (allowed to start root spans)
        ingestion_validator = ContextPropagationValidator(
            service_name="test_ingestion", is_ingestion=True
        )
        ingestion_validator.on_start(mock_span)
        mock_logger.error.assert_not_called()

    def test_extract_trace_context_empty_or_missing(self) -> None:
        """Verifies that extract_trace_context handles empty or missing traceparent values gracefully by returning an empty Context."""
        # 1. Missing attributes
        ctx1 = extract_trace_context(None)
        self.assertEqual(len(ctx1), 0)

        # 2. Missing traceparent key
        ctx2 = extract_trace_context({"foo": "bar"})
        self.assertEqual(len(ctx2), 0)

        # 3. Empty traceparent string
        ctx3 = extract_trace_context({"traceparent": ""})
        self.assertEqual(len(ctx3), 0)

        # 4. None traceparent string (in case casted improperly)
        ctx4 = extract_trace_context({"traceparent": None})  # type: ignore
        self.assertEqual(len(ctx4), 0)

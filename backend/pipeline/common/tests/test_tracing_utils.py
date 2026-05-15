import unittest

from opentelemetry.trace import get_current_span

from backend.pipeline.common.tracing_utils import (
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

import unittest
from unittest.mock import MagicMock, patch

from cloudevents.http.event import CloudEvent
from opentelemetry import baggage
from opentelemetry.trace import get_current_span

from backend.pipeline.common.tracing_utils import (
    ContextPropagationValidator,
    extract_cloud_event_attributes,
    extract_trace_context,
    get_current_traceparent,
    record_pipeline_stage,
    with_baggage_and_span,
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

    def test_extract_trace_context_variations(self) -> None:
        """Verifies that extract_trace_context correctly resolves trace context from different key formats and case variations."""
        traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        baggage_str = "key1=val1,key2=val2"

        # 1. Uppercase keys
        ctx_upper = extract_trace_context(
            {"TRACEPARENT": traceparent, "BAGGAGE": baggage_str}
        )
        self.assertGreater(len(ctx_upper), 0)

        # 2. Eventarc/CloudEvent style ce- prefix
        ctx_ce = extract_trace_context(
            {"ce-traceparent": traceparent, "ce-baggage": baggage_str}
        )
        self.assertGreater(len(ctx_ce), 0)

        # 3. Pub/Sub unwrapped header style
        ctx_pubsub = extract_trace_context(
            {
                "x-goog-pubsub-attr-traceparent": traceparent,
                "x-goog-pubsub-attr-baggage": baggage_str,
            }
        )
        self.assertGreater(len(ctx_pubsub), 0)

    def test_with_baggage_and_span(self) -> None:
        """Verifies that with_baggage_and_span attaches baggage and works within an active trace."""
        traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        self.assertEqual(baggage.get_baggage("test_key"), None)

        with with_tracer_context(traceparent, "parent_span", __name__):
            with with_baggage_and_span(
                {"test_key": "test_val"}, "baggage_span", __name__
            ) as span:
                self.assertTrue(span.get_span_context().is_valid)
                self.assertEqual(get_current_span(), span)
                self.assertEqual(baggage.get_baggage("test_key"), "test_val")

        self.assertEqual(baggage.get_baggage("test_key"), None)

    @patch("backend.pipeline.common.tracing_utils._pipeline_stage_counter")
    def test_record_pipeline_stage(self, mock_counter) -> None:
        """Verifies that record_pipeline_stage increments the pipeline counter with correct labels."""
        record_pipeline_stage("segmentation", "start")
        mock_counter.add.assert_called_once_with(
            1, {"stage": "segmentation", "status": "start"}
        )

        mock_counter.reset_mock()
        record_pipeline_stage("transcription", "success")
        mock_counter.add.assert_called_once_with(
            1, {"stage": "transcription", "status": "success"}
        )

    def test_extract_cloud_event_attributes(self) -> None:
        """Verifies that extract_cloud_event_attributes parses nested pub/sub, top-level attributes, and top-level fields."""
        # 1. Pub/Sub attributes
        ce1 = CloudEvent(
            attributes={
                "type": "google.cloud.pubsub.topic.v1.messagePublished",
                "source": "test",
            },
            data={"message": {"attributes": {"traceparent": "tp1"}}},
        )
        attrs1 = extract_cloud_event_attributes(ce1)
        self.assertEqual(attrs1.get("traceparent"), "tp1")

        # 2. CloudEvent top-level attributes (with prefix normalization)
        ce2 = CloudEvent(
            attributes={
                "type": "google.cloud.pubsub.topic.v1.messagePublished",
                "source": "test",
                "ce-traceparent": "tp2",
                "x-goog-pubsub-attr-baggage": "bg2",
            },
            data={},
        )
        attrs2 = extract_cloud_event_attributes(ce2)
        self.assertEqual(attrs2.get("ce-traceparent"), "tp2")
        self.assertEqual(attrs2.get("traceparent"), "tp2")
        self.assertEqual(attrs2.get("x-goog-pubsub-attr-baggage"), "bg2")
        self.assertEqual(attrs2.get("baggage"), "bg2")

        # 3. Top-level direct keys via dict representation
        ce3: dict[str, object] = {"traceparent": "tp3"}
        attrs3 = extract_cloud_event_attributes(ce3)
        self.assertEqual(attrs3.get("traceparent"), "tp3")

        # 4. Non-string type conversion and robust extraction
        ce4 = CloudEvent(
            attributes={
                "type": "google.cloud.pubsub.topic.v1.messagePublished",
                "source": "test",
            },
            data={
                "message": {
                    "attributes": {
                        "traceparent": "tp4",
                        "numeric_val": 12345,
                        "bool_val": True,
                    }
                }
            },
        )
        attrs4 = extract_cloud_event_attributes(ce4)
        self.assertEqual(attrs4.get("traceparent"), "tp4")
        self.assertEqual(attrs4.get("numeric_val"), "12345")
        self.assertEqual(attrs4.get("bool_val"), "True")

        # 5. Direct Message Payload (functions-framework style where data is the message itself)
        ce5 = CloudEvent(
            attributes={
                "type": "google.cloud.pubsub.topic.v1.messagePublished",
                "source": "test",
            },
            data={
                "attributes": {
                    "traceparent": "tp5",
                    "baggage": "bg5",
                },
                "data": "CiQ...",
            },
        )
        attrs5 = extract_cloud_event_attributes(ce5)
        self.assertEqual(attrs5.get("traceparent"), "tp5")
        self.assertEqual(attrs5.get("baggage"), "bg5")

        # 6. Eventarc / CloudEvent Extensions (formal CloudEvent object with x-goog-pubsub-attr-)
        ce6 = CloudEvent(
            attributes={
                "type": "google.cloud.pubsub.topic.v1.messagePublished",
                "source": "test",
                "x-goog-pubsub-attr-traceparent": "tp6",
                "x-goog-pubsub-attr-baggage": "bg6",
            },
            data={},
        )
        attrs6 = extract_cloud_event_attributes(ce6)
        self.assertEqual(attrs6.get("x-goog-pubsub-attr-traceparent"), "tp6")
        self.assertEqual(attrs6.get("traceparent"), "tp6")
        self.assertEqual(attrs6.get("x-goog-pubsub-attr-baggage"), "bg6")
        self.assertEqual(attrs6.get("baggage"), "bg6")

        # 7. Byte-key support (handling binary keys for top-level, message, and attributes)
        ce7: dict[bytes, object] = {
            b"data": {
                b"message": {
                    b"attributes": {
                        b"traceparent": b"tp7",
                        b"baggage": b"bg7",
                    }
                }
            }
        }
        attrs7 = extract_cloud_event_attributes(ce7)
        self.assertEqual(attrs7.get("traceparent"), "tp7")
        self.assertEqual(attrs7.get("baggage"), "bg7")

        # 8. Prefix normalization priority under conflict (plain key should not be overwritten)
        ce8 = CloudEvent(
            attributes={
                "type": "google.cloud.pubsub.topic.v1.messagePublished",
                "source": "test",
                "traceparent": "tp_plain",
                "ce-traceparent": "tp_prefixed",
                "x-goog-pubsub-attr-traceparent": "tp_goog_prefixed",
            },
            data={},
        )
        attrs8 = extract_cloud_event_attributes(ce8)
        self.assertEqual(attrs8.get("traceparent"), "tp_plain")

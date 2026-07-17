import os
import unittest
from unittest.mock import ANY, MagicMock, patch

from cloudevents.http.event import CloudEvent
from opentelemetry import baggage
from opentelemetry.trace import get_current_span

from backend.pipeline.common import tracing_utils
from backend.pipeline.common.log_helper import record_pipeline_stage
from backend.pipeline.common.tracing_utils import (
    ContextPropagationValidator,
    extract_cloud_event_attributes,
    extract_trace_context,
    get_current_traceparent,
    get_tracer,
    setup_tracing,
    traced_to_thread,
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

    @patch("backend.pipeline.common.log_helper.pipeline_metrics_logger")
    def test_record_pipeline_stage(self, mock_logger) -> None:
        """Verifies that record_pipeline_stage emits a log with correct json_fields."""
        record_pipeline_stage("segmentation", "start")
        mock_logger.info.assert_called_once_with(
            "Pipeline stage recorded: segmentation -> start",
            extra={
                "json_fields": {
                    "event_type": "pipeline_stage",
                    "stage": "segmentation",
                    "status": "start",
                }
            },
        )

        mock_logger.reset_mock()
        record_pipeline_stage("transcription", "success")
        mock_logger.info.assert_called_once_with(
            "Pipeline stage recorded: transcription -> success",
            extra={
                "json_fields": {
                    "event_type": "pipeline_stage",
                    "stage": "transcription",
                    "status": "success",
                }
            },
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

        # 9. Raw Pub/Sub push notification dictionary (message at top level of dict)
        ce9 = {
            "message": {
                "attributes": {
                    "traceparent": "tp9",
                    "baggage": "bg9",
                },
                "data": "ey...",
            }
        }
        attrs9 = extract_cloud_event_attributes(ce9)
        self.assertEqual(attrs9.get("traceparent"), "tp9")
        self.assertEqual(attrs9.get("baggage"), "bg9")

    @patch(
        "backend.pipeline.common.tracing_utils.is_gcp_env", return_value=True
    )
    @patch("backend.pipeline.common.tracing_utils.CloudTraceSpanExporter")
    @patch("backend.pipeline.common.tracing_utils.set_tracer_provider")
    def test_custom_provider_fallback(
        self, mock_set_global, mock_exporter, mock_is_gcp
    ) -> None:
        """Verifies that setup_tracing initializes our custom provider.

        Ensures that get_tracer uses it, even if the global OTel registration
        fails (e.g. if the singleton provider was already locked by the runner).
        """
        # Reset state
        tracing_utils._state.custom_provider = None

        # Mock set_tracer_provider to simulate that it was already called and throws
        mock_set_global.side_effect = ValueError("Already set")

        with patch.dict("os.environ", {"GOOGLE_CLOUD_PROJECT": "test-project"}):
            setup_tracing(service_name="test_service", use_batch=False)

        # Verify that _state.custom_provider is set, despite the global set failure!
        provider = tracing_utils._state.custom_provider
        self.assertIsNotNone(provider)
        assert provider is not None

        # Verify that calling get_tracer returns a tracer from our custom provider!
        tracer = get_tracer("test_tracer")
        self.assertEqual(tracer, provider.get_tracer("test_tracer"))

        # Verify that starting a span works and it attaches to the global context
        with tracer.start_as_current_span("test_span") as span:
            self.assertTrue(span.get_span_context().is_valid)
            self.assertEqual(get_current_span(), span)

        # Clean up
        tracing_utils._state.custom_provider = None

    @patch(
        "backend.pipeline.common.tracing_utils.is_gcp_env", return_value=True
    )
    @patch("backend.pipeline.common.tracing_utils.CloudTraceSpanExporter")
    @patch("backend.pipeline.common.tracing_utils.BatchSpanProcessor")
    def test_setup_tracing_batch_processor_config(
        self, mock_batch_processor, mock_exporter, mock_is_gcp
    ) -> None:
        """Verifies setup_tracing configures BatchSpanProcessor using env overrides."""
        tracing_utils._state.custom_provider = None

        with patch.dict(
            "os.environ",
            {
                "GOOGLE_CLOUD_PROJECT": "test-project",
                "OTEL_BSP_MAX_EXPORT_BATCH_SIZE": "256",
                "OTEL_BSP_SCHEDULE_DELAY": "5000",
            },
        ):
            setup_tracing(service_name="test_service", use_batch=True)

        # Verify BatchSpanProcessor was instantiated with overrides
        mock_batch_processor.assert_called_once_with(
            mock_exporter.return_value,
            max_export_batch_size=256,
            schedule_delay_millis=5000.0,
        )

        tracing_utils._state.custom_provider = None


class TestTracingForkSafety(unittest.TestCase):
    def setUp(self) -> None:
        # Reset state before each test
        tracing_utils._state.custom_provider = None
        tracing_utils._state.pid = None

    def tearDown(self) -> None:
        # Clean up state
        if tracing_utils._state.custom_provider is not None:
            tracing_utils._state.custom_provider = None
        tracing_utils._state.pid = None

    @patch(
        "backend.pipeline.common.tracing_utils.is_gcp_env", return_value=True
    )
    @patch("backend.pipeline.common.tracing_utils.CloudTraceSpanExporter")
    @patch("backend.pipeline.common.tracing_utils.set_tracer_provider")
    def test_fork_reinitializes_custom_provider(
        self, mock_set_global, mock_exporter, mock_is_gcp
    ) -> None:
        """Verifies that setup_tracing reinitializes the provider when PID changes (fork)."""
        with patch.dict("os.environ", {"GOOGLE_CLOUD_PROJECT": "test-project"}):
            setup_tracing(service_name="test_service", use_batch=False)

        first_provider = tracing_utils._state.custom_provider
        self.assertIsNotNone(first_provider)
        self.assertEqual(tracing_utils._state.pid, os.getpid())

        # Simulate fork by patching os.getpid to return a different PID
        new_pid = os.getpid() + 100
        with (
            patch("os.getpid", return_value=new_pid),
            patch.dict("os.environ", {"GOOGLE_CLOUD_PROJECT": "test-project"}),
        ):
            # Verify setup_tracing re-initializes
            setup_tracing(service_name="test_service", use_batch=False)

        second_provider = tracing_utils._state.custom_provider
        self.assertIsNotNone(second_provider)
        self.assertNotEqual(first_provider, second_provider)
        self.assertEqual(tracing_utils._state.pid, new_pid)

    @patch(
        "backend.pipeline.common.tracing_utils.is_gcp_env", return_value=True
    )
    @patch("backend.pipeline.common.tracing_utils.CloudTraceSpanExporter")
    @patch("backend.pipeline.common.tracing_utils.set_tracer_provider")
    @patch("backend.pipeline.common.tracing_utils.telemetry_logger")
    def test_fork_shutdown_resilient(
        self, mock_logger, mock_set_global, mock_exporter, mock_is_gcp
    ) -> None:
        """Verifies that provider shutdown is invoked on fork, and exception is handled gracefully."""
        # Initialize first provider
        mock_first_provider = MagicMock()
        tracing_utils._state.custom_provider = mock_first_provider
        tracing_utils._state.pid = os.getpid()

        # Mock shutdown to raise an exception
        mock_first_provider.shutdown.side_effect = RuntimeError(
            "Shutdown failed"
        )

        # Simulate fork PID change
        new_pid = os.getpid() + 100
        with (
            patch("os.getpid", return_value=new_pid),
            patch.dict("os.environ", {"GOOGLE_CLOUD_PROJECT": "test-project"}),
        ):
            # Verify setup_tracing still completes successfully and handles the exception
            setup_tracing(service_name="test_service", use_batch=False)

        # Assert shutdown was called on the stale provider
        mock_first_provider.shutdown.assert_called_once()
        # Assert exception was logged via telemetry_logger.exception
        mock_logger.exception.assert_called_once_with(
            "Failed to shutdown inherited tracer provider on fork reset"
        )

        # Verify a new provider was successfully initialized
        self.assertIsNotNone(tracing_utils._state.custom_provider)
        self.assertNotEqual(
            tracing_utils._state.custom_provider, mock_first_provider
        )
        self.assertEqual(tracing_utils._state.pid, new_pid)


class TestTracedToThread(unittest.IsolatedAsyncioTestCase):
    @patch("backend.pipeline.common.tracing_utils.get_tracer")
    async def test_traced_to_thread_success(self, mock_get_tracer) -> None:
        """Verifies that traced_to_thread creates a span, executes the task, and records thread events/attributes."""
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span
        mock_get_tracer.return_value = mock_tracer

        def my_blocking_fn(a: int, b: int) -> int:
            return a + b

        result = await traced_to_thread(
            "test_blocking_span", my_blocking_fn, 10, b=20
        )

        self.assertEqual(result, 30)
        mock_get_tracer.assert_called_once()
        mock_tracer.start_as_current_span.assert_called_once_with(
            "test_blocking_span"
        )

        # Verify that start and end attributes/events were called on the mock span
        mock_span.set_attribute.assert_any_call("thread.start_time", ANY)
        mock_span.set_attribute.assert_any_call("thread.end_time", ANY)
        mock_span.add_event.assert_any_call("thread_execution_start")
        mock_span.add_event.assert_any_call("thread_execution_end")

"""Utilities for distributed tracing in Apache Beam."""

import logging
import os
import threading
from collections.abc import Iterator
from contextlib import contextmanager

from opentelemetry.context import Context
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
from opentelemetry.sdk.trace import ReadableSpan, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SimpleSpanProcessor,
)
from opentelemetry.trace import (
    Span,
    get_current_span,
    get_tracer,
    get_tracer_provider,
    set_tracer_provider,
)
from opentelemetry.trace.propagation.tracecontext import (
    TraceContextTextMapPropagator,
)

from backend.pipeline.common.env import is_gcp_env

_setup_lock = threading.Lock()
telemetry_logger = logging.getLogger("telemetry.validation")


class ContextPropagationValidator(SpanProcessor):
    """Custom OTel SpanProcessor that flags lost trace contexts on downstream services."""

    def __init__(
        self, service_name: str, *, is_ingestion: bool = False
    ) -> None:
        self.service_name = service_name
        self.is_ingestion = is_ingestion

    def on_start(
        self, span: ReadableSpan, parent_context: Context | None = None
    ) -> None:
        # A root span has no parent span context.
        # If a downstream service starts a root span, the trace context was lost.
        if span.parent is None and not self.is_ingestion:
            telemetry_logger.error(
                f"Trace context propagation failure in service '{self.service_name}'. "
                f"Started new root span '{span.name}' because no parent trace context was received."
            )

    def on_end(self, span: ReadableSpan) -> None:
        pass


def setup_tracing(
    *,
    service_name: str | None = None,
    is_ingestion: bool | None = None,
    use_batch: bool = True,
) -> None:
    """Sets up tracing for the context thread-safely.

    Messages are sent to CloudTrace through the span provider and processor.

    NOTE: This configuration only protects against duplicate concurrent initialization
    within a single Python worker process space. Distributed Dataflow worker instances
    will spin up separate process environments.
    """
    if not is_gcp_env():
        return

    current_provider = get_tracer_provider()
    if isinstance(current_provider, TracerProvider):
        return

    with _setup_lock:
        # Double check locking pattern after acquiring lock
        current_provider = get_tracer_provider()
        if isinstance(current_provider, TracerProvider):
            return

        project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") or ""
        if not project_id:
            msg = "GOOGLE_CLOUD_PROJECT environment variable must be set in GCP environment."
            raise ValueError(msg)
        provider = TracerProvider()
        exporter = CloudTraceSpanExporter(project_id=project_id)

        # Resolve service metadata from environment if not explicitly provided
        if service_name is None:
            service_name = (
                os.environ.get("DATAFLOW_JOB_NAME")
                or os.environ.get("JOB_NAME")
                or os.environ.get("K_SERVICE")
                or os.environ.get("FUNCTION_TARGET")
                or "unknown_service"
            )
        if is_ingestion is None:
            is_ingestion = os.environ.get("IS_INGESTION_SERVICE") == "true"

        # Register the validator processor
        provider.add_span_processor(
            ContextPropagationValidator(
                service_name=service_name, is_ingestion=is_ingestion
            )
        )

        if use_batch:
            provider.add_span_processor(BatchSpanProcessor(exporter))
        else:
            provider.add_span_processor(SimpleSpanProcessor(exporter))

        set_tracer_provider(provider)


def get_current_traceparent() -> str:
    """Returns the W3C traceparent for the current context."""
    span = get_current_span()
    span_context = span.get_span_context()
    if span_context.is_valid:
        carrier = {}
        TraceContextTextMapPropagator().inject(carrier)
        return carrier.get("traceparent", "")
    return ""


def get_trace_attributes() -> dict[str, str]:
    """Returns a dictionary of trace attributes for the current context."""
    traceparent_parts = (get_current_traceparent() or "").split("-")
    if len(traceparent_parts) >= 3:
        trace_id = traceparent_parts[1]
        span_id = traceparent_parts[2]
        project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") or ""
        return {
            "trace": f"projects/{project_id}/traces/{trace_id}",
            "spanId": span_id,
        }
    return {
        "trace": "",
        "spanId": "",
    }


def extract_trace_context(attributes: dict[str, str] | None) -> Context:
    """Restores OpenTelemetry trace context from Message Attributes using W3C TraceContext.

    Args:
        attributes: Pub/Sub Message metadata attribute key-value pairs.

    Returns:
        An OpenTelemetry Context.
    """
    if attributes and attributes.get("traceparent"):
        return TraceContextTextMapPropagator().extract(carrier=attributes)

    return Context()


@contextmanager
def with_tracer_context(
    traceparent: str,
    span_name: str,
    tracer_name: str,
) -> Iterator[Span]:
    """Context manager to create a trace context and start a span.

    Args:
        traceparent: The trace parent string.
        span_name: The name of the span to create.
        tracer_name: The name of the tracer (usually __name__).
    """
    context = extract_trace_context({"traceparent": traceparent})
    tracer = get_tracer(tracer_name)
    with tracer.start_as_current_span(span_name, context=context) as span:
        yield span

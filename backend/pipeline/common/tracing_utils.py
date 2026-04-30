"""Utilities for distributed tracing in Apache Beam."""

import os
import threading

from opentelemetry.context import Context
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SimpleSpanProcessor,
)
from opentelemetry.trace import (
    get_current_span,
    get_tracer_provider,
    set_tracer_provider,
)
from opentelemetry.trace.propagation.tracecontext import (
    TraceContextTextMapPropagator,
)

from backend.pipeline.common.env import is_gcp_env

_setup_lock = threading.Lock()


def setup_tracing(*, use_batch: bool = True) -> None:
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
        provider = TracerProvider()
        exporter = CloudTraceSpanExporter(project_id=project_id)

        if use_batch:
            provider.add_span_processor(BatchSpanProcessor(exporter))
        else:
            provider.add_span_processor(SimpleSpanProcessor(exporter))

        set_tracer_provider(provider)


def get_current_trace_id() -> str:
    """Returns the trace ID for the current context from OpenTelemetry."""
    span = get_current_span()
    span_context = span.get_span_context()
    if span_context.is_valid:
        return format(span_context.trace_id, "032x")
    return ""


def extract_trace_context(attributes: dict[str, str] | None) -> Context:
    """Restores OpenTelemetry trace context from Message Attributes using W3C TraceContext.

    Args:
        attributes: Pub/Sub Message metadata attribute key-value pairs.

    Returns:
        An OpenTelemetry Context.
    """
    if attributes and "traceparent" in attributes:
        return TraceContextTextMapPropagator().extract(carrier=attributes)

    return Context()

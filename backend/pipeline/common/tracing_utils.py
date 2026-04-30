"""Utilities for distributed tracing in Apache Beam."""

import hashlib
import os

from opentelemetry.context import Context
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SimpleSpanProcessor,
)
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    TraceFlags,
    get_current_span,
    get_tracer_provider,
    set_span_in_context,
    set_tracer_provider,
)
from opentelemetry.trace.propagation.tracecontext import (
    TraceContextTextMapPropagator,
)

from backend.pipeline.common.env import is_gcp_env


def setup_tracing(*, use_batch: bool = True) -> None:
    """Sets up tracing for the context.

    Messages are sent to CloudTrace through the span provider and processor.
    """
    if is_gcp_env():
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


def extract_trace_context(
    attributes: dict[str, str] | None, fallback_trace_id: str | None = None
) -> Context:
    """Restores OpenTelemetry trace context from Message Attributes using W3C TraceContext.

    Args:
        attributes: Pub/Sub Message metadata attribute key-value pairs.
        fallback_trace_id: Trace ID string from Proto payload in case properties don't carry context.

    Returns:
        An OpenTelemetry Context.
    """
    if attributes and "traceparent" in attributes:
        return TraceContextTextMapPropagator().extract(carrier=attributes)

    if fallback_trace_id:
        try:
            # Instead of hardcoding static `span_id=1`, derive a reproducible pseudo-random 64-bit integer
            # from the trace ID to create valid Parent context mappings.
            hasher = hashlib.sha256(fallback_trace_id.encode("utf-8"))
            derived_span_id = int(hasher.hexdigest()[:16], 16)

            parent_context = SpanContext(
                trace_id=int(fallback_trace_id, 16),
                span_id=derived_span_id,
                is_remote=True,
                trace_flags=TraceFlags(1),
            )
            parent_span = NonRecordingSpan(parent_context)
            return set_span_in_context(parent_span)
        except ValueError:
            pass

    return Context()


def create_trace_context(trace_id: str) -> Context:
    """Creates a tracing context from a trace_id string.

    Args:
        trace_id: A 32-character hex string representing the trace ID.

    Returns:
        An OpenTelemetry Context object.
    """
    return extract_trace_context(None, trace_id)

"""Utilities for distributed tracing in Apache Beam."""

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
    set_span_in_context,
    set_tracer_provider,
)

from backend.pipeline.common.env import is_gcp_env


def setup_tracing(*, use_batch: bool = True) -> None:
    """Sets up tracing for the context.

    Messages are sent to CloudTrace through the span provider and processor.
    """
    if is_gcp_env():
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


def create_trace_context(trace_id: str) -> Context:
    """Creates a tracing context from a trace_id string.

    Args:
        trace_id: A 32-character hex string representing the trace ID.

    Returns:
        An OpenTelemetry Context object.
    """
    if not trace_id:
        return Context()

    parent_context = SpanContext(
        trace_id=int(trace_id, 16),
        span_id=1,
        is_remote=True,
        trace_flags=TraceFlags(1),
    )
    parent_span = NonRecordingSpan(parent_context)
    return set_span_in_context(parent_span)

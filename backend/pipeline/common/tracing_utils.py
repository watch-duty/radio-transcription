"""Utilities for distributed tracing in Apache Beam."""

import logging
import os
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager

from opentelemetry import baggage, metrics
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.context import Context, attach, detach, get_current
from opentelemetry.exporter.cloud_monitoring import (
    CloudMonitoringMetricsExporter,
)
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
from opentelemetry.metrics import get_meter_provider, set_meter_provider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.metrics.view import (
    ExplicitBucketHistogramAggregation,
    View,
)
from opentelemetry.sdk.resources import (
    SERVICE_INSTANCE_ID,
    SERVICE_NAME,
    Resource,
)
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

# Shared pipeline stage counter
_pipeline_meter = metrics.get_meter("pipeline_telemetry")
_pipeline_stage_counter = _pipeline_meter.create_counter(
    "pipeline_stage_count",
    description="Number of audio chunks that reached each pipeline stage",
)


def record_pipeline_stage(stage: str, status: str = "start") -> None:
    """Records that an audio chunk has reached a stage/status in the pipeline."""
    _pipeline_stage_counter.add(1, {"stage": stage, "status": status})


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
        # Do not set up tracing for local development or tests
        return

    # Disable OTel metrics exporter to prevent write-frequency errors in Cloud Monitoring
    # from automatic metric collection (we only use OTel for tracing).
    os.environ.setdefault("OTEL_METRICS_EXPORTER", "none")

    current_provider = get_tracer_provider()
    if isinstance(current_provider, TracerProvider):
        return

    with _setup_lock:
        # Double check locking pattern after acquiring lock
        current_provider = get_tracer_provider()
        if isinstance(current_provider, TracerProvider):
            return

        project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") or ""

        # Resolve service metadata from environment if not explicitly provided
        if service_name is None:
            service_name = (
                os.environ.get("DATAFLOW_JOB_NAME")
                or os.environ.get("JOB_NAME")
                or os.environ.get("K_SERVICE")
                or os.environ.get("FUNCTION_TARGET")
                or "unknown_service"
            )

        resource = Resource(
            attributes={
                SERVICE_NAME: service_name,
                SERVICE_INSTANCE_ID: str(uuid.uuid4()),
            }
        )

        if not project_id:
            msg = "GOOGLE_CLOUD_PROJECT environment variable must be set in GCP environment."
            raise ValueError(msg)
        provider = TracerProvider(resource=resource)
        exporter = CloudTraceSpanExporter(project_id=project_id)

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

        current_meter_provider = get_meter_provider()
        if not isinstance(current_meter_provider, MeterProvider):
            metrics_exporter = CloudMonitoringMetricsExporter(
                project_id=project_id
            )
            # Use default export interval
            reader = PeriodicExportingMetricReader(metrics_exporter)

            # Custom bucket boundaries for E2E latency. The default OTel boundaries cap at 10s,
            # causing p99/p95 percentiles to flatline at 10s in Cloud Monitoring.
            # This progressive scale goes from 500ms up to 30 minutes (1,800,000 ms) to capture
            # normal runs, transcription times, and long queued jobs while keeping bucket count low.
            latency_view = View(
                instrument_name="transcription_e2e_latency_ms",
                aggregation=ExplicitBucketHistogramAggregation(
                    boundaries=[
                        500.0,
                        1000.0,
                        2000.0,
                        3000.0,
                        4000.0,
                        5000.0,
                        7500.0,
                        10000.0,
                        15000.0,
                        20000.0,
                        30000.0,
                        45000.0,
                        60000.0,
                        90000.0,
                        120000.0,
                        180000.0,
                        240000.0,
                        300000.0,
                        420000.0,
                        540000.0,
                        660000.0,
                        780000.0,
                        900000.0,
                        1200000.0,
                        1500000.0,
                        1800000.0,
                    ]
                ),
            )

            meter_provider = MeterProvider(
                metric_readers=[reader], resource=resource, views=[latency_view]
            )
            set_meter_provider(meter_provider)


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


@contextmanager
def inject_baggage(baggage_items: dict[str, str]) -> Iterator[None]:
    """Context manager to attach baggage to the current context."""
    if not baggage_items:
        yield
        return

    ctx = get_current()
    for k, v in baggage_items.items():
        ctx = baggage.set_baggage(k, v, context=ctx)

    token = attach(ctx)
    try:
        yield
    finally:
        detach(token)


def inject_otel_context(attributes: dict[str, str]) -> None:
    """Injects OpenTelemetry traceparent and baggage into the attributes dict."""
    TraceContextTextMapPropagator().inject(attributes)
    W3CBaggagePropagator().inject(attributes)


def extract_trace_context(attributes: dict[str, str] | None) -> Context:
    """Restores OpenTelemetry trace context and baggage from Message Attributes.

    Args:
        attributes: Pub/Sub Message metadata attribute key-value pairs.

    Returns:
        An OpenTelemetry Context.
    """
    if not attributes:
        return Context()

    ctx = Context()
    if attributes.get("traceparent"):
        ctx = TraceContextTextMapPropagator().extract(
            carrier=attributes, context=ctx
        )
    if attributes.get("baggage"):
        ctx = W3CBaggagePropagator().extract(carrier=attributes, context=ctx)

    return ctx


@contextmanager
def with_tracer_context(
    traceparent_or_attrs: str | dict[str, str],
    span_name: str,
    tracer_name: str,
) -> Iterator[Span]:
    """Context manager to create a trace context and start a span.

    Args:
        traceparent_or_attrs: The trace parent string or Pub/Sub attributes dict.
        span_name: The name of the span to create.
        tracer_name: The name of the tracer (usually __name__).
    """
    if isinstance(traceparent_or_attrs, str):
        context = extract_trace_context({"traceparent": traceparent_or_attrs})
    else:
        context = extract_trace_context(traceparent_or_attrs)

    token = attach(context)
    try:
        tracer = get_tracer(tracer_name)
        with tracer.start_as_current_span(span_name) as span:
            yield span
    finally:
        detach(token)


@contextmanager
def with_baggage_and_span(
    baggage_items: dict[str, str],
    span_name: str,
    tracer_name: str,
) -> Iterator[Span]:
    """Context manager to attach baggage and start a span cleanly in one step.

    Args:
        baggage_items: Key-value baggage pairs to attach to the execution context.
        span_name: The name of the OpenTelemetry span to create.
        tracer_name: The name of the tracer (usually __name__).
    """
    with (
        inject_baggage(baggage_items),
        get_tracer(tracer_name).start_as_current_span(span_name) as span,
    ):
        yield span

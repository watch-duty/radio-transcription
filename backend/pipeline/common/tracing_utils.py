"""Utilities for distributed tracing in Apache Beam."""

import asyncio
import logging
import os
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from typing import Any, cast

from cloudevents.http.event import CloudEvent
from opentelemetry import baggage
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.context import Context, attach, detach, get_current
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
from opentelemetry.sdk.resources import (
    SERVICE_INSTANCE_ID,
    SERVICE_NAME,
    Resource,
)
from opentelemetry.sdk.trace import ReadableSpan, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
    SpanExporter,
)
from opentelemetry.trace import (
    Span,
    Tracer,
    get_current_span,
    set_tracer_provider,
)
from opentelemetry.trace import (
    get_tracer as otel_get_tracer,
)
from opentelemetry.trace.propagation.tracecontext import (
    TraceContextTextMapPropagator,
)

from backend.pipeline.common.env import is_gcp_env


class _TracingState:
    custom_provider: TracerProvider | None = None


_setup_lock = threading.Lock()
_state = _TracingState()
telemetry_logger = logging.getLogger("telemetry.validation")


def get_tracer(
    instrumenting_module_name: str,
    instrumenting_library_version: str | None = None,
    schema_url: str | None = None,
) -> Tracer:
    """Returns a tracer from our custom provider if initialized.

    Falls back to the global OTel get_tracer if not initialized.
    """
    if _state.custom_provider is not None:
        return _state.custom_provider.get_tracer(
            instrumenting_module_name,
            instrumenting_library_version,
            schema_url,
        )
    return otel_get_tracer(
        instrumenting_module_name,
        instrumenting_library_version,
        schema_url=schema_url,
    )


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


def _resolve_exporter(*, is_console: bool) -> SpanExporter:
    """Resolves and returns the appropriate OpenTelemetry SpanExporter."""
    if is_console:
        return ConsoleSpanExporter()

    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") or ""
    if not project_id:
        msg = "GOOGLE_CLOUD_PROJECT environment variable must be set in GCP environment."
        raise ValueError(msg)
    return CloudTraceSpanExporter(project_id=project_id)


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
    is_console = os.environ.get("OTEL_TRACES_EXPORTER") == "console"

    if not is_gcp_env() and not is_console:
        # Do not set up tracing for local development or tests
        return

    # Disable OTel metrics exporter to prevent write-frequency errors in Cloud Monitoring
    # from automatic metric collection (we only use OTel for tracing).
    os.environ.setdefault("OTEL_METRICS_EXPORTER", "none")

    if _state.custom_provider is not None:
        return

    with _setup_lock:
        # Double check locking pattern after acquiring lock
        if _state.custom_provider is not None:
            return

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

        provider = TracerProvider(resource=resource)
        exporter = _resolve_exporter(is_console=is_console)

        if is_ingestion is None:
            is_ingestion = os.environ.get("IS_INGESTION_SERVICE") == "true"

        # Register the validator processor
        provider.add_span_processor(
            ContextPropagationValidator(
                service_name=service_name, is_ingestion=is_ingestion
            )
        )

        if use_batch:
            # Configure smaller, more frequent batches to avoid 504 Deadline Exceeded errors
            # in high-throughput environments like the Dataflow segmentation pipeline.
            provider.add_span_processor(
                BatchSpanProcessor(
                    exporter,
                    max_export_batch_size=64,
                    schedule_delay_millis=1000,
                )
            )
        else:
            provider.add_span_processor(SimpleSpanProcessor(exporter))

        _state.custom_provider = provider
        # Try to set as global provider for auto-instrumentation / standard libraries.
        # This will log a warning if a global provider was already set, but that is fine.
        try:
            set_tracer_provider(provider)
        except Exception:
            telemetry_logger.debug(
                "Failed to set global tracer provider; "
                "utilizing fallback custom provider."
            )


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

    # Normalize keys to lowercase for robust lookup
    normalized = {}
    for k, v in attributes.items():
        if isinstance(v, str):
            normalized[k.lower()] = v

    carrier = {}
    traceparent_keys = [
        "traceparent",
        "ce-traceparent",
        "x-goog-pubsub-attr-traceparent",
    ]
    for k in traceparent_keys:
        if k in normalized:
            carrier["traceparent"] = normalized[k]
            break

    baggage_keys = ["baggage", "ce-baggage", "x-goog-pubsub-attr-baggage"]
    for k in baggage_keys:
        if k in normalized:
            carrier["baggage"] = normalized[k]
            break

    tracestate_keys = [
        "tracestate",
        "ce-tracestate",
        "x-goog-pubsub-attr-tracestate",
    ]
    for k in tracestate_keys:
        if k in normalized:
            carrier["tracestate"] = normalized[k]
            break

    ctx = Context()
    if "traceparent" in carrier:
        ctx = TraceContextTextMapPropagator().extract(
            carrier=carrier, context=ctx
        )
    if "baggage" in carrier:
        ctx = W3CBaggagePropagator().extract(carrier=carrier, context=ctx)

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


def _safe_str(val: Any) -> str:
    """Safely converts string or bytes to a standard string."""
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="ignore")
    return str(val)


def _extract_nested_pubsub_attributes(data: Any) -> dict[str, str]:
    """Helper to extract attributes from nested Pub/Sub messages."""
    attrs_dict: dict[str, str] = {}
    if isinstance(data, dict):
        data_dict: dict[Any, Any] = data
        message = data_dict.get("message") or data_dict.get(b"message")

        # Fallback: if there is no nested "message" field, but "attributes" is present at the top level,
        # then the data dictionary itself is the message!
        if message is None and (
            "attributes" in data_dict or b"attributes" in data_dict
        ):
            message = data_dict

        if isinstance(message, dict):
            msg_dict: dict[Any, Any] = message
            attrs = msg_dict.get("attributes") or msg_dict.get(b"attributes")
            if isinstance(attrs, dict):
                attrs_dict.update(
                    {_safe_str(k): _safe_str(v) for k, v in attrs.items()}
                )
    return attrs_dict


def _normalize_prefixed_attributes(attributes: dict[str, str]) -> None:
    """Helper to normalize prefixed CloudEvent/PubSub attributes in-place."""
    for k, v in list(attributes.items()):
        clean_key = None
        if k.startswith("x-goog-pubsub-attr-"):
            clean_key = k[19:]
        elif k.startswith("ce-"):
            clean_key = k[3:]

        if clean_key and clean_key not in attributes:
            attributes[clean_key] = v


def extract_cloud_event_attributes(
    cloud_event: CloudEvent | Mapping[Any, Any],
) -> dict[str, str]:
    """Extracts combined attributes from a CloudEvent payload and metadata.

    Args:
        cloud_event: The incoming CloudEvent triggered by GCP triggers/PubSub.

    Returns:
        A dictionary of the combined attributes.
    """
    combined_attributes: dict[str, str] = {}

    if hasattr(cloud_event, "data"):
        data = cloud_event.data
        ce_attrs = getattr(cloud_event, "attributes", None)
        pubsub_container = data
    else:
        data_dict: Mapping[Any, Any] = cloud_event
        data = data_dict.get("data") or data_dict.get(b"data")
        ce_attrs = data_dict.get("attributes") or data_dict.get(b"attributes")
        pubsub_container = data if data is not None else data_dict

    # 1. Extract from nested Pub/Sub message attributes
    combined_attributes.update(
        _extract_nested_pubsub_attributes(pubsub_container)
    )

    # 2. Extract from top-level CloudEvent attributes
    if isinstance(ce_attrs, dict):
        combined_attributes.update(
            {_safe_str(k): _safe_str(v) for k, v in ce_attrs.items()}
        )

    # 3. Extract from top-level CloudEvent fields if dict-like or via get method
    for k in [
        "traceparent",
        "baggage",
        "tracestate",
        "ce-traceparent",
        "ce-baggage",
        "ce-tracestate",
        "x-goog-pubsub-attr-traceparent",
        "x-goog-pubsub-attr-baggage",
    ]:
        try:
            val = cloud_event.get(k)
            if isinstance(val, (str, bytes, int, float, bool)):
                # We intentionally coerce scalar values to str to handle robust type conversion
                # for keys like traceparent/baggage which are expected to be scalar strings,
                # while ignoring complex objects or mock returns in unit tests.
                combined_attributes[k] = _safe_str(val)
        except Exception as e:
            telemetry_logger.debug("Field %s extraction failed: %s", k, e)

    # 4. Normalize prefixed attributes
    _normalize_prefixed_attributes(combined_attributes)

    return combined_attributes


def parse_pubsub_cloudevent(
    cloud_event: CloudEvent | Mapping[Any, Any],
) -> tuple[dict[str, str], str]:
    """Parses a CloudEvent containing a Pub/Sub message, validating its structure.

    Args:
        cloud_event: The incoming CloudEvent triggered by GCP triggers/PubSub.

    Returns:
        A tuple of (combined_attributes, raw_data).

    Raises:
        ValueError: If the CloudEvent envelope or Pub/Sub message structure is invalid.
        TypeError: If the Pub/Sub message is not a dictionary.
    """
    if hasattr(cloud_event, "data"):
        envelope = cloud_event.data
    else:
        envelope = cloud_event

    if (
        not envelope
        or not isinstance(envelope, dict)
        or "message" not in envelope
    ):
        err_msg = "Invalid CloudEvent envelope structure."
        raise ValueError(err_msg)

    pubsub_message = cast("Any", envelope).get("message")
    if not isinstance(pubsub_message, dict):
        type_err = "Invalid Pub/Sub message structure inside CloudEvent."
        raise TypeError(type_err)

    message_dict = {str(k): v for k, v in pubsub_message.items()}
    raw_val = message_dict.get("data", "")
    if isinstance(raw_val, bytes):
        raw_data = raw_val.decode("utf-8")
    else:
        raw_data = str(raw_val)
    combined_attributes = extract_cloud_event_attributes(cloud_event)

    return combined_attributes, raw_data


async def traced_to_thread[T](
    span_name: str,
    func: Callable[..., T],
    *args: Any,
    **kwargs: Any,
) -> T:
    """Wraps asyncio.to_thread in a child span, recording thread execution start and end.

    This captures thread pool queue delay, actual execution duration, and dispatch delay.
    """
    tracer = get_tracer(__name__)
    with tracer.start_as_current_span(span_name) as span:

        def worker() -> T:
            # Under asyncio.to_thread, contextvars are automatically propagated.
            # Record when execution starts inside the thread pool worker thread.
            start_time = time.time()
            span.set_attribute("thread.start_time", start_time)
            span.add_event("thread_execution_start")
            telemetry_logger.info(
                "Thread execution for span '%s' started", span_name
            )
            try:
                return func(*args, **kwargs)
            finally:
                end_time = time.time()
                span.set_attribute("thread.end_time", end_time)
                span.add_event("thread_execution_end")
                telemetry_logger.info(
                    "Thread execution for span '%s' finished", span_name
                )

        return await asyncio.to_thread(worker)

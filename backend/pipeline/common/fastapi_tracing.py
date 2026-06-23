import typing

from fastapi import Request, Response
from opentelemetry.trace import SpanKind
from opentelemetry.trace.propagation.tracecontext import (
    TraceContextTextMapPropagator,
)

from backend.pipeline.common.tracing_utils import get_tracer, setup_tracing


def setup_fastapi_tracing(app: typing.Any, *, service_name: str) -> None:
    """Sets up distributed tracing for a FastAPI application thread-safely.

    Args:
        app: The FastAPI application instance.
        service_name: Name of the service to register traces under.
    """
    tracer = get_tracer(service_name)

    @app.middleware("http")
    async def trace_middleware(
        request: Request, call_next: typing.Any
    ) -> Response:
        setup_tracing(use_batch=False)
        headers = dict(request.headers)
        context = TraceContextTextMapPropagator().extract(carrier=headers)

        span_name = f"{request.method} {request.url.path}"
        with tracer.start_as_current_span(
            span_name,
            context=context,
            kind=SpanKind.SERVER,
        ) as span:
            span.set_attribute("http.method", request.method)
            span.set_attribute("http.url", str(request.url))
            span.set_attribute("http.target", request.url.path)
            if request.client:
                span.set_attribute("http.client_ip", request.client.host)

            response = await call_next(request)

            span.set_attribute("http.status_code", response.status_code)
            return response

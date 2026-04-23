import contextvars
import functools
import logging
import uuid

from google.cloud import logging as cloud_logging

from backend.pipeline.common.env import is_gcp_env

logger = logging.getLogger(__name__)

# ContextVar is isolated between concurrent runs
_TRACE_ID = contextvars.ContextVar("trace_id", default="")


def set_trace_id(trace_id: str | None = None) -> str:
    """Sets the trace ID for the current context.

    Returns the trace ID that was set.
    """
    if trace_id is None:
        trace_id = str(uuid.uuid4())
    _TRACE_ID.set(trace_id)
    return trace_id


class TraceIdFilter(logging.Filter):
    """A logging filter that injects the trace ID into the log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = _TRACE_ID.get()
        return True


@functools.cache
def setup_logging() -> None:
    """Sets up logging for the application.

    If not running in a recognized GCP environment, it uses basicConfig
    with a standard format. Otherwise, it uses the Google Cloud Logging
    client.
    """
    root_logger = logging.getLogger()
    root_logger.addFilter(TraceIdFilter())

    if is_gcp_env():
        client = cloud_logging.Client()
        client.setup_logging()
    else:
        # Standardized format for local development or unsupported environments
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s [%(trace_id)s]: %(message)s",
            force=True,
        )
        # Log that we are not in a detected GCP environment
        logger.info(
            "Running without Cloud Logging. Logs will print to console."
        )

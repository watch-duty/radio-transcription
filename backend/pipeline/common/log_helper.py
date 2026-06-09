import functools
import json
import logging
import sys
from typing import Any

from google.cloud import logging as cloud_logging

from backend.pipeline.common.env import is_gcp_env
from backend.pipeline.common.tracing_utils import (
    get_trace_attributes,
    setup_tracing,
)

logger = logging.getLogger(__name__)


@functools.cache
def setup_logging() -> None:
    """Sets up logging for the application.

    If not running in a recognized GCP environment, it uses basicConfig
    with a standard format. Otherwise, it uses the Google Cloud Logging
    client.
    """
    if is_gcp_env():
        client = cloud_logging.Client()
        client.setup_logging()

        setup_tracing(use_batch=False)
    else:
        # Standardized format for local development or unsupported environments
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            force=True,
        )
        # Log that we are not in a detected GCP environment
        logger.info(
            "Running without Cloud Logging. Logs will print to console."
        )


class TaskJsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        message = record.getMessage()
        if record.exc_info:
            exc_text = self.formatException(record.exc_info)
            message = f"{message}\n{exc_text}"

        log_record = {
            "message": message,
            "severity": record.levelname,
            "logger": record.name,
        }
        # Extract attributes added by LoggerAdapter
        for attr in ["system", "component", "feed_id", "session_id"]:
            if hasattr(record, attr):
                log_record[attr] = getattr(record, attr)

        # Add trace info from OpenTelemetry
        trace_attrs = get_trace_attributes()
        if trace_attrs.get("trace"):
            log_record["logging.googleapis.com/trace"] = trace_attrs["trace"]
            log_record["logging.googleapis.com/spanId"] = trace_attrs["spanId"]

        return json.dumps(log_record)


def get_logger(name: str) -> logging.Logger:
    """Returns a logger configured to output JSON lines to stdout, with propagation disabled to prevent duplicates in Dataflow."""
    logger = logging.getLogger(name)
    logger.propagate = False

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(TaskJsonFormatter())
        logger.addHandler(handler)

    return logger


def get_task_logger(
    name: str, extra: dict[str, Any]
) -> logging.LoggerAdapter[logging.Logger]:
    """Returns a LoggerAdapter wrapping the configured JSON logger with contextual attributes."""
    logger = get_logger(name)
    return logging.LoggerAdapter(logger, extra)

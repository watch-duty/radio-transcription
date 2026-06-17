import asyncio
import functools
import json
import logging
import sys
import threading
from typing import Any

from google.cloud import logging as cloud_logging

from backend.pipeline.common.env import is_gcp_env
from backend.pipeline.common.tracing_utils import (
    get_trace_attributes,
)

logger = logging.getLogger(__name__)


def _handle_sys_exception(
    exc_type: Any,
    exc_value: BaseException,
    exc_traceback: Any,
) -> None:
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    unhandled_logger = logging.getLogger("unhandled_exception")
    unhandled_logger.critical(
        "Unhandled exception terminating process",
        exc_info=(exc_type, exc_value, exc_traceback),
    )


def _handle_thread_exception(args: threading.ExceptHookArgs) -> None:
    if issubclass(args.exc_type, KeyboardInterrupt):
        threading.__excepthook__(args)
        return
    thread_name = args.thread.name if args.thread else "unknown"
    thread_logger = logging.getLogger(f"unhandled_exception.{thread_name}")
    thread_logger.critical(
        "Unhandled exception in background thread %s",
        thread_name,
        exc_info=args.exc_value,
    )


def _asyncio_exception_handler(
    loop: asyncio.AbstractEventLoop,
    context: dict[str, Any],
) -> None:
    msg = context.get("message", "Unhandled asyncio exception")
    exc = context.get("exception")
    task = context.get("task") or context.get("future")
    task_name = task.get_name() if hasattr(task, "get_name") else str(task)

    extra = {
        "asyncio_context": {
            k: str(v)
            for k, v in context.items()
            if k not in ("exception", "message")
        }
    }

    asyncio_logger = logging.getLogger("asyncio.unhandled")
    if exc:
        asyncio_logger.error(
            "%s (task: %s)", msg, task_name, exc_info=exc, extra=extra
        )
    else:
        asyncio_logger.error("%s (task: %s)", msg, task_name, extra=extra)


def setup_asyncio_logging(
    loop: asyncio.AbstractEventLoop | None = None,
) -> None:
    """Installs a structured exception handler on the given (or running) event loop."""
    if loop is None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

    loop.set_exception_handler(_asyncio_exception_handler)


@functools.cache
def setup_logging() -> None:
    """Sets up logging for the application.

    If not running in a recognized GCP environment, it uses basicConfig
    with a standard format. Otherwise, it uses the Google Cloud Logging
    client.
    """
    sys.excepthook = _handle_sys_exception
    threading.excepthook = _handle_thread_exception
    setup_asyncio_logging()

    if is_gcp_env():
        client = cloud_logging.Client()
        client.setup_logging()
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

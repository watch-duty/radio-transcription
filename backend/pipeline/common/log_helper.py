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
pipeline_metrics_logger: logging.Logger = logging.getLogger("pipeline.metrics")


def record_pipeline_stage(stage: str, status: str = "start") -> None:
    """Records that an audio chunk has reached a stage/status in the pipeline.

    Emits a structured log to power Log-Based Metrics. This avoids data loss
    from scale-to-zero container destruction in Cloud Run / GCF.
    """
    active_logger = (
        pipeline_metrics_logger
        if pipeline_metrics_logger.handlers
        else get_logger("pipeline.metrics")
    )
    active_logger.info(
        f"Pipeline stage recorded: {stage} -> {status}",
        extra={
            "json_fields": {
                "event_type": "pipeline_stage",
                "stage": stage,
                "status": status,
            }
        },
    )


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


RESERVED_ATTRS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "message",
    "module",
    "msecs",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
    "taskName",
}


class TaskJsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        if record.__dict__.get("structured_formatted", False):
            return record.msg

        message = record.getMessage()
        if record.exc_info:
            exc_text = self.formatException(record.exc_info)
            message = f"{message}\n{exc_text}"

        log_record: dict[str, Any] = {
            "message": message,
            "severity": record.levelname,
            "logger": record.name,
        }

        # If the message is already serialized JSON, merge its fields to
        # avoid double-serialization.
        try:
            parsed_msg = json.loads(message)
            if isinstance(parsed_msg, dict):
                log_record.update(parsed_msg)
        except (json.JSONDecodeError, TypeError):
            pass

        # Extract custom extra attributes
        log_record.update(
            {
                key: value
                for key, value in record.__dict__.items()
                if key not in RESERVED_ATTRS and not key.startswith("_")
            }
        )
        if "json_fields" in log_record and isinstance(
            log_record["json_fields"], dict
        ):
            json_fields = log_record.pop("json_fields")
            log_record.update(json_fields)

        # Add trace info from OpenTelemetry
        trace_attrs = get_trace_attributes()
        if trace_attrs.get("trace"):
            log_record["logging.googleapis.com/trace"] = trace_attrs["trace"]
            log_record["logging.googleapis.com/spanId"] = trace_attrs["spanId"]

        return json.dumps(log_record)


class _LoggingState:
    structured_propagation = False


class StructuredMessageFilter(logging.Filter):
    """A filter that converts log record messages into JSON strings to preserve custom fields during propagation.

    Useful in environments like Dataflow where logs propagate to a runner's root logger
    which only transmits string messages.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if record.__dict__.get("structured_formatted", False):
            return True

        message = record.getMessage()
        if record.exc_info:
            exc_text = logging.Formatter().formatException(record.exc_info)
            message = f"{message}\n{exc_text}"
            record.exc_info = None
            record.exc_text = None

        log_record: dict[str, Any] = {
            "message": message,
            "severity": record.levelname,
            "logger": record.name,
        }

        # If the message is already serialized JSON, merge its fields to
        # avoid double-serialization.
        try:
            parsed_msg = json.loads(message)
            if isinstance(parsed_msg, dict):
                log_record.update(parsed_msg)
        except (json.JSONDecodeError, TypeError):
            pass

        # Extract custom extra attributes
        log_record.update(
            {
                key: value
                for key, value in record.__dict__.items()
                if key not in RESERVED_ATTRS and not key.startswith("_")
            }
        )
        if "json_fields" in log_record and isinstance(
            log_record["json_fields"], dict
        ):
            json_fields = log_record.pop("json_fields")
            log_record.update(json_fields)

        # Add trace info from OpenTelemetry
        trace_attrs = get_trace_attributes()
        if trace_attrs.get("trace"):
            log_record["logging.googleapis.com/trace"] = trace_attrs["trace"]
            log_record["logging.googleapis.com/spanId"] = trace_attrs["spanId"]

        record.msg = json.dumps(log_record)
        record.args = ()
        record.__dict__["structured_formatted"] = True
        return True


def enable_structured_propagation() -> None:
    """Enables propagation of structured JSON messages to the root logger globally.

    This should be called at application/component startup when running in environments
    (like Dataflow workers) where writing directly to stdout is wrapped/incorrectly parsed,
    but the root logger has correct transport handlers.
    """
    _LoggingState.structured_propagation = True

    # Retroactively update the root logger to remove StreamHandler to avoid duplicate stdout logs.
    root_logger = logging.getLogger()
    root_logger.handlers = [
        h
        for h in root_logger.handlers
        if not isinstance(h, logging.StreamHandler)
    ]

    # Retroactively update any loggers that have already been initialized.
    # This prevents import-order issues where modules initialize their loggers
    # at import-time before setup_logging() is called.
    for logger in logging.Logger.manager.loggerDict.values():
        if isinstance(logger, logging.Logger):
            logger.propagate = True
            logger.handlers = [
                h
                for h in logger.handlers
                if not isinstance(h, logging.StreamHandler)
            ]
            if not any(
                isinstance(f, StructuredMessageFilter) for f in logger.filters
            ):
                logger.addFilter(StructuredMessageFilter())


def get_logger(name: str) -> logging.Logger:
    """Returns a logger configured for the running environment.

    If structured propagation is enabled (e.g. in Dataflow), logs propagate to the root logger
    and use a filter to format messages as JSON strings, ensuring correct severity levels.
    Otherwise (local development, Cloud Functions), logs are written directly to stdout as JSON.
    """
    logger = logging.getLogger(name)

    if _LoggingState.structured_propagation:
        logger.propagate = True
        # If any StreamHandlers were previously registered (e.g. if get_logger()
        # was called prior to setup_logging() enabling structured propagation),
        # remove them to prevent double-logging to stdout.
        logger.handlers = [
            h
            for h in logger.handlers
            if not isinstance(h, logging.StreamHandler)
        ]
        if not any(
            isinstance(f, StructuredMessageFilter) for f in logger.filters
        ):
            logger.addFilter(StructuredMessageFilter())
    else:
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

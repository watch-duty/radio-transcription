import json
import logging
import sys
from typing import Any

from backend.pipeline.common.tracing_utils import get_trace_attributes


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
            log_record.update(trace_attrs)

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

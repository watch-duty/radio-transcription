import json
import logging
import sys


class TaskJsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_record = {
            "message": record.getMessage(),
            "severity": record.levelname,
            "logger": record.name,
        }
        # Extract attributes added by LoggerAdapter
        for attr in ["system", "component", "feed_id", "session_id"]:
            if hasattr(record, attr):
                log_record[attr] = getattr(record, attr)

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

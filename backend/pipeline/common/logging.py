import logging
import os
from typing import Any

# Try to import Cloud Logging but handle cases where it's not installed
_cloud_logging: Any = None
try:
    from google.cloud import logging as cloud_logging

    _cloud_logging = cloud_logging
except ImportError:
    pass

logger = logging.getLogger(__name__)


def setup_logging() -> None:
    """Sets up logging for the application.

    If LOCAL_DEV is set, it uses basicConfig with a standard format.
    Otherwise, it uses the Google Cloud Logging client.
    """
    if not os.environ.get("LOCAL_DEV"):
        if _cloud_logging is not None:
            client = _cloud_logging.Client()
            client.setup_logging()
        else:
            # Fallback if google-cloud-logging is not installed
            logging.basicConfig(level=logging.INFO)
            logger.warning(
                "google-cloud-logging not found, falling back to basicConfig"
            )
    else:
        # Standardized format for local development
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            force=True,
        )
        # Log that we are in local dev mode
        logger.info("Running in LOCAL_DEV mode. Logs will print to console.")

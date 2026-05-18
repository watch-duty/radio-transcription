import functools
import logging

from google.cloud import logging as cloud_logging

from backend.pipeline.common.env import is_gcp_env
from backend.pipeline.common.tracing_utils import setup_tracing

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

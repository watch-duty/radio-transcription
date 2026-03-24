import logging
import os

def setup_logging() -> None:
    """Sets up logging for the application.

    If LOCAL_DEV is set, it uses basicConfig with a standard format.
    Otherwise, it uses the Google Cloud Logging client.
    """
    if not os.environ.get("LOCAL_DEV"):
        try:
            import google.cloud.logging
            client = google.cloud.logging.Client()
            client.setup_logging()
        except ImportError:
            # Fallback if google-cloud-logging is not installed
            logging.basicConfig(level=logging.INFO)
            logging.warning("google-cloud-logging not found, falling back to basicConfig")
    else:
        # Standardized format for local development
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            force=True,
        )
        # Log that we are in local dev mode
        logging.getLogger(__name__).info("Running in LOCAL_DEV mode. Logs will print to console.")

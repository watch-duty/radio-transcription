import os


def is_gcp_env() -> bool:
    """
    Detects if the application is running inside a Google Cloud environment.
    This check relies on the explicit 'IS_GCP' environment variable being set
    to 'true' via Terraform in all production deployments (Cloud Run, MIG, Jobs).

    Returns True if running in GCP, False otherwise (e.g., local development).
    """
    return os.getenv("IS_GCP") == "true"

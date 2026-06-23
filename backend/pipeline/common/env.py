import os


def is_gcp_env() -> bool:
    """Returns True if the application is running in the GCP environment.

    In local dev, it returns False.
    """
    return os.getenv("IS_GCP") == "true"

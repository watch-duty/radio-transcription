import os


def is_gcp_env() -> bool:
    """
    Detects if the application is running inside a Google Cloud environment
    by checking for common implicit environment variables injected by standard
    GCP managed runtimes (Cloud Run, Cloud Functions 2nd Gen, Cloud Run Jobs).

    Returns True if running in GCP, False otherwise (e.g., local development).
    """
    # Check for variables injected by Cloud Run or Cloud Functions (2nd Gen)
    if any(var in os.environ for var in ["K_SERVICE", "CLOUD_RUN_JOB"]):
        return True

    return False

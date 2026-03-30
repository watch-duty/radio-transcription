import os


def is_gcp_env() -> bool:
    """
    Detects if the application is running inside a Google Cloud environment
    by checking for common implicit environment variables injected by standard
    GCP managed runtimes (Cloud Run, Cloud Functions, App Engine).

    Returns True if running in GCP, False otherwise (e.g., local development).
    """
    return any(
        env_var in os.environ
        for env_var in [
            "K_SERVICE",  # Cloud Run
            "FUNCTION_TARGET",  # Cloud Functions
            "GAE_ENV",  # App Engine
            "CLOUD_RUN_JOB",  # Cloud Run Jobs
        ]
    )

import os


def is_gcp_env() -> bool:
    """
    Detects if the application is running inside a Google Cloud environment.
    This check relies on the explicit 'IS_GCP' environment variable being set
    to 'true' via Terraform in all production deployments (Cloud Run, MIG, Jobs).
    As a fallback for environments where env vars are not easily propagated
    (such as Dataflow workers), it also returns True if GOOGLE_CLOUD_PROJECT
    is set to a non-local project ID.

    Returns True if running in GCP, False otherwise (e.g., local development).
    """
    if os.getenv("IS_GCP") == "true":
        return True
    project = os.getenv("GOOGLE_CLOUD_PROJECT")
    if project and project != "local-project":
        return True
    return False

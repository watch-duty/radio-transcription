import functools
import os
import urllib.error
import urllib.request


@functools.cache
def is_gcp_env() -> bool:
    """
    Detects if the application is running inside a Google Cloud environment
    by checking for common implicit environment variables injected by standard
    GCP managed runtimes (Cloud Run, Cloud Functions, App Engine) and falling
    back to a cached Metadata Server probe for GCE/GKE support.

    Returns True if running in GCP, False otherwise (e.g., local development).
    """
    # 1. Fast Path: Check for serverless-specific-runtime variables (0ms)
    # We check for K_SERVICE (Cloud Run / Functions 2nd Gen) and CLOUD_RUN_JOB
    if any(var in os.environ for var in ["K_SERVICE", "CLOUD_RUN_JOB"]):
        return True

    # 2. Universal Fallback: Probe the Google Metadata Server (Once per process)
    # 169.254.169.254 is the standard GCP metadata server IP.
    try:
        url = "http://metadata.google.internal"
        headers = {"Metadata-Flavor": "Google"}
        req = urllib.request.Request(url, headers=headers)
        # We use a very short timeout (200ms) to ensure local development
        # doesn't feel sluggish on the first "miss" call of a process.
        # ruff: noqa: S310
        with urllib.request.urlopen(req, timeout=0.2):
            return True
    except (urllib.error.URLError, TimeoutError, ConnectionError):
        return False

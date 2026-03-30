import functools
import os

import requests


@functools.cache
def is_gcp_env() -> bool:
    """
    Detects if the application is running inside a Google Cloud environment
    by checking for common implicit environment variables injected by standard
    GCP managed runtimes (Tier 1) and falling back to a Metadata Server probe
    (Tier 2) to support GCE/MIG/GKE with cached results.

    Returns True if running in GCP, False otherwise (e.g., local development).
    """
    # Tier 1: Fast Path (0ms) - Check for serverless-specific-runtime variables.
    # Covers Cloud Run and Cloud Functions (2nd Gen).
    if any(var in os.environ for var in ["K_SERVICE", "CLOUD_RUN_JOB"]):
        return True

    # Tier 2: Universal Fallback (100-200ms once per process) - Metadata Server.
    # Covers GCE, MIG, and GKE instances that don't set serverless variables.
    try:
        url = "http://metadata.google.internal"
        headers = {"Metadata-Flavor": "Google"}
        # A short timeout is crucial to avoid hanging in non-GCP environments.
        response = requests.get(url, headers=headers, timeout=0.2)
    except (requests.exceptions.RequestException, TimeoutError):
        return False
    else:
        return response.status_code == 200

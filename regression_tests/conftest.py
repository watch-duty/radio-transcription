import os

import pytest
from google.auth.transport.requests import Request
from google.oauth2 import id_token as google_id_token

os.environ.pop("STORAGE_EMULATOR_HOST", None)


@pytest.fixture(scope="session")
def google_client_id() -> str:
    """Return the Google OAuth Client ID. Errors if not set in the environment."""
    client_id = os.environ.get("REGRESSION_TEST_CLIENT_ID")
    if not client_id:
        pytest.fail(
            "REGRESSION_TEST_CLIENT_ID environment variable is not set."
        )
    return client_id


@pytest.fixture(scope="session")
def id_token(google_client_id: str) -> str:
    """Generate a valid Google ID token for the service account."""
    try:
        auth_req = Request()
        return google_id_token.fetch_id_token(auth_req, google_client_id)
    except Exception as e:
        pytest.fail(
            "Failed to generate Google ID token using Application Default Credentials. "
            "Please ensure you are authenticated (via gcloud auth application-default login) "
            f"or impersonating the service account. Error: {e}"
        )


@pytest.fixture(scope="session")
def fe_proxy_api_url() -> str:
    """
    Return the FE Proxy API URL, which is basically the API Gateway URL.

    Errors if not set in the environment.
    """
    url = os.environ.get("API_GATEWAY_URL")
    if not url:
        pytest.fail("API_GATEWAY_URL environment variable is not set.")
    return url


@pytest.fixture(scope="session")
def gcp_project() -> str:
    """Return the GCP Project ID. Errors if not set in the environment."""
    project = os.environ.get("GCP_PROJECT")
    if not project:
        pytest.fail("GCP_PROJECT environment variable is not set.")
    return project


@pytest.fixture(scope="session")
def gcp_region() -> str:
    """Return the GCP Region. Errors if not set in the environment."""
    region = os.environ.get("GCP_REGION")
    if not region:
        pytest.fail("GCP_REGION environment variable is not set.")
    return region


@pytest.fixture(scope="session")
def gcp_echo_bucket() -> str:
    """Return the GCS bucket name for Echo audio dispatches. Errors if not set."""
    bucket = os.environ.get("GCP_ECHO_BUCKET")
    if not bucket:
        pytest.fail("GCP_ECHO_BUCKET environment variable is not set.")
    return bucket

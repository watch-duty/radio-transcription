"""Broadcastify credential rotation Cloud Function.

Triggered by Cloud Scheduler to refresh Broadcastify auth credentials and
persist a new signed JWT in Secret Manager.
"""

from __future__ import annotations

import logging
import os
import time
from typing import TYPE_CHECKING, Any

import functions_framework
import httpx
import jwt
from google.cloud import secretmanager

from backend.pipeline.common.logging import setup_logging

if TYPE_CHECKING:
    import flask

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BROADCASTIFY_USERNAME = os.environ.get("BROADCASTIFY_USERNAME", "")
BROADCASTIFY_PASSWORD = os.environ.get("BROADCASTIFY_PASSWORD", "")
BROADCASTIFY_API_KEY = os.environ.get("BROADCASTIFY_API_KEY", "")
BROADCASTIFY_API_APP_ID = os.environ.get("BROADCASTIFY_API_APP_ID", "")
BROADCASTIFY_API_KEY_ID = os.environ.get("BROADCASTIFY_API_KEY_ID", "")
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "")
SECRET_JWT = os.environ.get("BROADCASTIFY_JWT_SECRET_ID", "")
AUTH_URL = "https://api.bcfy.io/common/v1/auth"

# ---------------------------------------------------------------------------
# Global state (persisted across warm invocations)
# ---------------------------------------------------------------------------

setup_logging()
logger = logging.getLogger(__name__)
secret_client: secretmanager.SecretManagerServiceClient | None = None


def add_secret_version(secret_id: str, payload: str) -> str:
    """Add a new version to an existing Secret Manager secret.

    Secret must already exist (Terraform should have created it). This function
    returns the name of the created secret version.

    Args:
        secret_id: Secret Manager secret ID.
        payload: Secret value to store as a new version.

    Returns:
        The created secret version resource name.
    """
    if secret_client is None:
        msg = (
            "Secret Manager client not initialized - "
            "broadcastify_credential_rotation must be called first"
        )
        raise RuntimeError(msg)
    if not PROJECT_ID:
        msg = "GOOGLE_CLOUD_PROJECT environment variable is not set"
        raise RuntimeError(msg)

    parent = secret_client.secret_path(PROJECT_ID, secret_id)
    response = secret_client.add_secret_version(
        request={"parent": parent, "payload": {"data": payload.encode()}}
    )
    return response.name


def _generate_jwt(auth_claims: dict[str, str] | None = None) -> str:
    """
    Generate a JWT for API authentication using PyJWT.

    Args:
        auth_claims: Optional dictionary of additional claims (e.g., sub, utk).

    Returns:
        The generated JWT string.
    """
    if not BROADCASTIFY_API_KEY:
        msg = "BROADCASTIFY_API_KEY environment variable is not set"
        raise RuntimeError(msg)
    if not BROADCASTIFY_API_APP_ID:
        msg = "BROADCASTIFY_API_APP_ID environment variable is not set"
        raise RuntimeError(msg)
    if not BROADCASTIFY_API_KEY_ID:
        msg = "BROADCASTIFY_API_KEY_ID environment variable is not set"
        raise RuntimeError(msg)

    now = int(time.time())
    headers = {"alg": "HS256", "typ": "JWT", "kid": BROADCASTIFY_API_KEY_ID}
    payload = {
        "iss": BROADCASTIFY_API_APP_ID,
        "iat": now,
        "exp": now + 2100,  # 35 minutes
    }
    if auth_claims:
        payload.update(auth_claims)

    return jwt.encode(
        payload, BROADCASTIFY_API_KEY, algorithm="HS256", headers=headers
    )


def _require_environment() -> None:
    """Validate required environment variables at invocation time."""
    required = {
        "BROADCASTIFY_USERNAME": BROADCASTIFY_USERNAME,
        "BROADCASTIFY_PASSWORD": BROADCASTIFY_PASSWORD,
        "BROADCASTIFY_API_KEY": BROADCASTIFY_API_KEY,
        "BROADCASTIFY_API_APP_ID": BROADCASTIFY_API_APP_ID,
        "BROADCASTIFY_API_KEY_ID": BROADCASTIFY_API_KEY_ID,
        "BROADCASTIFY_JWT_SECRET_ID": SECRET_JWT,
        "GOOGLE_CLOUD_PROJECT": PROJECT_ID,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        msg = f"Missing required environment variables: {', '.join(missing)}"
        raise RuntimeError(msg)


def _authenticate() -> dict[str, Any]:
    """Call Broadcastify auth endpoint and return parsed response data."""
    unauth_jwt_token = _generate_jwt()
    headers = {"Authorization": f"Bearer {unauth_jwt_token}"}
    data = {
        "username": BROADCASTIFY_USERNAME,
        "password": BROADCASTIFY_PASSWORD,
    }

    with httpx.Client(timeout=30.0) as http_client:
        response = http_client.post(AUTH_URL, headers=headers, data=data)

    if response.status_code != 200:
        msg = f"Authentication failed: {response.status_code} - {response.text}"
        raise RuntimeError(msg)

    try:
        auth_data = response.json()
    except ValueError as exc:
        msg = "Authentication response was not valid JSON"
        raise RuntimeError(msg) from exc

    if not isinstance(auth_data, dict):
        msg = f"Authentication response has unexpected type: {type(auth_data)}"
        raise TypeError(msg)

    return auth_data


def _rotate_credentials() -> None:
    """Generate and store a new Broadcastify auth JWT."""
    auth_data = _authenticate()

    uid = auth_data.get("uid")
    token = auth_data.get("token")
    if not uid or not token:
        msg = f"Authentication response missing expected fields: {auth_data}"
        raise RuntimeError(msg)

    auth_jwt_token = _generate_jwt({"sub": uid, "utk": token})
    if not SECRET_JWT:
        msg = "BROADCASTIFY_JWT_SECRET_ID environment variable is not set"
        raise RuntimeError(msg)
    add_secret_version(SECRET_JWT, auth_jwt_token)
    logger.info("Broadcastify credentials rotated successfully")


@functions_framework.http
def broadcastify_credential_rotation(request: flask.Request) -> tuple[str, int]:
    """HTTP entry point for Broadcastify credential rotation."""
    del request  # unused for scheduler-triggered requests
    global secret_client  # noqa: PLW0603

    _require_environment()
    if secret_client is None:
        secret_client = secretmanager.SecretManagerServiceClient()

    _rotate_credentials()
    return "Successfully updated broadcastify credentials", 200

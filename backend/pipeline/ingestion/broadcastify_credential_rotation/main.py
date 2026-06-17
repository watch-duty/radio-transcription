"""Broadcastify credential rotation Cloud Function.

Triggered by Cloud Scheduler to refresh Broadcastify auth credentials and
persist a new signed JWT in Secret Manager.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import functions_framework
import jwt
import requests
from google.cloud import secretmanager
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from backend.pipeline.common.log_helper import setup_logging
from backend.pipeline.ingestion.settings import _require_env

if TYPE_CHECKING:
    import flask

# ---------------------------------------------------------------------------
# Configuration
#
# Required env vars are validated at module import via `_require_env(...)`.
# These calls run BEFORE `setup_logging()` so that contract-drift (a missing
# or empty required env var) surfaces as the first error, not masked behind
# a logging-init traceback.
# ---------------------------------------------------------------------------
BROADCASTIFY_USERNAME = _require_env("BROADCASTIFY_USERNAME")
BROADCASTIFY_PASSWORD = _require_env("BROADCASTIFY_PASSWORD")
BROADCASTIFY_API_KEY = _require_env("BROADCASTIFY_API_KEY")
BROADCASTIFY_API_APP_ID = _require_env("BROADCASTIFY_API_APP_ID")
BROADCASTIFY_API_KEY_ID = _require_env("BROADCASTIFY_API_KEY_ID")
PROJECT_ID = _require_env("GOOGLE_CLOUD_PROJECT")
SECRET_JWT = _require_env("BROADCASTIFY_JWT_SECRET_ID")
AUTH_URL = "https://api.bcfy.io/common/v1/auth"

# ---------------------------------------------------------------------------
# Global state (persisted across warm invocations)
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)
secret_client: secretmanager.SecretManagerServiceClient | None = None


def cleanup_old_versions(
    secret_client: secretmanager.SecretManagerServiceClient,
    secret_id: str,
    hours_to_keep: int = 6,
) -> None:
    """Destroys old Secret Manager versions for the given secret.

    Args:
        secret_client: Secret Manager client used to list and destroy
            secret versions.
        secret_id: Secret Manager secret ID whose old versions should be
            cleaned up.
        hours_to_keep: Number of hours of secret versions to retain. Versions
            older than this cutoff are eligible for destruction.

    Returns:
        None.

    Only versions in the `ENABLED` or `DISABLED` state can be destroyed.
    Versions that are already destroyed or scheduled for destruction are
    skipped.
    """
    parent = secret_client.secret_path(PROJECT_ID, secret_id)
    now = datetime.now(UTC)
    cutoff = now - timedelta(hours=hours_to_keep)

    # List all versions for the secret
    for version in secret_client.list_secret_versions(
        request={"parent": parent}
    ):
        # Ignore versions already destroyed or scheduled for destruction
        if version.state not in [
            secretmanager.SecretVersion.State.ENABLED,
            secretmanager.SecretVersion.State.DISABLED,
        ]:
            continue

        create_time = version.create_time
        if isinstance(create_time, datetime):
            if create_time.tzinfo is None:
                version_create_time = create_time.replace(tzinfo=UTC)
            else:
                version_create_time = create_time.astimezone(UTC)
        else:
            version_create_time = datetime.fromtimestamp(
                create_time.seconds,
                tz=UTC,
            )
        logger.debug(
            f"Version {version.name} created at {version_create_time.isoformat()}, cutoff is {cutoff.isoformat()}"
        )
        if version_create_time < cutoff:
            logger.info(f"Destroying old secret version: {version.name}")
            secret_client.destroy_secret_version(request={"name": version.name})


def add_secret_version(
    secret_client: secretmanager.SecretManagerServiceClient,
    secret_id: str,
    payload: str,
) -> str:
    """Add a new version to an existing Secret Manager secret.

    Secret must already exist (Terraform should have created it). This function
    returns the name of the created secret version.

    Args:
        secret_client: Secret Manager client.
        secret_id: Secret Manager secret ID.
        payload: Secret value to store as a new version.

    Returns:
        The created secret version resource name.
    """
    parent = secret_client.secret_path(PROJECT_ID, secret_id)
    response = secret_client.add_secret_version(
        request={"parent": parent, "payload": {"data": payload.encode()}}
    )

    # Trigger cleanup after successfully adding a new version
    try:
        cleanup_old_versions(secret_client, secret_id)
    except Exception:
        # We log and continue so the rotation itself isn't considered a failure
        # if the cleanup fails (e.g., due to permission issues).
        logger.warning(
            "Failed to clean up old secret versions for secret_id=%s",
            secret_id,
            exc_info=True,
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
    now = int(time.time())
    headers = {"alg": "HS256", "typ": "JWT", "kid": BROADCASTIFY_API_KEY_ID}
    payload = {
        "iss": BROADCASTIFY_API_APP_ID,
        "iat": now,
        "exp": now + 3600,  # 1 hour to give scheduler room for error
    }
    if auth_claims:
        payload.update(auth_claims)

    return jwt.encode(
        payload, BROADCASTIFY_API_KEY, algorithm="HS256", headers=headers
    )


def _authenticate() -> dict[str, Any]:
    """Call Broadcastify auth endpoint and return parsed response data."""
    unauth_jwt_token = _generate_jwt()
    headers = {"Authorization": f"Bearer {unauth_jwt_token}"}
    data = {
        "username": BROADCASTIFY_USERNAME,
        "password": BROADCASTIFY_PASSWORD,
    }

    retries = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"],
    )
    adapter = HTTPAdapter(max_retries=retries)

    with requests.Session() as http_client:
        http_client.mount("https://", adapter)
        response = http_client.post(
            AUTH_URL, headers=headers, data=data, timeout=30.0
        )

    if response.status_code != 200:
        request_id = response.headers.get(
            "X-Request-ID"
        ) or response.headers.get("X-Correlation-ID")
        msg = f"Authentication failed with status {response.status_code}"
        if request_id:
            msg = f"{msg} (request_id={request_id})"
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


@functions_framework.http
def broadcastify_credential_rotation(request: flask.Request) -> tuple[str, int]:
    """HTTP entry point for Broadcastify credential rotation."""
    setup_logging()
    del request  # unused for scheduler-triggered requests
    global secret_client  # noqa: PLW0603

    if secret_client is None:
        secret_client = secretmanager.SecretManagerServiceClient()

    auth_data = _authenticate()

    uid = auth_data.get("uid")
    token = auth_data.get("token")
    if not uid or not token:
        missing_fields = []
        if not uid:
            missing_fields.append("uid")
        if not token:
            missing_fields.append("token")
        msg = (
            "Authentication response missing expected fields: "
            f"{', '.join(missing_fields)}"
        )
        raise RuntimeError(msg)

    auth_jwt_token = _generate_jwt({"sub": uid, "utk": token})
    add_secret_version(secret_client, SECRET_JWT, auth_jwt_token)
    logger.info(
        "Broadcastify credentials rotated successfully for username: %s",
        BROADCASTIFY_USERNAME,
    )
    return "Successfully updated Broadcastify credentials", 200

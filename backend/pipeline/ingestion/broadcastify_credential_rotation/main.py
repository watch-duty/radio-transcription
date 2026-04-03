import logging
import os
import time

import httpx
import jwt
from google.cloud import secretmanager

from backend.pipeline.common.logging import setup_logging

# 1. Setup Logging and Secret Manager client
setup_logging()
logger = logging.getLogger(__name__)
client = secretmanager.SecretManagerServiceClient()

# 2. Get environment variables
BROADCASTIFY_USERNAME = os.environ.get("BROADCASTIFY_USERNAME")
BROADCASTIFY_PASSWORD = os.environ.get("BROADCASTIFY_PASSWORD")
BROADCASTIFY_API_KEY = os.environ.get("BROADCASTIFY_API_KEY")
BROADCASTIFY_API_APP_ID = os.environ.get("BROADCASTIFY_API_APP_ID")
BROADCASTIFY_API_KEY_ID = os.environ.get("BROADCASTIFY_API_KEY_ID")
project_id = os.environ["GOOGLE_CLOUD_PROJECT"]

if not all(
    [
        BROADCASTIFY_USERNAME,
        BROADCASTIFY_PASSWORD,
        BROADCASTIFY_API_KEY,
        BROADCASTIFY_API_APP_ID,
        BROADCASTIFY_API_KEY_ID,
    ]
):
    logger.exception("All BROADCASTIFY_* environment variables must be set.")

# 3. Get secret IDs from environment variables or use defaults
SECRET_JWT = os.environ.get("BROADCASTIFY_JWT_SECRET_ID", "broadcastify-jwt")
SECRET_UTK = os.environ.get("BROADCASTIFY_UTK_SECRET_ID", "broadcastify-utk")
SECRET_UID = os.environ.get("BROADCASTIFY_UID_SECRET_ID", "broadcastify-uid")


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
    parent = client.secret_path(project_id, secret_id)
    response = client.add_secret_version(
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
        logger.error(msg)
        raise ValueError(msg)

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


def broadcastify_credential_rotation() -> tuple[str, int]:
    """
    Broadcastify credential rotation entry point.

    This function is to be triggered by Cloud Scheduler every 30 minutes to generate JWT, UTK, and UID.
    """
    # Generate unauth JWT
    unauth_jwt_token = _generate_jwt()

    # Make Auth call
    auth_url = "https://api.bcfy.io/common/v1/auth"
    headers = {"Authorization": f"Bearer {unauth_jwt_token}"}
    data = {
        "username": BROADCASTIFY_USERNAME,
        "password": BROADCASTIFY_PASSWORD,
    }

    response = httpx.Client().post(auth_url, headers=headers, data=data)

    if response.status_code == 200:
        res_data = response.json()
        logger.info("Authentication Successful")
    else:
        logger.error(f"Authentication Failed: {response.status_code}")
        msg = f"Authentication Failed: {response.status_code} - {response.text}"
        raise RuntimeError(msg)

    if not res_data.get("uid") or not res_data.get("token"):
        msg = f"Authentication response missing expected fields: {res_data}"
        logger.exception(msg)

    # Get auth JWT
    auth_jwt_token = _generate_jwt(
        {"sub": res_data["uid"], "utk": res_data["token"]}
    )

    # Update JWT, UTK, and UID in Secret Manager
    add_secret_version(SECRET_JWT, auth_jwt_token)
    add_secret_version(SECRET_UTK, res_data["token"])
    add_secret_version(SECRET_UID, res_data["uid"])

    # Cloud Functions require a return string or object
    return "Successfully updated broadcastify credentials", 200

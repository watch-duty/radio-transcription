from __future__ import annotations

import logging
import os
from typing import Annotated, Any

import google.auth.transport.requests
import google.oauth2.id_token
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

logger = logging.getLogger(__name__)

# Security scheme for FastAPI
security = HTTPBearer()


def get_id_token(audience: str) -> str:
    """
    Fetches an OIDC identity token for the given audience.

    When running on Google Cloud (Cloud Run, GCE, etc.), this uses the
    metadata server to get a token for the service account.
    """
    try:
        auth_req = google.auth.transport.requests.Request()
        return google.oauth2.id_token.fetch_id_token(auth_req, audience)
    except Exception:
        logger.exception(f"Failed to fetch ID token for audience {audience}")
        raise


async def verify_oidc_token(
    auth: Annotated[
        HTTPAuthorizationCredentials | None, Depends(HTTPBearer(auto_error=False))
    ] = None,
) -> dict[str, Any]:
    """
    FastAPI dependency to verify an OIDC token from Google.

    Returns the decoded token claims if valid.
    """
    if os.environ.get("LOCAL_DEV") == "true":
        return {"sub": "local-dev@example.com", "email": "local-dev@example.com"}

    if not auth:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        # In Cloud Run, the Google Front End (GFE) already verifies the token
        # signature and audience if 'Allow unauthenticated' is disabled.
        # This check provides defense-in-depth and facilitates local testing
        # if a valid token is provided.
        request = google.auth.transport.requests.Request()
        return google.oauth2.id_token.verify_oauth2_token(auth.credentials, request)
    except Exception as e:
        logger.warning(f"OIDC token verification failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired authentication token",
            headers={"WWW-Authenticate": "Bearer"},
        ) from e

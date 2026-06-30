from __future__ import annotations

import logging
from typing import Annotated, Any

import google.auth.transport.requests
import google.oauth2.id_token
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from backend.pipeline.common.env import is_gcp_env

logger = logging.getLogger(__name__)


async def verify_oidc_token(
    auth: Annotated[
        HTTPAuthorizationCredentials | None,
        Depends(HTTPBearer(auto_error=False)),
    ] = None,
) -> dict[str, Any]:
    """
    FastAPI dependency to verify an OIDC token from Google.

    Returns the decoded token claims if valid.
    """
    if not is_gcp_env():
        return {
            "sub": "local-dev@example.com",
            "email": "local-dev@example.com",
        }

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
        return google.oauth2.id_token.verify_oauth2_token(
            auth.credentials, request
        )
    except Exception as e:
        logger.warning(f"OIDC token verification failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired authentication token",
            headers={"WWW-Authenticate": "Bearer"},
        ) from e

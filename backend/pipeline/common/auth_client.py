import logging

import google.auth.transport.requests
import google.oauth2.id_token

logger = logging.getLogger(__name__)


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

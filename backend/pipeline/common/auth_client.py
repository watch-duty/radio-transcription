import logging
import threading
import time

import google.auth.transport.requests
import google.oauth2.id_token

logger = logging.getLogger(__name__)

# Thread-safe lock for cache access
_cache_lock = threading.Lock()
# Cache mapping audience to (token, expire_timestamp)
_token_cache: dict[str, tuple[str, float]] = {}
# Keep cached tokens for 45 minutes (2700 seconds)
CACHE_TTL_SECONDS = 2700


def get_id_token(audience: str) -> str:
    """
    Fetches an OIDC identity token for the given audience.

    When running on Google Cloud (Cloud Run, GCE, etc.), this uses the
    metadata server to get a token for the service account.
    """
    now = time.monotonic()
    with _cache_lock:
        if audience in _token_cache:
            token, expire_at = _token_cache[audience]
            if now < expire_at:
                return token

    try:
        auth_req = google.auth.transport.requests.Request()
        token = google.oauth2.id_token.fetch_id_token(auth_req, audience)
    except Exception:
        logger.exception(f"Failed to fetch ID token for audience {audience}")
        raise

    with _cache_lock:
        _token_cache[audience] = (token, now + CACHE_TTL_SECONDS)

    return token


import logging
import threading
import time

import google.auth.transport.requests
import google.oauth2.id_token

logger = logging.getLogger(__name__)

# Global lock protecting access to the token cache dictionary
_cache_lock = threading.Lock()
# Global lock protecting access to the audience locks registry
_locks_lock = threading.Lock()

# Cache mapping audience to (token, expire_timestamp)
_token_cache: dict[str, tuple[str, float]] = {}
# Registry mapping audience to its specific threading lock
_audience_locks: dict[str, threading.Lock] = {}

# Keep cached tokens for 45 minutes (2700 seconds)
CACHE_TTL_SECONDS = 2700


def get_id_token(audience: str) -> str:
    """
    Fetches an OIDC identity token for the given audience.

    When running on Google Cloud (Cloud Run, GCE, etc.), this uses the
    metadata server to get a token for the service account.
    """
    now = time.monotonic()

    # 1. Quick check: read from cache with global cache lock
    with _cache_lock:
        if audience in _token_cache:
            token, expire_at = _token_cache[audience]
            if now < expire_at:
                return token

    # 2. Get or create a lock specific to this audience
    with _locks_lock:
        if audience not in _audience_locks:
            _audience_locks[audience] = threading.Lock()
        audience_lock = _audience_locks[audience]

    # 3. Synchronize only for this audience during the fetch
    with audience_lock:
        # Double check cache inside the lock in case another thread just populated it
        with _cache_lock:
            if audience in _token_cache:
                token, expire_at = _token_cache[audience]
                if now < expire_at:
                    return token

        # Fetch token while holding only this audience's lock
        try:
            auth_req = google.auth.transport.requests.Request()
            token = google.oauth2.id_token.fetch_id_token(auth_req, audience)
        except Exception:
            logger.exception(f"Failed to fetch ID token for audience {audience}")
            raise

        # Write new token to cache
        with _cache_lock:
            _token_cache[audience] = (token, now + CACHE_TTL_SECONDS)

    return token

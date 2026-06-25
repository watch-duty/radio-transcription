import logging
import threading
import time

import google.auth.jwt
import google.auth.transport.requests
import google.oauth2.id_token

logger = logging.getLogger(__name__)


class OIDCTokenManager:
    def __init__(self) -> None:
        # Global lock protecting access to the token cache dictionary
        self._cache_lock = threading.Lock()
        # Global lock protecting access to the audience locks registry
        self._locks_lock = threading.Lock()

        # Cache mapping audience to (token, expire_timestamp)
        self._token_cache: dict[str, tuple[str, float]] = {}
        # Registry mapping audience to its specific threading lock
        self._audience_locks: dict[str, threading.Lock] = {}

        # Shared HTTP request object to avoid creating Session per miss
        self._auth_req = google.auth.transport.requests.Request()

        # Buffer to expire tokens before they actually expire on GCP (5 mins)
        self._expiration_buffer_seconds = 300

    def _get_token_expiration(self, token: str) -> float:
        """
        Decodes the JWT to find its true expiration timestamp.
        Returns the TTL in seconds from current real time.
        """
        try:
            claims = google.auth.jwt.decode(token, verify=False)
            exp_timestamp = claims.get("exp")
            if exp_timestamp:
                return max(0.0, exp_timestamp - time.time())
        except Exception:
            logger.warning(
                "Failed to decode token for exact expiration, defaulting to 1 hour TTL"
            )

        # Default fallback: 1 hour
        return 3600.0

    def get_id_token(self, audience: str) -> str:
        """
        Fetches an OIDC identity token for the given audience.

        When running on Google Cloud (Cloud Run, GCE, etc.), this uses the
        metadata server to get a token for the service account.
        """
        now = time.monotonic()

        # 1. Lock-free fast path: read from cache
        # Dictionary GET and tuple unpacking are atomic in CPython due to the GIL.
        cached = self._token_cache.get(audience)
        if cached:
            token, expire_at = cached
            if now < expire_at:
                return token

        # 2. Get or create a lock specific to this audience
        with self._locks_lock:
            if audience not in self._audience_locks:
                self._audience_locks[audience] = threading.Lock()
            audience_lock = self._audience_locks[audience]

        # 3. Synchronize only for this audience during the fetch
        with audience_lock:
            # Double check cache inside the lock in case another thread just populated it
            cached = self._token_cache.get(audience)
            if cached:
                token, expire_at = cached
                if now < expire_at:
                    return token

            # Fetch token while holding only this audience's lock
            try:
                token = google.oauth2.id_token.fetch_id_token(
                    self._auth_req, audience
                )
            except Exception:
                logger.exception(
                    f"Failed to fetch ID token for audience {audience}"
                )
                raise

            # Calculate TTL based on exact token expiration, minus a safe buffer
            ttl = self._get_token_expiration(token)
            cache_ttl_seconds = max(0.0, ttl - self._expiration_buffer_seconds)

            # Write new token to cache
            with self._cache_lock:
                self._token_cache[audience] = (token, now + cache_ttl_seconds)

        return token

    def clear(self) -> None:
        """Clear caches and locks for testing."""
        with self._cache_lock:
            self._token_cache.clear()
        with self._locks_lock:
            self._audience_locks.clear()


# Global instance for standard usage
_default_manager = OIDCTokenManager()


def get_id_token(audience: str) -> str:
    """
    Fetches an OIDC identity token for the given audience.
    """
    return _default_manager.get_id_token(audience)

import logging
import threading

import google.auth.credentials
import google.auth.exceptions
import google.auth.transport.requests
import google.oauth2.id_token

logger = logging.getLogger(__name__)


class OIDCTokenManager:
    def __init__(self) -> None:
        # Cache mapping audience to its Credentials object
        self._cache_lock = threading.Lock()
        self._credentials_cache: dict[
            str, google.auth.credentials.Credentials
        ] = {}

        # Locks mapping audience to its specific Lock to avoid thundering herd
        self._locks_lock = threading.Lock()
        self._audience_locks: dict[str, threading.Lock] = {}

    def get_id_token(self, audience: str) -> str:
        """
        Fetches or refreshes an OIDC identity token for the given audience.
        """
        # 1. Get or create the credentials object for this audience
        with self._cache_lock:
            creds = self._credentials_cache.get(audience)
            if creds is None:
                creds = google.oauth2.id_token.fetch_id_token_credentials(
                    audience
                )
                self._credentials_cache[audience] = creds

        # 2. Get or create a lock specific to this audience
        with self._locks_lock:
            audience_lock = self._audience_locks.get(audience)
            if audience_lock is None:
                audience_lock = threading.Lock()
                self._audience_locks[audience] = audience_lock

        # 3. Synchronize only for this audience during the refresh
        with audience_lock:
            # Check validity. Since we are inside the audience lock, we are safe
            # from concurrent refreshes for this audience.
            if not creds.valid:
                # Create a fresh Request transport object to avoid sharing across threads/audiences.
                request = google.auth.transport.requests.Request()
                creds.refresh(request)

            token = creds.token
            # If refresh succeeded but token is still not set or invalid, raise
            if not token:
                error_msg = f"Refresh completed but token is empty for audience {audience}"
                raise google.auth.exceptions.RefreshError(error_msg)

            return token

    def clear(self) -> None:
        """Clear caches and locks for testing."""
        with self._cache_lock:
            self._credentials_cache.clear()
        with self._locks_lock:
            self._audience_locks.clear()


# Global instance for standard usage
_default_manager = OIDCTokenManager()


def get_id_token(audience: str) -> str:
    """
    Fetches an OIDC identity token for the given audience.
    """
    return _default_manager.get_id_token(audience)

from __future__ import annotations

import aiohttp
from gcloud.aio.storage import Storage

# aiohttp's default TCPConnector cap is 100 simultaneous connections.
# CollectorRuntime runs up to max_feeds_per_worker concurrent GCS uploads,
# so the default causes 150+ uploads to queue behind the pool limit at 250
# feeds, adding latency and event-loop overhead. Callers should pass a limit
# sized to their actual concurrency.
_DEFAULT_MAX_CONNECTIONS = 100


class GcsClient:
    """Lazily initialized async Google Cloud Storage client.

    Args:
        max_connections: Total simultaneous connections allowed on the
            underlying ``aiohttp.TCPConnector``.  Set this to at least
            ``max_feeds_per_worker`` so concurrent GCS uploads are never
            queued waiting for a free connection slot.
    """

    def __init__(self, max_connections: int = _DEFAULT_MAX_CONNECTIONS) -> None:
        self._max_connections = max_connections
        self._session: aiohttp.ClientSession | None = None
        self._storage: Storage | None = None

    def get_storage(self) -> Storage:
        """Return a shared ``Storage`` client, creating one lazily."""
        if self._storage is None:
            connector = aiohttp.TCPConnector(
                limit=self._max_connections,
                # limit_per_host=0 (default): no per-host cap beyond the
                # total; all connections may target storage.googleapis.com.
                limit_per_host=0,
            )
            self._session = aiohttp.ClientSession(connector=connector)
            self._storage = Storage(session=self._session)
        return self._storage

    async def close(self) -> None:
        """Close shared storage and aiohttp session resources."""
        if self._storage is not None:
            await self._storage.close()
            self._storage = None
        if self._session is not None:
            await self._session.close()
            self._session = None

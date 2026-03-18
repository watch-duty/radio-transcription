from __future__ import annotations

import aiohttp
from gcloud.aio.storage import Storage


class GcsClient:
    """Lazily initialized async Google Cloud Storage client."""

    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None
        self._storage: Storage | None = None

    def get_storage(self) -> Storage:
        """Return a shared ``Storage`` client, creating one lazily."""
        if self._storage is None:
            self._session = aiohttp.ClientSession()
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

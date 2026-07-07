from __future__ import annotations

import asyncio
import logging
import os
from typing import TYPE_CHECKING

from tenacity import retry, stop_after_attempt, wait_exponential

if TYPE_CHECKING:
    from google.cloud import storage

logger = logging.getLogger(__name__)


class EchoClient:
    """Client for interacting with Echo's storage backend (GCS)."""

    def __init__(
        self, client: storage.Client, bucket_name: str | None = None
    ) -> None:
        self._client = client
        self._bucket_name = bucket_name or os.environ.get(
            "ECHO_RECORDINGS_BUCKET"
        )

    @property
    def is_configured(self) -> bool:
        """Returns True if the echo recordings bucket is configured."""
        return bool(self._bucket_name and self._bucket_name.strip())

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _verify_directory_exists_sync(self, source_feed_id: str) -> bool:
        """Synchronously checks GCS for directory presence."""
        if not self._bucket_name:
            msg = "Echo recordings bucket name is not configured"
            raise ValueError(msg)

        bucket = self._client.bucket(self._bucket_name)

        # Check for directory prefix
        prefix = f"{source_feed_id}/"
        blobs = list(
            self._client.list_blobs(bucket, prefix=prefix, max_results=1)
        )
        if blobs:
            return True

        # Fallback to check exact match marker
        exact_blob = list(
            self._client.list_blobs(
                bucket, prefix=source_feed_id, max_results=1
            )
        )
        if exact_blob:
            return True

        return False

    async def verify_directory_exists(self, source_feed_id: str) -> bool:
        """Asynchronously checks GCS prefix/marker for a source feed ID."""
        return await asyncio.to_thread(
            self._verify_directory_exists_sync, source_feed_id
        )

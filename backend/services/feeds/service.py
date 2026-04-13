from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING

from .models import Feed, FeedCreate

if TYPE_CHECKING:
    from backend.pipeline.storage.feed_store import FeedStore

logger = logging.getLogger(__name__)


class FeedService:
    """Service for managing feeds, handling interaction with the data from the FeedStore."""

    def __init__(self, store: FeedStore) -> None:
        self._store = store

    async def create_feed(self, feed_in: FeedCreate) -> Feed:
        """Creates a new feed."""
        store_feed = await self._store.create_feed(
            name=feed_in.name,
            source_type=feed_in.source_type,
            source_feed_id=feed_in.source_feed_id,
            external_id=feed_in.external_id,
        )
        return Feed.model_validate(store_feed)

    async def get_feed(self, feed_id: str) -> Feed | None:
        """Fetches a feed by ID."""
        try:
            uid = uuid.UUID(feed_id)
        except ValueError:
            return None

        store_feed = await self._store.get_feed(uid)
        if not store_feed:
            return None
        return Feed.model_validate(store_feed)

    async def list_feeds(self) -> list[Feed]:
        """Lists all feeds."""
        store_feeds = await self._store.list_feeds()
        return [Feed.model_validate(f) for f in store_feeds]

    async def delete_feed(self, feed_id: str) -> bool:
        """Deletes a feed by ID."""
        try:
            uid = uuid.UUID(feed_id)
        except ValueError:
            return False
        return await self._store.delete_feed(uid)

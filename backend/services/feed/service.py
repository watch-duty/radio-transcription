from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING

from .models import Feed, FeedUpdate

if TYPE_CHECKING:
    from backend.pipeline.storage.feed_store import FeedStore

logger = logging.getLogger(__name__)


class FeedService:
    """Service for managing feeds, handling interaction with the data from the FeedStore."""

    def __init__(self, store: FeedStore) -> None:
        self._store = store

    async def create_feed(self, feed: Feed) -> Feed:
        """Creates a feed."""
        data = feed.model_dump(exclude_unset=True)
        # Remove id and created_at if present to let DB generate them
        data.pop("id", None)
        data.pop("created_at", None)

        created_dict = await self._store.create_feed(
            name=data["name"],
            source_type=data["source_type"],
            source_feed_id=data.get("source_feed_id"),
            external_id=data.get("external_id"),
        )
        return Feed(**created_dict)

    async def get_feed(self, feed_id: str) -> Feed | None:
        """Fetches a feed by ID."""
        try:
            uid = uuid.UUID(feed_id)
        except ValueError:
            return None

        feed_dict = await self._store.get_feed(uid)
        if not feed_dict:
            return None
        return Feed(**feed_dict)

    async def list_feeds(self) -> list[Feed]:
        """Lists all feeds."""
        feed_dicts = await self._store.list_feeds()
        return [Feed(**d) for d in feed_dicts]

    async def update_feed(
        self, feed_id: str, update: FeedUpdate
    ) -> Feed | None:
        """Updates a feed."""
        try:
            uid = uuid.UUID(feed_id)
        except ValueError:
            return None

        update_data = update.model_dump(exclude_unset=True)
        updated_dict = await self._store.update_feed(uid, update_data)
        if not updated_dict:
            return None
        return Feed(**updated_dict)

    async def delete_feed(self, feed_id: str) -> bool:
        """Deletes a feed by ID."""
        try:
            uid = uuid.UUID(feed_id)
        except ValueError:
            return False
        return await self._store.delete_feed(uid)

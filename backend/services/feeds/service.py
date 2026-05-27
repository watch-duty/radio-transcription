from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING

from .models import Feed, FeedCreate, FeedUpdate

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
            tags=[t.model_dump() for t in feed_in.tags]
            if feed_in.tags
            else None,
        )
        return Feed.model_validate(store_feed)

    async def update_feed(
        self, feed_id: str, feed_in: FeedUpdate
    ) -> Feed | None:
        """Updates an existing feed."""
        try:
            uid = uuid.UUID(feed_id)
        except ValueError:
            return None

        store_feed = await self._store.update_feed(
            feed_id=uid,
            name=feed_in.name,
            external_id=feed_in.external_id,
            tags=[t.model_dump() for t in feed_in.tags]
            if feed_in.tags
            else None,
        )
        if not store_feed:
            return None
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

    async def deactivate_feed(self, feed_id: str) -> bool:
        """Deactivates a feed by ID."""
        try:
            uid = uuid.UUID(feed_id)
        except ValueError:
            return False
        success = await self._store.deactivate_feed(uid)
        if success:
            logger.info(
                "Feed deactivated",
                extra={
                    "json_fields": {
                        "event_type": "feed_deactivated",
                        "feed_id": str(uid),
                    },
                },
            )
        return success

    async def reset_feed(self, feed_id: str) -> Feed | None:
        """Reset a failed, quarantined, or deactivated feed to an unclaimed state.

        This clears the claim state, resets the failure count, clears
        `worker_id`, and updates `last_heartbeat`.
        """
        try:
            uid = uuid.UUID(feed_id)
        except ValueError:
            return None
        store_feed = await self._store.reset_feed(uid)
        if not store_feed:
            return None
        logger.info(
            "Feed reset",
            extra={
                "json_fields": {
                    "event_type": "feed_reset",
                    "feed_id": str(uid),
                },
            },
        )
        return Feed.model_validate(store_feed)

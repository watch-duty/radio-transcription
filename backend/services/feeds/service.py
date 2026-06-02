from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING

from backend.pipeline.storage.feed_store import SortOrder
from .models import Feed, FeedCreate, FeedUpdate, ListFeedsResponse

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

    async def list_feeds(
        self,
        limit: int = 100,
        next_token: str | None = None,
        order: SortOrder | str = SortOrder.DESC,
        *,
        source_types: list[str] | None = None,
        statuses: list[str] | None = None,
        tags: list[str] | None = None,
    ) -> ListFeedsResponse:
        """Lists all feeds with pagination and filters."""
        parsed_tags = []
        if tags:
            for tag in tags:
                if ":" in tag:
                    k, v = tag.split(":", 1)
                    parsed_tags.append({"key": k, "value": v})

        result = await self._store.list_feeds(
            limit=limit,
            next_token=next_token,
            order=order,
            source_types=source_types,
            statuses=statuses,
            tags=parsed_tags if parsed_tags else None,
        )
        return ListFeedsResponse(
            feeds=[Feed.model_validate(f) for f in result.feeds],
            next_token=result.next_token,
        )

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

    async def delete_feed(self, feed_id: str) -> bool:
        """Hard deletes a feed by ID, along with all referencing data."""
        try:
            uid = uuid.UUID(feed_id)
        except ValueError:
            return False
        success = await self._store.delete_feed(uid)
        if success:
            logger.info(
                "Feed hard deleted",
                extra={
                    "json_fields": {
                        "event_type": "feed_hard_deleted",
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

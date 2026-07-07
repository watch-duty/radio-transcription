from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING

from backend.pipeline.storage.pagination_utils import SortOrder

from .models import (
    Feed,
    FeedCreate,
    FeedHistoryEvent,
    FeedUpdate,
    ListFeedHistoryResponse,
    ListFeedsResponse,
)

if TYPE_CHECKING:
    from backend.pipeline.storage.feed_store import (
        FeedStatus,
        FeedStore,
        SourceType,
    )

logger = logging.getLogger(__name__)


class FeedService:
    """Service for managing feeds.

    Handles interaction with the data from the FeedStore.
    """

    def __init__(self, store: FeedStore) -> None:
        self._store = store

    async def create_feed(self, feed_in: FeedCreate, *, actor_id: str) -> Feed:
        """Creates a new feed."""
        store_feed = await self._store.create_feed(
            name=feed_in.name,
            source_type=feed_in.source_type,
            source_feed_id=feed_in.source_feed_id,
            tags=[t.model_dump() for t in feed_in.tags]
            if feed_in.tags
            else None,
            actor_id=actor_id,
        )
        return Feed.model_validate(store_feed)

    async def update_feed(
        self,
        feed_id: str,
        feed_in: FeedUpdate,
        *,
        actor_id: str,
    ) -> Feed | None:
        """Updates an existing feed."""
        try:
            uid = uuid.UUID(feed_id)
        except ValueError:
            return None

        store_feed = await self._store.update_feed(
            feed_id=uid,
            name=feed_in.name,
            tags=[t.model_dump() for t in feed_in.tags]
            if feed_in.tags
            else None,
            actor_id=actor_id,
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
        *,
        limit: int = 100,
        next_token: str | None = None,
        order: SortOrder = SortOrder.DESC,
        source_types: list[SourceType] | None = None,
        statuses: list[FeedStatus] | None = None,
        tags: list[dict[str, str]] | None = None,
        name: str | None = None,
    ) -> ListFeedsResponse:
        """Lists all feeds."""
        store_feeds = await self._store.list_feeds(
            limit=limit,
            next_token=next_token,
            order=order,
            source_types=source_types,
            statuses=statuses,
            tags=tags,
            name=name,
        )
        return ListFeedsResponse(
            feeds=[Feed.model_validate(f) for f in store_feeds.feeds],
            next_token=store_feeds.next_token,
            total=store_feeds.total,
        )

    async def deactivate_feed(self, feed_id: str, *, actor_id: str) -> bool:
        """Deactivates a feed by ID."""
        try:
            uid = uuid.UUID(feed_id)
        except ValueError:
            return False
        success = await self._store.deactivate_feed(
            uid,
            actor_id=actor_id,
        )
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

    async def delete_feed(self, feed_id: str, *, actor_id: str) -> bool:
        """Hard deletes a feed by ID, along with all referencing data."""
        try:
            uid = uuid.UUID(feed_id)
        except ValueError:
            return False
        success = await self._store.delete_feed(
            uid,
            actor_id=actor_id,
        )
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

    async def reset_feed(self, feed_id: str, *, actor_id: str) -> Feed | None:
        """Reset a failed, quarantined, or deactivated feed.

        Resets to an unclaimed state. This clears the claim state, resets the
        failure count, clears `worker_id`, and updates `last_heartbeat`.
        """
        try:
            uid = uuid.UUID(feed_id)
        except ValueError:
            return None
        store_feed = await self._store.reset_feed(
            uid,
            actor_id=actor_id,
        )
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

    async def list_feed_history(
        self,
        feed_id: str,
        *,
        limit: int = 100,
        next_token: str | None = None,
        order: SortOrder = SortOrder.DESC,
    ) -> ListFeedHistoryResponse | None:
        """List state history for a feed."""
        try:
            uid = uuid.UUID(feed_id)
        except ValueError:
            return None

        # Verify feed exists before retrieving history
        feed = await self._store.get_feed(uid)
        if feed is None:
            return None

        store_events = await self._store.list_feed_history_records(
            uid,
            limit=limit,
            next_token=next_token,
            order=order,
        )
        return ListFeedHistoryResponse(
            history_events=[
                FeedHistoryEvent(
                    id=e["id"],
                    feed_id=e["feed_id"],
                    action=e["action"],
                    actor=e["actor_id"],
                    occurred_at=e["occurred_at"],
                    feed_revision=e["feed_revision"],
                    before_values=e["before_values"],
                    after_values=e["after_values"],
                )
                for e in store_events.audit_events
            ],
            next_token=store_events.next_token,
            total=store_events.total,
        )

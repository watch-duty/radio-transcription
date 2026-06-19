from __future__ import annotations

import unittest
import uuid
from unittest import mock

from backend.pipeline.storage.feed_store import FeedStatus, SourceType
from backend.services.feeds.models import BcfyFeedsCreate, FeedUpdate, Tag
from backend.services.feeds.service import FeedService

_FEED_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
_FEEDS_SERVICE_ACTOR_ID = "service:feeds-service"


def _store_feed(**overrides: object) -> dict[str, object]:
    feed: dict[str, object] = {
        "id": _FEED_ID,
        "name": "Test Feed",
        "source_type": SourceType.BCFY_FEEDS,
        "source_feed_id": "123",
        "status": FeedStatus.UNCLAIMED,
        "last_heartbeat": None,
        "quarantine_reason": None,
        "status_reason": None,
        "last_speech_segment_timestamp": None,
        "tags": None,
    }
    feed.update(overrides)
    return feed


class TestFeedServiceAuditActor(unittest.IsolatedAsyncioTestCase):
    """Tests for Phase 2 feed service audit actor propagation."""

    async def test_create_feed_passes_service_actor_to_store(self) -> None:
        store = mock.AsyncMock()
        store.create_feed.return_value = _store_feed(
            tags=[{"key": "county", "value": "Fulton"}],
        )
        service = FeedService(store)
        feed_in = BcfyFeedsCreate(
            name="Test Feed",
            source_type=SourceType.BCFY_FEEDS,
            source_feed_id="123",
            tags=[Tag(key="county", value="Fulton")],
        )

        result = await service.create_feed(feed_in)

        self.assertEqual(result.id, _FEED_ID)
        store.create_feed.assert_awaited_once_with(
            name="Test Feed",
            source_type=SourceType.BCFY_FEEDS,
            source_feed_id="123",
            tags=[{"key": "county", "value": "Fulton"}],
            actor_id=_FEEDS_SERVICE_ACTOR_ID,
        )

    async def test_update_feed_passes_service_actor_to_store(self) -> None:
        store = mock.AsyncMock()
        store.update_feed.return_value = _store_feed(
            name="Updated Feed",
            tags=[{"key": "county", "value": "Fulton"}],
        )
        service = FeedService(store)
        feed_in = FeedUpdate(
            name="Updated Feed",
            tags=[Tag(key="county", value="Fulton")],
        )

        result = await service.update_feed(str(_FEED_ID), feed_in)

        assert result is not None
        self.assertEqual(result.name, "Updated Feed")
        store.update_feed.assert_awaited_once_with(
            feed_id=_FEED_ID,
            name="Updated Feed",
            tags=[{"key": "county", "value": "Fulton"}],
            actor_id=_FEEDS_SERVICE_ACTOR_ID,
        )

    async def test_update_feed_rejects_invalid_uuid_before_store(
        self,
    ) -> None:
        store = mock.AsyncMock()
        service = FeedService(store)
        feed_in = FeedUpdate(name="Updated Feed")

        result = await service.update_feed("not-a-uuid", feed_in)

        self.assertIsNone(result)
        store.update_feed.assert_not_awaited()

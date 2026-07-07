from __future__ import annotations

import inspect
import unittest
import uuid
from unittest import mock

from backend.pipeline.storage.feed_store import FeedStatus, SourceType
from backend.services.feeds.models import BcfyFeedsCreate, FeedUpdate, Tag
from backend.services.feeds.service import FeedService

_FEED_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
_ADMIN_ACTOR_ID = "user:google:admin@example.com"


def _store_feed(**overrides: object) -> dict[str, object]:
    feed: dict[str, object] = {
        "id": _FEED_ID,
        "name": "Test Feed",
        "source_type": SourceType.BCFY_FEEDS,
        "source_feed_id": "123",
        "status": FeedStatus.UNCLAIMED,
        "last_heartbeat": None,
        "status_reason": None,
        "status_reason_detail": None,
        "last_speech_segment_timestamp": None,
        "tags": None,
    }
    feed.update(overrides)
    return feed


class TestFeedServiceAuditActor(unittest.IsolatedAsyncioTestCase):
    """Tests for feed service audit actor propagation."""

    def test_admin_mutation_methods_require_keyword_only_actor_id(self) -> None:
        """Admin service methods cannot omit or positionally pass actor_id."""
        for method_name in (
            "create_feed",
            "update_feed",
            "deactivate_feed",
            "delete_feed",
            "reset_feed",
        ):
            with self.subTest(method_name=method_name):
                signature = inspect.signature(getattr(FeedService, method_name))
                actor = signature.parameters.get("actor_id")

                self.assertIsNotNone(actor)
                assert actor is not None
                self.assertEqual(
                    actor.kind,
                    inspect.Parameter.KEYWORD_ONLY,
                )
                self.assertIs(actor.default, inspect.Parameter.empty)

    async def test_create_feed_passes_admin_actor_to_store(self) -> None:
        store = mock.AsyncMock()
        store.create_feed.return_value = _store_feed(
            tags=[{"key": "county", "value": "Fulton"}],
            status_reason_detail="provider timeout",
        )
        service = FeedService(store)
        feed_in = BcfyFeedsCreate(
            name="Test Feed",
            source_type=SourceType.BCFY_FEEDS,
            source_feed_id="123",
            tags=[Tag(key="county", value="Fulton")],
        )

        result = await service.create_feed(feed_in, actor_id=_ADMIN_ACTOR_ID)

        self.assertEqual(result.id, _FEED_ID)
        self.assertEqual(result.status_reason_detail, "provider timeout")
        store.create_feed.assert_awaited_once_with(
            name="Test Feed",
            source_type=SourceType.BCFY_FEEDS,
            source_feed_id="123",
            tags=[{"key": "county", "value": "Fulton"}],
            actor_id=_ADMIN_ACTOR_ID,
        )

    async def test_update_feed_passes_admin_actor_to_store(self) -> None:
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

        result = await service.update_feed(
            str(_FEED_ID),
            feed_in,
            actor_id=_ADMIN_ACTOR_ID,
        )

        assert result is not None
        self.assertEqual(result.name, "Updated Feed")
        store.update_feed.assert_awaited_once_with(
            feed_id=_FEED_ID,
            name="Updated Feed",
            tags=[{"key": "county", "value": "Fulton"}],
            actor_id=_ADMIN_ACTOR_ID,
        )

    async def test_update_feed_rejects_invalid_uuid_before_store(
        self,
    ) -> None:
        store = mock.AsyncMock()
        service = FeedService(store)
        feed_in = FeedUpdate(name="Updated Feed")

        result = await service.update_feed(
            "not-a-uuid",
            feed_in,
            actor_id=_ADMIN_ACTOR_ID,
        )

        self.assertIsNone(result)
        store.update_feed.assert_not_awaited()

    async def test_deactivate_feed_passes_admin_actor_to_store(self) -> None:
        store = mock.AsyncMock()
        store.deactivate_feed.return_value = True
        service = FeedService(store)

        result = await service.deactivate_feed(
            str(_FEED_ID),
            actor_id=_ADMIN_ACTOR_ID,
        )

        self.assertTrue(result)
        store.deactivate_feed.assert_awaited_once_with(
            _FEED_ID,
            actor_id=_ADMIN_ACTOR_ID,
        )

    async def test_delete_feed_passes_admin_actor_to_store(self) -> None:
        store = mock.AsyncMock()
        store.delete_feed.return_value = True
        service = FeedService(store)

        result = await service.delete_feed(
            str(_FEED_ID),
            actor_id=_ADMIN_ACTOR_ID,
        )

        self.assertTrue(result)
        store.delete_feed.assert_awaited_once_with(
            _FEED_ID,
            actor_id=_ADMIN_ACTOR_ID,
        )

    async def test_reset_feed_passes_admin_actor_to_store(self) -> None:
        store = mock.AsyncMock()
        store.reset_feed.return_value = _store_feed(status=FeedStatus.UNCLAIMED)
        service = FeedService(store)

        result = await service.reset_feed(
            str(_FEED_ID),
            actor_id=_ADMIN_ACTOR_ID,
        )

        assert result is not None
        self.assertEqual(result.id, _FEED_ID)
        store.reset_feed.assert_awaited_once_with(
            _FEED_ID,
            actor_id=_ADMIN_ACTOR_ID,
        )

    async def test_lifecycle_methods_reject_invalid_uuid_before_store(
        self,
    ) -> None:
        store = mock.AsyncMock()
        service = FeedService(store)

        deactivate_result = await service.deactivate_feed(
            "not-a-uuid",
            actor_id=_ADMIN_ACTOR_ID,
        )
        delete_result = await service.delete_feed(
            "not-a-uuid",
            actor_id=_ADMIN_ACTOR_ID,
        )
        reset_result = await service.reset_feed(
            "not-a-uuid",
            actor_id=_ADMIN_ACTOR_ID,
        )

        self.assertFalse(deactivate_result)
        self.assertFalse(delete_result)
        self.assertIsNone(reset_result)
        store.deactivate_feed.assert_not_awaited()
        store.delete_feed.assert_not_awaited()
        store.reset_feed.assert_not_awaited()

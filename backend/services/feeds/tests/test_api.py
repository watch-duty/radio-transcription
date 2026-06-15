import unittest
import uuid
from unittest.mock import AsyncMock

from fastapi import status
from fastapi.testclient import TestClient

from backend.pipeline.common.auth import verify_oidc_token
from backend.pipeline.common.exceptions import (
    FeedAlreadyExistsError,
    FeedNameAlreadyExistsError,
)
from backend.pipeline.storage.feed_store import (
    FeedStatus,
    FeedStatusReason,
    SourceType,
)
from backend.pipeline.storage.pagination_utils import SortOrder
from backend.services.feeds.main import app
from backend.services.feeds.models import Feed, ListFeedsResponse, Tag


async def skip_auth() -> dict[str, str]:
    """Mock dependency to bypass authentication in tests."""
    return {"sub": "test@example.com", "email": "test@example.com"}


class TestFeedsAPI(unittest.TestCase):
    def setUp(self) -> None:
        """Set up a test client and dependency overrides before each test."""
        self.mock_service = AsyncMock()
        app.state.feed_service = self.mock_service

        app.dependency_overrides[verify_oidc_token] = skip_auth
        self.client = TestClient(app)

    def tearDown(self) -> None:
        """Clean up after each test."""
        app.dependency_overrides.clear()

    def test_backend_only_status_reason_serializes_as_unknown(self) -> None:
        """Backend-only status reasons do not leak through the public API."""
        feed = Feed.model_validate(
            {
                "id": uuid.uuid4(),
                "name": "Test Feed",
                "source_type": SourceType.BCFY_FEEDS,
                "source_feed_id": "123",
                "status": FeedStatus.FAILING,
                "last_heartbeat": None,
                "status_reason": (
                    FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED
                ),
            }
        )

        self.assertEqual(
            feed.model_dump(mode="json")["status_reason"],
            "unknown",
        )

    def test_create_feed_success(self) -> None:
        """Test creating a feed successfully."""
        payload = {
            "name": "Test Feed",
            "source_type": "bcfy_feeds",
            "source_feed_id": "123",
        }
        feed_id = uuid.uuid4()
        mock_feed = Feed(
            id=feed_id,
            name="Test Feed",
            source_type=SourceType.BCFY_FEEDS,
            source_feed_id="123",
            status=FeedStatus.ACTIVE,
            last_heartbeat=None,
        )
        self.mock_service.create_feed.return_value = mock_feed

        response = self.client.post("/v1/feeds", json=payload)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        data = response.json()
        self.assertEqual(data["id"], str(feed_id))
        self.mock_service.create_feed.assert_called_once()

    def test_create_feed_already_exists(self) -> None:
        """Test creating a feed that already exists returns 409."""
        payload = {
            "name": "Test Feed",
            "source_type": "bcfy_feeds",
            "source_feed_id": "123",
        }
        self.mock_service.create_feed.side_effect = FeedAlreadyExistsError(
            "bcfy_feeds", "123"
        )

        response = self.client.post("/v1/feeds", json=payload)

        self.assertEqual(response.status_code, status.HTTP_409_CONFLICT)
        self.assertIn(
            "Feed with source type 'bcfy_feeds' and source feed ID '123' already exists",
            response.json()["detail"],
        )
        self.mock_service.create_feed.assert_called_once()

    def test_create_feed_validation_error(self) -> None:
        """Test creating a feed with invalid data."""
        payload = {
            "name": "Test Feed",
            # missing source_type
            "source_feed_id": "123",
        }
        response = self.client.post("/v1/feeds", json=payload)
        self.assertEqual(
            response.status_code, status.HTTP_422_UNPROCESSABLE_CONTENT
        )

    def test_create_feed_with_tags(self) -> None:
        """Test creating a feed with tags."""
        payload = {
            "name": "Test Feed",
            "source_type": "bcfy_feeds",
            "source_feed_id": "123",
            "tags": [{"key": "county", "value": "Fulton"}],
        }
        feed_id = uuid.uuid4()
        mock_feed = Feed(
            id=feed_id,
            name="Test Feed",
            source_type=SourceType.BCFY_FEEDS,
            source_feed_id="123",
            status=FeedStatus.ACTIVE,
            last_heartbeat=None,
            tags=[Tag(key="county", value="Fulton")],
        )
        self.mock_service.create_feed.return_value = mock_feed

        response = self.client.post("/v1/feeds", json=payload)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        data = response.json()
        self.assertEqual(data["id"], str(feed_id))
        self.assertEqual(data["tags"], [{"key": "county", "value": "Fulton"}])
        self.mock_service.create_feed.assert_called_once()

    def test_get_feed_success(self) -> None:
        """Test fetching an existing feed."""
        feed_id = uuid.uuid4()
        mock_feed = Feed(
            id=feed_id,
            name="Test Feed",
            source_type=SourceType.BCFY_FEEDS,
            source_feed_id="123",
            status=FeedStatus.ACTIVE,
            last_heartbeat=None,
        )
        self.mock_service.get_feed.return_value = mock_feed

        response = self.client.get(f"/v1/feeds/{feed_id}")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.json()["id"], str(feed_id))
        self.mock_service.get_feed.assert_called_once_with(str(feed_id))

    def test_get_feed_with_tags(self) -> None:
        """Test fetching an existing feed with tags."""
        feed_id = uuid.uuid4()
        mock_feed = Feed(
            id=feed_id,
            name="Test Feed",
            source_type=SourceType.BCFY_FEEDS,
            source_feed_id="123",
            status=FeedStatus.ACTIVE,
            last_heartbeat=None,
            tags=[Tag(key="county", value="Fulton")],
        )
        self.mock_service.get_feed.return_value = mock_feed

        response = self.client.get(f"/v1/feeds/{feed_id}")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()
        self.assertEqual(data["id"], str(feed_id))
        self.assertEqual(data["tags"], [{"key": "county", "value": "Fulton"}])

    def test_get_feed_not_found(self) -> None:
        """Test fetching a non-existent feed returns 404."""
        self.mock_service.get_feed.return_value = None
        feed_id = uuid.uuid4()
        response = self.client.get(f"/v1/feeds/{feed_id}")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_list_feeds(self) -> None:
        """Test listing feeds with defaults."""
        feed_id = uuid.uuid4()
        mock_feed = Feed(
            id=feed_id,
            name="Test Feed",
            source_type=SourceType.BCFY_FEEDS,
            source_feed_id="123",
            status=FeedStatus.ACTIVE,
            last_heartbeat=None,
        )
        self.mock_service.list_feeds.return_value = ListFeedsResponse(
            feeds=[mock_feed],
            next_token=None,
            total=1,
        )
        response = self.client.get("/v1/feeds")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()
        self.assertIsInstance(data["feeds"], list)
        self.assertEqual(data["feeds"][0]["id"], str(feed_id))
        self.assertIsNone(data["next_token"])
        self.assertEqual(data["total"], 1)
        self.mock_service.list_feeds.assert_called_once_with(
            limit=100,
            next_token=None,
            order=SortOrder.DESC,
            source_types=None,
            statuses=None,
            tags=None,
            name=None,
        )

    def test_list_feeds_with_filters(self) -> None:
        """Test listing feeds with filters and custom pagination."""
        feed_id = uuid.uuid4()
        mock_feed = Feed(
            id=feed_id,
            name="Test Feed",
            source_type=SourceType.BCFY_FEEDS,
            source_feed_id="123",
            status=FeedStatus.ACTIVE,
            last_heartbeat=None,
        )
        self.mock_service.list_feeds.return_value = ListFeedsResponse(
            feeds=[mock_feed],
            next_token="token123",
            total=1,
        )
        url = (
            "/v1/feeds?limit=10&next_token=token123&order=asc"
            "&source_types=bcfy_feeds&statuses=active"
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()
        self.assertIsInstance(data["feeds"], list)
        self.assertEqual(data["feeds"][0]["id"], str(feed_id))
        self.assertEqual(data["next_token"], "token123")
        self.assertEqual(data["total"], 1)
        self.mock_service.list_feeds.assert_called_once_with(
            limit=10,
            next_token="token123",
            order=SortOrder.ASC,
            source_types=[SourceType.BCFY_FEEDS],
            statuses=[FeedStatus.ACTIVE],
            tags=None,
            name=None,
        )

    def test_list_feeds_with_tags(self) -> None:
        """Test listing feeds with tags."""
        feed_id = uuid.uuid4()
        mock_feed = Feed(
            id=feed_id,
            name="Test Feed",
            source_type=SourceType.BCFY_FEEDS,
            source_feed_id="123",
            status=FeedStatus.ACTIVE,
            last_heartbeat=None,
        )
        self.mock_service.list_feeds.return_value = ListFeedsResponse(
            feeds=[mock_feed],
            next_token=None,
            total=1,
        )
        url = (
            '/v1/feeds?tags=[{"key":"region","value":"West"},'
            '{"key":"county","value":"Fulton"}]'
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()
        self.assertIsInstance(data["feeds"], list)
        self.assertEqual(data["feeds"][0]["id"], str(feed_id))
        self.assertEqual(data["total"], 1)
        self.mock_service.list_feeds.assert_called_once_with(
            limit=100,
            next_token=None,
            order=SortOrder.DESC,
            source_types=None,
            statuses=None,
            tags=[
                {"key": "region", "value": "West"},
                {"key": "county", "value": "Fulton"},
            ],
            name=None,
        )

    def test_list_feeds_with_invalid_tag_format(self) -> None:
        """Test that invalid tag format returns 400 Bad Request."""
        response = self.client.get("/v1/feeds?tags=invalidtag")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        detail = response.json()["detail"]
        self.assertIn("format for tags", detail)

    def test_list_feeds_with_comma_separated_filters(self) -> None:
        """Test listing feeds with comma-separated filter query params."""
        feed_id = uuid.uuid4()
        mock_feed = Feed(
            id=feed_id,
            name="Test Feed",
            source_type=SourceType.BCFY_FEEDS,
            source_feed_id="123",
            status=FeedStatus.ACTIVE,
            last_heartbeat=None,
        )
        self.mock_service.list_feeds.return_value = ListFeedsResponse(
            feeds=[mock_feed],
            next_token=None,
            total=1,
        )
        url = (
            "/v1/feeds?source_types=bcfy_feeds , openmhz"
            "&statuses=active , failing"
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()
        self.assertEqual(data["total"], 1)
        self.mock_service.list_feeds.assert_called_once_with(
            limit=100,
            next_token=None,
            order=SortOrder.DESC,
            source_types=[SourceType.BCFY_FEEDS, SourceType.OPENMHZ],
            statuses=[FeedStatus.ACTIVE, FeedStatus.FAILING],
            tags=None,
            name=None,
        )

    def test_list_feeds_with_name_filter(self) -> None:
        """Test listing feeds with name query parameter."""
        feed_id = uuid.uuid4()
        mock_feed = Feed(
            id=feed_id,
            name="Test Feed",
            source_type=SourceType.BCFY_FEEDS,
            source_feed_id="123",
            status=FeedStatus.ACTIVE,
            last_heartbeat=None,
        )
        self.mock_service.list_feeds.return_value = ListFeedsResponse(
            feeds=[mock_feed],
            next_token=None,
            total=1,
        )
        response = self.client.get("/v1/feeds?name=Test%20Feed")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()
        self.assertEqual(data["total"], 1)
        self.mock_service.list_feeds.assert_called_once_with(
            limit=100,
            next_token=None,
            order=SortOrder.DESC,
            source_types=None,
            statuses=None,
            tags=None,
            name="Test Feed",
        )

    def test_list_feeds_with_invalid_source_type(self) -> None:
        """Test that invalid source type returns 400 Bad Request."""
        response = self.client.get("/v1/feeds?source_types=invalid_source")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("Invalid source_type", response.json()["detail"])

    def test_list_feeds_with_invalid_status(self) -> None:
        """Test that invalid status returns 400 Bad Request."""
        response = self.client.get("/v1/feeds?statuses=invalid_status")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("Invalid status", response.json()["detail"])

    def test_deactivate_feed_success(self) -> None:
        """Test deactivating a feed successfully."""
        feed_id = uuid.uuid4()
        self.mock_service.deactivate_feed.return_value = True

        response = self.client.post(f"/v1/feeds/{feed_id}/deactivate")

        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        self.mock_service.deactivate_feed.assert_called_once_with(str(feed_id))

    def test_deactivate_feed_not_found(self) -> None:
        """Test deactivating a non-existent feed returns 404."""
        feed_id = uuid.uuid4()
        self.mock_service.deactivate_feed.return_value = False
        response = self.client.post(f"/v1/feeds/{feed_id}/deactivate")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_delete_feed_success(self) -> None:
        """Test deleting a feed successfully."""
        feed_id = uuid.uuid4()
        self.mock_service.delete_feed.return_value = True

        response = self.client.delete(f"/v1/feeds/{feed_id}")

        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        self.mock_service.delete_feed.assert_called_once_with(str(feed_id))

    def test_delete_feed_not_found(self) -> None:
        """Test deleting a non-existent feed returns 404."""
        feed_id = uuid.uuid4()
        self.mock_service.delete_feed.return_value = False
        response = self.client.delete(f"/v1/feeds/{feed_id}")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_reset_feed_success(self) -> None:
        """Test resetting a feed successfully."""
        feed_id = uuid.uuid4()
        mock_feed = Feed(
            id=feed_id,
            name="Test Feed",
            source_type=SourceType.BCFY_FEEDS,
            source_feed_id="123",
            status=FeedStatus.ACTIVE,
            last_heartbeat=None,
        )
        self.mock_service.reset_feed.return_value = mock_feed

        response = self.client.post(f"/v1/feeds/{feed_id}/reset")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()
        self.assertEqual(data["id"], str(feed_id))
        self.mock_service.reset_feed.assert_called_once_with(str(feed_id))

    def test_reset_feed_not_found(self) -> None:
        """Test resetting a non-existent feed returns 404."""
        feed_id = uuid.uuid4()
        self.mock_service.reset_feed.return_value = None

        response = self.client.post(f"/v1/feeds/{feed_id}/reset")

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_update_feed_success(self) -> None:
        """Test updating a feed successfully."""
        feed_id = uuid.uuid4()
        payload = {
            "name": "Updated Feed",
        }
        mock_feed = Feed(
            id=feed_id,
            name="Updated Feed",
            source_type=SourceType.BCFY_FEEDS,
            source_feed_id="123",
            status=FeedStatus.ACTIVE,
            last_heartbeat=None,
        )
        self.mock_service.update_feed.return_value = mock_feed

        response = self.client.put(f"/v1/feeds/{feed_id}", json=payload)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()
        self.assertEqual(data["id"], str(feed_id))
        self.assertEqual(data["name"], "Updated Feed")
        self.mock_service.update_feed.assert_called_once()

    def test_update_feed_not_found(self) -> None:
        """Test updating a non-existent feed returns 404."""
        feed_id = uuid.uuid4()
        payload = {
            "name": "Updated Feed",
        }
        self.mock_service.update_feed.return_value = None

        response = self.client.put(f"/v1/feeds/{feed_id}", json=payload)

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_update_feed_name_already_exists(self) -> None:
        """Test updating a feed with conflicting name returns 409."""
        feed_id = uuid.uuid4()
        payload = {
            "name": "Updated Feed",
        }
        self.mock_service.update_feed.side_effect = FeedNameAlreadyExistsError(
            "Updated Feed"
        )

        response = self.client.put(f"/v1/feeds/{feed_id}", json=payload)

        self.assertEqual(response.status_code, status.HTTP_409_CONFLICT)

    def test_create_feed_bcfy_calls_validation(self) -> None:
        # Valid format
        payload = {
            "name": "Test Feed",
            "source_type": "bcfy_calls",
            "source_feed_id": "123-456",
        }
        self.mock_service.create_feed.return_value = Feed(
            id=uuid.uuid4(),
            name="Test Feed",
            source_type=SourceType.BCFY_CALLS,
            source_feed_id="123-456",
            status=FeedStatus.ACTIVE,
            last_heartbeat=None,
        )
        response = self.client.post("/v1/feeds", json=payload)
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        # Invalid format
        payload["source_feed_id"] = "123"
        response = self.client.post("/v1/feeds", json=payload)
        self.assertEqual(
            response.status_code, status.HTTP_422_UNPROCESSABLE_CONTENT
        )

    def test_create_feed_bcfy_feeds_validation(self) -> None:
        # Valid format
        payload = {
            "name": "Test Feed",
            "source_type": "bcfy_feeds",
            "source_feed_id": "12345",
        }
        self.mock_service.create_feed.return_value = Feed(
            id=uuid.uuid4(),
            name="Test Feed",
            source_type=SourceType.BCFY_FEEDS,
            source_feed_id="12345",
            status=FeedStatus.ACTIVE,
            last_heartbeat=None,
        )
        response = self.client.post("/v1/feeds", json=payload)
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        # Invalid format
        payload["source_feed_id"] = "123-456"
        response = self.client.post("/v1/feeds", json=payload)
        self.assertEqual(
            response.status_code, status.HTTP_422_UNPROCESSABLE_CONTENT
        )

    def test_create_feed_echo_validation(self) -> None:
        # Valid format
        payload = {
            "name": "Test Feed",
            "source_type": "echo",
            "source_feed_id": "feed-123_abc",
        }
        self.mock_service.create_feed.return_value = Feed(
            id=uuid.uuid4(),
            name="Test Feed",
            source_type=SourceType.ECHO,
            source_feed_id="feed-123_abc",
            status=FeedStatus.ACTIVE,
            last_heartbeat=None,
        )
        response = self.client.post("/v1/feeds", json=payload)
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        # Invalid format
        payload["source_feed_id"] = "feed.123"
        response = self.client.post("/v1/feeds", json=payload)
        self.assertEqual(
            response.status_code, status.HTTP_422_UNPROCESSABLE_CONTENT
        )

    def test_create_feed_fire_notifications_validation(self) -> None:
        # Valid format
        payload = {
            "name": "Test Feed",
            "source_type": "fire_notifications",
            "source_feed_id": "RECORDINGS/SAN-JOSE-DISP",
        }
        self.mock_service.create_feed.return_value = Feed(
            id=uuid.uuid4(),
            name="Test Feed",
            source_type=SourceType.FIRE_NOTIFICATIONS,
            source_feed_id="RECORDINGS/SAN-JOSE-DISP",
            status=FeedStatus.ACTIVE,
            last_heartbeat=None,
        )
        response = self.client.post("/v1/feeds", json=payload)
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        # Invalid format (lowercase)
        payload["source_feed_id"] = "recordings/san-jose"
        response = self.client.post("/v1/feeds", json=payload)
        self.assertEqual(
            response.status_code, status.HTTP_422_UNPROCESSABLE_CONTENT
        )

    def test_create_feed_openmhz_validation(self) -> None:
        # Valid format
        payload = {
            "name": "Test Feed",
            "source_type": "openmhz",
            "source_feed_id": "open_mhz_123",
        }
        self.mock_service.create_feed.return_value = Feed(
            id=uuid.uuid4(),
            name="Test Feed",
            source_type=SourceType.OPENMHZ,
            source_feed_id="open_mhz_123",
            status=FeedStatus.ACTIVE,
            last_heartbeat=None,
        )
        response = self.client.post("/v1/feeds", json=payload)
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        # Invalid format (dash)
        payload["source_feed_id"] = "open-mhz"
        response = self.client.post("/v1/feeds", json=payload)
        self.assertEqual(
            response.status_code, status.HTTP_422_UNPROCESSABLE_CONTENT
        )


if __name__ == "__main__":
    unittest.main()

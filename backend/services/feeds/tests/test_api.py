import unittest
import uuid
from unittest.mock import AsyncMock

from fastapi import status
from fastapi.testclient import TestClient

from backend.pipeline.common.auth import verify_oidc_token
from backend.pipeline.storage.feed_store import FeedStatus, SourceType
from backend.services.feeds.main import app
from backend.services.feeds.models import Feed


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

    def test_create_feed_success(self) -> None:
        """Test creating a feed successfully."""
        payload = {
            "name": "Test Feed",
            "source_type": "bcfy_feeds",
            "source_feed_id": "123",
            "external_id": "ext_123",
        }
        feed_id = uuid.uuid4()
        mock_feed = Feed(
            id=feed_id,
            name="Test Feed",
            source_type=SourceType.BCFY_FEEDS,
            source_feed_id="123",
            external_id="ext_123",
            status=FeedStatus.ACTIVE,
            last_heartbeat=None,
        )
        self.mock_service.create_feed.return_value = mock_feed

        response = self.client.post("/v1/feeds", json=payload)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        data = response.json()
        self.assertEqual(data["id"], str(feed_id))
        self.mock_service.create_feed.assert_called_once()

    def test_create_feed_validation_error(self) -> None:
        """Test creating a feed with invalid data."""
        payload = {
            "name": "Test Feed",
            # missing source_type
            "source_feed_id": "123",
            "external_id": "ext_123",
        }
        response = self.client.post("/v1/feeds", json=payload)
        self.assertEqual(
            response.status_code, status.HTTP_422_UNPROCESSABLE_CONTENT
        )

    def test_get_feed_success(self) -> None:
        """Test fetching an existing feed."""
        feed_id = uuid.uuid4()
        mock_feed = Feed(
            id=feed_id,
            name="Test Feed",
            source_type=SourceType.BCFY_FEEDS,
            source_feed_id="123",
            external_id="ext_123",
            status=FeedStatus.ACTIVE,
            last_heartbeat=None,
        )
        self.mock_service.get_feed.return_value = mock_feed

        response = self.client.get(f"/v1/feeds/{feed_id}")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.json()["id"], str(feed_id))
        self.mock_service.get_feed.assert_called_once_with(str(feed_id))

    def test_get_feed_not_found(self) -> None:
        """Test fetching a non-existent feed returns 404."""
        self.mock_service.get_feed.return_value = None
        feed_id = uuid.uuid4()
        response = self.client.get(f"/v1/feeds/{feed_id}")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_list_feeds(self) -> None:
        """Test listing feeds."""
        self.mock_service.list_feeds.return_value = []
        response = self.client.get("/v1/feeds")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIsInstance(response.json(), list)

    def test_delete_feed_success(self) -> None:
        """Test deleting a feed successfully."""
        feed_id = uuid.uuid4()
        self.mock_service.deactivate_feed.return_value = True

        response = self.client.delete(f"/v1/feeds/{feed_id}")

        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        self.mock_service.deactivate_feed.assert_called_once_with(str(feed_id))

    def test_delete_feed_not_found(self) -> None:
        """Test deleting a non-existent feed returns 404."""
        feed_id = uuid.uuid4()
        self.mock_service.deactivate_feed.return_value = False
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
            external_id="ext_123",
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


if __name__ == "__main__":
    unittest.main()

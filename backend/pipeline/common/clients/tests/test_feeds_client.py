from __future__ import annotations

import time
import unittest
from unittest import mock

import requests

from backend.pipeline.common.clients.feeds_client import FeedsClient


class TestFeedsClient(unittest.TestCase):
    def setUp(self) -> None:
        self.client = FeedsClient(base_url="http://fake-feeds-api")
        self.mock_session = mock.MagicMock(spec=requests.Session)
        self.client.session = self.mock_session

    def test_get_feed_tags_cache_miss_and_hit(self) -> None:
        mock_response = mock.MagicMock()
        mock_response.json.return_value = {
            "tags": [{"key": "county", "value": "Fulton"}]
        }
        self.mock_session.get.return_value = mock_response

        # First call: Cache miss, should call API
        tags1 = self.client.get_feed_tags("feed-1")
        self.assertIsNotNone(tags1)
        if tags1 is not None:
            self.assertEqual(len(tags1), 1)
            self.assertEqual(tags1[0].key, "county")
            self.assertEqual(tags1[0].value, "Fulton")
        self.mock_session.get.assert_called_once()

        # Reset mock to verify it's not called again
        self.mock_session.get.reset_mock()

        # Second call: Cache hit, should NOT call API
        tags2 = self.client.get_feed_tags("feed-1")
        self.assertIsNotNone(tags2)
        self.assertEqual(tags2, tags1)
        self.mock_session.get.assert_not_called()

    def test_get_feed_tags_returns_shallow_copies(self) -> None:
        mock_response = mock.MagicMock()
        mock_response.json.return_value = {
            "tags": [{"key": "county", "value": "Fulton"}]
        }
        self.mock_session.get.return_value = mock_response

        tags1 = self.client.get_feed_tags("feed-1")
        tags2 = self.client.get_feed_tags("feed-1")

        self.assertIsNotNone(tags1)
        self.assertIsNotNone(tags2)
        if tags1 is not None and tags2 is not None:
            self.assertEqual(tags1, tags2)
            self.assertIsNot(tags1, tags2)  # Should be different list instances

    def test_get_feed_tags_cache_ttl_expiration(self) -> None:
        client = FeedsClient(
            base_url="http://fake-feeds-api", cache_ttl_seconds=1.0
        )
        client.session = self.mock_session

        mock_response = mock.MagicMock()
        mock_response.json.return_value = {"tags": []}
        self.mock_session.get.return_value = mock_response

        client.get_feed_tags("feed-1")
        self.mock_session.get.assert_called_once()
        self.mock_session.get.reset_mock()

        # Call again immediately: should hit cache
        client.get_feed_tags("feed-1")
        self.mock_session.get.assert_not_called()

        # Wait for TTL to expire
        time.sleep(1.1)

        # Call after TTL: should miss cache and call API again
        client.get_feed_tags("feed-1")
        self.mock_session.get.assert_called_once()

    def test_get_feed_tags_disabled_cache(self) -> None:
        client = FeedsClient(
            base_url="http://fake-feeds-api", cache_ttl_seconds=None
        )
        client.session = self.mock_session

        mock_response = mock.MagicMock()
        mock_response.json.return_value = {"tags": []}
        self.mock_session.get.return_value = mock_response

        client.get_feed_tags("feed-1")
        client.get_feed_tags("feed-1")
        self.assertEqual(self.mock_session.get.call_count, 2)

    def test_get_feed_tags_cache_max_size_eviction(self) -> None:
        client = FeedsClient(base_url="http://fake-feeds-api", cache_max_size=2)
        client.session = self.mock_session

        mock_response = mock.MagicMock()
        mock_response.json.return_value = {"tags": []}
        self.mock_session.get.return_value = mock_response

        # Populate cache up to max size (2)
        client.get_feed_tags("feed-1")
        client.get_feed_tags("feed-2")
        self.assertEqual(self.mock_session.get.call_count, 2)
        self.mock_session.get.reset_mock()

        # Trigger eviction by adding a 3rd item
        client.get_feed_tags("feed-3")
        self.mock_session.get.assert_called_once()
        self.mock_session.get.reset_mock()

        # feed-2 and feed-3 should still be in cache (hits)
        client.get_feed_tags("feed-2")
        client.get_feed_tags("feed-3")
        self.mock_session.get.assert_not_called()

        # feed-1 should have been evicted (oldest), so it should miss
        client.get_feed_tags("feed-1")
        self.mock_session.get.assert_called_once()

    def test_get_feed_tags_no_retry_on_error(self) -> None:
        mock_err_response = mock.MagicMock()
        mock_err_response.raise_for_status.side_effect = (
            requests.exceptions.HTTPError(
                "Server Error", response=mock.MagicMock(status_code=500)
            )
        )
        self.mock_session.get.return_value = mock_err_response

        # Should return None immediately on failure (no retries)
        tags = self.client.get_feed_tags("feed-1")
        self.assertIsNone(tags)
        self.mock_session.get.assert_called_once()

    def test_get_feed_tags_404_not_found_returns_empty_and_caches(self) -> None:
        mock_404_response = mock.MagicMock()
        mock_404_response.raise_for_status.side_effect = (
            requests.exceptions.HTTPError(
                "Not Found", response=mock.MagicMock(status_code=404)
            )
        )
        self.mock_session.get.return_value = mock_404_response

        tags1 = self.client.get_feed_tags("feed-1")
        self.assertEqual(tags1, [])
        self.mock_session.get.assert_called_once()

        # Reset mock
        self.mock_session.get.reset_mock()

        # Second call should be a cache hit
        tags2 = self.client.get_feed_tags("feed-1")
        self.assertEqual(tags2, [])
        self.mock_session.get.assert_not_called()

    def test_get_feed_tags_parsing_error_returns_none_and_does_not_cache(
        self,
    ) -> None:
        mock_response = mock.MagicMock()
        mock_response.json.side_effect = ValueError("Invalid JSON")
        self.mock_session.get.return_value = mock_response

        tags = self.client.get_feed_tags("feed-1")
        self.assertIsNone(tags)

        self.mock_session.get.reset_mock()

        # Setup mock to succeed now
        mock_ok_response = mock.MagicMock()
        mock_ok_response.json.return_value = {"tags": []}
        self.mock_session.get.return_value = mock_ok_response

        # Second call should try again (not cached) and succeed
        tags2 = self.client.get_feed_tags("feed-1")
        self.assertEqual(tags2, [])
        self.mock_session.get.assert_called_once()

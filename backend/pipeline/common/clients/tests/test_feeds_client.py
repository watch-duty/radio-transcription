import unittest
from unittest.mock import MagicMock

import requests

from backend.pipeline.common.clients.feeds_client import FeedsClient
from backend.services.feeds.models import Tag


class TestFeedsClient(unittest.TestCase):
    def setUp(self) -> None:
        self.api_url = "http://test-api.com"
        self.client = FeedsClient(self.api_url)
        self.mock_session = MagicMock()
        self.client.session = self.mock_session

    def test_get_feed_tags_success(self) -> None:
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "tags": [{"key": "county", "value": "Fulton"}]
        }
        mock_response.raise_for_status.return_value = None
        self.mock_session.get.return_value = mock_response

        tags = self.client.get_feed_tags("feed-id")

        self.assertEqual(tags, [Tag(key="county", value="Fulton")])
        self.mock_session.get.assert_called_once_with(
            "http://test-api.com/v1/feeds/feed-id", headers={}, timeout=5
        )

    def test_get_feed_tags_404_returns_empty_list(self) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 404
        http_error = requests.exceptions.HTTPError(
            "404 Client Error", response=mock_response
        )
        mock_response.raise_for_status.side_effect = http_error
        self.mock_session.get.return_value = mock_response

        tags = self.client.get_feed_tags("feed-id")

        self.assertEqual(tags, [])

    def test_get_feed_tags_500_propagates_exception(self) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 500
        http_error = requests.exceptions.HTTPError(
            "500 Server Error", response=mock_response
        )
        mock_response.raise_for_status.side_effect = http_error
        self.mock_session.get.return_value = mock_response

        with self.assertRaises(requests.exceptions.HTTPError):
            self.client.get_feed_tags("feed-id")

    def test_get_feed_tags_network_error_propagates_exception(self) -> None:
        self.mock_session.get.side_effect = requests.exceptions.ConnectionError(
            "Connection failed"
        )

        with self.assertRaises(requests.exceptions.ConnectionError):
            self.client.get_feed_tags("feed-id")


if __name__ == "__main__":
    unittest.main()

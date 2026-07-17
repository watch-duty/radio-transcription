import unittest
from unittest.mock import MagicMock, patch

import httpx

from backend.pipeline.common.clients.feeds_client import FeedsClient


class TestFeedsClient(unittest.TestCase):
    def setUp(self) -> None:
        self.api_url = "http://test-feeds-api.com"
        self.client = FeedsClient(self.api_url)

    def tearDown(self) -> None:
        self.client.close()

    def test_init_initializes_with_http2(self) -> None:
        transport = self.client.client._transport
        self.assertIsNotNone(transport)
        self.assertIsInstance(transport, httpx.HTTPTransport)
        self.assertTrue(
            getattr(getattr(transport, "_pool", None), "_http2", False)
        )

    @patch("httpx.Client.get")
    def test_get_feed_tags_success(self, mock_get) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "id": "feed-123",
            "tags": [{"key": "department", "value": "fire"}],
        }
        mock_get.return_value = mock_response

        tags = self.client.get_feed_tags("feed-123")
        self.assertIsNotNone(tags)
        assert tags is not None
        self.assertEqual(len(tags), 1)
        self.assertEqual(tags[0].key, "department")
        self.assertEqual(tags[0].value, "fire")

        mock_get.assert_called_once_with(
            "http://test-feeds-api.com/v1/feeds/feed-123",
            headers={},
            timeout=5,
        )

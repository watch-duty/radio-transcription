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

    @patch("time.sleep", return_value=None)
    @patch("httpx.Client.get")
    def test_get_feed_tags_retry_on_transient_error_exhausted(
        self, mock_get, mock_sleep
    ) -> None:
        """Test that FeedsClient retries on transient errors and eventually returns None."""
        mock_response = MagicMock()
        mock_response.status_code = 503
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Service Unavailable",
            request=MagicMock(spec=httpx.Request),
            response=mock_response,
        )
        mock_get.return_value = mock_response

        tags = self.client.get_feed_tags("feed-123")
        self.assertIsNone(tags)
        self.assertEqual(mock_get.call_count, 4)

    @patch("time.sleep", return_value=None)
    @patch("httpx.Client.get")
    def test_get_feed_tags_retry_and_succeed(
        self, mock_get, mock_sleep
    ) -> None:
        """Test that FeedsClient succeeds after transient errors."""
        mock_rule_response = MagicMock()
        mock_rule_response.status_code = 200
        mock_rule_response.json.return_value = {
            "id": "feed-123",
            "tags": [{"key": "department", "value": "fire"}],
        }

        # First 2 times transient timeouts, then success
        mock_get.side_effect = [
            httpx.ReadTimeout("Timeout 1"),
            httpx.ReadTimeout("Timeout 2"),
            mock_rule_response,
        ]

        tags = self.client.get_feed_tags("feed-123")
        self.assertIsNotNone(tags)
        assert tags is not None
        self.assertEqual(len(tags), 1)
        self.assertEqual(tags[0].key, "department")
        self.assertEqual(tags[0].value, "fire")
        self.assertEqual(mock_get.call_count, 3)

    @patch("time.sleep", return_value=None)
    @patch("httpx.Client.get")
    def test_get_feed_tags_no_retry_on_404(self, mock_get, mock_sleep) -> None:
        """Test that FeedsClient fails immediately without retrying on client errors (404)."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Not Found",
            request=MagicMock(spec=httpx.Request),
            response=mock_response,
        )
        mock_get.return_value = mock_response

        tags = self.client.get_feed_tags("feed-123")
        self.assertIsNone(tags)
        self.assertEqual(mock_get.call_count, 1)
        mock_sleep.assert_not_called()

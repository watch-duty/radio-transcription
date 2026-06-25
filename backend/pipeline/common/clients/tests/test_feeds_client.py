import unittest
from unittest.mock import MagicMock, patch

import requests

from backend.pipeline.common.clients.feeds_client import FeedsClient


class TestFeedsClient(unittest.TestCase):
    def setUp(self) -> None:
        self.api_url = "http://test-api.com"
        self.client = FeedsClient(self.api_url)
        self.mock_session = MagicMock()
        self.client.session = self.mock_session

        self.feed_id = "feed-id-123"
        self.feed_payload = {
            "id": self.feed_id,
            "name": "Test Feed",
            "tags": [{"key": "tag1", "value": "val1"}],
        }

    def test_get_feed_tags_success(self) -> None:
        # Setup
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = self.feed_payload
        self.mock_session.get.return_value = mock_response

        # Execute
        tags = self.client.get_feed_tags(self.feed_id)

        # Verify
        assert tags is not None
        self.assertEqual(len(tags), 1)
        self.assertEqual(tags[0].key, "tag1")
        self.assertEqual(tags[0].value, "val1")
        self.mock_session.get.assert_called_once_with(
            f"{self.api_url}/v1/feeds/{self.feed_id}",
            headers={},
            timeout=5,
        )

    def test_get_feed_tags_404(self) -> None:
        # Setup
        mock_response = MagicMock()
        mock_response.status_code = 404
        http_error = requests.exceptions.HTTPError(response=mock_response)
        mock_response.raise_for_status.side_effect = http_error
        self.mock_session.get.return_value = mock_response

        # Execute
        tags = self.client.get_feed_tags(self.feed_id)

        # Verify
        self.assertEqual(tags, [])

    def test_get_feed_tags_500_raises(self) -> None:
        # Setup
        mock_response = MagicMock()
        mock_response.status_code = 500
        http_error = requests.exceptions.HTTPError(response=mock_response)
        mock_response.raise_for_status.side_effect = http_error
        self.mock_session.get.return_value = mock_response

        # Execute & Verify
        with self.assertRaises(requests.exceptions.HTTPError):
            self.client.get_feed_tags(self.feed_id)

    def test_get_feed_tags_timeout_raises(self) -> None:
        # Setup
        self.mock_session.get.side_effect = requests.exceptions.Timeout(
            "Timeout"
        )

        # Execute & Verify
        with self.assertRaises(requests.exceptions.Timeout):
            self.client.get_feed_tags(self.feed_id)

    @patch("backend.pipeline.common.clients.feeds_client.env.is_gcp_env")
    @patch(
        "backend.pipeline.common.clients.feeds_client.auth_client.get_id_token"
    )
    def test_get_feed_tags_adds_auth_in_gcp(
        self, mock_get_id_token, mock_is_gcp_env
    ) -> None:
        # Setup
        mock_is_gcp_env.return_value = True
        mock_get_id_token.return_value = "fake-token"

        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = self.feed_payload
        self.mock_session.get.return_value = mock_response

        # Execute
        self.client.get_feed_tags(self.feed_id)

        # Verify
        self.mock_session.get.assert_called_once_with(
            f"{self.api_url}/v1/feeds/{self.feed_id}",
            headers={"Authorization": "Bearer fake-token"},
            timeout=5,
        )


if __name__ == "__main__":
    unittest.main()

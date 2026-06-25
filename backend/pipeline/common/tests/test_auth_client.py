import time
import unittest
from unittest.mock import patch

from backend.pipeline.common.auth_client import _token_cache, get_id_token


class TestAuthClient(unittest.TestCase):
    def setUp(self) -> None:
        # Clear the module-level token cache before each test
        _token_cache.clear()

    @patch("google.oauth2.id_token.fetch_id_token")
    def test_get_id_token_fetches_and_caches(self, mock_fetch) -> None:
        mock_fetch.side_effect = ["token-1", "token-2"]

        # First call: should fetch
        token1 = get_id_token("http://audience-1")
        self.assertEqual(token1, "token-1")
        mock_fetch.assert_called_once()

        # Second call for same audience: should hit cache
        token2 = get_id_token("http://audience-1")
        self.assertEqual(token2, "token-1")
        # Assert fetch_id_token is still only called once
        mock_fetch.assert_called_once()

        # Call for a different audience: should fetch
        token3 = get_id_token("http://audience-2")
        self.assertEqual(token3, "token-2")
        self.assertEqual(mock_fetch.call_count, 2)

    @patch("google.oauth2.id_token.fetch_id_token")
    def test_get_id_token_expires_after_ttl(self, mock_fetch) -> None:
        mock_fetch.side_effect = ["token-1", "token-2"]

        # First call: fetches
        token1 = get_id_token("http://audience-1")
        self.assertEqual(token1, "token-1")
        mock_fetch.assert_called_once()

        # Mock time moving forward past TTL (e.g. 45 minutes / 2700s)
        # Using patch to shift time forward
        with patch("time.monotonic", return_value=time.monotonic() + 2701):
            token2 = get_id_token("http://audience-1")
            self.assertEqual(token2, "token-2")
            self.assertEqual(mock_fetch.call_count, 2)


if __name__ == "__main__":
    unittest.main()

import threading
import time
import unittest
from unittest.mock import patch

import google.auth.credentials
import google.auth.exceptions

from backend.pipeline.common.auth_client import (
    _default_manager,
    get_id_token,
)


class MockCredentials(google.auth.credentials.Credentials):
    def __init__(self, initial_token: str | None = None) -> None:
        super().__init__()
        self.token = initial_token
        self.refresh_count = 0
        self._valid = initial_token is not None
        self.should_fail = False

    @property
    def valid(self) -> bool:
        return self._valid

    def refresh(self, request) -> None:
        if self.should_fail:
            err_msg = "Network error"
            raise google.auth.exceptions.RefreshError(err_msg)
        self.refresh_count += 1
        self.token = f"refreshed-token-{self.refresh_count}"
        self._valid = True


class TestAuthClient(unittest.TestCase):
    def setUp(self) -> None:
        # Clear module-level caches before each test
        _default_manager.clear()

    @patch("google.oauth2.id_token.fetch_id_token_credentials")
    def test_get_id_token_fetches_and_caches(self, mock_fetch_creds) -> None:
        mock_creds_1 = MockCredentials()
        mock_creds_2 = MockCredentials()
        mock_fetch_creds.side_effect = [mock_creds_1, mock_creds_2]

        # First call: should fetch credentials and refresh
        token1 = get_id_token("http://audience-1")
        self.assertEqual(token1, "refreshed-token-1")
        mock_fetch_creds.assert_called_once_with("http://audience-1")
        self.assertEqual(mock_creds_1.refresh_count, 1)

        # Second call for same audience: should hit cache and not refresh
        token2 = get_id_token("http://audience-1")
        self.assertEqual(token2, "refreshed-token-1")
        self.assertEqual(mock_creds_1.refresh_count, 1)
        self.assertEqual(mock_fetch_creds.call_count, 1)

        # Call for a different audience: should fetch credentials and refresh
        token3 = get_id_token("http://audience-2")
        self.assertEqual(token3, "refreshed-token-1")
        self.assertEqual(mock_creds_2.refresh_count, 1)
        self.assertEqual(mock_fetch_creds.call_count, 2)

    @patch("google.oauth2.id_token.fetch_id_token_credentials")
    def test_get_id_token_expires_after_ttl(self, mock_fetch_creds) -> None:
        mock_creds = MockCredentials()
        mock_fetch_creds.return_value = mock_creds

        # First call: fetches and refreshes
        token1 = get_id_token("http://audience-1")
        self.assertEqual(token1, "refreshed-token-1")
        self.assertEqual(mock_creds.refresh_count, 1)

        # Simulate expiration
        mock_creds._valid = False

        # Next call should refresh again
        token2 = get_id_token("http://audience-1")
        self.assertEqual(token2, "refreshed-token-2")
        self.assertEqual(mock_creds.refresh_count, 2)

    @patch("google.oauth2.id_token.fetch_id_token_credentials")
    def test_concurrent_different_audiences_do_not_block(
        self, mock_fetch_creds
    ) -> None:
        class SlowMockCredentials(MockCredentials):
            def refresh(self, request) -> None:
                time.sleep(0.2)
                super().refresh(request)

        mock_creds_1 = SlowMockCredentials()
        mock_creds_2 = SlowMockCredentials()
        mock_fetch_creds.side_effect = [mock_creds_1, mock_creds_2]

        start_time = time.monotonic()
        results = {}

        def worker(aud):
            results[aud] = get_id_token(aud)

        # Start concurrent fetches for two different audiences
        t1 = threading.Thread(target=worker, args=("http://aud-1",))
        t2 = threading.Thread(target=worker, args=("http://aud-2",))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        duration = time.monotonic() - start_time

        # Ensure both got tokens
        self.assertEqual(results["http://aud-1"], "refreshed-token-1")
        self.assertEqual(results["http://aud-2"], "refreshed-token-1")

        # In parallel, it should take around 0.2 seconds.
        self.assertLess(duration, 0.35)

    @patch("google.oauth2.id_token.fetch_id_token_credentials")
    def test_refresh_failure_does_not_cache_token(
        self, mock_fetch_creds
    ) -> None:
        mock_creds = MockCredentials()
        mock_creds.should_fail = True
        mock_fetch_creds.return_value = mock_creds

        # First call: should try to refresh and fail
        with self.assertRaises(google.auth.exceptions.RefreshError):
            get_id_token("http://audience-failed")

        # Subsequent call: should still try to refresh (because .valid remains False)
        # We disable should_fail so it succeeds this time
        mock_creds.should_fail = False

        token = get_id_token("http://audience-failed")
        self.assertEqual(token, "refreshed-token-1")


if __name__ == "__main__":
    unittest.main()

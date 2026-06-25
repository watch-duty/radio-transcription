import threading
import time
import unittest
from unittest.mock import patch

from backend.pipeline.common.auth_client import (
    _audience_locks,
    _token_cache,
    get_id_token,
)


class TestAuthClient(unittest.TestCase):
    def setUp(self) -> None:
        # Clear module-level caches before each test
        _token_cache.clear()
        _audience_locks.clear()

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
        with patch("time.monotonic", return_value=time.monotonic() + 2701):
            token2 = get_id_token("http://audience-1")
            self.assertEqual(token2, "token-2")
            self.assertEqual(mock_fetch.call_count, 2)

    @patch("google.oauth2.id_token.fetch_id_token")
    def test_concurrent_different_audiences_do_not_block(self, mock_fetch) -> None:
        # We want to simulate that Thread A (audience-1) and Thread B (audience-2)
        # run in parallel, and their network fetches (which block on sleep)
        # happen concurrently rather than sequentially.

        def slow_fetch(auth_req, audience):
            time.sleep(0.2)
            return f"token-for-{audience}"

        mock_fetch.side_effect = slow_fetch

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

        self.assertEqual(results["http://aud-1"], "token-for-http://aud-1")
        self.assertEqual(results["http://aud-2"], "token-for-http://aud-2")
        # If they ran sequentially, it would take at least 0.4 seconds.
        # In parallel, it should take around 0.2 seconds.
        # We assert it takes less than 0.35s to verify parallel execution.
        self.assertLess(duration, 0.35)


if __name__ == "__main__":
    unittest.main()

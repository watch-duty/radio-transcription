import base64
import json
import threading
import time
import unittest
from unittest.mock import patch

from backend.pipeline.common.auth_client import (
    _default_manager,
    get_id_token,
)


def create_mock_jwt(exp_offset: int) -> str:
    """Create a mock JWT string with a specific expiration offset from now."""
    header = (
        base64.urlsafe_b64encode(b'{"alg":"RS256","typ":"JWT"}')
        .decode("utf-8")
        .rstrip("=")
    )
    exp_time = int(time.time()) + exp_offset
    payload_dict = {"exp": exp_time}
    payload = (
        base64.urlsafe_b64encode(json.dumps(payload_dict).encode("utf-8"))
        .decode("utf-8")
        .rstrip("=")
    )
    signature = (
        base64.urlsafe_b64encode(b"signature").decode("utf-8").rstrip("=")
    )
    return f"{header}.{payload}.{signature}"


class TestAuthClient(unittest.TestCase):
    def setUp(self) -> None:
        # Clear module-level caches before each test
        _default_manager.clear()

    @patch("google.oauth2.id_token.fetch_id_token")
    def test_get_id_token_fetches_and_caches(self, mock_fetch) -> None:
        token_str_1 = create_mock_jwt(3600)
        token_str_2 = create_mock_jwt(3600)
        mock_fetch.side_effect = [token_str_1, token_str_2]

        # First call: should fetch
        token1 = get_id_token("http://audience-1")
        self.assertEqual(token1, token_str_1)
        mock_fetch.assert_called_once()

        # Second call for same audience: should hit cache
        token2 = get_id_token("http://audience-1")
        self.assertEqual(token2, token_str_1)
        mock_fetch.assert_called_once()

        # Call for a different audience: should fetch
        token3 = get_id_token("http://audience-2")
        self.assertEqual(token3, token_str_2)
        self.assertEqual(mock_fetch.call_count, 2)

    @patch("google.oauth2.id_token.fetch_id_token")
    def test_get_id_token_expires_after_ttl(self, mock_fetch) -> None:
        # Give token 1 an expiration of 1 hour (3600 seconds)
        token_str_1 = create_mock_jwt(3600)
        token_str_2 = create_mock_jwt(3600)
        mock_fetch.side_effect = [token_str_1, token_str_2]

        # First call: fetches
        token1 = get_id_token("http://audience-1")
        self.assertEqual(token1, token_str_1)
        mock_fetch.assert_called_once()

        # Mock time moving forward past TTL
        # The TTL is calculated as 3600 - 300 (buffer) = 3300 seconds.
        # So we move monotonic time forward by 3301 seconds.
        with patch("time.monotonic", return_value=time.monotonic() + 3301):
            token2 = get_id_token("http://audience-1")
            self.assertEqual(token2, token_str_2)
            self.assertEqual(mock_fetch.call_count, 2)

    @patch("google.oauth2.id_token.fetch_id_token")
    def test_concurrent_different_audiences_do_not_block(
        self, mock_fetch
    ) -> None:
        def slow_fetch(auth_req, audience):
            time.sleep(0.2)
            return create_mock_jwt(3600)

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

        # Ensure both got tokens
        self.assertTrue(results["http://aud-1"].startswith("eyJ"))
        self.assertTrue(results["http://aud-2"].startswith("eyJ"))

        # In parallel, it should take around 0.2 seconds.
        self.assertLess(duration, 0.35)

    @patch("google.oauth2.id_token.fetch_id_token")
    def test_fallback_ttl_on_invalid_jwt(self, mock_fetch) -> None:
        # Return a completely invalid token
        mock_fetch.return_value = "invalid-token"

        token = get_id_token("http://audience-invalid")
        self.assertEqual(token, "invalid-token")

        # It should still cache it using the fallback TTL
        token2 = get_id_token("http://audience-invalid")
        self.assertEqual(token2, "invalid-token")
        mock_fetch.assert_called_once()


if __name__ == "__main__":
    unittest.main()

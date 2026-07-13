"""Unit tests for common utilities."""

import os
import unittest
from unittest.mock import patch

from backend.pipeline.common.utils import get_optimal_thread_pool_size


class UtilsTest(unittest.TestCase):
    @patch.dict(os.environ, {"MY_TEST_THREADS": "20"}, clear=True)
    def test_get_optimal_thread_pool_size_valid(self) -> None:
        """Verifies that a valid environment variable is parsed correctly."""
        self.assertEqual(get_optimal_thread_pool_size("MY_TEST_THREADS"), 20)

    @patch.dict(os.environ, {"MY_TEST_THREADS": "invalid_string"}, clear=True)
    @patch("backend.pipeline.common.utils.is_gcp_env", return_value=False)
    def test_get_optimal_thread_pool_size_invalid_fallback_local(
        self, mock_is_gcp_env
    ) -> None:
        """Verifies invalid string falls back to local default (16)."""
        self.assertEqual(get_optimal_thread_pool_size("MY_TEST_THREADS"), 16)

    @patch.dict(os.environ, {}, clear=True)
    @patch("backend.pipeline.common.utils.is_gcp_env", return_value=True)
    @patch("os.cpu_count", return_value=4)
    def test_get_optimal_thread_pool_size_unset_gcp(
        self, mock_cpu_count, mock_is_gcp_env
    ) -> None:
        """Verifies unset env in GCP derives from CPU count."""
        # Note: os.sched_getaffinity might be available on some platforms,
        # so we patch os.cpu_count to ensure the fallback branch calculates correctly.
        # Assuming os.sched_getaffinity fails, it falls back to cpu_count * 4 = 16.
        # If sched_getaffinity succeeds, it will use that. We can mock both to be safe.
        with patch("os.sched_getaffinity", side_effect=AttributeError):
            self.assertEqual(
                get_optimal_thread_pool_size("MY_TEST_THREADS", 3), 12
            )
            self.assertEqual(
                get_optimal_thread_pool_size("MY_TEST_THREADS", 4), 16
            )


if __name__ == "__main__":
    unittest.main()

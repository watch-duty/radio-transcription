"""WATCHDOG-01 D-29 parser unit tests.

Tests the cgroup memory limit + usage parsers in NormalizerRuntime,
covering cgroup v2, cgroup v1 fallback, the literal "max", and the
v1 unbounded sentinel >= 2**62.

These helpers are pure (no side effects, no asyncio) so we use
unittest.TestCase rather than IsolatedAsyncioTestCase.
"""

from __future__ import annotations

import unittest
from unittest import mock

from backend.pipeline.ingestion.normalizer_runtime import NormalizerRuntime


class TestResolveContainerMemoryLimit(unittest.TestCase):
    """D-29 unit tests for _resolve_container_memory_bytes."""

    def test_v2_max_returns_none(self) -> None:
        """Cgroup v2 'max' literal disables the watchdog (PITFALLS Pitfall 2)."""
        with mock.patch(
            "pathlib.Path.read_text",
            return_value="max\n",
        ):
            result = NormalizerRuntime._resolve_container_memory_bytes()
        self.assertIsNone(result)

    def test_v2_int_returns_value(self) -> None:
        """Cgroup v2 numeric limit is returned as int."""
        with mock.patch(
            "pathlib.Path.read_text",
            return_value="4294967296\n",
        ):
            result = NormalizerRuntime._resolve_container_memory_bytes()
        self.assertEqual(result, 4294967296)

    def test_v1_sentinel_returns_none(self) -> None:
        """Cgroup v1 sentinel (>= 2**62) means unbounded -> None."""
        with mock.patch(
            "pathlib.Path.read_text",
            side_effect=[OSError("v2 missing"), "9223372036854771712\n"],
        ):
            result = NormalizerRuntime._resolve_container_memory_bytes()
        self.assertIsNone(result)

    def test_v1_int_returns_value(self) -> None:
        """Cgroup v1 numeric limit is returned as int when v2 missing."""
        with mock.patch(
            "pathlib.Path.read_text",
            side_effect=[OSError("v2 missing"), "4294967296\n"],
        ):
            result = NormalizerRuntime._resolve_container_memory_bytes()
        self.assertEqual(result, 4294967296)

    def test_both_paths_oserror_returns_none(self) -> None:
        """Both cgroup paths unreadable -> None (watchdog disables itself)."""
        with mock.patch(
            "pathlib.Path.read_text",
            side_effect=[OSError("v2 missing"), OSError("v1 missing")],
        ):
            result = NormalizerRuntime._resolve_container_memory_bytes()
        self.assertIsNone(result)

    def test_v2_malformed_value_returns_none(self) -> None:
        """Garbage in memory.max -> None (defensive ValueError catch)."""
        with mock.patch(
            "pathlib.Path.read_text",
            return_value="not-an-integer\n",
        ):
            result = NormalizerRuntime._resolve_container_memory_bytes()
        self.assertIsNone(result)

    def test_v2_zero_limit_returns_none(self) -> None:
        """A 0 limit would cause ZeroDivisionError in the watchdog ratio."""
        with mock.patch(
            "pathlib.Path.read_text",
            return_value="0\n",
        ):
            result = NormalizerRuntime._resolve_container_memory_bytes()
        self.assertIsNone(result)

    def test_v1_malformed_value_returns_none(self) -> None:
        """Garbage in memory.limit_in_bytes -> None (defensive ValueError)."""
        with mock.patch(
            "pathlib.Path.read_text",
            side_effect=[OSError("v2 missing"), "garbage\n"],
        ):
            result = NormalizerRuntime._resolve_container_memory_bytes()
        self.assertIsNone(result)


class TestResolveContainerMemoryUsage(unittest.TestCase):
    """D-29 unit tests for _resolve_container_memory_usage_bytes."""

    def test_v2_usage_returned(self) -> None:
        """Cgroup v2 memory.current returns the parsed int."""
        with mock.patch(
            "pathlib.Path.read_text",
            return_value="3221225472\n",
        ):
            result = NormalizerRuntime._resolve_container_memory_usage_bytes()
        self.assertEqual(result, 3221225472)

    def test_v1_fallback_when_v2_missing(self) -> None:
        """When v2 read raises OSError, v1 memory.usage_in_bytes is used."""
        with mock.patch(
            "pathlib.Path.read_text",
            side_effect=[OSError("v2 missing"), "3221225472\n"],
        ):
            result = NormalizerRuntime._resolve_container_memory_usage_bytes()
        self.assertEqual(result, 3221225472)

    def test_both_paths_oserror_returns_none(self) -> None:
        """Both cgroup usage paths unreadable -> None."""
        with mock.patch(
            "pathlib.Path.read_text",
            side_effect=[OSError("v2 missing"), OSError("v1 missing")],
        ):
            result = NormalizerRuntime._resolve_container_memory_usage_bytes()
        self.assertIsNone(result)

    def test_malformed_value_returns_none(self) -> None:
        """Garbage in memory.current -> None (defensive ValueError catch)."""
        with mock.patch(
            "pathlib.Path.read_text",
            return_value="not-an-integer\n",
        ):
            result = NormalizerRuntime._resolve_container_memory_usage_bytes()
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()

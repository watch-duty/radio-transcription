"""Unit tests for cgroup memory limit and usage parsing."""

from __future__ import annotations

import unittest
from unittest import mock

from backend.pipeline.ingestion import cgroup_memory


class TestResolveContainerMemoryLimit(unittest.TestCase):
    """Parser tests for cgroup memory limits."""

    def test_v2_max_returns_none(self) -> None:
        """Cgroup v2 'max' literal disables the watchdog."""
        with mock.patch(
            "pathlib.Path.read_text",
            return_value="max\n",
        ):
            result = cgroup_memory.resolve_container_memory_limit_bytes()
        self.assertIsNone(result)

    def test_v2_int_returns_value(self) -> None:
        """Cgroup v2 numeric limit is returned as int."""
        with mock.patch(
            "pathlib.Path.read_text",
            return_value="4294967296\n",
        ):
            result = cgroup_memory.resolve_container_memory_limit_bytes()
        self.assertEqual(result, 4294967296)

    def test_v1_sentinel_returns_none(self) -> None:
        """Cgroup v1 sentinel (>= 2**62) means unbounded."""
        with mock.patch(
            "pathlib.Path.read_text",
            side_effect=[OSError("v2 missing"), "9223372036854771712\n"],
        ):
            result = cgroup_memory.resolve_container_memory_limit_bytes()
        self.assertIsNone(result)

    def test_v1_int_returns_value(self) -> None:
        """Cgroup v1 numeric limit is returned as int when v2 missing."""
        with mock.patch(
            "pathlib.Path.read_text",
            side_effect=[OSError("v2 missing"), "4294967296\n"],
        ):
            result = cgroup_memory.resolve_container_memory_limit_bytes()
        self.assertEqual(result, 4294967296)

    def test_both_paths_oserror_returns_none(self) -> None:
        """Both cgroup limit paths unreadable disables the watchdog."""
        with mock.patch(
            "pathlib.Path.read_text",
            side_effect=[OSError("v2 missing"), OSError("v1 missing")],
        ):
            result = cgroup_memory.resolve_container_memory_limit_bytes()
        self.assertIsNone(result)

    def test_v2_malformed_value_returns_none(self) -> None:
        """Garbage in memory.max returns None."""
        with mock.patch(
            "pathlib.Path.read_text",
            return_value="not-an-integer\n",
        ):
            result = cgroup_memory.resolve_container_memory_limit_bytes()
        self.assertIsNone(result)

    def test_v2_zero_limit_returns_none(self) -> None:
        """A zero limit cannot be used for ratio computation."""
        with mock.patch(
            "pathlib.Path.read_text",
            return_value="0\n",
        ):
            result = cgroup_memory.resolve_container_memory_limit_bytes()
        self.assertIsNone(result)

    def test_v1_malformed_value_returns_none(self) -> None:
        """Garbage in memory.limit_in_bytes returns None."""
        with mock.patch(
            "pathlib.Path.read_text",
            side_effect=[OSError("v2 missing"), "garbage\n"],
        ):
            result = cgroup_memory.resolve_container_memory_limit_bytes()
        self.assertIsNone(result)

    def test_v1_zero_limit_returns_none(self) -> None:
        """Cgroup v1 zero limit cannot be used for ratio computation."""
        with mock.patch(
            "pathlib.Path.read_text",
            side_effect=[OSError("v2 missing"), "0\n"],
        ):
            result = cgroup_memory.resolve_container_memory_limit_bytes()
        self.assertIsNone(result)

    def test_v1_negative_limit_returns_none(self) -> None:
        """Cgroup v1 negative value returns None."""
        with mock.patch(
            "pathlib.Path.read_text",
            side_effect=[OSError("v2 missing"), "-1\n"],
        ):
            result = cgroup_memory.resolve_container_memory_limit_bytes()
        self.assertIsNone(result)


class TestResolveContainerMemoryUsage(unittest.TestCase):
    """Parser tests for cgroup memory usage."""

    def test_v2_usage_returned(self) -> None:
        """Cgroup v2 memory.current returns the parsed int."""
        with mock.patch(
            "pathlib.Path.read_text",
            return_value="3221225472\n",
        ):
            result = cgroup_memory.resolve_container_memory_usage_bytes()
        self.assertEqual(result, 3221225472)

    def test_v1_fallback_when_v2_missing(self) -> None:
        """When v2 read raises OSError, v1 usage is used."""
        with mock.patch(
            "pathlib.Path.read_text",
            side_effect=[OSError("v2 missing"), "3221225472\n"],
        ):
            result = cgroup_memory.resolve_container_memory_usage_bytes()
        self.assertEqual(result, 3221225472)

    def test_both_paths_oserror_returns_none(self) -> None:
        """Both cgroup usage paths unreadable returns None."""
        with mock.patch(
            "pathlib.Path.read_text",
            side_effect=[OSError("v2 missing"), OSError("v1 missing")],
        ):
            result = cgroup_memory.resolve_container_memory_usage_bytes()
        self.assertIsNone(result)

    def test_malformed_value_returns_none(self) -> None:
        """Garbage in memory.current returns None."""
        with mock.patch(
            "pathlib.Path.read_text",
            return_value="not-an-integer\n",
        ):
            result = cgroup_memory.resolve_container_memory_usage_bytes()
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()

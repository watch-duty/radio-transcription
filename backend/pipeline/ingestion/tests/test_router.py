from __future__ import annotations

import asyncio
import unittest
import uuid
from unittest import mock

from backend.pipeline.ingestion.router import (
    _COLLECTOR_REGISTRY,
    route_capturer,
)
from backend.pipeline.storage.feed_store import LeasedFeed


def _make_feed(source_type: str) -> LeasedFeed:
    """Helper to create a dummy LeasedFeed for testing."""
    return LeasedFeed(
        id=uuid.uuid4(),
        name=f"test-{source_type}",
        source_type=source_type,
        last_processed_filename=None,
        fencing_token=0,
        stream_url="http://example.com/stream",
    )


class TestRouteCapturerRegistered(unittest.TestCase):
    """Tests that every registered source_type routes correctly."""

    @mock.patch("backend.pipeline.ingestion.router.importlib.import_module")
    def test_each_registered_source_type_routes_correctly(
        self, mock_import: mock.MagicMock
    ) -> None:
        """Each registry entry imports the right module and calls
        the expected function.
        """
        for source_type, (
            module_path,
            func_name,
        ) in _COLLECTOR_REGISTRY.items():
            with self.subTest(source_type=source_type):
                mock_import.reset_mock()
                sentinel = object()
                mock_module = mock.MagicMock()
                mock_fn = mock.MagicMock(return_value=sentinel)
                setattr(mock_module, func_name, mock_fn)
                mock_import.return_value = mock_module

                feed = _make_feed(source_type)
                shutdown_event = mock.MagicMock(spec=asyncio.Event)

                result = route_capturer(feed, shutdown_event)

                mock_import.assert_called_once_with(module_path)
                mock_fn.assert_called_once_with(feed, shutdown_event)
                self.assertIs(result, sentinel)


class TestRouteCapturerUnsupported(unittest.TestCase):
    """Tests that unregistered source_types are rejected."""

    def test_raises_value_error_for_unsupported_type(self) -> None:
        """Unsupported source_type raises ValueError."""
        feed = _make_feed("unknown_radio_type")
        shutdown_event = mock.MagicMock(spec=asyncio.Event)

        with self.assertRaises(ValueError) as ctx:
            route_capturer(feed, shutdown_event)

        self.assertIn("unknown_radio_type", str(ctx.exception))

    def test_error_message_contains_source_type(self) -> None:
        """The error message includes the offending source_type."""
        bad_type = "totally_bogus"
        feed = _make_feed(bad_type)
        shutdown_event = mock.MagicMock(spec=asyncio.Event)

        with self.assertRaises(ValueError) as ctx:
            route_capturer(feed, shutdown_event)

        self.assertEqual(
            str(ctx.exception),
            f"Unsupported source_type: {bad_type}",
        )


class TestCollectorRegistryIntegrity(unittest.TestCase):
    """Sanity checks on the registry itself."""

    def test_registry_is_not_empty(self) -> None:
        self.assertTrue(_COLLECTOR_REGISTRY)

    def test_all_entries_have_valid_shape(self) -> None:
        """Each value is a (module_path, func_name) 2-tuple of
        non-empty strings.
        """
        for source_type, entry in _COLLECTOR_REGISTRY.items():
            with self.subTest(source_type=source_type):
                self.assertIsInstance(entry, tuple)
                self.assertEqual(len(entry), 2)
                module_path, func_name = entry
                self.assertIsInstance(module_path, str)
                self.assertIsInstance(func_name, str)
                self.assertTrue(module_path)
                self.assertTrue(func_name)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import asyncio
import unittest
import uuid
from unittest import mock

from backend.pipeline.ingestion.router import (
    _COLLECTOR_REGISTRY,
    CollectorEntry,
    route_capturer,
    supported_source_types,
)
from backend.pipeline.storage.feed_store import LeasedFeed, SourceType


def _make_feed(source_type: SourceType) -> LeasedFeed:
    """Helper to create a dummy LeasedFeed for testing."""
    return LeasedFeed(
        id=uuid.uuid4(),
        name=f"test-{source_type}",
        source_type=source_type,
        last_processed_filename=None,
        fencing_token=0,
        source_feed_id="123",
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
            url_base,
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
                mock_fn.assert_called_once_with(
                    feed, shutdown_event, url_base=url_base
                )
                self.assertIs(result, sentinel)


class TestRouteCapturerUnsupported(unittest.TestCase):
    """Tests that source_types missing from the registry are rejected."""

    def test_raises_value_error_for_unregistered_source_type(self) -> None:
        """A valid SourceType not in the registry raises ValueError."""
        unregistered = SourceType.BCFY_CALLS
        feed = _make_feed(unregistered)
        shutdown_event = mock.MagicMock(spec=asyncio.Event)

        with mock.patch(
            "backend.pipeline.ingestion.router._COLLECTOR_REGISTRY", {}
        ):
            with self.assertRaises(ValueError) as ctx:
                route_capturer(feed, shutdown_event)

        self.assertIn(unregistered, str(ctx.exception))


class TestSupportedSourceTypes(unittest.TestCase):
    """Tests for the supported_source_types() helper."""

    def test_returns_registered_source_type_slugs(self) -> None:
        """Returns the string slugs of all registered source types."""
        result = supported_source_types()
        expected = [st.value for st in _COLLECTOR_REGISTRY]
        self.assertEqual(result, expected)

    def test_excludes_unregistered_types(self) -> None:
        """Source types not in the registry are not returned."""
        result = supported_source_types()
        self.assertNotIn(SourceType.ECHO.value, result)


class TestCollectorRegistryIntegrity(unittest.TestCase):
    """Sanity checks on the registry itself."""

    def test_registry_is_not_empty(self) -> None:
        self.assertTrue(_COLLECTOR_REGISTRY)

    def test_all_entries_have_valid_shape(self) -> None:
        """Each value is a CollectorEntry with non-empty string fields."""
        for source_type, entry in _COLLECTOR_REGISTRY.items():
            with self.subTest(source_type=source_type):
                self.assertIsInstance(entry, CollectorEntry)
                self.assertIsInstance(entry.module_path, str)
                self.assertIsInstance(entry.func_name, str)
                self.assertIsInstance(entry.url_base, str)
                self.assertTrue(entry.module_path)
                self.assertTrue(entry.func_name)
                self.assertTrue(entry.url_base)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import asyncio
import unittest
import uuid
from unittest import mock

from backend.pipeline.ingestion.router import (
    _COLLECTORS,
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
        last_bookmark_time=None,
        fencing_token=0,
        source_feed_id="123",
    )


class TestRouteCapturerRegistered(unittest.TestCase):
    """Tests that every registered source_type routes correctly."""

    def test_each_registered_source_type_routes_correctly(self) -> None:
        for source_type, (capture_fn, url_base) in _COLLECTORS.items():
            with self.subTest(source_type=source_type):
                sentinel = object()
                mock_fn = mock.MagicMock(return_value=sentinel)

                feed = _make_feed(source_type)
                shutdown_event = mock.MagicMock(spec=asyncio.Event)

                with mock.patch.dict(
                    "backend.pipeline.ingestion.router._COLLECTORS",
                    {source_type: (mock_fn, url_base)},
                ):
                    result = route_capturer(feed, shutdown_event)

                mock_fn.assert_called_once_with(feed, shutdown_event, url_base)
                self.assertIs(result, sentinel)


class TestRouteCapturerUnsupported(unittest.TestCase):
    """Tests that source_types missing from the registry are rejected."""

    def test_raises_value_error_for_unregistered_source_type(self) -> None:
        feed = _make_feed(SourceType.BCFY_CALLS)
        shutdown_event = mock.MagicMock(spec=asyncio.Event)

        with mock.patch.dict(
            "backend.pipeline.ingestion.router._COLLECTORS",
            {},
            clear=True,
        ):
            with self.assertRaises(ValueError) as ctx:
                route_capturer(feed, shutdown_event)

        self.assertIn("bcfy_calls", str(ctx.exception))


class TestSupportedSourceTypes(unittest.TestCase):
    """Tests for the supported_source_types() helper."""

    def test_returns_registered_source_type_slugs(self) -> None:
        result = supported_source_types()
        expected = [st.value for st in _COLLECTORS]
        self.assertEqual(result, expected)

    def test_excludes_unregistered_types(self) -> None:
        result = supported_source_types()
        self.assertNotIn(SourceType.ECHO.value, result)


class TestCollectorRegistryIntegrity(unittest.TestCase):
    """Sanity checks on the registry itself."""

    def test_registry_is_not_empty(self) -> None:
        self.assertTrue(_COLLECTORS)

    def test_all_entries_are_callable(self) -> None:
        for source_type, (capture_fn, url_base) in _COLLECTORS.items():
            with self.subTest(source_type=source_type):
                self.assertTrue(callable(capture_fn))
                self.assertIsInstance(url_base, str)
                self.assertTrue(url_base)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import asyncio
import unittest
import uuid
from unittest import mock

import aiohttp

from backend.pipeline.ingestion.models import CaptureResources
from backend.pipeline.ingestion.router import (
    _COLLECTORS,
    route_capturer,
    supported_source_types,
)
from backend.pipeline.storage.feed_store import LeasedFeed, SourceType


def _default_resources() -> CaptureResources:
    """No-op CaptureResources for unit tests."""
    return CaptureResources(
        http_session=mock.AsyncMock(spec=aiohttp.ClientSession),
    )


def _make_feed(source_type: SourceType) -> LeasedFeed:
    """Helper to create a dummy LeasedFeed for testing."""
    return LeasedFeed(
        id=uuid.uuid4(),
        name=f"test-{source_type}",
        source_type=source_type,
        last_processed_filename=None,
        last_bookmark_time=None,
        fencing_token=0,
        failure_count=0,
        status_reason=None,
        source_feed_id="123",
    )


class TestRouteCapturerRegistered(unittest.TestCase):
    """Tests that every registered source_type routes correctly."""

    def test_each_registered_source_type_routes_correctly(self) -> None:
        for source_type, (capture_fn, url_base) in _COLLECTORS.items():
            with self.subTest(source_type=source_type.value):
                sentinel = object()
                mock_fn = mock.MagicMock(return_value=sentinel)

                feed = _make_feed(source_type)
                shutdown_event = mock.MagicMock(spec=asyncio.Event)
                resources = _default_resources()

                with mock.patch.dict(
                    "backend.pipeline.ingestion.router._COLLECTORS",
                    {source_type: (mock_fn, url_base)},
                ):
                    result = route_capturer(feed, shutdown_event, resources)

                mock_fn.assert_called_once_with(
                    feed, shutdown_event, url_base, resources
                )
                self.assertIs(result, sentinel)


class TestRouteCapturerUnsupported(unittest.TestCase):
    """Tests that source_types missing from the registry are rejected."""

    def test_raises_value_error_for_unregistered_source_type(self) -> None:
        feed = _make_feed(SourceType.BCFY_CALLS)
        shutdown_event = mock.MagicMock(spec=asyncio.Event)
        resources = _default_resources()

        with mock.patch.dict(
            "backend.pipeline.ingestion.router._COLLECTORS",
            {},
            clear=True,
        ):
            with self.assertRaises(ValueError) as ctx:
                route_capturer(feed, shutdown_event, resources)

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
            with self.subTest(source_type=source_type.value):
                self.assertTrue(callable(capture_fn))
                self.assertIsInstance(url_base, str)
                # FIRE_NOTIFICATIONS may have empty default to fail lazily on claim due to api secret
                if source_type != SourceType.FIRE_NOTIFICATIONS:
                    self.assertTrue(url_base)


if __name__ == "__main__":
    unittest.main()

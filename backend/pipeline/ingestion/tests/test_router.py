from __future__ import annotations

import asyncio
import unittest
import uuid
from unittest import mock

import aiohttp

from backend.pipeline.ingestion import source_runtime_specs
from backend.pipeline.ingestion.models import CaptureResources
from backend.pipeline.ingestion.router import (
    _COLLECTORS,
    resolve_topic_path,
    route_capturer,
    supported_source_types,
)
from backend.pipeline.storage.feed_store import (
    LeasedFeed,
    SourceType,
)


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
        for source_type in _COLLECTORS:
            with self.subTest(source_type=source_type.value):
                sentinel = object()
                mock_fn = mock.MagicMock(return_value=sentinel)
                url_base = source_runtime_specs.url_base_for(source_type)

                feed = _make_feed(source_type)
                shutdown_event = mock.MagicMock(spec=asyncio.Event)
                resources = _default_resources()

                with mock.patch.dict(
                    "backend.pipeline.ingestion.router._COLLECTORS",
                    {source_type: mock_fn},
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


class TestResolveTopicPath(unittest.TestCase):
    """Tests that topic routing follows each source spec's topic kind."""

    @staticmethod
    def _settings() -> mock.Mock:
        return mock.Mock(
            continuous_pubsub_topic_path="projects/p/topics/continuous",
            segmented_pubsub_topic_path="projects/p/topics/segmented",
        )

    def test_continuous_sources_share_one_topic(self) -> None:
        """generic_icecast must land on the same topic as bcfy_feeds.

        Both are Icecast streams consumed by the Dataflow continuous
        segmentation pipeline; a separate topic would bypass it.
        """
        settings = self._settings()
        for source_type in (
            SourceType.BCFY_FEEDS,
            SourceType.GENERIC_ICECAST,
        ):
            with self.subTest(source_type=source_type.value):
                self.assertEqual(
                    resolve_topic_path(source_type, settings),
                    "projects/p/topics/continuous",
                )

    def test_segmented_source_uses_segmented_topic(self) -> None:
        self.assertEqual(
            resolve_topic_path(SourceType.OPENMHZ, self._settings()),
            "projects/p/topics/segmented",
        )


class TestCollectorRegistryIntegrity(unittest.TestCase):
    """Sanity checks on the registry itself."""

    def test_registry_is_not_empty(self) -> None:
        self.assertTrue(_COLLECTORS)

    def test_all_entries_are_callable(self) -> None:
        for source_type, capture_fn in _COLLECTORS.items():
            with self.subTest(source_type=source_type.value):
                self.assertTrue(callable(capture_fn))
                url_base = source_runtime_specs.url_base_for(source_type)
                self.assertIsInstance(url_base, str)
                # FIRE_NOTIFICATIONS may have empty default to fail lazily on claim due to api secret
                # GENERIC_ICECAST has no base by design: source_feed_id holds
                # the full stream URL, so there is nothing to prepend.
                if source_type not in (
                    SourceType.FIRE_NOTIFICATIONS,
                    SourceType.GENERIC_ICECAST,
                ):
                    self.assertTrue(url_base)


if __name__ == "__main__":
    unittest.main()

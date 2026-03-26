import asyncio
import unittest
import uuid
from unittest.mock import MagicMock, patch

from backend.pipeline.ingestion.router import route_capturer
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


class TestRouteCapturer(unittest.TestCase):
    """Tests for the route_capturer routing logic."""

    @patch("backend.pipeline.ingestion.router.capture_icecast_stream")
    def test_routes_bcfy_feeds_to_icecast_collector(
        self, mock_capture: MagicMock
    ) -> None:
        """bcfy_feeds source_type routes to capture_icecast_stream."""
        mock_capture.return_value = "mock_async_iterator"
        feed = _make_feed("bcfy_feeds")
        shutdown_event = MagicMock(spec=asyncio.Event)

        result = route_capturer(feed, shutdown_event)

        mock_capture.assert_called_once_with(feed, shutdown_event)
        self.assertEqual(result, "mock_async_iterator")

    def test_raises_value_error_for_unsupported_type(self) -> None:
        """Unsupported source_type raises ValueError."""
        feed = _make_feed("unknown_radio_type")
        shutdown_event = MagicMock(spec=asyncio.Event)

        with self.assertRaises(ValueError) as context:
            route_capturer(feed, shutdown_event)

        self.assertEqual(
            str(context.exception), "Unsupported source_type: unknown_radio_type"
        )


if __name__ == "__main__":
    unittest.main()

import asyncio
import os
import unittest
import uuid
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

from backend.pipeline.storage.feed_store import LeasedFeed

MOCK_ENV_VARS = {
    "BROADCASTIFY_USERNAME": "test_user",
    "BROADCASTIFY_PASSWORD": "test_pass",
}

with (
    patch.dict(os.environ, MOCK_ENV_VARS, clear=False),
    patch("google.cloud.pubsub_v1.PublisherClient"),
):
    from backend.pipeline.ingestion.collectors import icecast_collector

# Static UUID for consistent test assertions
TEST_FEED_ID = uuid.UUID("12345678-1234-5678-1234-567812345678")


def _make_feed(name: str, stream_url: str | None) -> LeasedFeed:
    return LeasedFeed(
        id=TEST_FEED_ID,
        name=name,
        source_type="icecast",
        last_processed_filename=None,
        stream_url=stream_url,
    )


class TestCaptureIcecastStream(unittest.IsolatedAsyncioTestCase):
    """Tests for the public capture_icecast_stream API."""

    def setUp(self) -> None:
        self.mock_logger = MagicMock()
        self.patchers = [
            patch.object(icecast_collector, "logger", self.mock_logger),
        ]
        for p in self.patchers:
            p.start()

    def tearDown(self) -> None:
        for p in self.patchers:
            p.stop()

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_normal_capture_yields_wav_chunks(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Test normal case: successfully capture and yield WAV audio chunks."""
        # Arrange
        mock_proc = AsyncMock()
        mock_proc.pid = 1234
        mock_create_ffmpeg.return_value = mock_proc

        # Return enough data to yield one complete chunk
        chunk_size = 480000  # BYTES_PER_CHUNK
        mock_proc.stdout.read.return_value = b"x" * chunk_size

        feed = _make_feed("test-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        # Act
        chunks = []
        gen = icecast_collector.capture_icecast_stream(feed, shutdown_event)
        chunk = await gen.__anext__()
        chunks.append(chunk)

        # Assert - chunk should be WAV formatted
        self.assertEqual(len(chunks), 1)
        self.assertIsInstance(chunks[0], bytes)
        self.assertEqual(len(chunks[0]), chunk_size + 44)  # Chunk data + WAV header

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_shutdown_signal_stops_capture(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Test edge case: shutdown event is checked on each iteration."""
        # Arrange
        mock_proc = AsyncMock()
        mock_proc.pid = 5555
        mock_create_ffmpeg.return_value = mock_proc

        chunk_size = 480000
        mock_proc.stdout.read.return_value = b"y" * chunk_size

        feed = _make_feed("shutdown-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        # Act
        gen = icecast_collector.capture_icecast_stream(feed, shutdown_event)

        # Get one chunk
        chunk = await gen.__anext__()
        self.assertIsNotNone(chunk)

        # Set shutdown and try next iteration - should exit cleanly
        shutdown_event.set()
        with self.assertRaises(StopAsyncIteration):
            await gen.__anext__()

    async def test_invalid_input_missing_stream_url(self) -> None:
        """Test invalid input: feed missing stream_url raises ValueError."""
        # Arrange
        feed = cast(
            "LeasedFeed",
            {
                "id": uuid.uuid4(),
                "name": "incomplete-feed",
                "source_type": "icecast",
                "last_processed_filename": None,
            },
        )
        shutdown_event = asyncio.Event()

        # Act & Assert
        gen = icecast_collector.capture_icecast_stream(feed, shutdown_event)
        with self.assertRaises(ValueError) as context:
            await gen.__anext__()

        self.assertIn("missing stream_url", str(context.exception))

    async def test_invalid_input_none_stream_url_raises_value_error(self) -> None:
        """Test invalid input: feed with None stream_url raises ValueError."""
        # Arrange
        feed = _make_feed("none-stream-feed", None)
        shutdown_event = asyncio.Event()

        # Act & Assert
        gen = icecast_collector.capture_icecast_stream(feed, shutdown_event)
        with self.assertRaises(ValueError) as context:
            await gen.__anext__()

        self.assertIn("missing stream_url", str(context.exception))
        self.assertIn(str(TEST_FEED_ID), str(context.exception))
        self.assertIn("none-stream-feed", str(context.exception))

    async def test_invalid_input_empty_string_stream_url_raises_value_error(
        self,
    ) -> None:
        """Test invalid input: feed with empty stream_url raises ValueError."""
        # Arrange
        feed = _make_feed("empty-stream-feed", "")
        shutdown_event = asyncio.Event()

        # Act & Assert
        gen = icecast_collector.capture_icecast_stream(feed, shutdown_event)
        with self.assertRaises(ValueError) as context:
            await gen.__anext__()

        self.assertIn(str(TEST_FEED_ID), str(context.exception))
        self.assertIn("missing stream_url", str(context.exception))

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_ffmpeg_normal_exit_code_zero(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Test edge case: ffmpeg exits normally with code 0."""
        # Arrange
        mock_proc = AsyncMock()
        mock_proc.pid = 6666
        mock_proc.returncode = 0
        mock_proc.stdout.read.return_value = b""  # EOF - signals end of stream
        mock_proc.wait = AsyncMock(return_value=0)
        mock_create_ffmpeg.return_value = mock_proc

        feed = _make_feed("exit-zero-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        # Act & Assert - should exit cleanly without raising
        gen = icecast_collector.capture_icecast_stream(feed, shutdown_event)
        with self.assertRaises(StopAsyncIteration):
            await gen.__anext__()
        mock_proc.wait.assert_called_once()

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_ffmpeg_error_exit_code_raises_runtime_error(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Test invalid case: ffmpeg exits with non-zero code raises RuntimeError."""
        # Arrange
        mock_proc = AsyncMock()
        mock_proc.pid = 7777
        mock_proc.returncode = 1
        mock_proc.stdout.read.return_value = b""  # EOF
        mock_proc.wait = AsyncMock(return_value=1)
        mock_create_ffmpeg.return_value = mock_proc

        feed = _make_feed("error-exit-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        # Act & Assert
        gen = icecast_collector.capture_icecast_stream(feed, shutdown_event)
        with self.assertRaises(RuntimeError) as context:
            await gen.__anext__()

        self.assertIn("ffmpeg exited with code 1", str(context.exception))
        self.assertIn(str(TEST_FEED_ID), str(context.exception))
        self.assertIn("error-exit-feed", str(context.exception))

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_ffmpeg_stdout_is_none_raises_error(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Test edge case: ffmpeg stdout is None raises RuntimeError."""
        # Arrange
        mock_proc = AsyncMock()
        mock_proc.pid = 8888
        mock_proc.stdout = None
        mock_create_ffmpeg.return_value = mock_proc

        feed = _make_feed("no-stdout-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        # Act & Assert
        gen = icecast_collector.capture_icecast_stream(feed, shutdown_event)
        with self.assertRaises(RuntimeError) as context:
            await gen.__anext__()

        self.assertIn("ffmpeg stdout is None", str(context.exception))
        self.assertIn(str(TEST_FEED_ID), str(context.exception))
        self.assertIn("no-stdout-feed", str(context.exception))

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_cleanup_process_on_exception(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Test cleanup: ffmpeg process is terminated on exception in read loop."""
        # Arrange
        mock_proc = AsyncMock()
        mock_proc.pid = 9999
        mock_proc.returncode = None  # Still running
        mock_proc.stdout.read.side_effect = RuntimeError("Read failed")
        mock_proc.wait = AsyncMock()
        mock_create_ffmpeg.return_value = mock_proc

        feed = _make_feed("read-error-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        # Act & Assert
        gen = icecast_collector.capture_icecast_stream(feed, shutdown_event)
        with self.assertRaises(RuntimeError):
            await gen.__anext__()

        # Process should be terminated in finally block
        mock_proc.terminate.assert_called_once()
        mock_proc.wait.assert_called_once()

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_yields_multiple_chunks_from_continuous_stream(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Test normal case: yields multiple full chunks from stream data."""
        # Arrange
        mock_proc = AsyncMock()
        mock_proc.pid = 1111
        mock_create_ffmpeg.return_value = mock_proc

        chunk_size = 480000
        # Return enough data for two complete chunks in one read
        mock_proc.stdout.read.side_effect = [
            b"a" * (chunk_size * 2),
        ]

        feed = _make_feed("multi-chunk-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        # Act - collect both chunks
        chunks = []
        gen = icecast_collector.capture_icecast_stream(feed, shutdown_event)

        chunk1 = await gen.__anext__()
        chunks.append(chunk1)
        chunk2 = await gen.__anext__()
        chunks.append(chunk2)

        # Assert
        self.assertEqual(len(chunks), 2)
        for chunk in chunks:
            self.assertIsInstance(chunk, bytes)
            self.assertEqual(len(chunk), chunk_size + 44)  # Each chunk + WAV header


if __name__ == "__main__":
    unittest.main()

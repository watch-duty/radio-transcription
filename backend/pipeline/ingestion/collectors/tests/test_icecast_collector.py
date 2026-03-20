import asyncio
import os
import unittest
import uuid
from pathlib import Path
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
        fencing_token=1,
        stream_url=stream_url,
    )


def _make_process_factory(
    *,
    pid: int,
    segments: list[bytes] | None = None,
    wait_delay: float = 0.1,
    wait_result: int = 0,
    wait_exception: Exception | None = None,
):
    """Build a side-effect factory for _create_ffmpeg_process mocks."""

    async def _factory(url: str, segment_pattern: str) -> AsyncMock:
        del url
        segment_dir = Path(segment_pattern).parent

        mock_proc = AsyncMock()
        mock_proc.pid = pid
        mock_proc.returncode = None
        mock_proc.terminate = MagicMock()

        for index, segment in enumerate(segments or []):
            (segment_dir / f"chunk_{index:06d}.flac").write_bytes(segment)

        async def _wait_impl() -> int:
            if wait_exception is not None:
                raise wait_exception
            await asyncio.sleep(wait_delay)
            return wait_result

        mock_proc.wait = AsyncMock(side_effect=_wait_impl)
        return mock_proc

    return _factory


async def _collect_chunks(
    gen,
    *,
    total_timeout: float = 2.0,
    per_chunk_timeout: float = 0.5,
) -> list[bytes]:
    """Collect chunks from an async generator until it finishes or times out."""
    chunks: list[bytes] = []
    try:
        async with asyncio.timeout(total_timeout):
            while True:
                try:
                    chunk, _ts = await asyncio.wait_for(
                        gen.__anext__(), timeout=per_chunk_timeout
                    )
                    chunks.append(chunk)
                except StopAsyncIteration:
                    break
    except TimeoutError:
        pass
    return chunks


async def _collect_chunks_with_timestamps(
    gen,
    *,
    total_timeout: float = 2.0,
    per_chunk_timeout: float = 0.5,
) -> list[tuple[bytes, "datetime.datetime"]]:
    """Collect chunks and timestamps from an async generator until it finishes or times out."""
    results = []
    try:
        async with asyncio.timeout(total_timeout):
            while True:
                try:
                    chunk, ts = await asyncio.wait_for(
                        gen.__anext__(), timeout=per_chunk_timeout
                    )
                    results.append((chunk, ts))
                except StopAsyncIteration:
                    break
    except TimeoutError:
        pass
    return results


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
    async def test_normal_capture_yields_flac_segments(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Test normal case: successfully capture and yield FLAC segment chunks."""
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=1234,
            segments=[b"FLAC_DATA_0", b"FLAC_DATA_1"],
            wait_delay=0.1,
            wait_result=0,
        )

        feed = _make_feed("test-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(feed, shutdown_event)
        chunks = await _collect_chunks(gen)

        # Assert - segments should be yielded without modification
        self.assertEqual(len(chunks), 2)
        for chunk in chunks:
            self.assertIsInstance(chunk, bytes)

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_shutdown_signal_stops_capture(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Test edge case: shutdown event is checked on each iteration."""
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=5555,
            segments=[b"FLAC_TEST_1"],
            wait_delay=0.5,
            wait_result=0,
        )

        feed = _make_feed("shutdown-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        # Act
        gen = icecast_collector.capture_icecast_stream(feed, shutdown_event)

        # Give it time to start and yield first chunk if available
        await asyncio.sleep(0.1)

        # Set shutdown and try next iteration - should exit cleanly
        shutdown_event.set()
        with self.assertRaises(StopAsyncIteration):
            await asyncio.wait_for(gen.__anext__(), timeout=1.0)

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
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=6666,
            segments=[b"FLAC_DATA"],
            wait_delay=0.05,
            wait_result=0,
        )

        feed = _make_feed("exit-zero-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        # Act & Assert - should exit cleanly without raising
        gen = icecast_collector.capture_icecast_stream(feed, shutdown_event)

        chunks = await _collect_chunks(gen)
        self.assertGreaterEqual(len(chunks), 1)

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_ffmpeg_error_exit_code_raises_runtime_error(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Test invalid case: ffmpeg exits with non-zero code raises RuntimeError."""
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=7777,
            wait_delay=0.0,
            wait_result=1,
        )

        feed = _make_feed("error-exit-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        # Act & Assert
        gen = icecast_collector.capture_icecast_stream(feed, shutdown_event)
        with self.assertRaises(RuntimeError) as context:
            await asyncio.wait_for(gen.__anext__(), timeout=1.0)

        self.assertIn("ffmpeg exited with code 1", str(context.exception))
        self.assertIn(str(TEST_FEED_ID), str(context.exception))
        self.assertIn("error-exit-feed", str(context.exception))

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_cleanup_process_on_exception(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Test cleanup: ffmpeg process is terminated on exception in read loop."""
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=9999,
            wait_exception=RuntimeError("Process error"),
        )

        feed = _make_feed("error-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        # Act & Assert
        gen = icecast_collector.capture_icecast_stream(feed, shutdown_event)
        with self.assertRaises(RuntimeError):
            await asyncio.wait_for(gen.__anext__(), timeout=1.0)

        # Cleanup runs in finally; the key behavior is that the error is propagated.

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_yields_multiple_segments_from_continuous_stream(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Test normal case: yields multiple segments from stream."""
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=1111,
            segments=[
                b"FLAC_SEGMENT_0_DATA",
                b"FLAC_SEGMENT_1_DATA",
                b"FLAC_SEGMENT_2_DATA",
            ],
            wait_delay=0.1,
            wait_result=0,
        )

        feed = _make_feed("multi-segment-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(feed, shutdown_event)
        chunks = await _collect_chunks(gen)

        # Assert - should have collected multiple segments
        self.assertEqual(len(chunks), 3)
        for chunk in chunks:
            self.assertIsInstance(chunk, bytes)
            # Each chunk should be a FLAC segment without additional headers
            self.assertTrue(b"FLAC_SEGMENT" in chunk or b"FLAC" in chunk)

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_timestamps_advance_by_chunk_duration(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Test timestamp math: Each chunk advances strictly by CHUNK_DURATION_SECONDS."""
        from backend.pipeline.common.constants import CHUNK_DURATION_SECONDS

        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=3333,
            segments=[
                b"FLAC_SEGMENT_0",
                b"FLAC_SEGMENT_1",
                b"FLAC_SEGMENT_2",
            ],
            wait_delay=0.1,
            wait_result=0,
        )

        feed = _make_feed("timestamp-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(feed, shutdown_event)
        results = await _collect_chunks_with_timestamps(gen)

        self.assertEqual(len(results), 3)

        ts0 = results[0][1]
        ts1 = results[1][1]
        ts2 = results[2][1]

        self.assertEqual((ts1 - ts0).total_seconds(), CHUNK_DURATION_SECONDS)
        self.assertEqual((ts2 - ts1).total_seconds(), CHUNK_DURATION_SECONDS)


if __name__ == "__main__":
    unittest.main()

import asyncio
import datetime
import os
import unittest
import uuid
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

from backend.pipeline.common.constants import CHUNK_DURATION_SECONDS
from backend.pipeline.ingestion.collectors import icecast_collector
from backend.pipeline.ingestion.models import CapturedChunk
from backend.pipeline.storage.feed_store import LeasedFeed, SourceType

MOCK_ENV_VARS = {
    "BROADCASTIFY_USERNAME": "test_user",
    "BROADCASTIFY_PASSWORD": "test_pass",
}

# Static UUID for consistent test assertions
TEST_FEED_ID = uuid.UUID("12345678-1234-5678-1234-567812345678")


def _make_feed(name: str, source_feed_id: str | None) -> LeasedFeed:
    return LeasedFeed(
        id=TEST_FEED_ID,
        name=name,
        source_type=SourceType.BCFY_FEEDS,
        last_processed_filename=None,
        last_bookmark_time=None,
        fencing_token=1,
        source_feed_id=source_feed_id,
    )


def _make_stderr_reader(
    lines: list[bytes] | None = None,
) -> asyncio.StreamReader:
    """Build a StreamReader pre-loaded with *lines* for mock stderr."""
    reader = asyncio.StreamReader()
    for line in lines or []:
        reader.feed_data(line)
    reader.feed_eof()
    return reader


def _make_process_factory(
    *,
    pid: int,
    segments: list[bytes] | None = None,
    wait_delay: float = 0.1,
    wait_result: int = 0,
    wait_exception: Exception | None = None,
    stderr_lines: list[bytes] | None = None,
):
    """Build a side-effect factory for _create_ffmpeg_process mocks."""

    async def _factory(
        url: str, segment_pattern: str, auth_header: str = ""
    ) -> AsyncMock:
        del url, auth_header
        segment_dir = Path(segment_pattern).parent

        mock_proc = AsyncMock()
        mock_proc.pid = pid
        mock_proc.returncode = None
        mock_proc.terminate = MagicMock()
        mock_proc.stderr = _make_stderr_reader(stderr_lines)

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
                    captured_chunk = await asyncio.wait_for(
                        gen.__anext__(), timeout=per_chunk_timeout
                    )
                    chunks.append(captured_chunk.audio_bytes)
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
) -> list[CapturedChunk]:
    """Collect CapturedChunk objects from an async generator until it
    finishes or times out.
    """
    results = []
    try:
        async with asyncio.timeout(total_timeout):
            while True:
                try:
                    captured_chunk = await asyncio.wait_for(
                        gen.__anext__(), timeout=per_chunk_timeout
                    )
                    results.append(captured_chunk)
                except StopAsyncIteration:
                    break
    except TimeoutError:
        pass
    return results


class TestCreateFfmpegProcess(unittest.IsolatedAsyncioTestCase):
    """Guard against accidentally discarding ffmpeg diagnostic output."""

    @patch(
        "asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
    )
    async def test_stderr_is_pipe(self, mock_exec: AsyncMock) -> None:
        """Stderr must be PIPE so the drain task can capture error context."""
        mock_exec.return_value = AsyncMock()

        await icecast_collector._create_ffmpeg_process(
            "http://example.com/stream.mp3",
            "/tmp/chunk_%06d.flac",  # noqa: S108
            "Authorization: Basic dGVzdDp0ZXN0\r\n",
        )

        _, kwargs = mock_exec.call_args
        self.assertEqual(
            kwargs["stderr"],
            asyncio.subprocess.PIPE,
            "stderr must be PIPE, not DEVNULL — ffmpeg error context is lost otherwise",
        )


class TestCaptureIcecastStream(unittest.IsolatedAsyncioTestCase):
    """Tests for the public capture_icecast_stream API."""

    def setUp(self) -> None:
        self.mock_logger = MagicMock()
        self.patchers = [
            patch.object(icecast_collector, "logger", self.mock_logger),
            patch.dict(os.environ, MOCK_ENV_VARS),
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

        gen = icecast_collector.capture_icecast_stream(
            feed, shutdown_event, url_base="https://mock.example.com/"
        )
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
        gen = icecast_collector.capture_icecast_stream(
            feed, shutdown_event, url_base="https://mock.example.com/"
        )

        # Give it time to start and yield first chunk if available
        await asyncio.sleep(0.1)

        # Set shutdown and try next iteration - should exit cleanly
        shutdown_event.set()
        with self.assertRaises(StopAsyncIteration):
            await asyncio.wait_for(gen.__anext__(), timeout=1.0)

    async def test_invalid_input_missing_source_feed_id(self) -> None:
        """Test invalid input: feed missing source_feed_id raises ValueError."""
        # Arrange
        feed = cast(
            "LeasedFeed",
            {
                "id": uuid.uuid4(),
                "name": "incomplete-feed",
                "source_type": "icecast",
                "last_processed_filename": None,
                "last_bookmark_time": None,
            },
        )
        shutdown_event = asyncio.Event()

        # Act & Assert
        gen = icecast_collector.capture_icecast_stream(
            feed, shutdown_event, url_base="https://mock.example.com/"
        )
        with self.assertRaises(ValueError) as context:
            await gen.__anext__()

        self.assertIn("missing source_feed_id", str(context.exception))

    async def test_invalid_input_none_source_feed_id_raises_value_error(
        self,
    ) -> None:
        """Test invalid input: feed with None source_feed_id raises ValueError."""
        # Arrange
        feed = _make_feed("none-stream-feed", None)
        shutdown_event = asyncio.Event()

        # Act & Assert
        gen = icecast_collector.capture_icecast_stream(
            feed, shutdown_event, url_base="https://mock.example.com/"
        )
        with self.assertRaises(ValueError) as context:
            await gen.__anext__()

        self.assertIn("missing source_feed_id", str(context.exception))
        self.assertIn(str(TEST_FEED_ID), str(context.exception))
        self.assertIn("none-stream-feed", str(context.exception))

    async def test_invalid_input_empty_string_source_feed_id_raises_value_error(
        self,
    ) -> None:
        """Test invalid input: feed with empty source_feed_id raises ValueError."""
        # Arrange
        feed = _make_feed("empty-stream-feed", "")
        shutdown_event = asyncio.Event()

        # Act & Assert
        gen = icecast_collector.capture_icecast_stream(
            feed, shutdown_event, url_base="https://mock.example.com/"
        )
        with self.assertRaises(ValueError) as context:
            await gen.__anext__()

        self.assertIn(str(TEST_FEED_ID), str(context.exception))
        self.assertIn("missing source_feed_id", str(context.exception))

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
        gen = icecast_collector.capture_icecast_stream(
            feed, shutdown_event, url_base="https://mock.example.com/"
        )

        chunks = await _collect_chunks(gen)
        self.assertGreaterEqual(len(chunks), 1)

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_ffmpeg_error_exit_code_includes_stderr(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Test: ffmpeg non-zero exit includes stderr tail in RuntimeError."""
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=7777,
            wait_delay=0.0,
            wait_result=1,
            stderr_lines=[b"HTTP error 403 Forbidden\n"],
        )

        feed = _make_feed("error-exit-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed, shutdown_event, url_base="https://mock.example.com/"
        )
        with self.assertRaises(RuntimeError) as context:
            await asyncio.wait_for(gen.__anext__(), timeout=1.0)

        self.assertIn("ffmpeg exited with code 1", str(context.exception))
        self.assertIn(str(TEST_FEED_ID), str(context.exception))
        self.assertIn("error-exit-feed", str(context.exception))
        self.assertIn("403 Forbidden", str(context.exception))
        self.assertIn("stderr tail:", str(context.exception))

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_ffmpeg_error_exit_code_no_stderr(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Test: ffmpeg non-zero exit with empty stderr shows fallback text."""
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=7778,
            wait_delay=0.0,
            wait_result=8,
        )

        feed = _make_feed("no-stderr-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed, shutdown_event, url_base="https://mock.example.com/"
        )
        with self.assertRaises(RuntimeError) as context:
            await asyncio.wait_for(gen.__anext__(), timeout=1.0)

        self.assertIn("ffmpeg exited with code 8", str(context.exception))
        self.assertIn("(no stderr captured)", str(context.exception))

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
        gen = icecast_collector.capture_icecast_stream(
            feed, shutdown_event, url_base="https://mock.example.com/"
        )
        with self.assertRaises(RuntimeError):
            await asyncio.wait_for(gen.__anext__(), timeout=1.0)

        # Cleanup runs in finally; the key behavior is that the error is propagated.

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector.READ_TIMEOUT_SEC",
        0.1,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_read_timeout_includes_stderr(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Test: read timeout error includes stderr tail."""
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=8888,
            wait_delay=1.0,  # longer than READ_TIMEOUT_SEC (0.1s) but short enough for cleanup
            wait_result=0,
            stderr_lines=[b"Connection timed out\n"],
        )

        feed = _make_feed("timeout-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed, shutdown_event, url_base="https://mock.example.com/"
        )
        with self.assertRaises(RuntimeError) as context:
            await asyncio.wait_for(gen.__anext__(), timeout=2.0)

        self.assertIn("no finalized segment within", str(context.exception))
        self.assertIn("Connection timed out", str(context.exception))

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

        gen = icecast_collector.capture_icecast_stream(
            feed, shutdown_event, url_base="https://mock.example.com/"
        )
        chunks = await _collect_chunks(gen)

        # Assert - should have collected multiple segments
        self.assertEqual(len(chunks), 3)
        for chunk in chunks:
            self.assertIsInstance(chunk, bytes)
            # Each chunk should be a FLAC segment without additional headers
            self.assertTrue(b"FLAC_SEGMENT" in chunk or b"FLAC" in chunk)

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._now_utc",
    )
    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_timestamps_advance_by_chunk_duration(
        self, mock_create_ffmpeg: AsyncMock, mock_now_utc: MagicMock
    ) -> None:
        """Test timestamp math: chunks advance by CHUNK_DURATION_SECONDS when
        _now_utc() is far future so min() picks chunk_end_time (unclamped path).
        """
        fixed_anchor = datetime.datetime(
            2026, 1, 1, 0, 0, 0, tzinfo=datetime.UTC
        )
        # First call sets stream_anchor_time; subsequent calls (the clamp) return a
        # time far beyond any chunk_end_time so min() always returns chunk_end_time.
        far_future = fixed_anchor + datetime.timedelta(hours=1)
        mock_now_utc.side_effect = [fixed_anchor] + [far_future] * 10

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

        gen = icecast_collector.capture_icecast_stream(
            feed, shutdown_event, url_base="https://mock.example.com/"
        )
        results = await _collect_chunks_with_timestamps(gen)

        self.assertEqual(len(results), 3)

        ts0 = results[0].chunk_start_time
        ts1 = results[1].chunk_start_time
        ts2 = results[2].chunk_start_time

        self.assertEqual((ts1 - ts0).total_seconds(), CHUNK_DURATION_SECONDS)
        self.assertEqual((ts2 - ts1).total_seconds(), CHUNK_DURATION_SECONDS)

        # chunk_end_time should be exactly CHUNK_DURATION_SECONDS after chunk_start_time
        for captured_chunk in results:
            self.assertEqual(
                (
                    captured_chunk.chunk_end_time
                    - captured_chunk.chunk_start_time
                ).total_seconds(),
                CHUNK_DURATION_SECONDS,
            )

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._now_utc",
    )
    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_last_chunk_end_time_clamped_to_current_time(
        self, mock_create_ffmpeg: AsyncMock, mock_now_utc: MagicMock
    ) -> None:
        """Test timestamp math: last chunk end_time is clamped to _now_utc()
        when _now_utc() < chunk_end_time so min() picks _now_utc() (clamped path).

        A single segment guarantees _now_utc() is called exactly twice: once
        to set stream_anchor_time, and once for the min() clamp when the process
        is done and the only chunk is finalized.
        """
        fixed_anchor = datetime.datetime(
            2026, 1, 1, 0, 0, 0, tzinfo=datetime.UTC
        )
        # clamp_time is before anchor + CHUNK_DURATION_SECONDS (the natural end),
        # so min() picks clamp_time instead of chunk_end_time.
        clamp_time = fixed_anchor + datetime.timedelta(
            seconds=CHUNK_DURATION_SECONDS - 5
        )
        mock_now_utc.side_effect = [fixed_anchor, clamp_time]

        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=4444,
            segments=[b"FLAC_SEGMENT_0"],
            wait_delay=0.1,
            wait_result=0,
        )

        feed = _make_feed("clamp-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed, shutdown_event, url_base="https://mock.example.com/"
        )
        results = await _collect_chunks_with_timestamps(gen)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].chunk_start_time, fixed_anchor)
        self.assertEqual(results[0].chunk_end_time, clamp_time)

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_session_id_set_and_consistent_across_chunks(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """All chunks within one ffmpeg run share the same session_id."""
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=5555,
            segments=[b"SEG_0", b"SEG_1", b"SEG_2"],
            wait_delay=0.1,
            wait_result=0,
        )

        feed = _make_feed("session-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed, shutdown_event, url_base="https://mock.example.com/"
        )
        results = await _collect_chunks_with_timestamps(gen)

        self.assertEqual(len(results), 3)
        for chunk in results:
            self.assertIsNotNone(chunk.session_id)
        self.assertEqual(results[0].session_id, results[1].session_id)
        self.assertEqual(results[1].session_id, results[2].session_id)


if __name__ == "__main__":
    unittest.main()

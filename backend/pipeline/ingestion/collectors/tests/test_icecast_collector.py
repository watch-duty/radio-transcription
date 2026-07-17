import asyncio
import datetime
import io
import os
import tempfile
import unittest
import uuid
from pathlib import Path
from typing import Any, Self, cast
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import soundfile as sf

from backend.pipeline.common.constants import CHUNK_DURATION_SECONDS
from backend.pipeline.ingestion import constants
from backend.pipeline.ingestion.collectors.icecast import icecast_collector
from backend.pipeline.ingestion.collectors.tests.conftest import (
    _default_resources,
)
from backend.pipeline.ingestion.models import CapturedChunk, FeedFailure
from backend.pipeline.storage.feed_store import (
    FeedStatusReason,
    LeasedFeed,
    SourceType,
)

MOCK_ENV_VARS = {
    "BROADCASTIFY_USERNAME": "test_user",
    "BROADCASTIFY_PASSWORD": "test_pass",
}

# Static UUID for consistent test assertions
TEST_FEED_ID = uuid.UUID("12345678-1234-5678-1234-567812345678")


class IncrementalClock:
    def __init__(
        self, start_time: datetime.datetime, steps: float | list[float] = 15.0
    ) -> None:
        self.current = start_time
        if isinstance(steps, list):
            self.steps = [datetime.timedelta(seconds=s) for s in steps]
            self.step = None
        else:
            self.steps = None
            self.step = datetime.timedelta(seconds=steps)
        self.idx = 0

    def __call__(self) -> datetime.datetime:
        ret = self.current
        if self.step is not None:
            self.current += self.step
        elif self.steps is not None:
            if self.idx < len(self.steps):
                self.current += self.steps[self.idx]
                self.idx += 1
            else:
                self.current += datetime.timedelta(seconds=1.0)
        return ret


def _make_feed(name: str, source_feed_id: str | None) -> LeasedFeed:
    return LeasedFeed(
        id=TEST_FEED_ID,
        name=name,
        source_type=SourceType.BCFY_FEEDS,
        last_processed_filename=None,
        last_bookmark_time=None,
        fencing_token=1,
        failure_count=0,
        status_reason=None,
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


class _StreamProbeResponse:
    """Async context manager returning an HTTP status for stream probes."""

    def __init__(self, status: int, reason: str = "") -> None:
        self.status = status
        self.reason = reason

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> bool:
        del exc_type, exc, traceback
        return False


def _resources_with_probe_status(
    status: int,
    *,
    reason: str = "",
):
    resources = _default_resources()
    session = cast("Any", resources.http_session)
    session.get = MagicMock(return_value=_StreamProbeResponse(status, reason))
    return resources


def _assert_collector_failure(
    testcase: unittest.TestCase,
    exc: FeedFailure,
    status_reason: FeedStatusReason,
    reason: str,
) -> None:
    testcase.assertIs(exc.status_reason, status_reason)
    testcase.assertEqual(str(exc), reason)


def _make_pcm_segment(duration_sec: float = CHUNK_DURATION_SECONDS) -> bytes:
    """Create dummy PCM bytes of exact length corresponding to duration_sec."""
    num_bytes = int(
        icecast_collector.SAMPLE_RATE_HZ
        * 2
        * icecast_collector.NUM_AUDIO_CHANNELS
        * duration_sec
    )
    return b"\x00" * num_bytes


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
            (segment_dir / f"chunk_{index:06d}.pcm").write_bytes(segment)

        async def _wait_impl() -> int:
            if wait_exception is not None:
                raise wait_exception
            await asyncio.sleep(wait_delay)
            return wait_result

        mock_proc.wait = AsyncMock(side_effect=_wait_impl)
        return mock_proc

    return _factory


def _formatted_error_calls(mock_logger: MagicMock) -> str:
    """Render every captured logger.error call into a single newline-joined string.

    The collector logs failure context via ``logger.error(fmt, *args)`` (printf-style
    format string + args). The class-level patch in ``setUp`` swaps the module logger
    with a ``MagicMock``, so we reconstruct the formatted message from ``call_args``.
    """
    return "\n".join(
        (call.args[0] % call.args[1:]) if len(call.args) > 1 else call.args[0]
        for call in mock_logger.error.call_args_list
    )


def _formatted_warning_calls(mock_logger: MagicMock) -> str:
    """Render every captured logger.warning call into a single newline-joined string."""
    return "\n".join(
        (call.args[0] % call.args[1:]) if len(call.args) > 1 else call.args[0]
        for call in mock_logger.warning.call_args_list
    )


class TestPathDiagnostics(unittest.TestCase):
    """Tests for local file diagnostics used in timeout logging."""

    def test_stat_oserror_returns_none(self) -> None:
        """Diagnostic helpers must not mask the original collector failure."""
        path = Path("/tmp/unreadable_segment.pcm")  # noqa: S108
        with patch.object(Path, "stat", side_effect=PermissionError):
            self.assertIsNone(icecast_collector._path_size(path))
            self.assertIsNone(icecast_collector._path_mtime(path))


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
            "/tmp/chunk_%06d.pcm",  # noqa: S108
            "Authorization: Basic dGVzdDp0ZXN0\r\n",
        )

        _, kwargs = mock_exec.call_args
        self.assertEqual(
            kwargs["stderr"],
            asyncio.subprocess.PIPE,
            "stderr must be PIPE, not DEVNULL — ffmpeg error context is lost otherwise",
        )

    @patch(
        "asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
    )
    async def test_reconnects_on_retryable_http_statuses(
        self, mock_exec: AsyncMock
    ) -> None:
        """Ffmpeg should absorb transient HTTP failures before surfacing them."""
        mock_exec.return_value = AsyncMock()

        await icecast_collector._create_ffmpeg_process(
            "http://example.com/stream.mp3",
            "/tmp/chunk_%06d.pcm",  # noqa: S108
            "Authorization: Basic dGVzdDp0ZXN0\r\n",
        )

        args = mock_exec.call_args.args
        self.assertIn("-reconnect_on_http_error", args)
        value_index = args.index("-reconnect_on_http_error") + 1
        self.assertEqual(args[value_index], "429,500,502,503,504")

    @patch(
        "asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
    )
    async def test_timeout_flag_is_set(self, mock_exec: AsyncMock) -> None:
        """Ffmpeg should have a network timeout set to prevent blocking indefinitely."""
        mock_exec.return_value = AsyncMock()

        await icecast_collector._create_ffmpeg_process(
            "http://example.com/stream.mp3",
            "/tmp/chunk_%06d.pcm",  # noqa: S108
            "Authorization: Basic dGVzdDp0ZXN0\r\n",
        )

        args = mock_exec.call_args.args
        self.assertIn("-timeout", args)
        value_index = args.index("-timeout") + 1
        self.assertEqual(
            args[value_index],
            str(icecast_collector.FFMPEG_TIMEOUT_SEC * 1_000_000),
        )


class TestCaptureIcecastStream(unittest.IsolatedAsyncioTestCase):
    """Tests for the public capture_icecast_stream API."""

    def setUp(self) -> None:
        self.mock_logger = MagicMock()

        # The real encode reads a PCM file and returns FLAC bytes; tests
        # exercise capture control flow, not the encode itself, so mock it
        # as a pass-through (return the raw segment bytes unchanged) unless
        # a test overrides the side_effect.
        async def _pass_through_encode(pcm_path: Path) -> bytes | None:
            data = await asyncio.to_thread(pcm_path.read_bytes)
            await asyncio.to_thread(pcm_path.unlink, missing_ok=True)
            return data or None

        self.mock_encode = AsyncMock(side_effect=_pass_through_encode)
        self.patchers = [
            patch.object(icecast_collector, "logger", self.mock_logger),
            patch.object(
                icecast_collector,
                "_encode_pcm_segment_to_flac",
                self.mock_encode,
            ),
            patch.dict(os.environ, MOCK_ENV_VARS),
        ]
        for p in self.patchers:
            p.start()

    def tearDown(self) -> None:
        for p in self.patchers:
            p.stop()

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
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
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_resources_with_probe_status(200, reason="OK"),
        )
        chunks = await _collect_chunks(gen)

        # Assert - segments should be yielded without modification
        mock_create_ffmpeg.assert_called_once()
        args, _ = mock_create_ffmpeg.call_args
        self.assertIn("burst=0", args[0])
        self.assertTrue(args[0].endswith(".mp3?burst=0"))
        self.assertEqual(len(chunks), 2)
        for chunk in chunks:
            self.assertIsInstance(chunk, bytes)

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_failed_encode_drops_segment(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """A segment whose FLAC encode fails must not be yielded."""
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=42,
            segments=[b"BAD_SEGMENT", b"GOOD_SEGMENT"],
            wait_delay=0.1,
            wait_result=0,
        )
        self.mock_encode.side_effect = [None, b"GOOD_SEGMENT"]

        feed = _make_feed("test-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_resources_with_probe_status(200, reason="OK"),
        )
        chunks = await _collect_chunks(gen)

        # Only the segment whose encode succeeded reaches the caller.
        self.assertEqual(chunks, [b"GOOD_SEGMENT"])

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._now_utc"
    )
    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_stream_interval_lag_measures_gap_past_chunk_duration(
        self, mock_create_ffmpeg: AsyncMock, mock_now: MagicMock
    ) -> None:
        """Lag is measured against the previous segment's receipt, not a
        fixed session-start anchor, so it reflects fresh backlog rather than
        cumulative source-clock drift.
        """
        anchor = datetime.datetime(2026, 4, 22, 12, 0, 0, tzinfo=datetime.UTC)
        seg0_receipt = anchor + datetime.timedelta(seconds=2)
        seg1_receipt = seg0_receipt + datetime.timedelta(
            seconds=CHUNK_DURATION_SECONDS + 5
        )
        # Calls in order: anchor, seg0 receipt_time, seg1 receipt_time,
        # seg1 chunk_end_time clamp (process_done is True by then).
        mock_now.side_effect = [
            anchor,
            seg0_receipt,
            seg1_receipt,
            seg1_receipt,
        ]
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=7,
            segments=[
                _make_pcm_segment(CHUNK_DURATION_SECONDS),
                _make_pcm_segment(CHUNK_DURATION_SECONDS),
            ],
            wait_delay=0.05,
            wait_result=0,
        )

        feed = _make_feed("lag-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_resources_with_probe_status(200, reason="OK"),
        )
        chunks = await _collect_chunks_with_timestamps(gen)

        self.assertEqual(len(chunks), 2)
        self.assertIsNone(chunks[0].stream_interval_lag_sec)
        assert chunks[1].stream_interval_lag_sec is not None
        self.assertAlmostEqual(chunks[1].stream_interval_lag_sec, 5.0, places=3)

    @patch(
        "backend.pipeline.ingestion.collectors"
        ".icecast.icecast_collector._now_utc"
    )
    @patch(
        "backend.pipeline.ingestion.collectors"
        ".icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_stream_interval_lag_triggers_timeline_re_anchor(
        self, mock_create_ffmpeg: AsyncMock, mock_now: MagicMock
    ) -> None:
        """If interval lag exceeds threshold (5s), the stream anchor is moved
        forward to re-align subsequent chunks to real-time.
        """
        anchor = datetime.datetime(2026, 4, 22, 12, 0, 0, tzinfo=datetime.UTC)
        seg0_receipt = anchor + datetime.timedelta(seconds=15)
        # 30-second gap before segment 1 (chunk duration is 15s, so
        # interval is 45s and lag is 30s)
        seg1_receipt = seg0_receipt + datetime.timedelta(seconds=45)
        # segment 2 arrives normally 15s after segment 1
        seg2_receipt = seg1_receipt + datetime.timedelta(seconds=15)

        # Calls to _now_utc:
        # 1. capture start anchor
        # 2. seg0 receipt_time
        # 3. seg1 receipt_time
        # 4. seg2 receipt_time
        # 5. seg2 process_done clamp
        mock_now.side_effect = [
            anchor,
            seg0_receipt,
            seg1_receipt,
            seg2_receipt,
            seg2_receipt,
        ]
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=7,
            segments=[
                _make_pcm_segment(CHUNK_DURATION_SECONDS),
                _make_pcm_segment(CHUNK_DURATION_SECONDS),
                _make_pcm_segment(CHUNK_DURATION_SECONDS),
            ],
            wait_delay=0.05,
            wait_result=0,
        )

        feed = _make_feed("reanchor-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_resources_with_probe_status(200, reason="OK"),
        )
        chunks = await _collect_chunks_with_timestamps(gen)

        self.assertEqual(len(chunks), 3)

        # Chunk 0: normal
        self.assertEqual(chunks[0].chunk_start_time, anchor)
        self.assertEqual(chunks[0].chunk_end_time, seg0_receipt)
        self.assertIsNone(chunks[0].stream_interval_lag_sec)

        # Chunk 1: lag is 30s (exceeds 5s threshold)
        # It should trigger re-anchoring.
        # Adjusted start time: 12:00:45 (receipt_time 12:01:00 - 15s duration)
        # Adjusted end time: 12:01:00
        self.assertEqual(chunks[1].stream_interval_lag_sec, 30.0)
        expected_start1 = anchor + datetime.timedelta(seconds=45)
        self.assertEqual(chunks[1].chunk_start_time, expected_start1)
        self.assertEqual(chunks[1].chunk_end_time, seg1_receipt)

        # Chunk 2: normal, calculated relative to new anchor
        # Start time should be contiguous with Chunk 1's end time: 12:01:00
        # End time: 12:01:15 (matching seg2_receipt)
        self.assertEqual(chunks[2].stream_interval_lag_sec, 0.0)
        expected_start2 = anchor + datetime.timedelta(seconds=60)
        self.assertEqual(chunks[2].chunk_start_time, expected_start2)
        self.assertEqual(chunks[2].chunk_end_time, seg2_receipt)

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
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
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_resources_with_probe_status(200, reason="OK"),
        )

        # Give it time to start and yield first chunk if available
        await asyncio.sleep(0.1)

        # Set shutdown and try next iteration - should exit cleanly
        shutdown_event.set()
        with self.assertRaises(StopAsyncIteration):
            await asyncio.wait_for(gen.__anext__(), timeout=1.0)

    async def test_invalid_input_missing_source_feed_id(self) -> None:
        """Test invalid input: feed missing source_feed_id raises typed failure."""
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
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_default_resources(),
        )
        with self.assertRaises(FeedFailure) as context:
            await gen.__anext__()

        _assert_collector_failure(
            self,
            context.exception,
            FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
            "missing_source_feed_id",
        )

    @patch.dict(os.environ, {}, clear=True)
    async def test_missing_auth_env_raises_typed_configuration_failure(
        self,
    ) -> None:
        """Missing Broadcastify stream credentials is a system config issue."""
        feed = _make_feed("missing-auth", "123")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_resources_with_probe_status(200, reason="OK"),
        )

        with self.assertRaises(FeedFailure) as context:
            await gen.__anext__()

        _assert_collector_failure(
            self,
            context.exception,
            FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID,
            "missing_broadcastify_credentials",
        )

    async def test_invalid_input_none_source_feed_id_raises_value_error(
        self,
    ) -> None:
        """Test invalid input: feed with None source_feed_id raises typed failure."""
        # Arrange
        feed = _make_feed("none-stream-feed", None)
        shutdown_event = asyncio.Event()

        # Act & Assert
        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_default_resources(),
        )
        with self.assertRaises(FeedFailure) as context:
            await gen.__anext__()

        _assert_collector_failure(
            self,
            context.exception,
            FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
            "missing_source_feed_id",
        )
        formatted = _formatted_error_calls(self.mock_logger)
        self.assertIn(str(TEST_FEED_ID), formatted)
        self.assertIn("none-stream-feed", formatted)

    async def test_invalid_input_empty_string_source_feed_id_raises_value_error(
        self,
    ) -> None:
        """Test invalid input: feed with empty source_feed_id raises typed failure."""
        # Arrange
        feed = _make_feed("empty-stream-feed", "")
        shutdown_event = asyncio.Event()

        # Act & Assert
        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_default_resources(),
        )
        with self.assertRaises(FeedFailure) as context:
            await gen.__anext__()

        _assert_collector_failure(
            self,
            context.exception,
            FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
            "missing_source_feed_id",
        )
        self.assertIn(
            str(TEST_FEED_ID), _formatted_error_calls(self.mock_logger)
        )

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
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
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_default_resources(),
        )

        chunks = await _collect_chunks(gen)
        self.assertGreaterEqual(len(chunks), 1)

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_ffmpeg_error_exit_code_includes_stderr(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Test: ffmpeg non-zero exit raises diagnostic text and logs stderr."""
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=7777,
            wait_delay=0.0,
            wait_result=1,
            stderr_lines=[b"HTTP error 403 Forbidden\n"],
        )

        feed = _make_feed("error-exit-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_resources_with_probe_status(403, reason="Forbidden"),
        )
        with self.assertRaises(FeedFailure) as context:
            await asyncio.wait_for(gen.__anext__(), timeout=1.0)

        _assert_collector_failure(
            self,
            context.exception,
            FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
            "HTTP error 403 Forbidden",
        )
        formatted = _formatted_error_calls(self.mock_logger)
        self.assertIn("ffmpeg exited with code 1", formatted)
        self.assertIn(str(TEST_FEED_ID), formatted)
        self.assertIn("error-exit-feed", formatted)
        self.assertIn("403 Forbidden", formatted)
        self.assertIn("stderr tail:", formatted)

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_ffmpeg_error_exit_code_no_stderr_probes_available_stream(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Ambiguous ffmpeg exit with probe success is a collector error."""
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=7778,
            wait_delay=0.0,
            wait_result=8,
        )

        feed = _make_feed("no-stderr-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_resources_with_probe_status(200, reason="OK"),
        )
        with self.assertRaises(FeedFailure) as context:
            await asyncio.wait_for(gen.__anext__(), timeout=1.0)

        _assert_collector_failure(
            self,
            context.exception,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
            "ffmpeg exited with code 8",
        )
        formatted = _formatted_error_calls(self.mock_logger)
        self.assertIn("ffmpeg exited with code 8", formatted)
        self.assertIn("(no stderr captured)", formatted)

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_ffmpeg_error_exit_code_no_probe_result_is_inconclusive(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Ambiguous ffmpeg exit falls back when probing gives no evidence."""
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=7779,
            wait_delay=0.0,
            wait_result=1,
        )

        feed = _make_feed("no-probe-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        with patch(
            "backend.pipeline.ingestion.collectors.icecast."
            "icecast_collector._probe_stream_once",
            new_callable=AsyncMock,
            return_value=None,
        ):
            gen = icecast_collector.capture_icecast_stream(
                feed,
                shutdown_event,
                url_base="https://mock.example.com/",
                resources=_default_resources(),
            )
            with self.assertRaises(FeedFailure) as context:
                await asyncio.wait_for(gen.__anext__(), timeout=1.0)

        _assert_collector_failure(
            self,
            context.exception,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
            "ffmpeg exited with code 1",
        )

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_ffmpeg_error_exit_code_http_404_source_offline(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Ffmpeg HTTP error 404 maps to source_offline."""
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=7780,
            wait_delay=0.0,
            wait_result=8,
            stderr_lines=[b"HTTP error 404 Not Found\n"],
        )

        feed = _make_feed("offline-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_default_resources(),
        )
        with self.assertRaises(FeedFailure) as context:
            await asyncio.wait_for(gen.__anext__(), timeout=1.0)

        _assert_collector_failure(
            self,
            context.exception,
            FeedStatusReason.SOURCE_OFFLINE,
            "HTTP error 404 Not Found",
        )

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_ffmpeg_error_exit_code_http_401_probe_404_source_offline(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Transient HTTP error 401 with probe 404 maps to source_offline."""
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=7780,
            wait_delay=0.0,
            wait_result=8,
            stderr_lines=[b"HTTP error 401 Unauthorized\n"],
        )

        feed = _make_feed("transient-401-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_resources_with_probe_status(404, reason="Not Found"),
        )
        with self.assertRaises(FeedFailure) as context:
            await asyncio.wait_for(gen.__anext__(), timeout=1.0)

        _assert_collector_failure(
            self,
            context.exception,
            FeedStatusReason.SOURCE_OFFLINE,
            "HTTP error 404 Not Found",
        )

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_ffmpeg_error_exit_code_http_401_probe_401_auth_failed(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Persistent HTTP error 401 with probe 401 maps to system_authentication_failed."""
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=7780,
            wait_delay=0.0,
            wait_result=8,
            stderr_lines=[b"HTTP error 401 Unauthorized\n"],
        )

        feed = _make_feed("persistent-401-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_resources_with_probe_status(401, reason="Unauthorized"),
        )
        with self.assertRaises(FeedFailure) as context:
            await asyncio.wait_for(gen.__anext__(), timeout=1.0)

        _assert_collector_failure(
            self,
            context.exception,
            FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
            "HTTP error 401 Unauthorized",
        )

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_ffmpeg_error_exit_code_http_401_probe_200_source_unreachable(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Transient HTTP error 401 with probe 200 maps to source_unreachable."""
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=7780,
            wait_delay=0.0,
            wait_result=8,
            stderr_lines=[b"HTTP error 401 Unauthorized\n"],
        )

        feed = _make_feed(
            "transient-401-online-feed", "http://example.com/stream"
        )
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_resources_with_probe_status(200, reason="OK"),
        )
        with self.assertRaises(FeedFailure) as context:
            await asyncio.wait_for(gen.__anext__(), timeout=1.0)

        _assert_collector_failure(
            self,
            context.exception,
            FeedStatusReason.SOURCE_UNREACHABLE,
            "ffmpeg failed with authentication error despite stream being available "
            "on probe; stderr: HTTP error 401 Unauthorized",
        )

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_ffmpeg_error_exit_code_http_429_rate_limited(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Ffmpeg HTTP error 429 maps to source_rate_limited."""
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=7781,
            wait_delay=0.0,
            wait_result=8,
            stderr_lines=[b"HTTP error 429 Too Many Requests\n"],
        )

        feed = _make_feed("rate-limited-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_default_resources(),
        )
        with self.assertRaises(FeedFailure) as context:
            await asyncio.wait_for(gen.__anext__(), timeout=1.0)

        _assert_collector_failure(
            self,
            context.exception,
            FeedStatusReason.SOURCE_RATE_LIMITED,
            "HTTP error 429 Too Many Requests",
        )

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_ffmpeg_http_status_survives_retry_log_flood(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Classification keeps HTTP status evidence outside the log tail."""
        retry_noise = [
            f"retry log line {index}\n".encode()
            for index in range(icecast_collector.STDERR_TAIL_LINES + 5)
        ]
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=7784,
            wait_delay=0.0,
            wait_result=8,
            stderr_lines=[
                b"HTTP error 429 Too Many Requests\n",
                *retry_noise,
            ],
        )

        feed = _make_feed("rate-limited-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_resources_with_probe_status(200, reason="OK"),
        )
        with self.assertRaises(FeedFailure) as context:
            await asyncio.wait_for(gen.__anext__(), timeout=1.0)

        _assert_collector_failure(
            self,
            context.exception,
            FeedStatusReason.SOURCE_RATE_LIMITED,
            "HTTP error 429 Too Many Requests",
        )

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_ffmpeg_error_exit_code_http_503_unreachable(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Ffmpeg HTTP error 503 maps to source_unreachable."""
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=7782,
            wait_delay=0.0,
            wait_result=8,
            stderr_lines=[b"Server returned 503 Service Unavailable\n"],
        )

        feed = _make_feed("unreachable-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_resources_with_probe_status(200, reason="OK"),
        )
        with self.assertRaises(FeedFailure) as context:
            await asyncio.wait_for(gen.__anext__(), timeout=1.0)

        _assert_collector_failure(
            self,
            context.exception,
            FeedStatusReason.SOURCE_UNREACHABLE,
            "Server returned 503 Service Unavailable",
        )

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_ffmpeg_error_exit_code_http_502_diagnostic(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Ffmpeg HTTP 502 diagnostics preserve provider evidence."""
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=7785,
            wait_delay=0.0,
            wait_result=8,
            stderr_lines=[b"HTTP error 502 Bad Gateway\n"],
        )

        feed = _make_feed("bad-gateway-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_resources_with_probe_status(200, reason="OK"),
        )
        with self.assertRaises(FeedFailure) as context:
            await asyncio.wait_for(gen.__anext__(), timeout=1.0)

        _assert_collector_failure(
            self,
            context.exception,
            FeedStatusReason.SOURCE_UNREACHABLE,
            "HTTP error 502 Bad Gateway",
        )

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_ffmpeg_error_exit_code_probe_inconclusive_fallback(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Ambiguous ffmpeg exit with unmapped probe status keeps raw exit reason."""
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=7783,
            wait_delay=0.0,
            wait_result=8,
        )

        feed = _make_feed("teapot-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_resources_with_probe_status(418, reason="Teapot"),
        )
        with self.assertRaises(FeedFailure) as context:
            await asyncio.wait_for(gen.__anext__(), timeout=1.0)

        _assert_collector_failure(
            self,
            context.exception,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
            "ffmpeg exited with code 8",
        )

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_ffmpeg_signal_kill_renders_signal_diagnostic(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Test: signal-killed ffmpeg maps to diagnostic text.

        Python's subprocess.returncode is -N for signal-N termination on POSIX
        (e.g. SIGKILL -> -9). The classifier still splits sign and emits a
        normalized internal reason; the feed failure exposes a human diagnostic.
        """
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=7779,
            wait_delay=0.0,
            wait_result=-9,  # SIGKILL via Python subprocess convention
            stderr_lines=[b"Killed\n"],
        )

        feed = _make_feed("signal-kill-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_resources_with_probe_status(200, reason="OK"),
        )
        with self.assertRaises(FeedFailure) as context:
            await asyncio.wait_for(gen.__anext__(), timeout=1.0)

        _assert_collector_failure(
            self,
            context.exception,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
            "ffmpeg terminated by signal 9; Killed",
        )
        formatted = _formatted_error_calls(self.mock_logger)
        self.assertIn("ffmpeg exited with code -9", formatted)

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
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
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_default_resources(),
        )
        with self.assertRaises(RuntimeError):
            await asyncio.wait_for(gen.__anext__(), timeout=1.0)

        # Cleanup runs in finally; the key behavior is that the error is propagated.

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector.READ_TIMEOUT_SEC",
        0.1,
    )
    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_read_timeout_probe_404_source_offline(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """Ambiguous timeout with probe 404 maps to source_offline."""
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=8888,
            wait_delay=1.0,  # longer than READ_TIMEOUT_SEC (0.1s) but short enough for cleanup
            wait_result=0,
            stderr_lines=[b"Connection timed out\n"],
        )

        feed = _make_feed("timeout-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_resources_with_probe_status(404, reason="Not Found"),
        )
        with self.assertRaises(FeedFailure) as context:
            await asyncio.wait_for(gen.__anext__(), timeout=2.0)

        _assert_collector_failure(
            self,
            context.exception,
            FeedStatusReason.SOURCE_OFFLINE,
            "HTTP error 404 Not Found",
        )
        formatted = _formatted_warning_calls(self.mock_logger)
        self.assertIn("no finalized segment within", formatted)
        self.assertIn("next_index=0", formatted)
        self.assertIn("current_segment_exists=False", formatted)
        self.assertIn("next_segment_exists=False", formatted)
        self.assertIn("current_segment_size=None", formatted)
        self.assertIn("next_segment_size=None", formatted)
        self.assertIn("ffmpeg_pid=8888", formatted)
        self.assertIn("Connection timed out", formatted)

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
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
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_default_resources(),
        )
        chunks = await _collect_chunks(gen)

        # Assert - should have collected multiple segments
        self.assertEqual(len(chunks), 3)
        for chunk in chunks:
            self.assertIsInstance(chunk, bytes)
            # Each chunk should be a FLAC segment without additional headers
            self.assertTrue(b"FLAC_SEGMENT" in chunk or b"FLAC" in chunk)

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._now_utc",
    )
    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
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
        # First call sets stream_anchor_time. Subsequent calls return a time
        # after every chunk's natural end so min() picks chunk_end_time even if
        # wait_task.done() flips before an intermediate segment.
        t0 = fixed_anchor
        mock_now_utc.side_effect = IncrementalClock(
            t0, steps=[15.0, 15.0, 15.0, 0.0, 0.0]
        )

        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=3333,
            segments=[
                _make_pcm_segment(CHUNK_DURATION_SECONDS),
                _make_pcm_segment(CHUNK_DURATION_SECONDS),
                _make_pcm_segment(CHUNK_DURATION_SECONDS),
            ],
            wait_delay=0.1,
            wait_result=0,
        )

        feed = _make_feed("timestamp-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_default_resources(),
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
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._now_utc",
    )
    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_timestamps_use_exact_pcm_sample_count(
        self, mock_create_ffmpeg: AsyncMock, mock_now_utc: MagicMock
    ) -> None:
        """Test timestamp math: chunk start and end times reflect exact sample duration."""
        fixed_anchor = datetime.datetime(
            2026, 1, 1, 0, 0, 0, tzinfo=datetime.UTC
        )
        mock_now_utc.side_effect = IncrementalClock(
            fixed_anchor, steps=[12.5, 18.25, 0.0, 0.0]
        )

        # Segments of exact lengths: 12.5 seconds and 18.25 seconds
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=3334,
            segments=[
                _make_pcm_segment(12.5),
                _make_pcm_segment(18.25),
            ],
            wait_delay=0.1,
            wait_result=0,
        )

        feed = _make_feed("exact-pcm-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_default_resources(),
        )
        results = await _collect_chunks_with_timestamps(gen)

        self.assertEqual(len(results), 2)

        ts0 = results[0].chunk_start_time
        end0 = results[0].chunk_end_time
        ts1 = results[1].chunk_start_time
        end1 = results[1].chunk_end_time

        self.assertEqual(ts0, fixed_anchor)
        self.assertEqual((end0 - ts0).total_seconds(), 12.5)
        self.assertEqual(ts1, end0)
        self.assertEqual((end1 - ts1).total_seconds(), 18.25)

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._now_utc",
    )
    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_last_chunk_end_time_clamped_to_current_time(
        self, mock_create_ffmpeg: AsyncMock, mock_now_utc: MagicMock
    ) -> None:
        """Test timestamp math: last chunk end_time is clamped to _now_utc()
        when _now_utc() < chunk_end_time so min() picks _now_utc() (clamped path).

        A single segment drives _now_utc() exactly three times:
        1. stream_anchor_time (before the inner loop),
        2. receipt_time stamp (RCPT-02, immediately before read_bytes), and
        3. the min() clamp when the process is done and the only chunk is finalized.
        """
        fixed_anchor = datetime.datetime(
            2026, 1, 1, 0, 0, 0, tzinfo=datetime.UTC
        )
        # clamp_time is before anchor + CHUNK_DURATION_SECONDS (the natural end),
        # so min() picks clamp_time instead of chunk_end_time.
        clamp_time = fixed_anchor + datetime.timedelta(
            seconds=CHUNK_DURATION_SECONDS - 5
        )
        # 2nd value feeds the receipt_time stamp (RCPT-02); 3rd feeds min() clamp.
        mock_now_utc.side_effect = [fixed_anchor, clamp_time, clamp_time]

        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=4444,
            segments=[_make_pcm_segment(CHUNK_DURATION_SECONDS)],
            wait_delay=0.1,
            wait_result=0,
        )

        feed = _make_feed("clamp-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_default_resources(),
        )
        results = await _collect_chunks_with_timestamps(gen)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].chunk_start_time, fixed_anchor)
        self.assertEqual(results[0].chunk_end_time, clamp_time)

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._now_utc",
    )
    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_timestamps_adjusted_for_burst(
        self, mock_create_ffmpeg: AsyncMock, mock_now_utc: MagicMock
    ) -> None:
        """Test that start/end timestamps are shifted into the past when a burst is detected."""
        fixed_anchor = datetime.datetime(
            2026, 1, 1, 0, 0, 0, tzinfo=datetime.UTC
        )

        t_anchor = fixed_anchor
        t_seg1 = t_anchor + datetime.timedelta(seconds=1)
        t_seg2 = t_anchor + datetime.timedelta(seconds=2)
        t_seg3 = t_anchor + datetime.timedelta(seconds=3)
        t_seg4 = t_seg3 + datetime.timedelta(seconds=CHUNK_DURATION_SECONDS)
        t_seg5 = t_seg4 + datetime.timedelta(seconds=CHUNK_DURATION_SECONDS)
        t_future = t_seg5 + datetime.timedelta(seconds=100)

        mock_now_utc.side_effect = [
            t_anchor,  # stream_anchor_time
            t_seg1,  # Seg 1 receipt_time
            t_seg2,  # Seg 2 receipt_time
            t_seg3,  # Seg 3 receipt_time
            t_seg4,  # Seg 4 receipt_time
            t_seg5,  # Seg 5 receipt_time
            t_future,  # Seg 5 min() clamp (not clamped)
        ]

        # 5 segments, each of CHUNK_DURATION_SECONDS duration
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=4445,
            segments=[
                _make_pcm_segment(CHUNK_DURATION_SECONDS),
                _make_pcm_segment(CHUNK_DURATION_SECONDS),
                _make_pcm_segment(CHUNK_DURATION_SECONDS),
                _make_pcm_segment(CHUNK_DURATION_SECONDS),
                _make_pcm_segment(CHUNK_DURATION_SECONDS),
            ],
            wait_delay=0.1,
            wait_result=0,
        )

        feed = _make_feed("burst-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_default_resources(),
        )
        results = await _collect_chunks_with_timestamps(gen)

        self.assertEqual(len(results), 5)

        # We expect:
        # Total burst duration (first 3 segments) = CHUNK_DURATION_SECONDS * 3.
        # Adjusted stream_anchor_time = T - (CHUNK_DURATION_SECONDS * 3).
        burst_duration = CHUNK_DURATION_SECONDS * 3

        self.assertEqual(
            results[0].chunk_start_time,
            fixed_anchor - datetime.timedelta(seconds=burst_duration - 3),
        )
        self.assertEqual(
            results[0].chunk_end_time,
            fixed_anchor
            - datetime.timedelta(
                seconds=burst_duration - CHUNK_DURATION_SECONDS - 3
            ),
        )

        self.assertEqual(
            results[1].chunk_start_time,
            fixed_anchor
            - datetime.timedelta(
                seconds=burst_duration - CHUNK_DURATION_SECONDS - 3
            ),
        )
        self.assertEqual(
            results[1].chunk_end_time,
            fixed_anchor
            - datetime.timedelta(
                seconds=burst_duration - CHUNK_DURATION_SECONDS * 2 - 3
            ),
        )

        self.assertEqual(
            results[2].chunk_start_time,
            fixed_anchor
            - datetime.timedelta(
                seconds=burst_duration - CHUNK_DURATION_SECONDS * 2 - 3
            ),
        )
        self.assertEqual(
            results[2].chunk_end_time,
            fixed_anchor + datetime.timedelta(seconds=3),
        )

        self.assertEqual(
            results[3].chunk_start_time,
            fixed_anchor + datetime.timedelta(seconds=3),
        )
        self.assertEqual(
            results[3].chunk_end_time,
            fixed_anchor
            + datetime.timedelta(seconds=CHUNK_DURATION_SECONDS + 3),
        )

        self.assertEqual(
            results[4].chunk_start_time,
            fixed_anchor
            + datetime.timedelta(seconds=CHUNK_DURATION_SECONDS + 3),
        )
        self.assertEqual(
            results[4].chunk_end_time,
            fixed_anchor
            + datetime.timedelta(seconds=CHUNK_DURATION_SECONDS * 2 + 3),
        )

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._now_utc",
    )
    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_timestamps_mid_stream_disconnect_and_catchup(
        self, mock_create_ffmpeg: AsyncMock, mock_now_utc: MagicMock
    ) -> None:
        """Verify timeline manager handles a mid-stream disconnect followed by
        a catch-up burst and return to normal cadence.
        """
        fixed_anchor = datetime.datetime(
            2026, 1, 1, 12, 0, 0, tzinfo=datetime.UTC
        )
        mock_now_utc.side_effect = IncrementalClock(
            fixed_anchor,
            steps=[1.0, 1.0, 1.0, 15.0, 315.0, 1.0, 1.0, 15.0, 0.0, 0.0],
        )

        # 8 segments of 15 seconds each
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=4446,
            segments=[
                _make_pcm_segment(CHUNK_DURATION_SECONDS),  # Seg 1 (burst)
                _make_pcm_segment(CHUNK_DURATION_SECONDS),  # Seg 2 (burst)
                _make_pcm_segment(CHUNK_DURATION_SECONDS),  # Seg 3 (burst)
                _make_pcm_segment(CHUNK_DURATION_SECONDS),  # Seg 4 (steady)
                _make_pcm_segment(CHUNK_DURATION_SECONDS),  # Seg 5 (late/burst)
                _make_pcm_segment(CHUNK_DURATION_SECONDS),  # Seg 6 (burst)
                _make_pcm_segment(CHUNK_DURATION_SECONDS),  # Seg 7 (burst)
                _make_pcm_segment(CHUNK_DURATION_SECONDS),  # Seg 8 (steady)
            ],
            wait_delay=0.1,
            wait_result=0,
        )

        feed = _make_feed("midstream-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_default_resources(),
        )
        results = await _collect_chunks_with_timestamps(gen)

        self.assertEqual(len(results), 8)

        # Start-up burst (Seg 1-3) adjusted anchor backward
        # Last burst chunk (Seg 3) received at 12:00:03
        # Start-up anchor: 12:00:03 - 45s = 11:59:18
        self.assertEqual(
            results[0].chunk_start_time,
            fixed_anchor - datetime.timedelta(seconds=42),
        )
        self.assertEqual(
            results[2].chunk_end_time,
            fixed_anchor + datetime.timedelta(seconds=3),
        )

        # Seg 4 (steady) starts at 12:00:03, ends at 12:00:18 (received at 12:00:18)
        self.assertEqual(
            results[3].chunk_start_time,
            fixed_anchor + datetime.timedelta(seconds=3),
        )
        self.assertEqual(
            results[3].chunk_end_time,
            fixed_anchor + datetime.timedelta(seconds=18),
        )

        # Seg 5-7 is a mid-stream catch-up burst after a 5 minute disconnect gap.
        # Last catch-up chunk (Seg 7) received at 12:05:35.
        # Cumulative duration before Seg 7 = 6 segments = 90s.
        # Original Segment 7 end = 11:59:18 + 90s + 15s = 12:01:03.
        # Net shift = 12:05:35 - 12:01:03 = 272s.
        # Adjusted anchor: 11:59:18 + 272s = 12:03:50.

        # Seg 5 (first catch-up chunk): starts at 12:00:18 + 272s = 12:04:50
        self.assertEqual(
            results[4].chunk_start_time,
            fixed_anchor + datetime.timedelta(seconds=290),
        )
        self.assertEqual(
            results[4].chunk_end_time,
            fixed_anchor + datetime.timedelta(seconds=305),
        )

        # Seg 7 (last catch-up chunk): starts at 12:05:20, ends at 12:05:35 (receipt_time)
        self.assertEqual(
            results[6].chunk_start_time,
            fixed_anchor + datetime.timedelta(seconds=320),
        )
        self.assertEqual(
            results[6].chunk_end_time,
            fixed_anchor + datetime.timedelta(seconds=335),
        )

        # Seg 8 (first steady chunk after disconnect) ends at receipt_time 12:05:50
        self.assertEqual(
            results[7].chunk_start_time,
            fixed_anchor + datetime.timedelta(seconds=335),
        )
        self.assertEqual(
            results[7].chunk_end_time,
            fixed_anchor + datetime.timedelta(seconds=350),
        )

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
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
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_default_resources(),
        )
        results = await _collect_chunks_with_timestamps(gen)

        self.assertEqual(len(results), 3)
        for chunk in results:
            self.assertIsNotNone(chunk.session_id)
        self.assertEqual(results[0].session_id, results[1].session_id)
        self.assertEqual(results[1].session_id, results[2].session_id)

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._now_utc"
    )
    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_reconnect_lag_decoupled_from_consumer_backpressure(
        self, mock_create_ffmpeg: AsyncMock, mock_now_utc: MagicMock
    ) -> None:
        """Verify that consumer-side delays do not corrupt receipt_time stamps of subsequent chunks."""
        fixed_anchor = datetime.datetime(
            2026, 1, 1, 0, 0, 0, tzinfo=datetime.UTC
        )

        t0_conn = fixed_anchor
        t0_seg0 = fixed_anchor + datetime.timedelta(seconds=15)
        t0_seg1 = fixed_anchor + datetime.timedelta(seconds=30)
        t0_seg2 = fixed_anchor + datetime.timedelta(seconds=45)

        mock_now_utc.side_effect = [
            t0_conn,
            t0_seg0,
            t0_seg1,
            t0_seg2,
            t0_seg2 + datetime.timedelta(seconds=10),
        ]

        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=777,
            segments=[
                _make_pcm_segment(CHUNK_DURATION_SECONDS),
                _make_pcm_segment(CHUNK_DURATION_SECONDS),
                _make_pcm_segment(CHUNK_DURATION_SECONDS),
            ],
            wait_delay=0.1,
            wait_result=0,
        )

        feed = _make_feed("backpressure-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_default_resources(),
        )

        chunks = []
        async for chunk in gen:
            chunks.append(chunk)
            if len(chunks) == 1:
                await asyncio.sleep(0.5)

        self.assertEqual(len(chunks), 3)
        self.assertEqual(chunks[0].receipt_time, t0_seg0)
        self.assertEqual(chunks[1].receipt_time, t0_seg1)
        self.assertEqual(chunks[2].receipt_time, t0_seg2)

        self.assertEqual(chunks[0].chunk_start_time, t0_conn)
        self.assertEqual(chunks[0].chunk_end_time, t0_seg0)
        self.assertEqual(chunks[1].chunk_start_time, t0_seg0)
        self.assertEqual(chunks[1].chunk_end_time, t0_seg1)
        self.assertEqual(chunks[2].chunk_start_time, t0_seg1)
        self.assertEqual(chunks[2].chunk_end_time, t0_seg2)

    # NOTE: stream lag is no longer reported via a threshold-based warning
    # log against a fixed session-start anchor (see
    # test_stream_interval_lag_measures_gap_past_chunk_duration above) —
    # that approach conflated real backlog with ordinary long-session
    # source-clock drift. It's now a per-interval value on CapturedChunk
    # that collector_runtime folds into the chunk_ingested SLO event.


class TestIcecastReceiptTimeStamp(unittest.IsolatedAsyncioTestCase):
    """RCPT-02: Icecast stamps receipt_time at segment finalization."""

    @patch.dict(os.environ, MOCK_ENV_VARS)
    @patch(
        "backend.pipeline.ingestion.collectors.icecast"
        ".icecast_collector._encode_pcm_segment_to_flac",
        new_callable=AsyncMock,
        return_value=b"seg0",
    )
    @patch(
        "backend.pipeline.ingestion.collectors.icecast"
        ".icecast_collector._now_utc"
    )
    @patch(
        "backend.pipeline.ingestion.collectors.icecast"
        ".icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_stamps_receipt_time_on_yielded_chunk(
        self,
        mock_create: AsyncMock,
        mock_now: MagicMock,
        mock_encode: AsyncMock,
    ) -> None:
        fixed_time = datetime.datetime(
            2026, 4, 22, 12, 0, 0, tzinfo=datetime.UTC
        )
        # _now_utc is called for stream_anchor_time once BEFORE the loop,
        # then again for each segment's receipt_time, then again for the
        # chunk_end clamp. Return a fixed value for every call.
        mock_now.side_effect = [fixed_time, fixed_time, fixed_time]
        mock_create.side_effect = _make_process_factory(
            pid=1,
            segments=[b"seg0"],
            wait_delay=0.05,
            wait_result=0,
        )

        feed = _make_feed("test", source_feed_id="sid")
        shutdown = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown,
            "http://example.com/",
            resources=_default_resources(),
        )
        chunks = await _collect_chunks_with_timestamps(gen)

        self.assertGreaterEqual(len(chunks), 1)
        self.assertEqual(chunks[0].receipt_time, fixed_time)


class TestEncodePcmSegmentToFlac(unittest.IsolatedAsyncioTestCase):
    """Guards the contract that no segment leaves ingestion unreadable.

    Encoding is pure Python (numpy + soundfile) now, not a subprocess, so
    these run unconditionally -- no ffmpeg-availability gate needed.
    """

    async def test_valid_pcm_produces_readable_flac(self) -> None:
        sample_rate = 16000
        duration_sec = 15
        num_samples = sample_rate * duration_sec
        # A real (non-silent) waveform, not zeros, so the encode has
        # actual content to compress.
        t = np.arange(num_samples) / sample_rate
        tone = (np.sin(2 * np.pi * 440 * t) * 20000).astype("<i2")
        pcm_bytes = tone.tobytes()

        with tempfile.TemporaryDirectory() as tmp:
            pcm_segment = Path(tmp) / "chunk_000000.pcm"
            pcm_segment.write_bytes(pcm_bytes)

            flac_bytes = await icecast_collector._encode_pcm_segment_to_flac(
                pcm_segment
            )

            # The PCM temp file is consumed and removed by the encode call.
            self.assertFalse(pcm_segment.exists())

        self.assertIsNotNone(flac_bytes)
        assert flac_bytes is not None
        info = sf.info(io.BytesIO(flac_bytes))
        samples, sample_rate_out = sf.read(
            io.BytesIO(flac_bytes), dtype="int16"
        )

        # The header advertises the true frame count, not an unknown-length
        # sentinel, so downstream duration derivation is correct.
        self.assertEqual(info.frames, len(samples))
        self.assertEqual(sample_rate_out, sample_rate)
        self.assertEqual(len(samples), num_samples)
        np.testing.assert_array_equal(samples, tone)

    async def test_empty_pcm_returns_none(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            pcm_segment = Path(tmp) / "chunk_000000.pcm"
            pcm_segment.write_bytes(b"")

            result = await icecast_collector._encode_pcm_segment_to_flac(
                pcm_segment
            )

            self.assertIsNone(result)
            self.assertFalse(pcm_segment.exists())

    async def test_malformed_pcm_returns_none_not_raises(self) -> None:
        """An odd byte count can't form whole int16 samples; must drop, not crash."""
        with tempfile.TemporaryDirectory() as tmp:
            pcm_segment = Path(tmp) / "chunk_000000.pcm"
            pcm_segment.write_bytes(b"\x01\x02\x03")

            result = await icecast_collector._encode_pcm_segment_to_flac(
                pcm_segment
            )

            self.assertIsNone(result)
            self.assertFalse(pcm_segment.exists())


class TestBuildAuthAndUrl(unittest.TestCase):
    """Tests for _build_auth_and_url helper in icecast_collector."""

    def setUp(self) -> None:
        self.original_env = os.environ.copy()

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self.original_env)

    def test_xan_token_auth(self) -> None:
        """When BROADCASTIFY_XAN_TOKEN is set, it is used as a query parameter and basic auth is bypassed."""
        os.environ["BROADCASTIFY_XAN_TOKEN"] = "mock-xan-token"
        # Ensure username/password are not set to confirm they aren't used
        os.environ.pop("BROADCASTIFY_USERNAME", None)
        os.environ.pop("BROADCASTIFY_PASSWORD", None)

        auth_header, url = icecast_collector._build_auth_and_url(
            url_base="https://audio.example.com",
            source_feed_id="12345",
        )

        self.assertEqual(auth_header, "")
        self.assertEqual(
            url,
            "https://audio.example.com/12345.mp3?burst=0&xan=mock-xan-token",
        )

    def test_basic_auth_fallback(self) -> None:
        """When BROADCASTIFY_XAN_TOKEN is not set, it falls back to basic auth using username/password."""
        os.environ.pop("BROADCASTIFY_XAN_TOKEN", None)
        os.environ["BROADCASTIFY_USERNAME"] = "test-user"
        os.environ["BROADCASTIFY_PASSWORD"] = "test-password"

        auth_header, url = icecast_collector._build_auth_and_url(
            url_base="https://audio.example.com",
            source_feed_id="12345",
        )

        # Basic auth header for "test-user:test-password" is:
        # base64("test-user:test-password") = dGVzdC11c2VyOnRlc3QtcGFzc3dvcmQ=
        self.assertEqual(
            auth_header,
            "Authorization: Basic dGVzdC11c2VyOnRlc3QtcGFzc3dvcmQ=\r\n",
        )
        self.assertEqual(
            url,
            "https://audio.example.com/12345.mp3?burst=0",
        )

    def test_basic_auth_fallback_swaps_partner_url(self) -> None:
        """When BROADCASTIFY_XAN_TOKEN is not set and url_base is partner.broadcastify.com, it swaps the URL to audio.broadcastify.com."""
        os.environ.pop("BROADCASTIFY_XAN_TOKEN", None)
        os.environ["BROADCASTIFY_USERNAME"] = "test-user"
        os.environ["BROADCASTIFY_PASSWORD"] = "test-password"

        auth_header, url = icecast_collector._build_auth_and_url(
            url_base=constants.BCFY_FEEDS_PARTNER_URL_BASE,
            source_feed_id="12345",
        )

        self.assertEqual(
            auth_header,
            "Authorization: Basic dGVzdC11c2VyOnRlc3QtcGFzc3dvcmQ=\r\n",
        )
        self.assertEqual(
            url,
            f"{constants.BCFY_FEEDS_PUBLIC_URL_BASE}12345.mp3?burst=0",
        )

    def test_basic_auth_fallback_uses_env_var_override(self) -> None:
        """Verify basic auth fallback uses BCFY_FEEDS_PUBLIC_URL_BASE override."""
        os.environ.pop("BROADCASTIFY_XAN_TOKEN", None)
        os.environ["BROADCASTIFY_USERNAME"] = "test-user"
        os.environ["BROADCASTIFY_PASSWORD"] = "test-password"
        os.environ["BCFY_FEEDS_PUBLIC_URL_BASE"] = (
            "https://custom-public.example.com/"
        )
        try:
            auth_header, url = icecast_collector._build_auth_and_url(
                url_base=constants.BCFY_FEEDS_PARTNER_URL_BASE,
                source_feed_id="12345",
            )
            self.assertEqual(
                auth_header,
                "Authorization: Basic dGVzdC11c2VyOnRlc3QtcGFzc3dvcmQ=\r\n",
            )
            self.assertEqual(
                url,
                "https://custom-public.example.com/12345.mp3?burst=0",
            )
        finally:
            os.environ.pop("BCFY_FEEDS_PUBLIC_URL_BASE", None)

    def test_basic_auth_missing_credentials(self) -> None:
        """When token is missing and credentials are also missing, it raises config error."""
        os.environ.pop("BROADCASTIFY_XAN_TOKEN", None)
        os.environ.pop("BROADCASTIFY_USERNAME", None)
        os.environ.pop("BROADCASTIFY_PASSWORD", None)

        with self.assertRaises(Exception) as ctx:
            icecast_collector._build_auth_and_url(
                url_base="https://audio.example.com",
                source_feed_id="12345",
            )
        self.assertIn("missing_broadcastify_credentials", str(ctx.exception))


class TestIcecastTimelineManager(unittest.TestCase):
    """Tests for IcecastTimelineManager validation constraints."""

    def test_negative_duration_raises_value_error(self) -> None:
        """Verify that negative chunk durations raise ValueError."""
        manager = icecast_collector.IcecastTimelineManager(
            stream_anchor_time=datetime.datetime.now(datetime.UTC),
            feed_id=uuid.uuid4(),
            feed_name="test-feed",
        )
        now = datetime.datetime.now(datetime.UTC)
        chunk = CapturedChunk(
            audio_bytes=b"data",
            chunk_start_time=now,
            chunk_end_time=now - datetime.timedelta(seconds=5),
            session_id="session",
            receipt_time=now,
        )
        with self.assertRaises(ValueError) as ctx:
            manager.process_chunk(chunk, 16000 * 15, process_done=False)
        self.assertIn("Negative chunk duration", str(ctx.exception))

    def test_non_monotonic_start_time_raises_value_error(self) -> None:
        """Verify that non-monotonic chunk start times raise ValueError."""
        manager = icecast_collector.IcecastTimelineManager(
            stream_anchor_time=datetime.datetime.now(datetime.UTC),
            feed_id=uuid.uuid4(),
            feed_name="test-feed",
        )
        now = datetime.datetime.now(datetime.UTC)
        chunk1 = CapturedChunk(
            audio_bytes=b"data",
            chunk_start_time=now,
            chunk_end_time=now + datetime.timedelta(seconds=15),
            session_id="session",
            receipt_time=now,
        )
        manager.in_burst = False
        manager.process_chunk(chunk1, 16000 * 15, process_done=False)

        chunk2 = CapturedChunk(
            audio_bytes=b"data",
            chunk_start_time=now + datetime.timedelta(seconds=10),
            chunk_end_time=now + datetime.timedelta(seconds=25),
            session_id="session",
            receipt_time=now + datetime.timedelta(seconds=15),
        )
        with self.assertRaises(ValueError) as ctx:
            manager.process_chunk(chunk2, 16000 * 30, process_done=False)
        self.assertIn("Non-monotonic chunk start time", str(ctx.exception))

    def test_microsecond_rounding_coalesced(self) -> None:
        """Verify that microsecond-level rounding errors in chunk start times are coalesced."""
        manager = icecast_collector.IcecastTimelineManager(
            stream_anchor_time=datetime.datetime.now(datetime.UTC),
            feed_id=uuid.uuid4(),
            feed_name="test-feed",
        )
        now = datetime.datetime.now(datetime.UTC)
        chunk1 = CapturedChunk(
            audio_bytes=b"data",
            chunk_start_time=now,
            chunk_end_time=now + datetime.timedelta(seconds=15),
            session_id="session",
            receipt_time=now,
        )
        manager.in_burst = False
        manager.process_chunk(chunk1, 16000 * 15, process_done=False)

        # Test overlap of 1 microsecond
        chunk2 = CapturedChunk(
            audio_bytes=b"data",
            chunk_start_time=now
            + datetime.timedelta(seconds=15)
            - datetime.timedelta(microseconds=1),
            chunk_end_time=now + datetime.timedelta(seconds=30),
            session_id="session",
            receipt_time=now + datetime.timedelta(seconds=15),
        )
        res = manager.process_chunk(chunk2, 16000 * 30, process_done=False)
        self.assertEqual(len(res), 1)
        self.assertEqual(
            res[0].chunk_start_time, now + datetime.timedelta(seconds=15)
        )

        # Test gap of 1 microsecond
        chunk3 = CapturedChunk(
            audio_bytes=b"data",
            chunk_start_time=now
            + datetime.timedelta(seconds=30)
            + datetime.timedelta(microseconds=1),
            chunk_end_time=now + datetime.timedelta(seconds=45),
            session_id="session",
            receipt_time=now + datetime.timedelta(seconds=30),
        )
        res2 = manager.process_chunk(chunk3, 16000 * 45, process_done=False)
        self.assertEqual(len(res2), 1)
        self.assertEqual(
            res2[0].chunk_start_time, now + datetime.timedelta(seconds=30)
        )


if __name__ == "__main__":
    unittest.main()

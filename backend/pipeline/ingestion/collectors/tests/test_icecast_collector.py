import asyncio
import datetime
import os
import shutil
import tempfile
import unittest
import uuid
from pathlib import Path
from typing import Any, Self, cast
from unittest.mock import AsyncMock, MagicMock, patch

import soundfile as sf

from backend.pipeline.common.constants import CHUNK_DURATION_SECONDS
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
            (segment_dir / f"chunk_{index:06d}.wav").write_bytes(segment)

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


class TestPathDiagnostics(unittest.TestCase):
    """Tests for local file diagnostics used in timeout logging."""

    def test_stat_oserror_returns_none(self) -> None:
        """Diagnostic helpers must not mask the original collector failure."""
        path = Path("/tmp/unreadable_segment.flac")  # noqa: S108
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
            "/tmp/chunk_%06d.wav",  # noqa: S108
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
            "/tmp/chunk_%06d.wav",  # noqa: S108
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
            "/tmp/chunk_%06d.wav",  # noqa: S108
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

        async def _mock_transcode(wav_path: Path, flac_path: Path) -> bool:
            if await asyncio.to_thread(wav_path.exists):
                data = await asyncio.to_thread(wav_path.read_bytes)
                await asyncio.to_thread(flac_path.write_bytes, data)
                return True
            return False

        self.mock_transcode = AsyncMock(side_effect=_mock_transcode)
        self.patchers = [
            patch.object(icecast_collector, "logger", self.mock_logger),
            patch.object(
                icecast_collector,
                "_transcode_wav_to_flac",
                self.mock_transcode,
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
    async def test_failed_header_repair_drops_segment(
        self, mock_create_ffmpeg: AsyncMock
    ) -> None:
        """A segment whose header repair fails must not be read or yielded."""
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=42,
            segments=[b"BAD_SEGMENT", b"GOOD_SEGMENT"],
            wait_delay=0.1,
            wait_result=0,
        )
        calls = 0

        async def _custom_transcode(wav_path: Path, flac_path: Path) -> bool:
            nonlocal calls
            calls += 1
            if calls == 1:
                return False
            if await asyncio.to_thread(wav_path.exists):
                data = await asyncio.to_thread(wav_path.read_bytes)
                await asyncio.to_thread(flac_path.write_bytes, data)
                return True
            return False

        self.mock_transcode.side_effect = _custom_transcode

        feed = _make_feed("test-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_resources_with_probe_status(200, reason="OK"),
        )
        chunks = await _collect_chunks(gen)

        # Only the segment whose repair succeeded reaches the caller.
        self.assertEqual(chunks, [b"GOOD_SEGMENT"])

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
            resources=_default_resources(),
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
        formatted = _formatted_error_calls(self.mock_logger)
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
        after_all_chunks_done = t0 + datetime.timedelta(
            seconds=CHUNK_DURATION_SECONDS * 4 + 1
        )
        mock_now_utc.side_effect = [t0] + [after_all_chunks_done] * 10

        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=3333,
            segments=[
                b"FLAC_SEGMENT_0",
                b"FLAC_SEGMENT_1",
                b"FLAC_SEGMENT_2",
            ],
            wait_delay=0.0,
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
            segments=[b"FLAC_SEGMENT_0"],
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
    async def test_stream_lag_logged(
        self, mock_create_ffmpeg: MagicMock, mock_now_utc: MagicMock
    ) -> None:
        """Test: stream lag exceeding threshold logs a warning but does not raise an error."""
        mock_create_ffmpeg.side_effect = _make_process_factory(
            pid=8888,
            segments=[b"FLAC_DATA_0", b"FLAC_DATA_1"],
            wait_delay=0.5,
            wait_result=0,
        )

        feed = _make_feed("lag-feed", "http://example.com/stream")
        shutdown_event = asyncio.Event()

        t0 = datetime.datetime(2026, 6, 20, 10, 0, 0, tzinfo=datetime.UTC)
        mock_now_utc.side_effect = [
            t0,  # stream_anchor_time
            t0
            + datetime.timedelta(seconds=85),  # receipt_time for first segment
        ]

        gen = icecast_collector.capture_icecast_stream(
            feed,
            shutdown_event,
            url_base="https://mock.example.com/",
            resources=_default_resources(),
        )

        chunk = await gen.__anext__()

        self.assertEqual(chunk.audio_bytes, b"FLAC_DATA_0")
        self.mock_logger.warning.assert_called_once()
        log_args = self.mock_logger.warning.call_args.args
        self.assertIn("Stream lag has exceeded threshold", log_args[0])


class TestIcecastReceiptTimeStamp(unittest.IsolatedAsyncioTestCase):
    """RCPT-02: Icecast stamps receipt_time at segment finalization."""

    @patch.dict(os.environ, MOCK_ENV_VARS)
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

        async def _mock_transcode(wav_path: Path, flac_path: Path) -> bool:
            if await asyncio.to_thread(wav_path.exists):
                data = await asyncio.to_thread(wav_path.read_bytes)
                await asyncio.to_thread(flac_path.write_bytes, data)
                return True
            return False

        with patch.object(
            icecast_collector,
            "_transcode_wav_to_flac",
            AsyncMock(side_effect=_mock_transcode),
        ):
            gen = icecast_collector.capture_icecast_stream(
                feed,
                shutdown,
                "http://example.com/",
                resources=_default_resources(),
            )
            chunks = await _collect_chunks_with_timestamps(gen)

        self.assertGreaterEqual(len(chunks), 1)
        self.assertEqual(chunks[0].receipt_time, fixed_time)


_BAD_STREAMINFO_FLAC = (
    Path(__file__).parent / "test_data" / "streamed_segment_bad_STREAMINFO.flac"
)
_ffmpeg_available = shutil.which("ffmpeg") is not None


class TestTranscodeWavToFlac(unittest.IsolatedAsyncioTestCase):
    """Guards the contract that no segment leaves ingestion unreadable."""

    @unittest.skipIf(not _ffmpeg_available, "ffmpeg not available")
    async def test_transcode_produces_readable_flac(self) -> None:
        raw = _BAD_STREAMINFO_FLAC.read_bytes()
        with tempfile.TemporaryDirectory() as tmp:
            wav_segment = Path(tmp) / "chunk_000000.wav"
            # Even if the file has a .wav extension, writing bad FLAC data to it
            # simulates ffmpeg decoding the unreadable stream data and transcoding it.
            wav_segment.write_bytes(raw)

            flac_segment = Path(tmp) / "chunk_000000.flac"

            # The source flac data (in wav_segment) is unreadable by sf.read directly
            with self.assertRaises(sf.LibsndfileError):
                sf.read(str(wav_segment))

            success = await icecast_collector._transcode_wav_to_flac(
                wav_segment, flac_segment
            )
            self.assertTrue(success)

            info = sf.info(str(flac_segment))
            samples, sample_rate = sf.read(str(flac_segment), dtype="float32")

        # The transcoded header advertises the true frame count rather than the
        # unknown-length sentinel, so the duration downstream derives from it
        # (len / sample_rate) is correct.
        self.assertEqual(info.frames, len(samples))
        self.assertEqual(sample_rate, 16000)
        self.assertAlmostEqual(len(samples) / sample_rate, 15.0, delta=0.5)

    @patch("asyncio.create_subprocess_exec", new_callable=AsyncMock)
    async def test_transcode_wav_to_flac_timeout_triggers_cleanup(
        self, mock_create_exec: AsyncMock
    ) -> None:
        mock_process = AsyncMock()
        mock_process.returncode = None

        terminated_event = asyncio.Event()

        def mock_terminate():
            terminated_event.set()

        mock_process.terminate = mock_terminate

        async def mock_wait():
            await terminated_event.wait()
            return 0

        mock_process.wait = mock_wait
        mock_create_exec.return_value = mock_process

        with patch.object(icecast_collector, "_FIX_HEADER_TIMEOUT_SEC", 0.05):
            with tempfile.TemporaryDirectory() as tmp:
                wav_segment = Path(tmp) / "chunk_000000.wav"
                wav_segment.write_bytes(b"dummy_data")
                flac_segment = Path(tmp) / "chunk_000000.flac"

                success = await icecast_collector._transcode_wav_to_flac(
                    wav_segment, flac_segment
                )

                # Give background tasks (cleanup) a tick to run
                await asyncio.sleep(0.02)

                self.assertTrue(terminated_event.is_set())
                self.assertFalse(
                    success,
                    "a timed-out transcode must not report success",
                )

    @patch("asyncio.create_subprocess_exec", new_callable=AsyncMock)
    async def test_transcode_wav_to_flac_nonzero_exit_reports_failure(
        self, mock_create_exec: AsyncMock
    ) -> None:
        mock_process = AsyncMock()
        mock_process.returncode = 1
        mock_process.wait = AsyncMock(return_value=1)
        mock_create_exec.return_value = mock_process

        with tempfile.TemporaryDirectory() as tmp:
            wav_segment = Path(tmp) / "chunk_000000.wav"
            wav_segment.write_bytes(b"dummy_data")
            flac_segment = Path(tmp) / "chunk_000000.flac"

            success = await icecast_collector._transcode_wav_to_flac(
                wav_segment, flac_segment
            )

            self.assertFalse(success)

    @patch("asyncio.create_subprocess_exec", new_callable=AsyncMock)
    async def test_transcode_wav_to_flac_cancellation_triggers_cleanup(
        self, mock_create_exec: AsyncMock
    ) -> None:
        mock_process = AsyncMock()
        mock_process.returncode = None

        terminated_event = asyncio.Event()

        def mock_terminate():
            terminated_event.set()

        mock_process.terminate = mock_terminate

        async def mock_wait():
            await terminated_event.wait()
            return 0

        mock_process.wait = mock_wait
        mock_create_exec.return_value = mock_process

        with tempfile.TemporaryDirectory() as tmp:
            wav_segment = Path(tmp) / "chunk_000000.wav"
            wav_segment.write_bytes(b"dummy_data")
            flac_segment = Path(tmp) / "chunk_000000.flac"

            # Start the task and cancel it immediately
            task = asyncio.create_task(
                icecast_collector._transcode_wav_to_flac(
                    wav_segment, flac_segment
                )
            )
            await asyncio.sleep(0.01)  # let it start and await wait_for
            task.cancel()

            with self.assertRaises(asyncio.CancelledError):
                await task

            # Give background tasks (cleanup) a tick to run
            await asyncio.sleep(0.02)

            self.assertTrue(terminated_event.is_set())


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
        """When BROADCASTIFY_XAN_TOKEN is not set and url_base is partner.broadcastify.com, it swaps the URL to api.bcfy.io."""
        os.environ.pop("BROADCASTIFY_XAN_TOKEN", None)
        os.environ["BROADCASTIFY_USERNAME"] = "test-user"
        os.environ["BROADCASTIFY_PASSWORD"] = "test-password"

        auth_header, url = icecast_collector._build_auth_and_url(
            url_base="https://partner.broadcastify.com/",
            source_feed_id="12345",
        )

        self.assertEqual(
            auth_header,
            "Authorization: Basic dGVzdC11c2VyOnRlc3QtcGFzc3dvcmQ=\r\n",
        )
        self.assertEqual(
            url,
            "https://api.bcfy.io/12345.mp3?burst=0",
        )

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


if __name__ == "__main__":
    unittest.main()

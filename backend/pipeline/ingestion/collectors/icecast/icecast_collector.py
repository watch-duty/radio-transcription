from __future__ import annotations

import asyncio
import base64
import collections
import contextlib
import dataclasses
import datetime
import logging
import os
import tempfile
import time
import uuid
from enum import Enum, auto
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlencode, urljoin

import aiohttp

from backend.pipeline.common.constants import (
    AUDIO_FORMAT,
    CHUNK_DURATION_SECONDS,
    FLAC_COMPRESSION_LEVEL,
    NUM_AUDIO_CHANNELS,
    SAMPLE_RATE_HZ,
)
from backend.pipeline.ingestion import status_reason_detail
from backend.pipeline.ingestion.collectors.failure_classification import (
    collector_failure,
    missing_source_feed_id_failure,
)
from backend.pipeline.ingestion.failure_classifiers import (
    ffmpeg as ffmpeg_classifier,
)
from backend.pipeline.ingestion.failure_classifiers import (
    http_status,
)
from backend.pipeline.ingestion.models import (
    CapturedChunk,
    CaptureResources,
    FeedFailure,
)
from backend.pipeline.storage.feed_store import FeedStatusReason

_CHUNK_DURATION = int(
    os.environ.get("INGESTION_SEGMENT_TIME_SEC", str(CHUNK_DURATION_SECONDS))
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from backend.pipeline.storage.feed_store import LeasedFeed

logger = logging.getLogger(__name__)

# Audio processing constants
SAMPLE_FORMAT = "s16"  # 16-bit signed integer

READ_TIMEOUT_SEC = 30  # Max seconds without a finalized segment before timeout
POLL_INTERVAL_SEC = 0.25  # Polling interval for segment file checks
STDERR_TAIL_LINES = 30  # Ring buffer size for ffmpeg stderr diagnostics

_STREAM_PROBE_TIMEOUT_SEC = 10
_MAX_ALLOWED_LAG_SECONDS = 60.0
FFMPEG_TIMEOUT_SEC = 15  # Network socket timeout for ffmpeg (in seconds)


# Stream endpoint semantics differ from item/API endpoints: a stream 404 means
# the configured mount/feed is currently unavailable, while other 4xx statuses
# are too ambiguous to classify without preserving the raw ffmpeg/probe reason.
_ICECAST_STREAM_HTTP_POLICY = http_status.HTTPStatusPolicy(
    exact={
        **http_status.DEFAULT_HTTP_STATUS_POLICY.exact,
        404: FeedStatusReason.SOURCE_OFFLINE,
    },
    default_4xx=None,
)


class _StreamProbeOutcome(Enum):
    TERMINAL_FAILURE = auto()
    STREAM_AVAILABLE = auto()
    INCONCLUSIVE = auto()


@dataclasses.dataclass(frozen=True)
class _StreamProbeResult:
    outcome: _StreamProbeOutcome
    failure: FeedFailure | None = None


def _build_auth_header() -> str:
    """Build Basic Auth header from env vars, raising if missing."""
    user = os.getenv("BROADCASTIFY_USERNAME")
    password = os.getenv("BROADCASTIFY_PASSWORD")
    if not user or not password:
        raise collector_failure(
            FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID,
            "missing_broadcastify_credentials",
        )
    credentials = f"{user}:{password}"
    encoded = base64.b64encode(credentials.encode()).decode()
    return f"Authorization: Basic {encoded}\r\n"


def _now_utc() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.UTC)


def _classify_stream_http_status(
    status: int,
    reason: str | None = None,
) -> FeedFailure | None:
    """Classify stream endpoint HTTP status into a typed feed failure."""
    status_reason = http_status.classify_http_status(
        status,
        policy=_ICECAST_STREAM_HTTP_POLICY,
    )
    if status_reason is None:
        return None
    http_diagnostic = f"HTTP error {status}"
    if reason:
        http_diagnostic = f"{http_diagnostic} {reason}"
    return collector_failure(
        status_reason,
        http_diagnostic,
    )


def _feed_failure_from_ffmpeg_info(
    info: ffmpeg_classifier.FfmpegFailureInfo,
    diagnostic_text: str | None = None,
) -> FeedFailure:
    """Convert ffmpeg failure info into an Icecast feed failure."""
    return collector_failure(
        info.status_reason,
        ffmpeg_classifier.render_ffmpeg_diagnostic(info, diagnostic_text),
    )


def _is_raw_ffmpeg_failure(info: ffmpeg_classifier.FfmpegFailureInfo) -> bool:
    """Return whether probe evidence should decide a raw ffmpeg fallback."""
    return (
        info.status_reason is FeedStatusReason.SYSTEM_COLLECTOR_ERROR
        and info.kind
        in {
            ffmpeg_classifier.FfmpegFailureKind.PROCESS_EXIT,
            ffmpeg_classifier.FfmpegFailureKind.PROCESS_SIGNAL,
            ffmpeg_classifier.FfmpegFailureKind.TIMEOUT,
        }
    )


def _headers_from_ffmpeg_auth_header(auth_header: str) -> dict[str, str]:
    """Convert ffmpeg header text into aiohttp headers for same-URL probes."""
    headers = {}
    for line in auth_header.splitlines():
        key, separator, value = line.partition(":")
        if separator:
            headers[key.strip()] = value.strip()
    return headers


async def _probe_stream_once(
    resources: CaptureResources,
    url: str,
    auth_header: str,
) -> _StreamProbeResult:
    """Probe the same stream URL once after ambiguous ffmpeg failures."""
    try:
        async with resources.http_session.get(
            url,
            headers=_headers_from_ffmpeg_auth_header(auth_header),
            timeout=aiohttp.ClientTimeout(total=_STREAM_PROBE_TIMEOUT_SEC),
        ) as response:
            classified = _classify_stream_http_status(
                response.status,
                response.reason,
            )
            if classified is not None:
                return _StreamProbeResult(
                    _StreamProbeOutcome.TERMINAL_FAILURE,
                    classified,
                )
            if response.status == 200:
                return _StreamProbeResult(_StreamProbeOutcome.STREAM_AVAILABLE)
            return _StreamProbeResult(_StreamProbeOutcome.INCONCLUSIVE)
    except Exception as exc:
        logger.warning("stream probe failed", exc_info=True)
        return _StreamProbeResult(
            _StreamProbeOutcome.TERMINAL_FAILURE,
            collector_failure(
                FeedStatusReason.SOURCE_UNREACHABLE,
                f"stream_probe_failed: {status_reason_detail.exception_text(exc)}",
            ),
        )


async def _build_stream_capture_failure(
    resources: CaptureResources,
    url: str,
    auth_header: str,
    *,
    exit_code: int | None,
    timed_out: bool,
    classification_text: str,
    stderr_snippet: str | None,
) -> FeedFailure:
    """Build the feed failure for terminal ffmpeg stream-capture evidence."""
    info = ffmpeg_classifier.classify_ffmpeg_failure(
        exit_code=exit_code,
        stderr_text=classification_text,
        timed_out=timed_out,
        http_policy=_ICECAST_STREAM_HTTP_POLICY,
    )
    if info is None:
        return collector_failure(
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
            "ffmpeg failed without classifiable terminal evidence",
        )

    if not _is_raw_ffmpeg_failure(info):
        return _feed_failure_from_ffmpeg_info(info, classification_text)

    probe = await _probe_stream_once(resources, url, auth_header)
    if (
        probe is not None
        and probe.outcome is _StreamProbeOutcome.TERMINAL_FAILURE
        and probe.failure is not None
    ):
        return probe.failure

    return _feed_failure_from_ffmpeg_info(info, stderr_snippet)


async def _drain_stderr(
    stderr: asyncio.StreamReader,
    tail: collections.deque[str],
    http_status_lines: collections.deque[str],
) -> None:
    """Read stderr line-by-line, keeping only the last *STDERR_TAIL_LINES* in *tail*.

    Draining prevents the OS pipe buffer from filling, which would deadlock
    ffmpeg on long-running streams.  The tail buffer provides error context
    when the process exits with a non-zero code.

    HTTP status lines are retained separately from the diagnostic tail because
    ffmpeg can emit many retry lines after the first 429/5xx.  Classification
    must not depend on the original HTTP error surviving the rolling log tail.

    Exceptions are caught and logged so they cannot mask exceptions from
    the caller's ``try`` block when this task is awaited in ``finally``.
    """
    try:
        while True:
            line = await stderr.readline()
            if not line:  # EOF — process closed stderr
                break
            text = line.decode("utf-8", errors="replace").rstrip()
            tail.append(text)
            if (
                ffmpeg_classifier.extract_http_status_from_ffmpeg_stderr(text)
                is not None
            ):
                http_status_lines.append(text)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.warning("stderr drain failed", exc_info=True)


def _segment_path(directory: Path, index: int) -> Path:
    return directory / f"chunk_{index:06d}.{AUDIO_FORMAT}"


def _path_size(path: Path) -> int | None:
    try:
        return path.stat().st_size
    except OSError:
        return None


def _path_mtime(path: Path) -> float | None:
    try:
        return path.stat().st_mtime
    except OSError:
        return None


async def _fix_flac_header(file_path: Path) -> None:
    """Re-encodes a FLAC segment in-place to write a valid STREAMINFO header.

    This is a critical performance and correctness fix: ffmpeg's segment muxer
    writes sequential FLAC files without seek tables and with an unknown number of frames
    in the header. When downstream services try to read these files, soundfile/libsndfile
    attempts to allocate an infinite array (2^63 - 1 frames) and crashes or thrashes.
    Re-encoding the segment in-place takes ~10ms but writes a perfect, standard FLAC header.
    """
    temp_path = file_path.with_name(f"{file_path.name}.fixed.flac")
    try:
        process = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-y",
            "-nostdin",
            "-i",
            str(file_path),
            "-acodec",
            "flac",
            str(temp_path),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await process.wait()
        if process.returncode == 0:
            temp_path.replace(file_path)
        else:
            logger.error(
                "ffmpeg header fix failed for %s with exit code %s",
                file_path,
                process.returncode,
            )
    except Exception as e:
        logger.exception(
            "Exception fixing FLAC header for %s: %s", file_path, e
        )
    finally:
        temp_path.unlink(missing_ok=True)


async def capture_icecast_stream(  # noqa: PLR0915, PLR0912
    feed: LeasedFeed,
    shutdown_event: asyncio.Event,
    url_base: str,
    resources: CaptureResources,
) -> AsyncIterator[CapturedChunk]:
    """
    Capture audio chunks from an Icecast stream using ffmpeg segment muxing.

    This implementation asks ffmpeg to write complete audio files for fixed
    CHUNK_DURATION_SECONDS windows. Each yielded chunk is therefore a standalone
    decodable file rather than an arbitrary slice of a continuous bytestream.

    Args:
        feed: Leased feed containing source_feed_id and metadata
        shutdown_event: Signals graceful shutdown request
        url_base: The base URL to prepend to the source_feed_id for stream access
        resources: Runtime-owned CaptureResources (currently unused by
            icecast — http_session is consumed by bcfy_calls).

    Yields:
        A CapturedChunk containing:
        - audio_bytes: Complete audio file bytes for the segment
        - chunk_start_time: The exact audio start time of the segment window
        - chunk_end_time: The exact audio end time of the segment window

    Raises:
        FeedFailure: If a known feed-level stream failure is detected.
        RuntimeError: If an unclassified ffmpeg/process failure is detected.

    """
    session_id = str(uuid.uuid4())
    source_feed_id = feed.get("source_feed_id")
    feed_id = feed.get("id")
    feed_name = feed.get("name")
    if not source_feed_id:
        logger.error(
            "Feed %s (%s) missing source_feed_id in feed_properties",
            feed_id,
            feed_name,
        )
        raise missing_source_feed_id_failure()

    auth_header = _build_auth_header()
    normalized_url_base = url_base if url_base.endswith("/") else f"{url_base}/"
    # Disable burst-on-connect behavior to prevent sputtering during initial ffmpeg streaming.
    # Note: Some Icecast servers may not support this parameter.
    params = urlencode({"burst": 0})
    url = urljoin(normalized_url_base, f"{source_feed_id.strip()}.mp3?{params}")

    segment_dir_parent = resources.segment_temp_dir
    with tempfile.TemporaryDirectory(
        prefix="icecast_segments_", dir=segment_dir_parent
    ) as tmp_dir:
        segment_dir = Path(tmp_dir)
        segment_pattern = str(segment_dir / f"chunk_%06d.{AUDIO_FORMAT}")

        process = await _create_ffmpeg_process(
            url, segment_pattern, auth_header
        )
        if (
            process.stderr is None
        ):  # pragma: no cover — guaranteed by stderr=PIPE
            msg = "stderr is None; _create_ffmpeg_process must use stderr=PIPE"
            raise RuntimeError(msg)
        stderr_tail: collections.deque[str] = collections.deque(
            maxlen=STDERR_TAIL_LINES
        )
        stderr_http_status_lines: collections.deque[str] = collections.deque(
            maxlen=STDERR_TAIL_LINES
        )
        drain_task = asyncio.create_task(
            _drain_stderr(process.stderr, stderr_tail, stderr_http_status_lines)
        )
        logger.info(
            "Feed %s (%s): Started ffmpeg segmenter (PID: %s)",
            feed_id,
            feed_name,
            process.pid,
        )

        next_index = 0
        last_activity_time = time.monotonic()
        wait_task = asyncio.create_task(process.wait())

        # Anchor the stream timeline to the exact moment ffmpeg starts
        stream_anchor_time = _now_utc()

        try:
            while True:
                if shutdown_event.is_set():
                    logger.info(
                        "Feed %s (%s): Shutdown requested, stopping capture",
                        feed_id,
                        feed_name,
                    )
                    return

                current_segment = _segment_path(segment_dir, next_index)
                next_segment = _segment_path(segment_dir, next_index + 1)
                process_done = wait_task.done()

                # Read a segment only once we know ffmpeg finished writing it.
                # A segment is considered finalized when either:
                # - the next segment exists, or
                # - ffmpeg has exited.
                if current_segment.exists() and (
                    next_segment.exists() or process_done
                ):
                    # SLO: receipt_time stamp — Icecast segment finalized, bytes available
                    receipt_time = _now_utc()

                    # Fix the FLAC header in-place so downstream decoders don't crash
                    await _fix_flac_header(current_segment)

                    segment_bytes = await asyncio.to_thread(
                        current_segment.read_bytes
                    )
                    if segment_bytes:
                        # Calculate the start and end times of this specific chunk's window
                        chunk_start_time = (
                            stream_anchor_time
                            + datetime.timedelta(
                                seconds=next_index * _CHUNK_DURATION
                            )
                        )
                        chunk_end_time = chunk_start_time + datetime.timedelta(
                            seconds=_CHUNK_DURATION
                        )
                        if process_done:
                            chunk_end_time = min(chunk_end_time, _now_utc())

                        # Guard against cumulative network or system lag by measuring the drift
                        # between wall-clock receipt time and expected stream time.
                        lag = (receipt_time - chunk_end_time).total_seconds()
                        if lag > _MAX_ALLOWED_LAG_SECONDS:
                            logger.warning(
                                "Feed %s (%s): Stream lag has exceeded threshold "
                                "(%.1fs > %.1fs).",
                                feed_id,
                                feed_name,
                                lag,
                                _MAX_ALLOWED_LAG_SECONDS,
                            )

                        yield CapturedChunk(
                            audio_bytes=segment_bytes,
                            chunk_start_time=chunk_start_time,
                            chunk_end_time=chunk_end_time,
                            session_id=session_id,
                            receipt_time=receipt_time,
                        )

                        last_activity_time = time.monotonic()
                    await asyncio.to_thread(
                        current_segment.unlink, missing_ok=True
                    )
                    next_index += 1
                    continue

                # If ffmpeg is done and there is no pending finalized segment,
                # we are finished.
                if process_done and not current_segment.exists():
                    exit_code = wait_task.result()
                    if exit_code != 0:
                        stderr_snippet = (
                            "\n".join(stderr_tail) if stderr_tail else None
                        )
                        stderr_log_text = (
                            stderr_snippet or "(no stderr captured)"
                        )
                        logger.error(
                            "Feed %s (%s) ffmpeg exited with code %d; stderr tail:\n%s",
                            feed_id,
                            feed_name,
                            exit_code,
                            stderr_log_text,
                        )
                        classification_text = (
                            "\n".join(stderr_http_status_lines)
                            if stderr_http_status_lines
                            else stderr_snippet or ""
                        )
                        failure = await _build_stream_capture_failure(
                            resources,
                            url,
                            auth_header,
                            exit_code=exit_code,
                            timed_out=False,
                            classification_text=classification_text,
                            stderr_snippet=stderr_snippet,
                        )
                        raise failure
                    logger.info(
                        "Feed %s (%s): ffmpeg exited normally",
                        feed_id,
                        feed_name,
                    )
                    return

                last_activity_age_sec = time.monotonic() - last_activity_time
                if last_activity_age_sec > READ_TIMEOUT_SEC:
                    stderr_snippet = (
                        "\n".join(stderr_tail) if stderr_tail else None
                    )
                    stderr_log_text = stderr_snippet or "(no stderr captured)"
                    logger.error(
                        "Feed %s (%s) no finalized segment within %ss; "
                        "next_index=%s current_segment_exists=%s "
                        "next_segment_exists=%s current_segment_size=%s "
                        "next_segment_size=%s current_segment_mtime=%s "
                        "next_segment_mtime=%s last_activity_age_sec=%.3f "
                        "read_timeout_sec=%s ffmpeg_pid=%s "
                        "ffmpeg_returncode=%s; stderr tail:\n%s",
                        feed_id,
                        feed_name,
                        READ_TIMEOUT_SEC,
                        next_index,
                        current_segment.exists(),
                        next_segment.exists(),
                        _path_size(current_segment),
                        _path_size(next_segment),
                        _path_mtime(current_segment),
                        _path_mtime(next_segment),
                        last_activity_age_sec,
                        READ_TIMEOUT_SEC,
                        process.pid,
                        process.returncode,
                        stderr_log_text,
                    )
                    classification_text = (
                        "\n".join(stderr_http_status_lines)
                        if stderr_http_status_lines
                        else stderr_snippet or ""
                    )
                    failure = await _build_stream_capture_failure(
                        resources,
                        url,
                        auth_header,
                        exit_code=process.returncode,
                        timed_out=True,
                        classification_text=classification_text,
                        stderr_snippet=stderr_snippet,
                    )
                    raise failure

                await asyncio.sleep(POLL_INTERVAL_SEC)

        finally:
            drain_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await drain_task
            if not wait_task.done():
                wait_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await wait_task
            await _cleanup_ffmpeg_process(process, str(feed_id), str(feed_name))


async def _create_ffmpeg_process(
    url: str,
    segment_pattern: str,
    auth_header: str,
) -> asyncio.subprocess.Process:
    """
    Create and launch ffmpeg subprocess configured for segmented audio output.

    Args:
        url: The stream URL to connect to
        segment_pattern: Segment filename pattern for ffmpeg
        auth_header: HTTP Authorization header for the stream

    Returns:
        The subprocess process object

    """
    # Low-latency live stream network optimizations used below:
    # 1. -analyzeduration 0 / -probesize 32768: Bypasses the default 5-second/5MB
    #    initialization handicap, instantly locking the demuxer on the first 32KB of data.
    #    This reduces the time-to-first-byte from the start timestamp when ffmpeg starts recording.
    # 2. -fflags nobuffer+flush_packets: Drops the demuxer/muxer packet buffering
    #    for true real-time network flow.
    # 3. discardcorrupt: Mitigates parsing crashes over TCP jitter, which is necessary
    #    since our micro probesize doesn't deeply validate stream integrity.
    # 4. -reconnect 1 / -reconnect_at_eof 1 / -reconnect_streamed 1: Enables native
    #    HTTP/TCP reconnects for short internet drops.
    # 5. -timeout: Sets a socket I/O timeout to prevent ffmpeg from
    #    hanging indefinitely on a stalled TCP connection, allowing it to exit
    #    gracefully instead of requiring a hard force-kill.
    return await asyncio.create_subprocess_exec(
        "ffmpeg", "-nostdin",
        "-reconnect", "1",
        "-reconnect_at_eof", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "2",
        "-reconnect_on_http_error", "429,500,502,503,504",
        "-analyzeduration", "0",
        "-probesize", "32768",
        "-fflags", "nobuffer+flush_packets+discardcorrupt",
        "-timeout", str(FFMPEG_TIMEOUT_SEC * 1_000_000),
        "-headers", auth_header,
        "-i", url,
        "-vn", "-sn", "-dn",
        "-acodec", AUDIO_FORMAT,
        "-ar", str(SAMPLE_RATE_HZ),
        "-sample_fmt", SAMPLE_FORMAT,
        "-ac", str(NUM_AUDIO_CHANNELS),
        "-compression_level", FLAC_COMPRESSION_LEVEL,
        "-f", "segment",
        "-segment_time", str(_CHUNK_DURATION),
        "-segment_format", AUDIO_FORMAT,
        "-reset_timestamps", "1",
        "-segment_start_number", "0",
        segment_pattern,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )  # fmt: skip


async def _cleanup_ffmpeg_process(
    process: asyncio.subprocess.Process,
    feed_id: str,
    feed_name: str,
) -> None:
    """
    Clean up and terminate ffmpeg process.

    Args:
        process: The ffmpeg subprocess process object
        feed_id: The feed ID for logging
        feed_name: The feed name for logging

    """
    if process.returncode is None:
        try:
            process.terminate()
            await asyncio.wait_for(process.wait(), timeout=5)
        except TimeoutError:
            process.kill()
            logger.warning(
                "Feed %s (%s): Force-killed ffmpeg process", feed_id, feed_name
            )
            await process.wait()
        except Exception as e:
            logger.exception(
                "Feed %s (%s): Error terminating ffmpeg: %s",
                feed_id,
                feed_name,
                e,
            )

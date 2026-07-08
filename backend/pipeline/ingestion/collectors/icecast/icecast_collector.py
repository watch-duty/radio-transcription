from __future__ import annotations

import asyncio
import base64
import collections
import contextlib
import dataclasses
import datetime
import io
import logging
import os
import tempfile
import time
import uuid
from enum import Enum, auto
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode, urljoin

import aiohttp
import numpy as np
import soundfile as sf

from backend.pipeline.common.constants import (
    CHUNK_DURATION_SECONDS,
    NUM_AUDIO_CHANNELS,
    SAMPLE_RATE_HZ,
)
from backend.pipeline.ingestion import constants, status_reason_detail
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


READ_TIMEOUT_SEC = 30  # Max seconds without a finalized segment before timeout
POLL_INTERVAL_SEC = 0.25  # Polling interval for segment file checks
STDERR_TAIL_LINES = 30  # Ring buffer size for ffmpeg stderr diagnostics

_STREAM_PROBE_TIMEOUT_SEC = 10
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


def _build_auth_and_url(url_base: str, source_feed_id: str) -> tuple[str, str]:
    """Build the auth header and stream URL, supporting both XAN token and Basic Auth."""
    xan_token = os.getenv("BROADCASTIFY_XAN_TOKEN")
    params: dict[str, int | str] = {"burst": 0}

    if xan_token:
        # XAN token streams are served on the partner relay endpoint (e.g. partner.broadcastify.com)
        normalized_url_base = (
            url_base if url_base.endswith("/") else f"{url_base}/"
        )
        params["xan"] = xan_token
        url = urljoin(
            normalized_url_base,
            f"{source_feed_id.strip()}.mp3?{urlencode(params)}",
        )
        return "", url

    # Basic Auth (non-XAN) requests must go to the public Icecast relay endpoint
    public_url_base = os.getenv(
        "BCFY_FEEDS_PUBLIC_URL_BASE",
        constants.BCFY_FEEDS_PUBLIC_URL_BASE,
    )
    basic_auth_url_base = (
        public_url_base
        if url_base.strip().rstrip("/")
        == constants.BCFY_FEEDS_PARTNER_URL_BASE.rstrip("/")
        else url_base
    )
    normalized_url_base = (
        basic_auth_url_base
        if basic_auth_url_base.endswith("/")
        else f"{basic_auth_url_base}/"
    )

    user = os.getenv("BROADCASTIFY_USERNAME")
    password = os.getenv("BROADCASTIFY_PASSWORD")
    if not user or not password:
        raise collector_failure(
            FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID,
            "missing_broadcastify_credentials",
        )
    credentials = f"{user}:{password}"
    encoded = base64.b64encode(credentials.encode()).decode()
    auth_header = f"Authorization: Basic {encoded}\r\n"
    url = urljoin(
        normalized_url_base,
        f"{source_feed_id.strip()}.mp3?{urlencode(params)}",
    )
    return auth_header, url


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


def _segment_path(directory: Path, index: int, ext: str = "pcm") -> Path:
    return directory / f"chunk_{index:06d}.{ext}"


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


def _encode_pcm_bytes_to_flac(pcm_bytes: bytes) -> bytes:
    """Encodes raw 16-bit little-endian PCM bytes into FLAC format in memory.

    Args:
        pcm_bytes: Raw PCM audio data bytes.

    Returns:
        Encoded FLAC audio data as bytes.
    """
    samples = np.frombuffer(pcm_bytes, dtype="<i2")
    if NUM_AUDIO_CHANNELS > 1:
        samples = samples.reshape(-1, NUM_AUDIO_CHANNELS)
    buf = io.BytesIO()
    sf.write(buf, samples, SAMPLE_RATE_HZ, format="FLAC", subtype="PCM_16")
    return buf.getvalue()


async def _encode_pcm_segment_to_flac(pcm_path: Path) -> bytes | None:
    """Reads a raw PCM segment and encodes it to FLAC bytes in-process.

    The primary ffmpeg process segments to headerless raw PCM rather than
    FLAC: ffmpeg's FLAC muxer only patches STREAMINFO's sample count when a
    packet carries side-data emitted by its own encoder on close, which
    stream-copied/segment-rotated packets never carry (verified against
    ffmpeg's flacenc.c) -- so a segment written directly to FLAC would need
    a full second re-encode anyway to be readable downstream. Encoding a
    complete, already-buffered PCM segment here with soundfile gets a
    correct header for free (no live/open-ended stream to guess against),
    without a second ffmpeg subprocess.

    Args:
        pcm_path: Path to the raw PCM segment file on disk.

    Returns:
        Encoded FLAC audio bytes if successful, or None if encoding fails or file is empty.
    """
    try:
        pcm_bytes = await asyncio.to_thread(pcm_path.read_bytes)
    finally:
        await asyncio.to_thread(pcm_path.unlink, missing_ok=True)

    if not pcm_bytes:
        return None

    try:
        return await asyncio.to_thread(_encode_pcm_bytes_to_flac, pcm_bytes)
    except Exception:
        logger.exception(
            "In-process FLAC encode failed for segment %s (%d bytes)",
            pcm_path,
            len(pcm_bytes),
        )
        return None


def _validate_feed_and_build_url(
    *,
    feed: LeasedFeed,
    url_base: str,
) -> tuple[Any, Any, str, str]:
    """Validates leased feed properties and builds connection URL and auth header.

    Args:
        feed: The leased feed from database.
        url_base: Base URL for stream connection.

    Returns:
        A tuple of (feed_id, feed_name, auth_header, url).

    Raises:
        FeedFailure: If source_feed_id is missing.
    """
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

    auth_header, url = _build_auth_and_url(url_base, source_feed_id)
    return feed_id, feed_name, auth_header, url


async def _cleanup_capture_tasks(
    *,
    drain_task: asyncio.Task[None],
    wait_task: asyncio.Task[int],
    process: asyncio.subprocess.Process,
    feed_id: Any,
    feed_name: Any,
) -> None:
    """Cancels background tasks and cleans up ffmpeg process.

    Args:
        drain_task: Task draining stderr.
        wait_task: Task waiting on process exit.
        process: The ffmpeg process.
        feed_id: Feed ID for logging.
        feed_name: Feed name for logging.
    """
    drain_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await drain_task
    if not wait_task.done():
        wait_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await wait_task
    await _cleanup_ffmpeg_process(process, str(feed_id), feed_name)


async def _process_finalized_segment(
    *,
    current_segment_pcm: Path,
    next_index: int,
    stream_anchor_time: datetime.datetime,
    process_done: bool,
    previous_receipt_time: datetime.datetime | None,
    session_id: str,
    feed_id: Any,
    feed_name: Any,
) -> tuple[CapturedChunk | None, datetime.datetime]:
    """Processes a finalized PCM segment on disk into a decodable FLAC CapturedChunk.

    Args:
        current_segment_pcm: Path to the finalized PCM segment file.
        next_index: Sequence index of the segment.
        stream_anchor_time: UTC timestamp when stream capture started.
        process_done: Whether ffmpeg has exited.
        previous_receipt_time: Receipt timestamp of the previous segment.
        session_id: Unique capture session identifier.
        feed_id: The feed ID for logging.
        feed_name: The feed name for logging.

    Returns:
        A tuple of (CapturedChunk or None if encoding failed, updated previous_receipt_time).
    """
    # SLO: receipt_time stamp — Icecast segment finalized, bytes available
    receipt_time = _now_utc()
    segment_bytes = await _encode_pcm_segment_to_flac(current_segment_pcm)
    if segment_bytes is None:
        logger.warning(
            "Feed %s (%s): dropping segment %s after failed FLAC encode",
            feed_id,
            feed_name,
            current_segment_pcm,
        )
        return (
            None,
            receipt_time
            if previous_receipt_time is None
            else previous_receipt_time,
        )

    chunk_start_time = stream_anchor_time + datetime.timedelta(
        seconds=next_index * _CHUNK_DURATION
    )
    chunk_end_time = chunk_start_time + datetime.timedelta(
        seconds=_CHUNK_DURATION
    )
    if process_done:
        chunk_end_time = min(chunk_end_time, _now_utc())

    stream_interval_lag_sec = None
    if previous_receipt_time is not None:
        stream_interval_lag_sec = (
            receipt_time - previous_receipt_time
        ).total_seconds() - _CHUNK_DURATION

    chunk = CapturedChunk(
        audio_bytes=segment_bytes,
        chunk_start_time=chunk_start_time,
        chunk_end_time=chunk_end_time,
        session_id=session_id,
        receipt_time=receipt_time,
        stream_interval_lag_sec=stream_interval_lag_sec,
    )
    return chunk, receipt_time


async def _handle_process_done_no_segment(
    *,
    process_done: bool,
    current_exists: bool,
    wait_task: asyncio.Task[int],
    resources: CaptureResources,
    url: str,
    auth_header: str,
    feed_id: Any,
    feed_name: Any,
    stderr_tail: collections.deque[str],
    stderr_http_status_lines: collections.deque[str],
) -> bool:
    """Checks whether ffmpeg exited normally or failed when no segment is pending.

    Args:
        process_done: Whether the ffmpeg wait task has completed.
        current_exists: Whether the current segment file exists on disk.
        wait_task: The asyncio task waiting on ffmpeg process termination.
        resources: Runtime-owned CaptureResources.
        url: The stream URL.
        auth_header: HTTP Authorization header for the stream.
        feed_id: The feed ID for logging.
        feed_name: The feed name for logging.
        stderr_tail: Ring buffer containing recent ffmpeg stderr lines.
        stderr_http_status_lines: Ring buffer containing HTTP status lines.

    Returns:
        True if capture is finished and loop should exit; False otherwise.

    Raises:
        FeedFailure: If ffmpeg exited with a non-zero status.
    """
    if not (process_done and not current_exists):
        return False

    exit_code = wait_task.result()
    if exit_code != 0:
        stderr_snippet = "\n".join(stderr_tail) if stderr_tail else None
        stderr_log_text = stderr_snippet or "(no stderr captured)"
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
        if (
            failure.status_reason.owner == "source"
            or "Stream capture timed out" in failure.reason
        ):
            logger.warning(
                "Feed %s (%s) ffmpeg exited with code %d; stderr tail:\n%s",
                feed_id,
                feed_name,
                exit_code,
                stderr_log_text,
            )
        else:
            logger.error(
                "Feed %s (%s) ffmpeg exited with code %d; stderr tail:\n%s",
                feed_id,
                feed_name,
                exit_code,
                stderr_log_text,
            )
        raise failure
    logger.info(
        "Feed %s (%s): ffmpeg exited normally",
        feed_id,
        feed_name,
    )
    return True


async def _check_read_timeout(
    *,
    last_activity_age_sec: float,
    segment_dir: Path,
    next_index: int,
    current_exists: bool,
    next_exists: bool,
    process: asyncio.subprocess.Process,
    resources: CaptureResources,
    url: str,
    auth_header: str,
    feed_id: Any,
    feed_name: Any,
    stderr_tail: collections.deque[str],
    stderr_http_status_lines: collections.deque[str],
) -> None:
    """Checks for segment read timeout and raises an appropriate FeedFailure.

    Args:
        last_activity_age_sec: Seconds since last forward progress.
        segment_dir: Temporary directory where segments are written.
        next_index: Index of the current expected segment.
        current_exists: Whether current segment file exists on disk.
        next_exists: Whether next segment file exists on disk.
        process: The running ffmpeg subprocess.
        resources: Runtime-owned CaptureResources.
        url: The stream URL.
        auth_header: HTTP Authorization header for the stream.
        feed_id: The feed ID for logging.
        feed_name: The feed name for logging.
        stderr_tail: Ring buffer containing recent ffmpeg stderr lines.
        stderr_http_status_lines: Ring buffer containing HTTP status lines.

    Raises:
        FeedFailure: When read timeout is exceeded.
    """
    if last_activity_age_sec <= READ_TIMEOUT_SEC:
        return

    stderr_snippet = "\n".join(stderr_tail) if stderr_tail else None
    stderr_log_text = stderr_snippet or "(no stderr captured)"
    current_segment_pcm = _segment_path(segment_dir, next_index, "pcm")
    next_segment_pcm = _segment_path(segment_dir, next_index + 1, "pcm")
    (
        current_segment_size,
        next_segment_size,
        current_segment_mtime,
        next_segment_mtime,
    ) = await asyncio.gather(
        asyncio.to_thread(_path_size, current_segment_pcm),
        asyncio.to_thread(_path_size, next_segment_pcm),
        asyncio.to_thread(_path_mtime, current_segment_pcm),
        asyncio.to_thread(_path_mtime, next_segment_pcm),
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
    log_msg = (
        "Feed %s (%s) no finalized segment within %ss; "
        "next_index=%s current_segment_exists=%s "
        "next_segment_exists=%s current_segment_size=%s "
        "next_segment_size=%s current_segment_mtime=%s "
        "next_segment_mtime=%s last_activity_age_sec=%.3f "
        "read_timeout_sec=%s ffmpeg_pid=%s "
        "ffmpeg_returncode=%s; stderr tail:\n%s"
    )
    log_args = (
        feed_id,
        feed_name,
        READ_TIMEOUT_SEC,
        next_index,
        current_exists,
        next_exists,
        current_segment_size,
        next_segment_size,
        current_segment_mtime,
        next_segment_mtime,
        last_activity_age_sec,
        READ_TIMEOUT_SEC,
        process.pid,
        process.returncode,
        stderr_log_text,
    )
    if (
        failure.status_reason.owner == "source"
        or "Stream capture timed out" in failure.reason
    ):
        logger.warning(log_msg, *log_args)
    else:
        logger.error(log_msg, *log_args)
    raise failure


async def capture_icecast_stream(
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
    feed_id, feed_name, auth_header, url = _validate_feed_and_build_url(
        feed=feed, url_base=url_base
    )

    segment_dir_parent = resources.segment_temp_dir
    with tempfile.TemporaryDirectory(
        prefix="icecast_segments_", dir=segment_dir_parent
    ) as tmp_dir:
        segment_dir = Path(tmp_dir)
        segment_pattern_pcm = str(segment_dir / "chunk_%06d.pcm")

        process = await _create_ffmpeg_process(
            url, segment_pattern_pcm, auth_header
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
        # Tracks receipt_time of the previously yielded segment so lag can be
        # measured as a per-interval delta rather than accumulated against a
        # fixed anchor, which would conflate real backlog with ordinary
        # long-session source-clock drift.
        previous_receipt_time: datetime.datetime | None = None

        try:
            while True:
                if shutdown_event.is_set():
                    logger.info(
                        "Feed %s (%s): Shutdown requested, stopping capture",
                        feed_id,
                        feed_name,
                    )
                    return

                current_segment_pcm = _segment_path(
                    segment_dir, next_index, "pcm"
                )
                next_segment_pcm = _segment_path(
                    segment_dir, next_index + 1, "pcm"
                )
                process_done = wait_task.done()

                # Run file checks in threadpool to prevent event loop stalls on disk latency
                current_exists = await asyncio.to_thread(
                    current_segment_pcm.exists
                )
                next_exists = await asyncio.to_thread(next_segment_pcm.exists)

                # Read a segment only once we know ffmpeg finished writing it.
                # A segment is considered finalized when either:
                # - the next segment exists, or
                # - ffmpeg has exited.
                if current_exists and (next_exists or process_done):
                    last_activity_time = time.monotonic()
                    (
                        chunk,
                        previous_receipt_time,
                    ) = await _process_finalized_segment(
                        current_segment_pcm=current_segment_pcm,
                        next_index=next_index,
                        stream_anchor_time=stream_anchor_time,
                        process_done=process_done,
                        previous_receipt_time=previous_receipt_time,
                        session_id=session_id,
                        feed_id=feed_id,
                        feed_name=feed_name,
                    )
                    next_index += 1
                    if chunk is not None:
                        yield chunk
                    continue

                # If ffmpeg is done and there is no pending finalized segment,
                # we are finished.
                if await _handle_process_done_no_segment(
                    process_done=process_done,
                    current_exists=current_exists,
                    wait_task=wait_task,
                    resources=resources,
                    url=url,
                    auth_header=auth_header,
                    feed_id=feed_id,
                    feed_name=feed_name,
                    stderr_tail=stderr_tail,
                    stderr_http_status_lines=stderr_http_status_lines,
                ):
                    return

                last_activity_age_sec = time.monotonic() - last_activity_time
                await _check_read_timeout(
                    last_activity_age_sec=last_activity_age_sec,
                    segment_dir=segment_dir,
                    next_index=next_index,
                    current_exists=current_exists,
                    next_exists=next_exists,
                    process=process,
                    resources=resources,
                    url=url,
                    auth_header=auth_header,
                    feed_id=feed_id,
                    feed_name=feed_name,
                    stderr_tail=stderr_tail,
                    stderr_http_status_lines=stderr_http_status_lines,
                )

                await asyncio.sleep(POLL_INTERVAL_SEC)

        finally:
            await _cleanup_capture_tasks(
                drain_task=drain_task,
                wait_task=wait_task,
                process=process,
                feed_id=feed_id,
                feed_name=feed_name,
            )


async def _create_ffmpeg_process(
    url: str,
    segment_pattern: str,
    auth_header: str,
) -> asyncio.subprocess.Process:
    """Create and launch ffmpeg subprocess configured for segmented audio output.

    Args:
        url: The stream URL to connect to
        segment_pattern: Segment filename pattern for ffmpeg (should end in .pcm)
        auth_header: HTTP Authorization header for the stream, if applicable

    Returns:
        The subprocess process object

    """
    # Low-latency live stream network optimizations:
    # 1. -analyzeduration 0 / -probesize 32768: Bypasses the default
    #    5-second/5MB initialization handicap, instantly locking the demuxer
    #    on the first 32KB of data.
    # 2. -fflags nobuffer+flush_packets+discardcorrupt: Drops the demuxer/muxer
    #    packet buffering for true real-time network flow.
    # 3. -flags low_delay: Configures decoders/demuxers to minimize delay.
    # 4. -reconnect 1 / -reconnect_streamed 1: Enables native HTTP/TCP
    #    reconnects.
    # 5. -reconnect_delay_max 30: Wait up to 30 seconds backoff between
    #    reconnect attempts.
    # 6. -timeout: Sets socket timeout to prevent indefinite hangs.
    # 7. -af aresample: dynamic audio resampling to absorb clock drift and
    #    prevent delay pools.
    cmd = [
        "ffmpeg",
        "-nostdin",
        "-reconnect",
        "1",
        "-reconnect_at_eof",
        "1",
        "-reconnect_streamed",
        "1",
        "-reconnect_delay_max",
        "30",
        "-reconnect_on_http_error",
        "429,500,502,503,504",
        "-analyzeduration",
        "0",
        "-probesize",
        "32768",
        "-fflags",
        "nobuffer+flush_packets+discardcorrupt",
        "-flags",
        "low_delay",
        "-timeout",
        str(FFMPEG_TIMEOUT_SEC * 1_000_000),
    ]

    if auth_header:
        cmd.extend(["-headers", auth_header])

    cmd.extend(
        [
            "-i",
            url,
            "-vn",
            "-sn",
            "-dn",
            "-af",
            "aresample=async=1:min_hard_comp=0.100:first_pts=0",
            "-acodec",
            "pcm_s16le",
            "-ar",
            str(SAMPLE_RATE_HZ),
            "-ac",
            str(NUM_AUDIO_CHANNELS),
            "-f",
            "segment",
            "-segment_time",
            str(_CHUNK_DURATION),
            "-segment_format",
            "s16le",
            "-reset_timestamps",
            "1",
            "-segment_start_number",
            "0",
            segment_pattern,
        ]
    )

    return await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )


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

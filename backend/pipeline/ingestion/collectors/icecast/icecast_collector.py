from __future__ import annotations

import asyncio
import base64
import collections
import contextlib
import datetime
import logging
import os
import tempfile
import time
import uuid
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
from backend.pipeline.ingestion.collectors.failure_classification import (
    FailureClassification,
    collector_failure,
    missing_source_feed_id_failure,
)
from backend.pipeline.ingestion.collectors.failure_classifiers import (
    ffmpeg as ffmpeg_classifier,
)
from backend.pipeline.ingestion.collectors.failure_classifiers import (
    http_status,
)
from backend.pipeline.ingestion.models import (
    CapturedChunk,
    CaptureEvent,
    CaptureResources,
    FeedFailure,
)
from backend.pipeline.storage.feed_store import FeedStatusReason

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


def _build_auth_header() -> str:
    """Build Basic Auth header from env vars, raising if missing."""
    user = os.getenv("BROADCASTIFY_USERNAME")
    password = os.getenv("BROADCASTIFY_PASSWORD")
    if not user or not password:
        raise collector_failure(
            FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
            "missing_broadcastify_credentials",
        )
    credentials = f"{user}:{password}"
    encoded = base64.b64encode(credentials.encode()).decode()
    return f"Authorization: Basic {encoded}\r\n"


def _now_utc() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.UTC)


def _classify_stream_http_status(status: int) -> FeedFailure | None:
    """Classify stream endpoint HTTP status into a typed feed failure."""
    classification = http_status.classify_http_status(
        status,
        reason_prefix="stream_http",
        policy=_ICECAST_STREAM_HTTP_POLICY,
    )
    if classification is None:
        return None
    return collector_failure(
        classification.status_reason,
        classification.reason,
    )


def _feed_failure_from_classification(
    classification: FailureClassification,
) -> FeedFailure:
    """Convert neutral classifier output into an Icecast feed failure."""
    return collector_failure(
        classification.status_reason,
        classification.reason,
    )


def _is_raw_ffmpeg_failure(classification: FailureClassification) -> bool:
    """Return whether probe evidence should decide a raw ffmpeg fallback."""
    return (
        classification.status_reason is FeedStatusReason.SYSTEM_COLLECTOR_ERROR
        and (
            classification.reason.startswith("ffmpeg_")
            or classification.reason == "capture_timeout"
        )
    )


def _headers_from_ffmpeg_auth_header(auth_header: str) -> dict[str, str]:
    """Convert ffmpeg header text into aiohttp headers for same-URL probes."""
    headers = {}
    for line in auth_header.splitlines():
        key, separator, value = line.partition(":")
        if separator:
            headers[key.strip()] = value.strip()
    return headers


def _probe_keeps_raw_reason(probe_failure: FeedFailure) -> bool:
    """Return whether ambiguous probe evidence should preserve raw ffmpeg reason."""
    return (
        probe_failure.status_reason is FeedStatusReason.SYSTEM_COLLECTOR_ERROR
        and str(probe_failure)
        in {"stream_available", "stream_probe_inconclusive"}
    )


async def _probe_stream_once(
    resources: CaptureResources,
    url: str,
    auth_header: str,
) -> FeedFailure | None:
    """Probe the same stream URL once after ambiguous ffmpeg failures."""
    try:
        async with resources.http_session.get(
            url,
            headers=_headers_from_ffmpeg_auth_header(auth_header),
            timeout=aiohttp.ClientTimeout(total=_STREAM_PROBE_TIMEOUT_SEC),
        ) as response:
            classified = _classify_stream_http_status(response.status)
            if classified is not None:
                return classified
            if response.status == 200:
                return collector_failure(
                    FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
                    "stream_available",
                )
            return collector_failure(
                FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
                "stream_probe_inconclusive",
            )
    except Exception:
        logger.warning("stream probe failed", exc_info=True)
        return collector_failure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "stream_probe_failed",
        )


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


async def capture_icecast_stream(  # noqa: PLR0912, PLR0915
    feed: LeasedFeed,
    shutdown_event: asyncio.Event,
    url_base: str,
    resources: CaptureResources,
) -> AsyncIterator[CaptureEvent]:
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

    with tempfile.TemporaryDirectory(prefix="icecast_segments_") as tmp_dir:
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
                    segment_bytes = await asyncio.to_thread(
                        current_segment.read_bytes
                    )
                    if segment_bytes:
                        # Calculate the start and end times of this specific chunk's window
                        chunk_start_time = (
                            stream_anchor_time
                            + datetime.timedelta(
                                seconds=next_index * CHUNK_DURATION_SECONDS
                            )
                        )
                        chunk_end_time = chunk_start_time + datetime.timedelta(
                            seconds=CHUNK_DURATION_SECONDS
                        )
                        if process_done:
                            chunk_end_time = min(chunk_end_time, _now_utc())
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
                            "\n".join(stderr_tail)
                            if stderr_tail
                            else "(no stderr captured)"
                        )
                        logger.error(
                            "Feed %s (%s) ffmpeg exited with code %d; stderr tail:\n%s",
                            feed_id,
                            feed_name,
                            exit_code,
                            stderr_snippet,
                        )
                        classification_text = (
                            "\n".join(stderr_http_status_lines)
                            if stderr_http_status_lines
                            else stderr_snippet
                        )
                        classification = (
                            ffmpeg_classifier.classify_ffmpeg_failure(
                                exit_code=exit_code,
                                stderr_text=classification_text,
                                http_policy=_ICECAST_STREAM_HTTP_POLICY,
                            )
                        )
                        if classification is None:
                            msg = "ffmpeg_failed_without_classification"
                            raise RuntimeError(msg)
                        if not _is_raw_ffmpeg_failure(classification):
                            raise _feed_failure_from_classification(
                                classification
                            )
                        probe_failure = await _probe_stream_once(
                            resources,
                            url,
                            auth_header,
                        )
                        if probe_failure is None or _probe_keeps_raw_reason(
                            probe_failure
                        ):
                            raise _feed_failure_from_classification(
                                classification
                            )
                        raise probe_failure
                    logger.info(
                        "Feed %s (%s): ffmpeg exited normally",
                        feed_id,
                        feed_name,
                    )
                    return

                if time.monotonic() - last_activity_time > READ_TIMEOUT_SEC:
                    stderr_snippet = (
                        "\n".join(stderr_tail)
                        if stderr_tail
                        else "(no stderr captured)"
                    )
                    logger.error(
                        "Feed %s (%s) no finalized segment within %ss; stderr tail:\n%s",
                        feed_id,
                        feed_name,
                        READ_TIMEOUT_SEC,
                        stderr_snippet,
                    )
                    classification_text = (
                        "\n".join(stderr_http_status_lines)
                        if stderr_http_status_lines
                        else stderr_snippet
                    )
                    classification = ffmpeg_classifier.classify_ffmpeg_failure(
                        exit_code=process.returncode,
                        stderr_text=classification_text,
                        timed_out=True,
                        http_policy=_ICECAST_STREAM_HTTP_POLICY,
                    )
                    if classification is None:
                        msg = "capture_timeout"
                        raise RuntimeError(msg)
                    if not _is_raw_ffmpeg_failure(classification):
                        raise _feed_failure_from_classification(classification)
                    probe_failure = await _probe_stream_once(
                        resources,
                        url,
                        auth_header,
                    )
                    if probe_failure is None or _probe_keeps_raw_reason(
                        probe_failure
                    ):
                        raise _feed_failure_from_classification(classification)
                    raise probe_failure

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
    #    HTTP/TCP reconnects for short internet drops. The external Python timeout
    #    (30s) acts as a secondary dead-man's switch if ffmpeg stalls.
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
        "-headers", auth_header,
        "-i", url,
        "-vn", "-sn", "-dn",
        "-acodec", AUDIO_FORMAT,
        "-ar", str(SAMPLE_RATE_HZ),
        "-sample_fmt", SAMPLE_FORMAT,
        "-ac", str(NUM_AUDIO_CHANNELS),
        "-compression_level", FLAC_COMPRESSION_LEVEL,
        "-f", "segment",
        "-segment_time", str(CHUNK_DURATION_SECONDS),
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

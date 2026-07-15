"""Classify terminal ffmpeg process evidence."""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from enum import StrEnum

from backend.pipeline.ingestion import status_reason_detail
from backend.pipeline.ingestion.failure_classifiers import (
    http_status,
)
from backend.pipeline.storage import feed_store

_HTTP_STATUS_RE = re.compile(
    r"(?:HTTP error|Server returned|HTTP/\d(?:\.\d)?)\s+(\d{3})",
    re.IGNORECASE,
)
FFPROBE_STDERR_TAIL_LINES = 30


class FfmpegFailureKind(StrEnum):
    """Classified ffmpeg failure shape."""

    HTTP_STATUS = "http_status"
    PROCESS_EXIT = "process_exit"
    PROCESS_SIGNAL = "process_signal"
    TIMEOUT = "timeout"


class FfmpegEvidenceSource(StrEnum):
    """Where the ffmpeg failure evidence came from."""

    STDERR = "stderr"
    PROBE = "probe"
    PROCESS = "process"


@dataclass(frozen=True)
class FfmpegFailureInfo:
    """Typed ffmpeg failure details before feed-scope rendering."""

    status_reason: feed_store.FeedStatusReason
    kind: FfmpegFailureKind
    source: FfmpegEvidenceSource
    http_status: int | None = None
    exit_code: int | None = None
    signal_number: int | None = None
    timeout_sec: float | None = None


def extract_http_status_from_ffmpeg_stderr(stderr_text: str) -> int | None:
    """Extract a stable HTTP status pattern emitted by ffmpeg."""
    match = _HTTP_STATUS_RE.search(stderr_text)
    if match is None:
        return None
    return int(match.group(1))


def _iter_http_statuses_from_ffmpeg_stderr(
    stderr_text: str,
) -> list[int]:
    """Extract all stable HTTP status patterns emitted by ffmpeg."""
    return [
        int(match.group(1)) for match in _HTTP_STATUS_RE.finditer(stderr_text)
    ]


def classify_ffmpeg_failure(
    *,
    exit_code: int | None,
    stderr_text: str = "",
    timed_out: bool = False,
    probe_http_status: int | None = None,
    http_policy: http_status.HTTPStatusPolicy = (
        http_status.DEFAULT_HTTP_STATUS_POLICY
    ),
) -> FfmpegFailureInfo | None:
    """Classify terminal ffmpeg evidence without running subprocesses.

    Callers own ffmpeg execution, timeout handling, and same-endpoint probing.
    This helper only interprets bounded evidence; callers render the final
    status reason detail with the source context and bounded stderr tail.
    """
    if not timed_out and exit_code in (None, 0):
        return None

    stderr_statuses = _iter_http_statuses_from_ffmpeg_stderr(stderr_text)
    if probe_http_status is not None and probe_http_status not in (401, 403):
        # If the probe request successfully authenticated (e.g. got 200 or 404),
        # then any 401/403 in the stderr tail was a transient false positive
        # (e.g. from a temporary auth backend outage on Broadcastify's end).
        # We filter it out so we do not quarantine the feed.
        stderr_statuses = [s for s in stderr_statuses if s not in (401, 403)]

    for stderr_status in reversed(stderr_statuses):
        status_reason = http_status.classify_http_status(
            stderr_status,
            policy=http_policy,
        )
        if status_reason is not None:
            return FfmpegFailureInfo(
                status_reason=status_reason,
                kind=FfmpegFailureKind.HTTP_STATUS,
                source=FfmpegEvidenceSource.STDERR,
                http_status=stderr_status,
                exit_code=exit_code,
            )

    if probe_http_status is not None:
        status_reason = http_status.classify_http_status(
            probe_http_status,
            policy=http_policy,
        )
        if status_reason is not None:
            return FfmpegFailureInfo(
                status_reason=status_reason,
                kind=FfmpegFailureKind.HTTP_STATUS,
                source=FfmpegEvidenceSource.PROBE,
                http_status=probe_http_status,
                exit_code=exit_code,
            )

    if timed_out:
        return FfmpegFailureInfo(
            status_reason=feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
            kind=FfmpegFailureKind.TIMEOUT,
            source=FfmpegEvidenceSource.PROCESS,
            exit_code=exit_code,
        )
    if exit_code is not None and exit_code < 0:
        return FfmpegFailureInfo(
            status_reason=feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
            kind=FfmpegFailureKind.PROCESS_SIGNAL,
            source=FfmpegEvidenceSource.PROCESS,
            exit_code=exit_code,
            signal_number=-exit_code,
        )

    return FfmpegFailureInfo(
        status_reason=feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        kind=FfmpegFailureKind.PROCESS_EXIT,
        source=FfmpegEvidenceSource.PROCESS,
        exit_code=exit_code,
    )


def render_ffmpeg_diagnostic(
    info: FfmpegFailureInfo,
    diagnostic_text: str | None = None,
) -> str:
    """Render typed ffmpeg failure info into operator diagnostics."""
    if info.kind is FfmpegFailureKind.HTTP_STATUS and diagnostic_text:
        return diagnostic_text

    base = _humanize_ffmpeg_info(info)
    return _render_with_diagnostic_text(base, diagnostic_text)


def classify_ffprobe_exception(
    exc: BaseException,
) -> FfmpegFailureInfo | None:
    """Classify terminal ffprobe subprocess evidence from an exception."""
    if isinstance(exc, subprocess.TimeoutExpired):
        return FfmpegFailureInfo(
            status_reason=feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
            kind=FfmpegFailureKind.TIMEOUT,
            source=FfmpegEvidenceSource.PROCESS,
            timeout_sec=(
                float(exc.timeout) if exc.timeout is not None else None
            ),
        )

    if not isinstance(exc, subprocess.CalledProcessError):
        return None

    if exc.returncode < 0:
        return FfmpegFailureInfo(
            status_reason=feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
            kind=FfmpegFailureKind.PROCESS_SIGNAL,
            source=FfmpegEvidenceSource.PROCESS,
            exit_code=exc.returncode,
            signal_number=-exc.returncode,
        )

    return FfmpegFailureInfo(
        status_reason=feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        kind=FfmpegFailureKind.PROCESS_EXIT,
        source=FfmpegEvidenceSource.PROCESS,
        exit_code=exc.returncode,
    )


def ffprobe_exception_diagnostic_text(exc: BaseException) -> str | None:
    """Return a bounded ffprobe stderr tail from a subprocess exception."""
    stderr = getattr(exc, "stderr", None)
    return _bounded_stderr_tail(stderr, line_limit=FFPROBE_STDERR_TAIL_LINES)


def render_ffprobe_diagnostic(
    info: FfmpegFailureInfo,
    diagnostic_text: str | None = None,
) -> str:
    """Render typed ffprobe failure info into operator diagnostics."""
    return _render_with_diagnostic_text(
        _humanize_ffprobe_info(info),
        diagnostic_text,
    )


def render_ffprobe_exception_diagnostic(exc: BaseException) -> str | None:
    """Render ffprobe subprocess evidence from *exc*, if present."""
    info = classify_ffprobe_exception(exc)
    if info is None:
        return None
    return render_ffprobe_diagnostic(
        info, ffprobe_exception_diagnostic_text(exc)
    )


def ffprobe_exception_failure_reason(exc: BaseException) -> str:
    """Render ffprobe subprocess evidence, falling back to exception text."""
    return render_ffprobe_exception_diagnostic(
        exc
    ) or status_reason_detail.exception_text(exc)


def _render_with_diagnostic_text(
    base: str,
    diagnostic_text: str | None,
) -> str:
    if diagnostic_text:
        return f"{base}; {diagnostic_text}"
    return base


def _humanize_ffmpeg_info(info: FfmpegFailureInfo) -> str:
    if info.kind is FfmpegFailureKind.HTTP_STATUS:
        if info.http_status is None:
            return "HTTP stream error"
        return f"HTTP error {info.http_status}"
    if info.kind is FfmpegFailureKind.PROCESS_EXIT:
        return f"ffmpeg exited with code {info.exit_code}"
    if info.kind is FfmpegFailureKind.PROCESS_SIGNAL:
        return f"ffmpeg terminated by signal {info.signal_number}"
    if info.kind is FfmpegFailureKind.TIMEOUT:
        return "Stream capture timed out"
    return "ffmpeg failed"


def _humanize_ffprobe_info(info: FfmpegFailureInfo) -> str:
    if info.kind is FfmpegFailureKind.PROCESS_EXIT:
        return f"ffprobe exited with code {info.exit_code}"
    if info.kind is FfmpegFailureKind.PROCESS_SIGNAL:
        return f"ffprobe terminated by signal {info.signal_number}"
    if info.kind is FfmpegFailureKind.TIMEOUT:
        if info.timeout_sec is not None:
            return f"ffprobe timed out after {info.timeout_sec:g}s"
        return "ffprobe timed out"
    return "ffprobe failed"


def _bounded_stderr_tail(
    stderr: bytes | str | None,
    *,
    line_limit: int,
) -> str | None:
    text = _decode_process_output(stderr)
    lines = text.splitlines()
    if not lines:
        return None
    return "\n".join(lines[-line_limit:])


def _decode_process_output(output: bytes | str | None) -> str:
    if output is None:
        return ""
    if isinstance(output, bytes):
        return output.decode("utf-8", errors="replace")
    return output

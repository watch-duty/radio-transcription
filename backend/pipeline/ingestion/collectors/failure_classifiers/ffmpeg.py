"""Classify terminal ffmpeg process evidence."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum

from backend.pipeline.ingestion.collectors.failure_classifiers import (
    http_status,
)
from backend.pipeline.storage import feed_store

_HTTP_STATUS_RE = re.compile(
    r"(?:HTTP error|Server returned|HTTP/\d(?:\.\d)?)\s+(\d{3})",
    re.IGNORECASE,
)


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
    quarantine reason with the source context and bounded stderr tail.
    """
    if not timed_out and exit_code in (None, 0):
        return None

    for stderr_status in _iter_http_statuses_from_ffmpeg_stderr(stderr_text):
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

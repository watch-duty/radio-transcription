"""Classify terminal ffmpeg process evidence."""

from __future__ import annotations

import re

from backend.pipeline.ingestion.collectors import failure_classification
from backend.pipeline.ingestion.collectors.failure_classifiers import (
    http_status,
)
from backend.pipeline.storage import feed_store

_HTTP_STATUS_RE = re.compile(
    r"(?:HTTP error|Server returned|HTTP/\d(?:\.\d)?)\s+(\d{3})",
    re.IGNORECASE,
)


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
) -> failure_classification.FailureClassification | None:
    """Classify terminal ffmpeg evidence without running subprocesses.

    Callers own ffmpeg execution, timeout handling, and same-endpoint probing.
    This helper only interprets bounded evidence that is safe to expose as a
    feed reason tag; full stderr stays in logs and must not become a reason.
    """
    if not timed_out and exit_code in (None, 0):
        return None

    for stderr_status in _iter_http_statuses_from_ffmpeg_stderr(stderr_text):
        classification = http_status.classify_http_status(
            stderr_status,
            reason_prefix="stream_http",
            policy=http_policy,
        )
        if classification is not None:
            return classification

    if probe_http_status is not None:
        classification = http_status.classify_http_status(
            probe_http_status,
            reason_prefix="stream_http",
            policy=http_policy,
        )
        if classification is not None:
            return classification

    if timed_out:
        reason = "capture_timeout"
    elif exit_code is not None and exit_code < 0:
        reason = f"ffmpeg_signal_{-exit_code}"
    else:
        reason = f"ffmpeg_exit_{exit_code}"

    return failure_classification.FailureClassification(
        feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        reason,
    )

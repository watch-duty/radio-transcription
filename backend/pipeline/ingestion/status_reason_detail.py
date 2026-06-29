"""Shared ingestion helpers for status reason detail text."""

from __future__ import annotations

from backend.pipeline.storage import (
    status_reason_detail as storage_status_reason_detail,
)

MAX_STATUS_REASON_DETAIL_LENGTH = (
    storage_status_reason_detail.MAX_STATUS_REASON_DETAIL_LENGTH
)
cap_status_reason_detail_for_storage = (
    storage_status_reason_detail.cap_status_reason_detail_for_storage
)


def exception_text(exc: BaseException) -> str:
    """Return a one-line exception detail without operation-specific wording."""
    message = " ".join(str(exc).split())
    if message:
        return f"{type(exc).__name__}: {message}"
    return type(exc).__name__

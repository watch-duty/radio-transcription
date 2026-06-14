"""Shared ingestion helpers for quarantine diagnostic text."""

from __future__ import annotations

from backend.pipeline.storage import (
    quarantine_reason as storage_quarantine_reason,
)

MAX_QUARANTINE_REASON_LENGTH = (
    storage_quarantine_reason.MAX_QUARANTINE_REASON_LENGTH
)
cap_quarantine_reason_for_storage = (
    storage_quarantine_reason.cap_quarantine_reason_for_storage
)


def exception_text(exc: BaseException) -> str:
    """Return a one-line exception detail without operation-specific wording."""
    message = " ".join(str(exc).split())
    if message:
        return f"{type(exc).__name__}: {message}"
    return type(exc).__name__

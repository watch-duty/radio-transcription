"""Shared storage boundary helpers for feed quarantine reasons."""

from __future__ import annotations

MAX_QUARANTINE_REASON_LENGTH = 2048
_TRUNCATION_MARKER = " [truncated]"


def cap_quarantine_reason_for_storage(text: str) -> str:
    """Cap quarantine text while keeping a visible truncation marker."""
    if len(text) <= MAX_QUARANTINE_REASON_LENGTH:
        return text
    prefix_len = MAX_QUARANTINE_REASON_LENGTH - len(_TRUNCATION_MARKER)
    return f"{text[:prefix_len].rstrip()}{_TRUNCATION_MARKER}"


def exception_text(exc: BaseException) -> str:
    """Return a one-line exception detail without operation-specific wording."""
    message = " ".join(str(exc).split())
    if message:
        return f"{type(exc).__name__}: {message}"
    return type(exc).__name__

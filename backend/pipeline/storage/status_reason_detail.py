"""Storage-boundary helpers for feed status reason details."""

from __future__ import annotations

MAX_STATUS_REASON_DETAIL_LENGTH = 2048
_TRUNCATION_MARKER = " [truncated]"


def cap_status_reason_detail_for_storage(text: str) -> str:
    """Cap status reason detail while keeping a visible truncation marker."""
    if len(text) <= MAX_STATUS_REASON_DETAIL_LENGTH:
        return text
    prefix_len = MAX_STATUS_REASON_DETAIL_LENGTH - len(_TRUNCATION_MARKER)
    return f"{text[:prefix_len].rstrip()}{_TRUNCATION_MARKER}"

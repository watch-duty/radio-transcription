"""Storage-boundary helpers for feed status reason details."""

from __future__ import annotations

import unicodedata

MAX_STATUS_REASON_DETAIL_LENGTH = 2048
_TRUNCATION_MARKER = " [truncated]"


def _normalize_storage_controls(text: str) -> str:
    """Normalize unsafe controls at the storage boundary."""
    return "".join(
        "\n"
        if char == "\n"
        else " "
        if char == "\r" or unicodedata.category(char) == "Cc"
        else char
        for char in text
    )


def cap_status_reason_detail_for_storage(text: str) -> str:
    """Cap status reason detail while keeping a visible truncation marker."""
    normalized = _normalize_storage_controls(text)
    if len(normalized) <= MAX_STATUS_REASON_DETAIL_LENGTH:
        return normalized
    prefix_len = MAX_STATUS_REASON_DETAIL_LENGTH - len(_TRUNCATION_MARKER)
    return f"{normalized[:prefix_len].rstrip()}{_TRUNCATION_MARKER}"

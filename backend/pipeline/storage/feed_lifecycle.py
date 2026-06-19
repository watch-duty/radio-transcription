"""Shared feed lifecycle storage-boundary helpers."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from backend.pipeline.storage import quarantine_reason

if TYPE_CHECKING:
    from backend.pipeline.storage import feed_store
DEFAULT_FAILURE_THRESHOLD = 5
DEFAULT_BACKOFF_BASE_SEC = 15
DEFAULT_BACKOFF_MAX_SEC = 600
_WHITESPACE_RE = re.compile(r"\s+")
_CREDENTIAL_RE = re.compile(
    r"(?i)\b("
    r"authorization|api[-_ ]?key|access[-_ ]?token|refresh[-_ ]?token|"
    r"id[-_ ]?token|password|passwd|secret"
    r")\b(\s*[:=]\s*)("
    r"bearer\s+[^\s,;]+|\"[^\"]*\"|'[^']*'|[^\s,;]+"
    r")"
)
_BEARER_TOKEN_RE = re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+")
_SECRET_LITERAL_RE = re.compile(
    r"\b(sk-[A-Za-z0-9_-]{8,}|ya29\.[A-Za-z0-9._-]+|"
    r"AIza[A-Za-z0-9_-]{10,})\b"
)
_REDACTED_VALUE = "[redacted]"


def status_reason_storage_value(
    status_reason: feed_store.FeedStatusReason | None,
) -> str | None:
    """Return the database value for a nullable feed status reason.

    Args:
        status_reason: Canonical status reason enum, or ``None`` to let SQL use
            its compatibility fallback.

    Returns:
        The enum value to persist, or ``None``.

    Raises:
        TypeError: If called with a raw string or non-enum value.
    """
    if status_reason is None:
        return None
    from backend.pipeline.storage import feed_store as feed_store_module  # noqa: I001, PLC0415

    if not isinstance(status_reason, feed_store_module.FeedStatusReason):
        msg = "status_reason must be a FeedStatusReason or None"
        raise TypeError(msg)
    return status_reason.value


def quarantine_reason_storage_value(reason: str | None) -> str | None:
    """Return the database value for a nullable quarantine reason."""
    return status_reason_detail_storage_value(reason)


def status_reason_detail_storage_value(reason: str | None) -> str | None:
    """Return the bounded, sanitized diagnostic detail for storage."""
    if reason is None:
        return None
    normalized = _WHITESPACE_RE.sub(" ", reason).strip()
    if not normalized:
        return None
    normalized = _CREDENTIAL_RE.sub(
        lambda match: f"{match.group(1)}{match.group(2)}{_REDACTED_VALUE}",
        normalized,
    )
    normalized = _BEARER_TOKEN_RE.sub("Bearer [redacted]", normalized)
    normalized = _SECRET_LITERAL_RE.sub(_REDACTED_VALUE, normalized)
    return quarantine_reason.cap_quarantine_reason_for_storage(normalized)

"""Shared feed lifecycle storage-boundary helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from backend.pipeline.storage import status_reason_detail

if TYPE_CHECKING:
    from backend.pipeline.storage import feed_store
DEFAULT_FAILURE_THRESHOLD = 5
DEFAULT_BACKOFF_BASE_SEC = 15
DEFAULT_BACKOFF_MAX_SEC = 600


def status_reason_storage_value(
    status_reason: feed_store.FeedStatusReason | None,
) -> str | None:
    """Return the database value for a nullable feed status reason.

    Args:
        status_reason: Canonical status reason enum, or ``None`` to let SQL use
            its default fallback.

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


def status_reason_detail_storage_value(reason: str | None) -> str | None:
    """Return the bounded diagnostic detail for storage."""
    if reason is None:
        return None
    return status_reason_detail.cap_status_reason_detail_for_storage(reason)

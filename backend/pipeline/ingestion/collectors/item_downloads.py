"""Shared item-download failure helpers."""

from __future__ import annotations

from backend.pipeline.ingestion import quarantine_reason
from backend.pipeline.ingestion.collectors import failure_classification
from backend.pipeline.ingestion.failure_classifiers import (
    http_status,
)
from backend.pipeline.storage import feed_store

ITEM_HTTP_REASON_PREFIX = "item_http"
ITEM_DOWNLOAD_FAILED_REASON = "item_download_failed"


def item_http_failure(
    status: int,
) -> failure_classification.ItemFailure:
    """Build an item-scoped failure for terminal item HTTP evidence."""
    status_reason = http_status.classify_http_status(status)
    if status_reason is not None:
        return failure_classification.ItemFailure(
            status_reason,
            f"{ITEM_HTTP_REASON_PREFIX}_{status}",
        )

    return failure_classification.ItemFailure(
        feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        f"{ITEM_HTTP_REASON_PREFIX}_{status}",
    )


def item_download_failed(
    exc: BaseException | None = None,
) -> failure_classification.ItemFailure:
    """Classify retry exhaustion without terminal HTTP evidence."""
    reason = ITEM_DOWNLOAD_FAILED_REASON
    if exc is not None:
        reason = f"{reason}: {quarantine_reason.exception_text(exc)}"
    return failure_classification.ItemFailure(
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        reason,
    )

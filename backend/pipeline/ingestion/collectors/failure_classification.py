"""Shared collector failure classification primitives.

The helper functions here encode policy that is easier to miss from an
individual collector: feed-level classification should be bounded,
source-aware, and promoted only at a source-specific observation boundary.
See README.md in this directory for the operator-facing rationale.
"""

from __future__ import annotations

import dataclasses
from typing import NoReturn

from backend.pipeline.ingestion.models import CollectorFailure
from backend.pipeline.storage.feed_store import FeedStatusReason

MIXED_ITEM_FAILURE_REASON = "mixed_item_failures"
MISSING_SOURCE_FEED_ID_REASON = "missing_source_feed_id"


@dataclasses.dataclass(frozen=True)
class ItemFailure:
    """Classified per-item failure within a collector observation boundary.

    A boundary is the source unit where "all items failed" is meaningful:
    one API page, one file-list poll, or another collector-specific batch.
    A lone item failure is normally skipped; it becomes feed-level only when
    the whole batch fails and ItemBatchOutcome promotes it.
    """

    status_reason: FeedStatusReason
    reason: str


@dataclasses.dataclass(frozen=True)
class ItemDownloadResult:
    """Classified result of downloading one discrete audio item."""

    audio_bytes: bytes | None = None
    failure: ItemFailure | None = None
    content_type: str | None = None

    def __post_init__(self) -> None:
        if self.audio_bytes is not None and self.failure is not None:
            msg = "ItemDownloadResult cannot contain both audio_bytes and failure"
            raise ValueError(msg)


@dataclasses.dataclass
class ItemBatchOutcome:
    """Track one collector item batch.

    Collectors use this for pages, polls, or batches where an all-items-failed
    result can mean the feed itself is unhealthy. A successful chunk suppresses
    promotion even if other items in the same observation failed.
    """

    _attempted_count: int = 0
    _chunk_produced: bool = False
    _failures: list[ItemFailure] = dataclasses.field(default_factory=list)

    def record_attempt(self) -> None:
        """Record that the collector attempted one source item."""
        self._attempted_count += 1

    def record_failure(self, failure: ItemFailure) -> None:
        """Record a classified failure for an attempted source item."""
        self._failures.append(failure)

    def record_chunk_produced(self) -> None:
        """Record that at least one chunk crossed the runtime boundary."""
        self._chunk_produced = True

    def promoted_failure(self) -> ItemFailure | None:
        """Promote all-items-failed observations to a feed-level failure.

        This avoids blaming a feed for isolated object races or corrupt files.
        Mixed canonical reasons are treated as system_collector_error because
        the collector no longer has a single reliable source/system owner to
        report.
        """
        if self._attempted_count <= 0:
            return None
        if self._chunk_produced:
            return None
        if len(self._failures) != self._attempted_count:
            return None

        first_failure = self._failures[0]
        if all(
            f.status_reason is first_failure.status_reason
            for f in self._failures
        ):
            return first_failure

        return ItemFailure(
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
            MIXED_ITEM_FAILURE_REASON,
        )


def collector_failure(
    status_reason: FeedStatusReason,
    reason: str,
) -> CollectorFailure:
    """Build a typed feed-level collector failure."""
    return CollectorFailure(status_reason=status_reason, reason=reason)


def standardize_item_download_result(
    result: ItemDownloadResult | bytes | None,
) -> ItemDownloadResult:
    """Adapt legacy item-download test doubles into the typed result."""
    if isinstance(result, ItemDownloadResult):
        return result
    if isinstance(result, bytes):
        return ItemDownloadResult(audio_bytes=result)
    return ItemDownloadResult()


def item_download_http_failure(
    status: int,
    *,
    reason_prefix: str = "item_http",
) -> ItemFailure:
    """Classify terminal HTTP status for one discrete audio item download."""
    reason = f"{reason_prefix}_{status}"
    if status in {401, 403}:
        return ItemFailure(
            FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
            reason,
        )
    if status == 429:
        return ItemFailure(FeedStatusReason.SOURCE_RATE_LIMITED, reason)
    return ItemFailure(FeedStatusReason.SOURCE_UNREACHABLE, reason)


def raise_item_failure(failure: ItemFailure) -> NoReturn:
    """Raise a typed collector failure from a promoted item failure."""
    raise collector_failure(failure.status_reason, failure.reason)


def missing_source_feed_id_failure() -> CollectorFailure:
    """Build the typed failure for feeds missing source-specific ids."""
    return collector_failure(
        FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        MISSING_SOURCE_FEED_ID_REASON,
    )

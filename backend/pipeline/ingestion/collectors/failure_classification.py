"""Shared collector failure information primitives.

The helper functions here encode policy that is easier to miss from an
individual collector: feed-level ownership should be bounded, source-aware,
and promoted only at a source-specific observation boundary. Quarantine
reasons remain diagnostic text. See README.md in this directory for the
operator-facing rationale.
"""

from __future__ import annotations

import dataclasses

from backend.pipeline.ingestion.models import (
    FeedFailure,
)
from backend.pipeline.storage.feed_store import FeedStatusReason

MIXED_ITEM_FAILURE_REASON = "mixed_item_failures"
MISSING_SOURCE_FEED_ID_REASON = "missing_source_feed_id"


@dataclasses.dataclass(frozen=True)
class FailureInfo:
    """Feed status plus quarantine-reason text before scope is applied."""

    status_reason: FeedStatusReason
    reason: str


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
        """Record a failed attempted source item with status/quarantine text."""
        self._failures.append(failure)

    def record_chunk_produced(self) -> None:
        """Record that at least one chunk crossed the runtime boundary."""
        self._chunk_produced = True

    @property
    def attempted_count(self) -> int:
        """Number of source items attempted in this observation boundary."""
        return self._attempted_count

    @property
    def chunk_produced(self) -> bool:
        """Whether any chunk crossed the runtime boundary."""
        return self._chunk_produced

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
) -> FeedFailure:
    """Build a typed feed-level collector failure."""
    return FeedFailure(
        status_reason=status_reason,
        reason=reason,
    )


def missing_source_feed_id_failure() -> FeedFailure:
    """Build the typed failure for feeds missing source-specific ids."""
    return collector_failure(
        FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        MISSING_SOURCE_FEED_ID_REASON,
    )

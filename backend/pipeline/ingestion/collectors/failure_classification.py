"""Shared collector failure classification primitives.

The helper functions here encode policy that is easier to miss from an
individual collector: feed-level classification should be bounded,
source-aware, and promoted only at a source-specific observation boundary.
See README.md in this directory for the operator-facing rationale.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

from backend.pipeline.ingestion.models import CollectorFailure
from backend.pipeline.storage.feed_store import FeedStatusReason

if TYPE_CHECKING:
    from collections.abc import Sequence

MIXED_ITEM_FAILURE_REASON = "mixed_item_failures"
MISSING_SOURCE_FEED_ID_REASON = "missing_source_feed_id"


@dataclasses.dataclass(frozen=True)
class ItemFailure:
    """Classified per-item failure within a collector observation boundary.

    A boundary is the source unit where "all items failed" is meaningful:
    one API page, one file-list poll, or another collector-specific batch.
    A lone item failure is normally skipped; it becomes feed-level only when
    the whole boundary fails and aggregate_item_failures promotes it.
    """

    status_reason: FeedStatusReason
    reason: str


def collector_failure(
    status_reason: FeedStatusReason,
    reason: str,
) -> CollectorFailure:
    """Build a typed feed-level collector failure."""
    return CollectorFailure(status_reason=status_reason, reason=reason)


def missing_source_feed_id_failure() -> CollectorFailure:
    """Build the typed failure for feeds missing source-specific ids."""
    return collector_failure(
        FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        MISSING_SOURCE_FEED_ID_REASON,
    )


def aggregate_item_failures(
    failures: Sequence[ItemFailure],
    *,
    attempted_count: int,
    succeeded_count: int,
) -> ItemFailure | None:
    """Promote all-items-failed observations to a feed-level failure.

    This avoids blaming a feed for isolated object races or corrupt files.
    Mixed canonical reasons are treated as system_collector_error because the
    collector no longer has a single reliable source/system owner to report.
    """
    if attempted_count <= 0:
        return None
    if succeeded_count > 0:
        return None
    if len(failures) != attempted_count:
        return None

    first_failure = failures[0]
    if all(f.status_reason is first_failure.status_reason for f in failures):
        return first_failure

    return ItemFailure(
        FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        MIXED_ITEM_FAILURE_REASON,
    )

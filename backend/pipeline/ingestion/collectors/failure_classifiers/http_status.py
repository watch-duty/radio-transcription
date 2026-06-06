"""Classify terminal HTTP status evidence into bounded failure reasons."""

from __future__ import annotations

import dataclasses
from types import MappingProxyType
from typing import TYPE_CHECKING

from backend.pipeline.ingestion.collectors import failure_classification
from backend.pipeline.storage import feed_store

if TYPE_CHECKING:
    from collections.abc import Mapping


@dataclasses.dataclass(frozen=True)
class HTTPStatusPolicy:
    """Policy for mapping HTTP status families to feed status reasons.

    The policy object is the source of truth for status mappings. Keep
    source-specific overrides near the collector endpoint they describe so the
    code explains whether a status came from an API, item download, or stream.
    Set a family default to ``None`` when that endpoint cannot classify the
    whole status family safely.
    """

    exact: Mapping[int, feed_store.FeedStatusReason | None] = dataclasses.field(
        default_factory=dict
    )
    default_4xx: feed_store.FeedStatusReason | None = (
        feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR
    )
    default_5xx: feed_store.FeedStatusReason | None = (
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE
    )
    default_other_failure: feed_store.FeedStatusReason | None = (
        feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR
    )


DEFAULT_HTTP_STATUS_POLICY = HTTPStatusPolicy(
    exact=MappingProxyType(
        {
            401: feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
            403: feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
            408: feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            429: feed_store.FeedStatusReason.SOURCE_RATE_LIMITED,
        }
    ),
)

# Default terminal HTTP policy for source-owned endpoints. Auth and rate-limit
# statuses have stable cross-source meaning. Unmapped 4xx statuses default to
# system_collector_error because item URLs, API endpoints, and streams use
# endpoint-specific 4xx semantics. 5xx defaults to source_unreachable unless a
# collector narrows the endpoint semantics locally.


def classify_http_status(
    status: int,
    *,
    reason_prefix: str,
    policy: HTTPStatusPolicy = DEFAULT_HTTP_STATUS_POLICY,
) -> failure_classification.FailureClassification | None:
    """Classify a terminal HTTP status using an explicit source policy."""
    if 100 <= status < 400:
        return None

    if status in policy.exact:
        status_reason = policy.exact[status]
    elif 400 <= status < 500:
        status_reason = policy.default_4xx
    elif 500 <= status < 600:
        status_reason = policy.default_5xx
    else:
        status_reason = policy.default_other_failure

    if status_reason is None:
        return None

    return failure_classification.FailureClassification(
        status_reason,
        f"{reason_prefix}_{status}",
    )

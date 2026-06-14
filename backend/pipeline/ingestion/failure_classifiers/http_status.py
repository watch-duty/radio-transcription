"""Classify terminal HTTP status evidence into bounded failure reasons."""

from __future__ import annotations

import dataclasses
from types import MappingProxyType
from typing import TYPE_CHECKING

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
    default_4xx: feed_store.FeedStatusReason | None = None
    default_5xx: feed_store.FeedStatusReason | None = (
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE
    )
    default_other_failure: feed_store.FeedStatusReason | None = None


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

_RETRYABLE_HTTP_STATUSES = frozenset({408, 429})

# Default terminal HTTP policy for evidence with stable cross-collector meaning.
# Ambiguous 4xx and nonstandard statuses intentionally return None so endpoint
# code must choose the item/feed semantics locally.


def is_retryable_http_status(status: int) -> bool:
    """Return whether a terminal HTTP status should be retried first."""
    return status in _RETRYABLE_HTTP_STATUSES or 500 <= status <= 599


def classify_http_status(
    status: int,
    *,
    policy: HTTPStatusPolicy = DEFAULT_HTTP_STATUS_POLICY,
) -> feed_store.FeedStatusReason | None:
    """Classify terminal HTTP status evidence using an explicit source policy."""
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

    return status_reason

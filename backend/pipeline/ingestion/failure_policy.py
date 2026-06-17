"""Pure failure policy vocabulary and classification helpers."""

from __future__ import annotations

from enum import StrEnum

from backend.pipeline.storage import feed_store


class ExecutedAction(StrEnum):
    """Policy action selected for runtime/store execution."""

    INCREMENT_FEED_FAILURE_BUDGET = "increment_feed_failure_budget"
    RETRY_WITHOUT_FEED_BUDGET = "retry_without_feed_budget"
    RECORD_POST_BOOKMARK_PUBLISH_GAP = "record_post_bookmark_publish_gap"


_POLICY_ACTIONS = {
    feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED: (
        ExecutedAction.RECORD_POST_BOOKMARK_PUBLISH_GAP
    ),
    feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID: (
        ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET
    ),
    feed_store.FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID: (
        ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET
    ),
}


def classify_failure_policy(
    status_reason: feed_store.FeedStatusReason,
) -> ExecutedAction:
    """Classify a status reason into a side-effect-free policy action."""
    return _POLICY_ACTIONS.get(
        status_reason,
        ExecutedAction.RETRY_WITHOUT_FEED_BUDGET,
    )

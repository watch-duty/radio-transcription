"""Shared failure-policy telemetry emitted after durable persistence."""

from __future__ import annotations

import logging  # noqa: TC003 - public hints resolve at runtime.
import uuid  # noqa: TC003 - public hints resolve at runtime.

from backend.pipeline.ingestion import failure_policy
from backend.pipeline.storage import feed_store


def emit_failure_policy_decision(
    event_logger: logging.Logger,
    *,
    feed_id: uuid.UUID,
    source_type: feed_store.SourceType,
    plan: failure_policy.FailurePersistencePlan,
) -> None:
    """Emit persisted policy and known post-bookmark gap evidence.

    Args:
        event_logger: Logger associated with the owning ingestion path.
        feed_id: Permanent Feed UUID whose failure was persisted.
        source_type: Feed source family.
        plan: Exact failure treatment applied by durable storage.

    Returns:
        None.
    """
    status_reason = plan.status_reason
    known_gap = (
        status_reason
        is feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED
    )
    if known_gap:
        executed_action = "record_post_bookmark_publish_gap"
    elif isinstance(plan.treatment, failure_policy.ConsumeFailureBudget):
        executed_action = "increment_feed_failure_budget"
    else:
        executed_action = "retry_without_feed_budget"

    reason = plan.reason or status_reason.value
    fields: dict[str, object] = {
        "event_type": "feed_failure_policy_decision",
        "feed_id": str(feed_id),
        "source_type": source_type.value,
        "reason": reason,
        "status_reason": status_reason.value,
        "replay_missing": known_gap,
        "data_gap_known": known_gap,
        "executed_action": executed_action,
    }
    if isinstance(plan.treatment, failure_policy.RetryWithoutBudget):
        fields["retry_after"] = plan.treatment.retry_after.isoformat()
    event_logger.info(
        "Feed failure policy decision",
        extra={"json_fields": fields},
    )

    if known_gap:
        emit_post_bookmark_publish_failure(
            event_logger,
            feed_id=feed_id,
            source_type=source_type,
            reason=reason,
        )


def emit_post_bookmark_publish_failure(
    event_logger: logging.Logger,
    *,
    feed_id: uuid.UUID,
    source_type: feed_store.SourceType,
    reason: str,
) -> None:
    """Emit one known data-gap event without claiming a child transition.

    SID-level failure promotion deliberately preserves child failure budgets.
    A post-bookmark publication gap remains true even when no Feed failure
    transition is written, so that path emits only this narrower event.

    Args:
        event_logger: Logger associated with the owning ingestion path.
        feed_id: Permanent Feed UUID whose publication failed.
        source_type: Feed source family.
        reason: Bounded diagnostic detail for the failed publication.

    Returns:
        None.
    """
    status_reason = (
        feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED
    )
    gap_fields: dict[str, object] = {
        "event_type": "post_bookmark_publish_failure",
        "feed_id": str(feed_id),
        "source_type": source_type.value,
        "reason": reason,
        "status_reason": status_reason.value,
        "replay_missing": True,
        "data_gap_known": True,
        "executed_action": "record_post_bookmark_publish_gap",
    }
    event_logger.error(
        "Post-bookmark publish failure",
        extra={"json_fields": gap_fields},
    )

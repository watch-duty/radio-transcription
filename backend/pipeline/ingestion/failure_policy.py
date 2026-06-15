"""Pure failure policy vocabulary and classification helpers."""

from __future__ import annotations

import dataclasses
from enum import StrEnum

from backend.pipeline.storage import feed_store


class OwnerScope(StrEnum):
    """Operational owner scope for a failure policy decision."""

    FEED = "feed"
    CREDENTIAL_SCOPE = "credential_scope"
    SOURCE_CLASS = "source_class"
    PIPELINE = "pipeline"
    UNKNOWN = "unknown"


class FailureScope(StrEnum):
    """Observation scope that produced the failure signal."""

    ITEM = "item"
    OBSERVATION = "observation"
    FEED = "feed"
    CLASS = "class"
    PIPELINE = "pipeline"
    UNKNOWN = "unknown"


class EndpointKind(StrEnum):
    """Stable endpoint or stage family associated with a failure."""

    STREAM = "stream"
    CALLS_API = "calls_api"
    CALLS_METADATA = "calls_metadata"
    CALLS_MEDIA = "calls_media"
    FIRE_POLL = "fire_poll"
    OPENMHZ_WS_UPGRADE = "openmhz_ws_upgrade"
    PUBSUB_PUBLISH = "pubsub_publish"
    GCS_UPLOAD = "gcs_upload"
    BOOKMARK_WRITE = "bookmark_write"
    FEED_CONFIGURATION = "feed_configuration"
    UNKNOWN = "unknown"


class PolicyIntent(StrEnum):
    """What policy lane should own this failure."""

    QUARANTINE_FEED = "quarantine_feed"
    SUPPRESS_RETRY = "suppress_retry"
    HOLD_FOR_REPLAY = "hold_for_replay"
    TELEMETRY_GAP = "telemetry_gap"
    OPEN_BREAKER = "open_breaker"


class ExecutedAction(StrEnum):
    """Action executed by the runtime/store for a policy decision."""

    INCREMENT_FEED_FAILURE_BUDGET = "increment_feed_failure_budget"
    RELEASE_NON_BUDGETED_FAILURE = "release_non_budgeted_failure"
    SUPPRESS_FEED_QUARANTINE_RECORD_PUBLISH_GAP = (
        "suppress_feed_quarantine_record_publish_gap"
    )
    SUPPRESS_FEED_QUARANTINE_TELEMETRY_GAP = (
        "suppress_feed_quarantine_telemetry_gap"
    )


class PipelineStage(StrEnum):
    """Post-capture runtime pipeline stage."""

    GCS_UPLOAD = "gcs_upload"
    BOOKMARK_WRITE = "bookmark_write"
    PUBSUB_PUBLISH = "pubsub_publish"


@dataclasses.dataclass(frozen=True)
class FailurePolicyEvidence:
    """Machine-readable facts for classifying an ingestion failure."""

    owner_scope: OwnerScope
    failure_scope: FailureScope
    endpoint_kind: EndpointKind
    pipeline_stage: PipelineStage | None = None


@dataclasses.dataclass(frozen=True)
class FailurePolicyDecision:
    """Pure policy decision derived from status reason plus evidence."""

    status_reason: feed_store.FeedStatusReason
    evidence: FailurePolicyEvidence
    policy_intent: PolicyIntent
    executed_action: ExecutedAction
    feed_budget_eligible: bool
    quarantine_feed: bool


def classify_failure_policy(
    status_reason: feed_store.FeedStatusReason,
    evidence: FailurePolicyEvidence,
) -> FailurePolicyDecision:
    """Classify structured evidence into a side-effect-free policy decision."""
    if (
        status_reason
        is feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED
        or evidence.pipeline_stage is PipelineStage.PUBSUB_PUBLISH
        and evidence.owner_scope is OwnerScope.PIPELINE
    ):
        return FailurePolicyDecision(
            status_reason=status_reason,
            evidence=evidence,
            policy_intent=PolicyIntent.HOLD_FOR_REPLAY,
            executed_action=(
                ExecutedAction.SUPPRESS_FEED_QUARANTINE_RECORD_PUBLISH_GAP
            ),
            feed_budget_eligible=False,
            quarantine_feed=False,
        )

    if evidence.owner_scope is OwnerScope.PIPELINE:
        return FailurePolicyDecision(
            status_reason=status_reason,
            evidence=evidence,
            policy_intent=PolicyIntent.SUPPRESS_RETRY,
            executed_action=ExecutedAction.RELEASE_NON_BUDGETED_FAILURE,
            feed_budget_eligible=False,
            quarantine_feed=False,
        )

    if evidence.owner_scope in {
        OwnerScope.CREDENTIAL_SCOPE,
        OwnerScope.SOURCE_CLASS,
    }:
        return FailurePolicyDecision(
            status_reason=status_reason,
            evidence=evidence,
            policy_intent=PolicyIntent.OPEN_BREAKER,
            executed_action=ExecutedAction.RELEASE_NON_BUDGETED_FAILURE,
            feed_budget_eligible=False,
            quarantine_feed=False,
        )

    if (
        evidence.owner_scope is OwnerScope.FEED
        and evidence.failure_scope is FailureScope.FEED
        and evidence.endpoint_kind is EndpointKind.FEED_CONFIGURATION
    ):
        return FailurePolicyDecision(
            status_reason=status_reason,
            evidence=evidence,
            policy_intent=PolicyIntent.QUARANTINE_FEED,
            executed_action=ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET,
            feed_budget_eligible=True,
            quarantine_feed=True,
        )

    if evidence.owner_scope is OwnerScope.UNKNOWN:
        return FailurePolicyDecision(
            status_reason=status_reason,
            evidence=evidence,
            policy_intent=PolicyIntent.TELEMETRY_GAP,
            executed_action=(
                ExecutedAction.SUPPRESS_FEED_QUARANTINE_TELEMETRY_GAP
            ),
            feed_budget_eligible=False,
            quarantine_feed=False,
        )

    return FailurePolicyDecision(
        status_reason=status_reason,
        evidence=evidence,
        policy_intent=PolicyIntent.SUPPRESS_RETRY,
        executed_action=ExecutedAction.RELEASE_NON_BUDGETED_FAILURE,
        feed_budget_eligible=False,
        quarantine_feed=False,
    )


def is_feed_quarantine(decision: FailurePolicyDecision) -> bool:
    """Return whether a decision should quarantine a feed."""
    return decision.quarantine_feed


def is_feed_budget_eligible(decision: FailurePolicyDecision) -> bool:
    """Return whether a decision may increment the feed failure budget."""
    return decision.feed_budget_eligible


def is_pipeline_hold(decision: FailurePolicyDecision) -> bool:
    """Return whether a decision belongs in a pipeline hold/replay lane."""
    return decision.policy_intent is PolicyIntent.HOLD_FOR_REPLAY


def is_source_class_breaker(decision: FailurePolicyDecision) -> bool:
    """Return whether a decision belongs to a shared source/credential lane."""
    return decision.policy_intent is PolicyIntent.OPEN_BREAKER

"""Shared collector failure information primitives.

The helper functions here encode policy that is easier to miss from an
individual collector: feed-level ownership should be bounded, source-aware,
and promoted only at a source-specific observation boundary. Quarantine
reasons remain diagnostic text. See README.md in this directory for the
operator-facing rationale.
"""

from __future__ import annotations

import dataclasses

from backend.pipeline.ingestion import failure_policy
from backend.pipeline.ingestion.models import (
    FeedFailure,
)
from backend.pipeline.storage.feed_store import FeedStatusReason

MIXED_ITEM_FAILURE_REASON = "mixed_item_failures"
MISSING_SOURCE_FEED_ID_REASON = "missing_source_feed_id"
_SOURCE_STATUS_REASONS = frozenset(
    {
        FeedStatusReason.SOURCE_OFFLINE,
        FeedStatusReason.SOURCE_UNREACHABLE,
        FeedStatusReason.SOURCE_RATE_LIMITED,
    }
)
_STATUS_OWNER_SCOPES = {
    **dict.fromkeys(
        _SOURCE_STATUS_REASONS,
        failure_policy.OwnerScope.SOURCE_CLASS,
    ),
    FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED: (
        failure_policy.OwnerScope.CREDENTIAL_SCOPE
    ),
    FeedStatusReason.SYSTEM_CONFIGURATION_INVALID: failure_policy.OwnerScope.FEED,
    FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID: (
        failure_policy.OwnerScope.SOURCE_CLASS
    ),
    FeedStatusReason.SYSTEM_CREDENTIAL_ACCESS_FAILED: (
        failure_policy.OwnerScope.CREDENTIAL_SCOPE
    ),
    FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID: (
        failure_policy.OwnerScope.SOURCE_CLASS
    ),
    FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED: (
        failure_policy.OwnerScope.PIPELINE
    ),
    FeedStatusReason.SYSTEM_PIPELINE_ERROR: failure_policy.OwnerScope.PIPELINE,
}


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


def owner_scope_for_status_reason(
    status_reason: FeedStatusReason,
) -> failure_policy.OwnerScope:
    """Map a status enum to the broad owner scope for collector evidence."""
    return _STATUS_OWNER_SCOPES.get(
        status_reason,
        failure_policy.OwnerScope.UNKNOWN,
    )


def policy_evidence_for_status_reason(
    status_reason: FeedStatusReason,
    *,
    failure_scope: failure_policy.FailureScope,
    endpoint_kind: failure_policy.EndpointKind,
    pipeline_stage: failure_policy.PipelineStage | None = None,
) -> failure_policy.FailurePolicyEvidence:
    """Build facts-only collector evidence from a typed status enum."""
    return failure_policy.FailurePolicyEvidence(
        owner_scope=owner_scope_for_status_reason(status_reason),
        failure_scope=failure_scope,
        endpoint_kind=endpoint_kind,
        pipeline_stage=pipeline_stage,
    )


def collector_failure(
    status_reason: FeedStatusReason,
    reason: str,
    *,
    policy_evidence: failure_policy.FailurePolicyEvidence,
) -> FeedFailure:
    """Build a typed feed-level collector failure."""
    return FeedFailure(
        status_reason=status_reason,
        reason=reason,
        policy_evidence=policy_evidence,
    )


def missing_source_feed_id_failure() -> FeedFailure:
    """Build the typed failure for feeds missing source-specific ids."""
    return collector_failure(
        FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        MISSING_SOURCE_FEED_ID_REASON,
        policy_evidence=failure_policy.FailurePolicyEvidence(
            owner_scope=failure_policy.OwnerScope.FEED,
            failure_scope=failure_policy.FailureScope.FEED,
            endpoint_kind=failure_policy.EndpointKind.FEED_CONFIGURATION,
        ),
    )

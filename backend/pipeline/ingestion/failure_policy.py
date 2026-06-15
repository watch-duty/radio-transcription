"""Pure failure policy vocabulary and classification helpers."""

from __future__ import annotations

import dataclasses
import itertools
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


class ExecutedAction(StrEnum):
    """Policy action selected for runtime/store execution."""

    INCREMENT_FEED_FAILURE_BUDGET = "increment_feed_failure_budget"
    RELEASE_NON_BUDGETED_FAILURE = "release_non_budgeted_failure"
    RECORD_POST_BOOKMARK_PUBLISH_GAP = "record_post_bookmark_publish_gap"


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
class _EvidencePattern:
    """Rule-side matcher for concrete failure policy evidence."""

    owner_scopes: frozenset[OwnerScope] | None = None
    failure_scopes: frozenset[FailureScope] | None = None
    endpoint_kinds: frozenset[EndpointKind] | None = None
    pipeline_stages: frozenset[PipelineStage | None] | None = None

    def matches(self, evidence: FailurePolicyEvidence) -> bool:
        """Return whether concrete evidence satisfies this pattern."""
        if (
            self.owner_scopes is not None
            and evidence.owner_scope not in self.owner_scopes
        ):
            return False
        if (
            self.failure_scopes is not None
            and evidence.failure_scope not in self.failure_scopes
        ):
            return False
        if (
            self.endpoint_kinds is not None
            and evidence.endpoint_kind not in self.endpoint_kinds
        ):
            return False
        return not (
            self.pipeline_stages is not None
            and evidence.pipeline_stage not in self.pipeline_stages
        )


@dataclasses.dataclass(frozen=True)
class PolicyRuleConflict:
    """Concrete evidence that matches multiple policy actions."""

    status_reason: feed_store.FeedStatusReason
    evidence: FailurePolicyEvidence
    executed_actions: frozenset[ExecutedAction]
    matching_rule_indexes: tuple[int, ...]


@dataclasses.dataclass(frozen=True)
class _FailurePolicyRule:
    """One explicit status/evidence policy route."""

    status_reason: feed_store.FeedStatusReason
    evidence_pattern: _EvidencePattern
    executed_action: ExecutedAction

    def matches(
        self,
        status_reason: feed_store.FeedStatusReason,
        evidence: FailurePolicyEvidence,
    ) -> bool:
        """Return whether this rule explicitly covers the evidence."""
        if status_reason is not self.status_reason:
            return False
        return self.evidence_pattern.matches(evidence)


_QUARANTINE_FEED_ACTION = ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET
_NON_BUDGETED_RETRY_ACTION = ExecutedAction.RELEASE_NON_BUDGETED_FAILURE
_PIPELINE_GAP_ACTION = ExecutedAction.RECORD_POST_BOOKMARK_PUBLISH_GAP


def _policy_rule(
    *,
    status_reason: feed_store.FeedStatusReason,
    executed_action: ExecutedAction,
    owner_scopes: frozenset[OwnerScope] | None = None,
    failure_scopes: frozenset[FailureScope] | None = None,
    endpoint_kinds: frozenset[EndpointKind] | None = None,
    pipeline_stages: frozenset[PipelineStage | None] | None = None,
) -> _FailurePolicyRule:
    """Build a rule without losing field types."""
    return _FailurePolicyRule(
        status_reason=status_reason,
        evidence_pattern=_EvidencePattern(
            owner_scopes=owner_scopes,
            failure_scopes=failure_scopes,
            endpoint_kinds=endpoint_kinds,
            pipeline_stages=pipeline_stages,
        ),
        executed_action=executed_action,
    )


_SOURCE_CLASS_ENDPOINTS = frozenset(
    {
        EndpointKind.STREAM,
        EndpointKind.CALLS_API,
        EndpointKind.CALLS_METADATA,
        EndpointKind.CALLS_MEDIA,
        EndpointKind.FIRE_POLL,
        EndpointKind.OPENMHZ_WS_UPGRADE,
        EndpointKind.UNKNOWN,
    }
)

_SOURCE_FAILURE_SCOPES = frozenset(
    {
        FailureScope.ITEM,
        FailureScope.OBSERVATION,
        FailureScope.FEED,
        FailureScope.CLASS,
    }
)

_AUTH_ENDPOINTS = frozenset(
    {
        EndpointKind.STREAM,
        EndpointKind.CALLS_API,
        EndpointKind.CALLS_METADATA,
        EndpointKind.CALLS_MEDIA,
        EndpointKind.FIRE_POLL,
        EndpointKind.OPENMHZ_WS_UPGRADE,
        EndpointKind.UNKNOWN,
    }
)

_FEED_CONFIG_ENDPOINTS = frozenset(
    {
        EndpointKind.FEED_CONFIGURATION,
    }
)

_PROVIDER_CONTROL_CONFIG_ENDPOINTS = frozenset(
    {
        EndpointKind.CALLS_API,
        EndpointKind.FIRE_POLL,
        EndpointKind.OPENMHZ_WS_UPGRADE,
    }
)

_RUNTIME_CONFIG_ENDPOINTS = frozenset(
    {
        EndpointKind.FEED_CONFIGURATION,
        EndpointKind.STREAM,
        EndpointKind.CALLS_API,
        EndpointKind.FIRE_POLL,
        EndpointKind.OPENMHZ_WS_UPGRADE,
        EndpointKind.UNKNOWN,
    }
)

_SOURCE_PAYLOAD_ENDPOINTS = frozenset(
    {
        EndpointKind.CALLS_API,
        EndpointKind.CALLS_METADATA,
        EndpointKind.CALLS_MEDIA,
        EndpointKind.FIRE_POLL,
        EndpointKind.OPENMHZ_WS_UPGRADE,
        EndpointKind.UNKNOWN,
    }
)

_POLICY_RULES = (
    _policy_rule(
        status_reason=(
            feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED
        ),
        executed_action=_PIPELINE_GAP_ACTION,
        owner_scopes=frozenset({OwnerScope.PIPELINE}),
        failure_scopes=frozenset({FailureScope.PIPELINE}),
        endpoint_kinds=frozenset({EndpointKind.PUBSUB_PUBLISH}),
        pipeline_stages=frozenset({PipelineStage.PUBSUB_PUBLISH}),
    ),
    _policy_rule(
        status_reason=feed_store.FeedStatusReason.SOURCE_OFFLINE,
        executed_action=ExecutedAction.RELEASE_NON_BUDGETED_FAILURE,
        owner_scopes=frozenset({OwnerScope.SOURCE_CLASS}),
        failure_scopes=_SOURCE_FAILURE_SCOPES,
        endpoint_kinds=_SOURCE_CLASS_ENDPOINTS,
        pipeline_stages=frozenset({None}),
    ),
    _policy_rule(
        status_reason=feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        executed_action=ExecutedAction.RELEASE_NON_BUDGETED_FAILURE,
        owner_scopes=frozenset({OwnerScope.SOURCE_CLASS}),
        failure_scopes=_SOURCE_FAILURE_SCOPES,
        endpoint_kinds=_SOURCE_CLASS_ENDPOINTS,
        pipeline_stages=frozenset({None}),
    ),
    _policy_rule(
        status_reason=feed_store.FeedStatusReason.SOURCE_RATE_LIMITED,
        executed_action=ExecutedAction.RELEASE_NON_BUDGETED_FAILURE,
        owner_scopes=frozenset({OwnerScope.SOURCE_CLASS}),
        failure_scopes=_SOURCE_FAILURE_SCOPES,
        endpoint_kinds=_SOURCE_CLASS_ENDPOINTS,
        pipeline_stages=frozenset({None}),
    ),
    _policy_rule(
        status_reason=feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
        executed_action=_NON_BUDGETED_RETRY_ACTION,
        owner_scopes=frozenset({OwnerScope.CREDENTIAL_SCOPE}),
        failure_scopes=frozenset(
            {
                FailureScope.ITEM,
                FailureScope.OBSERVATION,
                FailureScope.FEED,
                FailureScope.CLASS,
            }
        ),
        endpoint_kinds=_AUTH_ENDPOINTS,
        pipeline_stages=frozenset({None}),
    ),
    _policy_rule(
        status_reason=feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        executed_action=_QUARANTINE_FEED_ACTION,
        owner_scopes=frozenset({OwnerScope.FEED}),
        failure_scopes=frozenset({FailureScope.FEED}),
        endpoint_kinds=_FEED_CONFIG_ENDPOINTS,
        pipeline_stages=frozenset({None}),
    ),
    _policy_rule(
        status_reason=feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        executed_action=_NON_BUDGETED_RETRY_ACTION,
        owner_scopes=frozenset({OwnerScope.FEED}),
        failure_scopes=frozenset(
            {
                FailureScope.FEED,
                FailureScope.OBSERVATION,
            }
        ),
        endpoint_kinds=_PROVIDER_CONTROL_CONFIG_ENDPOINTS,
        pipeline_stages=frozenset({None}),
    ),
    _policy_rule(
        status_reason=(
            feed_store.FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID
        ),
        executed_action=_QUARANTINE_FEED_ACTION,
        owner_scopes=frozenset({OwnerScope.SOURCE_CLASS}),
        failure_scopes=_SOURCE_FAILURE_SCOPES,
        endpoint_kinds=_RUNTIME_CONFIG_ENDPOINTS,
        pipeline_stages=frozenset({None}),
    ),
    _policy_rule(
        status_reason=(
            feed_store.FeedStatusReason.SYSTEM_CREDENTIAL_ACCESS_FAILED
        ),
        executed_action=_NON_BUDGETED_RETRY_ACTION,
        owner_scopes=frozenset({OwnerScope.CREDENTIAL_SCOPE}),
        failure_scopes=frozenset(
            {
                FailureScope.ITEM,
                FailureScope.OBSERVATION,
                FailureScope.FEED,
                FailureScope.CLASS,
            }
        ),
        endpoint_kinds=_AUTH_ENDPOINTS,
        pipeline_stages=frozenset({None}),
    ),
    _policy_rule(
        status_reason=feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
        executed_action=_NON_BUDGETED_RETRY_ACTION,
        owner_scopes=frozenset({OwnerScope.SOURCE_CLASS}),
        failure_scopes=_SOURCE_FAILURE_SCOPES,
        endpoint_kinds=_SOURCE_PAYLOAD_ENDPOINTS,
        pipeline_stages=frozenset({None}),
    ),
    _policy_rule(
        status_reason=feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        executed_action=_NON_BUDGETED_RETRY_ACTION,
        owner_scopes=frozenset({OwnerScope.UNKNOWN}),
    ),
    _policy_rule(
        status_reason=feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        executed_action=_NON_BUDGETED_RETRY_ACTION,
        owner_scopes=frozenset({OwnerScope.PIPELINE}),
        failure_scopes=frozenset({FailureScope.PIPELINE}),
        endpoint_kinds=frozenset(
            {
                EndpointKind.GCS_UPLOAD,
                EndpointKind.BOOKMARK_WRITE,
            }
        ),
        pipeline_stages=frozenset(
            {
                PipelineStage.GCS_UPLOAD,
                PipelineStage.BOOKMARK_WRITE,
            }
        ),
    ),
    _policy_rule(
        status_reason=feed_store.FeedStatusReason.SYSTEM_UNEXPECTED_ERROR,
        executed_action=_NON_BUDGETED_RETRY_ACTION,
        owner_scopes=frozenset({OwnerScope.UNKNOWN}),
    ),
)


def classify_failure_policy(
    status_reason: feed_store.FeedStatusReason,
    evidence: FailurePolicyEvidence,
) -> ExecutedAction:
    """Classify structured evidence into a side-effect-free policy action."""
    for rule in _POLICY_RULES:
        if rule.matches(status_reason, evidence):
            return rule.executed_action
    return ExecutedAction.RELEASE_NON_BUDGETED_FAILURE


def _iter_concrete_policy_evidence() -> tuple[FailurePolicyEvidence, ...]:
    """Return every concrete evidence tuple covered by policy dimensions."""
    pipeline_stages = (None, *tuple(PipelineStage))
    return tuple(
        FailurePolicyEvidence(
            owner_scope=owner_scope,
            failure_scope=failure_scope,
            endpoint_kind=endpoint_kind,
            pipeline_stage=pipeline_stage,
        )
        for (
            owner_scope,
            failure_scope,
            endpoint_kind,
            pipeline_stage,
        ) in itertools.product(
            tuple(OwnerScope),
            tuple(FailureScope),
            tuple(EndpointKind),
            pipeline_stages,
        )
    )


def find_policy_rule_conflicts(
    rules: tuple[_FailurePolicyRule, ...] = _POLICY_RULES,
) -> tuple[PolicyRuleConflict, ...]:
    """Return concrete rule overlaps that route to different actions."""
    conflicts: list[PolicyRuleConflict] = []
    for status_reason, evidence in itertools.product(
        tuple(feed_store.FeedStatusReason),
        _iter_concrete_policy_evidence(),
    ):
        matching_rules = tuple(
            (index, rule)
            for index, rule in enumerate(rules)
            if rule.matches(status_reason, evidence)
        )
        executed_actions = frozenset(
            rule.executed_action for _, rule in matching_rules
        )
        if len(executed_actions) > 1:
            conflicts.append(
                PolicyRuleConflict(
                    status_reason=status_reason,
                    evidence=evidence,
                    executed_actions=executed_actions,
                    matching_rule_indexes=tuple(
                        index for index, _ in matching_rules
                    ),
                )
            )
    return tuple(conflicts)


def is_feed_quarantine(action: ExecutedAction) -> bool:
    """Return whether an action should quarantine a feed."""
    return action is ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET


def is_feed_budget_eligible(action: ExecutedAction) -> bool:
    """Return whether an action may increment the feed failure budget."""
    return is_feed_quarantine(action)


def is_pipeline_hold(action: ExecutedAction) -> bool:
    """Return whether an action belongs in a pipeline hold/replay lane."""
    return action is ExecutedAction.RECORD_POST_BOOKMARK_PUBLISH_GAP

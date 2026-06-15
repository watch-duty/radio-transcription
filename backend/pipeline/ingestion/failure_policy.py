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
    executed_action: ExecutedAction


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
    executed_action: ExecutedAction
    owner_scopes: frozenset[OwnerScope] | None = None
    failure_scopes: frozenset[FailureScope] | None = None
    endpoint_kinds: frozenset[EndpointKind] | None = None
    pipeline_stages: frozenset[PipelineStage | None] | None = None

    def matches(
        self,
        status_reason: feed_store.FeedStatusReason,
        evidence: FailurePolicyEvidence,
    ) -> bool:
        """Return whether this rule explicitly covers the evidence."""
        if status_reason is not self.status_reason:
            return False
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


_QUARANTINE_FEED_ACTION = ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET
_NON_BUDGETED_RETRY_ACTION = ExecutedAction.RELEASE_NON_BUDGETED_FAILURE
_PIPELINE_GAP_ACTION = (
    ExecutedAction.SUPPRESS_FEED_QUARANTINE_RECORD_PUBLISH_GAP
)
_NON_BUDGETED_TELEMETRY_ACTION = (
    ExecutedAction.SUPPRESS_FEED_QUARANTINE_TELEMETRY_GAP
)


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
        executed_action=executed_action,
        owner_scopes=owner_scopes,
        failure_scopes=failure_scopes,
        endpoint_kinds=endpoint_kinds,
        pipeline_stages=pipeline_stages,
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
    _FailurePolicyRule(
        status_reason=feed_store.FeedStatusReason.SOURCE_OFFLINE,
        owner_scopes=frozenset({OwnerScope.SOURCE_CLASS}),
        failure_scopes=_SOURCE_FAILURE_SCOPES,
        endpoint_kinds=_SOURCE_CLASS_ENDPOINTS,
        pipeline_stages=frozenset({None}),
        executed_action=ExecutedAction.RELEASE_NON_BUDGETED_FAILURE,
    ),
    _FailurePolicyRule(
        status_reason=feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        owner_scopes=frozenset({OwnerScope.SOURCE_CLASS}),
        failure_scopes=_SOURCE_FAILURE_SCOPES,
        endpoint_kinds=_SOURCE_CLASS_ENDPOINTS,
        pipeline_stages=frozenset({None}),
        executed_action=ExecutedAction.RELEASE_NON_BUDGETED_FAILURE,
    ),
    _FailurePolicyRule(
        status_reason=feed_store.FeedStatusReason.SOURCE_RATE_LIMITED,
        owner_scopes=frozenset({OwnerScope.SOURCE_CLASS}),
        failure_scopes=_SOURCE_FAILURE_SCOPES,
        endpoint_kinds=_SOURCE_CLASS_ENDPOINTS,
        pipeline_stages=frozenset({None}),
        executed_action=ExecutedAction.RELEASE_NON_BUDGETED_FAILURE,
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
        executed_action=_NON_BUDGETED_TELEMETRY_ACTION,
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
        executed_action=_NON_BUDGETED_TELEMETRY_ACTION,
        owner_scopes=frozenset({OwnerScope.UNKNOWN}),
    ),
)


def _decision_from_rule(
    status_reason: feed_store.FeedStatusReason,
    evidence: FailurePolicyEvidence,
    rule: _FailurePolicyRule,
) -> FailurePolicyDecision:
    return FailurePolicyDecision(
        status_reason=status_reason,
        evidence=evidence,
        executed_action=rule.executed_action,
    )


def _non_budgeted_telemetry_decision(
    status_reason: feed_store.FeedStatusReason,
    evidence: FailurePolicyEvidence,
) -> FailurePolicyDecision:
    return FailurePolicyDecision(
        status_reason=status_reason,
        evidence=evidence,
        executed_action=ExecutedAction.SUPPRESS_FEED_QUARANTINE_TELEMETRY_GAP,
    )


def classify_failure_policy(
    status_reason: feed_store.FeedStatusReason,
    evidence: FailurePolicyEvidence,
) -> FailurePolicyDecision:
    """Classify structured evidence into a side-effect-free policy decision."""
    for rule in _POLICY_RULES:
        if rule.matches(status_reason, evidence):
            return _decision_from_rule(status_reason, evidence, rule)
    return _non_budgeted_telemetry_decision(status_reason, evidence)


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


def is_feed_quarantine(decision: FailurePolicyDecision) -> bool:
    """Return whether a decision should quarantine a feed."""
    return (
        decision.executed_action is ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET
    )


def is_feed_budget_eligible(decision: FailurePolicyDecision) -> bool:
    """Return whether a decision may increment the feed failure budget."""
    return is_feed_quarantine(decision)


def is_pipeline_hold(decision: FailurePolicyDecision) -> bool:
    """Return whether a decision belongs in a pipeline hold/replay lane."""
    return (
        decision.executed_action
        is ExecutedAction.SUPPRESS_FEED_QUARANTINE_RECORD_PUBLISH_GAP
    )

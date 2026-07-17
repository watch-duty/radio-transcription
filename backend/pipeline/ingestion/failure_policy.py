"""Shared failure-budget classification and persistence planning."""

from __future__ import annotations

import dataclasses
import datetime
import typing

from backend.pipeline.storage import feed_store


@dataclasses.dataclass(frozen=True, slots=True)
class ConsumeFailureBudget:
    """Configured treatment for a failure that may quarantine.

    Attributes:
        failure_threshold: Consecutive failures that trigger quarantine.
        backoff_base_sec: Initial exponential-backoff delay in seconds.
        backoff_max_sec: Maximum exponential-backoff delay in seconds.
    """

    failure_threshold: int
    backoff_base_sec: int
    backoff_max_sec: int

    def __post_init__(self) -> None:
        for name, value in (
            ("failure_threshold", self.failure_threshold),
            ("backoff_base_sec", self.backoff_base_sec),
            ("backoff_max_sec", self.backoff_max_sec),
        ):
            if isinstance(value, bool) or not isinstance(value, int):
                msg = f"{name} must be an integer"
                raise TypeError(msg)
            if value <= 0:
                msg = f"{name} must be positive"
                raise ValueError(msg)
        if self.backoff_base_sec > self.backoff_max_sec:
            msg = "backoff_base_sec must not exceed backoff_max_sec"
            raise ValueError(msg)


@dataclasses.dataclass(frozen=True, slots=True)
class RetryWithoutBudget:
    """Configured treatment for a failure that cannot quarantine.

    Attributes:
        retry_after: UTC-aware time when recovery becomes eligible.
    """

    retry_after: datetime.datetime

    def __post_init__(self) -> None:
        if not isinstance(self.retry_after, datetime.datetime):
            msg = "retry_after must be a datetime"
            raise TypeError(msg)
        if self.retry_after.utcoffset() != datetime.timedelta(0):
            msg = "retry_after must be UTC-aware"
            raise ValueError(msg)


type FailureTreatment = ConsumeFailureBudget | RetryWithoutBudget
type NonBudgetedTreatmentProvider = typing.Callable[[], RetryWithoutBudget]


@dataclasses.dataclass(frozen=True, slots=True)
class FailurePersistencePlan:
    """Canonical failure evidence paired with one selected treatment.

    Attributes:
        status_reason: Canonical failure classification.
        reason: Optional operator-facing diagnostic detail.
        treatment: Closed budgeted or non-budgeted persistence action.
    """

    status_reason: feed_store.FeedStatusReason
    reason: str | None
    treatment: FailureTreatment

    def __post_init__(self) -> None:
        if not isinstance(self.status_reason, feed_store.FeedStatusReason):
            msg = "status_reason must be a FeedStatusReason"
            raise TypeError(msg)
        if self.reason is not None and not isinstance(self.reason, str):
            msg = "reason must be a string or None"
            raise TypeError(msg)
        if not isinstance(
            self.treatment,
            (ConsumeFailureBudget, RetryWithoutBudget),
        ):
            msg = "treatment must be a closed FailureTreatment"
            raise TypeError(msg)
        if consumes_failure_budget(self.status_reason) != isinstance(
            self.treatment,
            ConsumeFailureBudget,
        ):
            msg = "status_reason classification must match treatment"
            raise ValueError(msg)


_BUDGETED_REASONS = frozenset(
    {
        feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        feed_store.FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID,
    }
)


def consumes_failure_budget(
    status_reason: feed_store.FeedStatusReason,
) -> bool:
    """Return whether one canonical reason consumes the failure budget.

    Args:
        status_reason: Canonical reason to classify.

    Returns:
        ``True`` only for configuration-invalid reasons.

    """
    return status_reason in _BUDGETED_REASONS


def plan_failure(
    status_reason: feed_store.FeedStatusReason,
    reason: str | None,
    *,
    budgeted: ConsumeFailureBudget,
    non_budgeted: NonBudgetedTreatmentProvider,
) -> FailurePersistencePlan:
    """Select one immutable persistence treatment from the shared policy.

    Args:
        status_reason: Canonical reason to classify.
        reason: Optional operator-facing diagnostic detail.
        budgeted: Configured treatment for budget-consuming failures.
        non_budgeted: Lazy provider for the non-budgeted retry treatment.

    Returns:
        Canonical failure evidence with one materialized treatment.

    """
    if consumes_failure_budget(status_reason):
        treatment: FailureTreatment = budgeted
    else:
        treatment = non_budgeted()
    return FailurePersistencePlan(status_reason, reason, treatment)

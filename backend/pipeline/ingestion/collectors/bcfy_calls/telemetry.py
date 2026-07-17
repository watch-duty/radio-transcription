"""Bounded Broadcastify Calls telemetry with one signed-URL exception."""

from __future__ import annotations

import dataclasses
import enum
import logging
import math
import typing

from backend.pipeline.ingestion import slo_contract
from backend.pipeline.storage import feed_store, ingestion_lease_store

if typing.TYPE_CHECKING:
    import datetime

    from backend.pipeline.ingestion.collectors.bcfy_calls import cursor_policy

__all__ = [
    "MissingCallEvent",
    "MissingCallStage",
    "ReplayWindowTruncatedEvent",
    "SidPollEvidence",
    "SidPollLifecycleScope",
    "SidPollOutcome",
    "SidPollResponseLastPosState",
    "emit_missing_call",
    "emit_replay_window_truncated",
    "emit_sid_poll",
    "missing_call_json_fields",
    "replay_window_truncated_json_fields",
    "sid_poll_json_fields",
]

logger = logging.getLogger(__name__)


class SidPollOutcome(enum.StrEnum):
    """Closed terminal outcome for one started logical SID poll."""

    SUCCESS = "success"
    MEMBERSHIP_UNCERTAIN = "membership_uncertain"
    MEMBERSHIP_INVALID = "membership_invalid"
    PROVIDER_FAILED = "provider_failed"
    PAGE_FAILED = "page_failed"
    STOPPED = "stopped"
    AUTHORITY_LOST = "authority_lost"


class SidPollResponseLastPosState(enum.StrEnum):
    """Closed interpretation of one provider response boundary."""

    NOT_OBSERVED = "not_observed"
    MISSING = "missing"
    INVALID = "invalid"
    REGRESSIVE = "regressive"
    VALID = "valid"


class SidPollLifecycleScope(enum.StrEnum):
    """Closed lifecycle scope newly selected by one settled page."""

    NONE = "none"
    FAILED_FEEDS = "failed_feeds"
    LEASE_ONLY = "lease_only"


def _require_nonnegative_integer(value: int, *, name: str) -> int:
    if isinstance(value, bool):
        message = f"{name} must be an integer"
        raise TypeError(message)
    if not 0 <= value <= slo_contract.BCFY_CALLS_SID_POLL_COUNT_MAX:
        message = f"{name} is outside the bounded poll contract"
        raise ValueError(message)
    return value


def _require_optional_nonnegative_integer(
    value: int | None,
    *,
    name: str,
) -> int | None:
    if value is None:
        return None
    return _require_nonnegative_integer(value, name=name)


def _require_nonnegative_seconds(
    value: float,
    *,
    name: str,
) -> float:
    if isinstance(value, bool):
        message = f"{name} must be a number"
        raise TypeError(message)
    numeric = float(value)
    if (
        not math.isfinite(numeric)
        or not 0.0 <= numeric <= slo_contract.BCFY_CALLS_SID_POLL_SECONDS_MAX
    ):
        message = f"{name} is outside the bounded poll contract"
        raise ValueError(message)
    return numeric


def _require_optional_nonnegative_seconds(
    value: float | None,
    *,
    name: str,
) -> float | None:
    if value is None:
        return None
    return _require_nonnegative_seconds(value, name=name)


@dataclasses.dataclass(frozen=True, slots=True)
class SidPollEvidence:
    """One bounded scalar description of a settled logical SID poll."""

    sid: str
    source_type: str
    owner_worker_id: str
    fencing_token: int
    membership_revision: int | None
    outcome: SidPollOutcome
    duration_seconds: float
    provider_observed: bool
    http_attempt_count: int
    response_row_count: int | None
    response_byte_count: int | None
    response_distinct_audio_url_count: int | None
    request_pos: int | None
    response_last_pos: int | None
    response_last_pos_state: SidPollResponseLastPosState
    matched_call_count: int
    deduplicated_call_count: int
    admitted_record_count: int
    admitted_cohort_count: int
    lease_cursor_lag_seconds: float | None
    maximum_feed_cursor_lag_seconds: float | None
    null_feed_cursor_count: int
    total_queue_wait_seconds: float
    maximum_queue_wait_seconds: float
    oldest_queue_age_seconds: float
    pressure_encountered: bool
    pressure_wait_count: int
    pressure_wait_seconds: float
    maximum_held_count: int
    maximum_queue_depth: int
    maximum_worker_utilization_numerator: int
    worker_utilization_denominator: int
    early_flush_attempt_count: int
    final_flush_attempt_count: int
    total_flush_latency_seconds: float
    maximum_flush_latency_seconds: float
    participating_feed_count: int
    successful_feed_count: int
    failed_feed_count: int
    item_to_feed_promotion_count: int
    lifecycle_scope: SidPollLifecycleScope
    lifecycle_reason: str | None
    fence_rejection_count: int
    membership_rejection_count: int
    terminal_record_count: int
    replay_blocked_record_count: int

    def __post_init__(self) -> None:
        for name, value in (
            ("sid", self.sid),
            ("source_type", self.source_type),
            ("owner_worker_id", self.owner_worker_id),
        ):
            _require_bounded_string(
                value,
                name=name,
                maximum=(slo_contract.BCFY_CALLS_SID_POLL_STRING_MAX_LENGTH),
            )
        for name, value in (
            ("fencing_token", self.fencing_token),
            ("http_attempt_count", self.http_attempt_count),
            ("matched_call_count", self.matched_call_count),
            ("deduplicated_call_count", self.deduplicated_call_count),
            ("admitted_record_count", self.admitted_record_count),
            ("admitted_cohort_count", self.admitted_cohort_count),
            ("null_feed_cursor_count", self.null_feed_cursor_count),
            ("pressure_wait_count", self.pressure_wait_count),
            ("maximum_held_count", self.maximum_held_count),
            ("maximum_queue_depth", self.maximum_queue_depth),
            (
                "maximum_worker_utilization_numerator",
                self.maximum_worker_utilization_numerator,
            ),
            (
                "worker_utilization_denominator",
                self.worker_utilization_denominator,
            ),
            ("early_flush_attempt_count", self.early_flush_attempt_count),
            ("final_flush_attempt_count", self.final_flush_attempt_count),
            ("participating_feed_count", self.participating_feed_count),
            ("successful_feed_count", self.successful_feed_count),
            ("failed_feed_count", self.failed_feed_count),
            (
                "item_to_feed_promotion_count",
                self.item_to_feed_promotion_count,
            ),
            ("fence_rejection_count", self.fence_rejection_count),
            ("membership_rejection_count", self.membership_rejection_count),
            ("terminal_record_count", self.terminal_record_count),
            (
                "replay_blocked_record_count",
                self.replay_blocked_record_count,
            ),
        ):
            _require_nonnegative_integer(value, name=name)
        for name, value in (
            ("membership_revision", self.membership_revision),
            ("response_row_count", self.response_row_count),
            ("response_byte_count", self.response_byte_count),
            (
                "response_distinct_audio_url_count",
                self.response_distinct_audio_url_count,
            ),
            ("request_pos", self.request_pos),
            ("response_last_pos", self.response_last_pos),
        ):
            _require_optional_nonnegative_integer(value, name=name)
        for name, value in (
            ("duration_seconds", self.duration_seconds),
            ("total_queue_wait_seconds", self.total_queue_wait_seconds),
            (
                "maximum_queue_wait_seconds",
                self.maximum_queue_wait_seconds,
            ),
            ("oldest_queue_age_seconds", self.oldest_queue_age_seconds),
            ("pressure_wait_seconds", self.pressure_wait_seconds),
            (
                "total_flush_latency_seconds",
                self.total_flush_latency_seconds,
            ),
            (
                "maximum_flush_latency_seconds",
                self.maximum_flush_latency_seconds,
            ),
        ):
            _require_nonnegative_seconds(value, name=name)
        for name, value in (
            ("lease_cursor_lag_seconds", self.lease_cursor_lag_seconds),
            (
                "maximum_feed_cursor_lag_seconds",
                self.maximum_feed_cursor_lag_seconds,
            ),
        ):
            _require_optional_nonnegative_seconds(value, name=name)
        self._validate_provider_fields()
        self._validate_scheduler_fields()
        self._validate_verdict_fields()

    def _validate_provider_fields(self) -> None:
        response_values = (
            self.response_row_count,
            self.response_byte_count,
            self.response_distinct_audio_url_count,
        )
        if not self.provider_observed:
            if any(value is not None for value in response_values) or (
                self.response_last_pos is not None
                or self.response_last_pos_state
                is not SidPollResponseLastPosState.NOT_OBSERVED
            ):
                message = "unobserved provider response has response evidence"
                raise ValueError(message)
            return
        if any(value is None for value in response_values):
            message = "observed provider response lacks scalar evidence"
            raise ValueError(message)
        if (
            self.response_distinct_audio_url_count is not None
            and self.response_row_count is not None
            and self.response_distinct_audio_url_count > self.response_row_count
        ):
            message = "distinct provider audio URLs exceed response rows"
            raise ValueError(message)
        if (
            self.response_last_pos_state
            is SidPollResponseLastPosState.NOT_OBSERVED
        ):
            message = "observed provider response has no lastPos state"
            raise ValueError(message)
        has_last_pos = self.response_last_pos is not None
        if has_last_pos != (
            self.response_last_pos_state is SidPollResponseLastPosState.VALID
        ):
            message = "lastPos value and closed state disagree"
            raise ValueError(message)

    def _validate_scheduler_fields(self) -> None:
        if self.admitted_cohort_count > self.admitted_record_count:
            message = "admitted cohorts exceed admitted records"
            raise ValueError(message)
        if self.terminal_record_count > self.admitted_record_count:
            message = "terminal records exceed admitted records"
            raise ValueError(message)
        if not (
            self.oldest_queue_age_seconds
            <= self.maximum_queue_wait_seconds
            <= self.total_queue_wait_seconds
        ):
            message = "queue age and wait totals disagree"
            raise ValueError(message)
        if self.maximum_queue_depth > self.maximum_held_count:
            message = "queue depth exceeds held work"
            raise ValueError(message)
        if (
            self.maximum_worker_utilization_numerator
            > self.worker_utilization_denominator
        ):
            message = "worker utilization exceeds its denominator"
            raise ValueError(message)
        if self.pressure_encountered != (self.pressure_wait_count > 0):
            message = "pressure flag and wait count disagree"
            raise ValueError(message)
        if (
            self.maximum_flush_latency_seconds
            > self.total_flush_latency_seconds
        ):
            message = "maximum flush latency exceeds its total"
            raise ValueError(message)

    def _validate_verdict_fields(self) -> None:
        if not (
            self.item_to_feed_promotion_count
            <= self.failed_feed_count
            <= self.participating_feed_count
        ):
            message = "poll promotion counts disagree"
            raise ValueError(message)
        if self.successful_feed_count > self.participating_feed_count:
            message = "successful Feeds exceed participating Feeds"
            raise ValueError(message)
        neutral = self.lifecycle_scope is SidPollLifecycleScope.NONE
        if neutral != (self.lifecycle_reason is None):
            message = "lifecycle scope and canonical reason disagree"
            raise ValueError(message)
        if self.lifecycle_reason is not None:
            _require_bounded_string(
                self.lifecycle_reason,
                name="lifecycle_reason",
                maximum=(slo_contract.BCFY_CALLS_SID_POLL_STRING_MAX_LENGTH),
            )
        if (
            self.outcome
            in {
                SidPollOutcome.STOPPED,
                SidPollOutcome.AUTHORITY_LOST,
            }
            and not neutral
        ):
            message = "neutral terminal poll cannot select lifecycle scope"
            raise ValueError(message)


def sid_poll_json_fields(event: SidPollEvidence) -> dict[str, object]:
    """Build the exact schema-version-one SID poll payload."""
    fields = dataclasses.asdict(event)
    fields["event_type"] = slo_contract.EVENT_TYPE_BCFY_CALLS_SID_POLL
    fields["schema_version"] = slo_contract.BCFY_CALLS_SID_POLL_SCHEMA_VERSION
    fields["outcome"] = event.outcome.value
    fields["response_last_pos_state"] = event.response_last_pos_state.value
    fields["lifecycle_scope"] = event.lifecycle_scope.value
    supported_fields = (
        slo_contract.BCFY_CALLS_SID_POLL_REQUIRED_FIELDS
        + slo_contract.BCFY_CALLS_SID_POLL_OPTIONAL_FIELDS
    )
    if tuple(sorted(fields)) != tuple(sorted(supported_fields)):
        message = "SID poll payload drifted from schema version one"
        raise ValueError(message)
    return fields


def emit_sid_poll(event: SidPollEvidence) -> None:
    """Emit one best-effort event without changing poll settlement."""
    try:
        fields = sid_poll_json_fields(event)
        logger.info(
            "Broadcastify Calls SID poll settled",
            extra={"json_fields": fields},
        )
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        return


@dataclasses.dataclass(frozen=True, slots=True)
class ReplayWindowTruncatedEvent:
    """Complete grant identity plus one exact known replay-loss interval."""

    grant: ingestion_lease_store.LeaseGrant
    floor_reached: cursor_policy.ReplayFloorReached

    def __post_init__(self) -> None:
        if self.grant.source_type is not feed_store.SourceType.BCFY_CALLS:
            message = "replay-window telemetry requires a bcfy_calls grant"
            raise ValueError(message)


def _replay_timestamp(value: datetime.datetime) -> str:
    """Return one canonical UTC event timestamp."""
    return value.isoformat()


def replay_window_truncated_json_fields(
    event: ReplayWindowTruncatedEvent,
) -> dict[str, object]:
    """Build the exact scalar schema-version-one truncation payload."""
    grant = event.grant
    evidence = event.floor_reached
    fields: dict[str, object] = {
        "cause": evidence.cause.value,
        "event_type": (
            slo_contract.EVENT_TYPE_BCFY_CALLS_REPLAY_WINDOW_TRUNCATED
        ),
        "fencing_token": grant.fencing_token,
        "floor_start": _replay_timestamp(evidence.floor_start),
        "lost_duration_seconds": evidence.lost_duration_seconds,
        "lost_span_end": _replay_timestamp(evidence.lost_span_end),
        "lost_span_start": _replay_timestamp(evidence.lost_span_start),
        "owner_worker_id": str(grant.owner_worker_id),
        "requested_start": _replay_timestamp(evidence.requested_start),
        "schema_version": (
            slo_contract.BCFY_CALLS_REPLAY_WINDOW_TRUNCATED_SCHEMA_VERSION
        ),
        "sid": grant.lease_key,
        "source_type": grant.source_type.value,
    }
    if tuple(sorted(fields)) != tuple(
        sorted(slo_contract.BCFY_CALLS_REPLAY_WINDOW_TRUNCATED_REQUIRED_FIELDS)
    ):
        message = "replay-window payload drifted from schema version one"
        raise ValueError(message)
    for name in ("sid", "source_type", "owner_worker_id", "cause"):
        _require_bounded_string(
            typing.cast("str", fields[name]),
            name=name,
            maximum=(
                slo_contract.BCFY_CALLS_REPLAY_WINDOW_TRUNCATED_STRING_MAX_LENGTH
            ),
        )
    duration = evidence.lost_duration_seconds
    if (
        not math.isfinite(duration)
        or duration < 0
        or duration
        > slo_contract.BCFY_CALLS_REPLAY_WINDOW_TRUNCATED_SECONDS_MAX
    ):
        message = "lost_duration_seconds is outside the bounded event contract"
        raise ValueError(message)
    return fields


def emit_replay_window_truncated(event: ReplayWindowTruncatedEvent) -> None:
    """Emit one best-effort CRITICAL event without changing cursor policy."""
    try:
        fields = replay_window_truncated_json_fields(event)
        logger.critical(
            "Broadcastify Calls replay window truncated",
            extra={"json_fields": fields},
        )
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        return


class MissingCallStage(enum.StrEnum):
    """Closed irreversible stage for one missing call."""

    TERMINAL_ITEM_SKIP = "terminal_item_skip"
    PUBLICATION_EXHAUSTED = "post_bookmark_publication_exhausted"
    PUBLICATION_ABANDONED = "committed_publication_abandonment"


def _require_bounded_string(
    value: str,
    *,
    name: str,
    maximum: int,
) -> str:
    if not value or len(value) > maximum:
        message = f"{name} must be nonempty and at most {maximum} chars"
        raise ValueError(message)
    return value


@dataclasses.dataclass(frozen=True, slots=True)
class MissingCallEvent:
    """Exact bounded fields for the sole URL-unredacted log event."""

    audio_url: str = dataclasses.field(repr=False)
    sid: str
    feed_id: str
    feed_name: str
    source_type: str
    source_feed_id: str
    group_id: str
    provider_ts: str | None
    gap_stage: MissingCallStage
    status_reason: str
    reason: str
    attempt_count: int

    def __post_init__(self) -> None:
        _require_bounded_string(
            self.audio_url,
            name="audio_url",
            maximum=(slo_contract.BCFY_CALLS_MISSING_CALL_AUDIO_URL_MAX_LENGTH),
        )
        for name, value in (
            ("sid", self.sid),
            ("feed_id", self.feed_id),
            ("feed_name", self.feed_name),
            ("source_type", self.source_type),
            ("source_feed_id", self.source_feed_id),
            ("group_id", self.group_id),
            ("status_reason", self.status_reason),
        ):
            _require_bounded_string(
                value,
                name=name,
                maximum=(
                    slo_contract.BCFY_CALLS_MISSING_CALL_IDENTITY_MAX_LENGTH
                ),
            )
        if self.provider_ts is not None:
            _require_bounded_string(
                self.provider_ts,
                name="provider_ts",
                maximum=(
                    slo_contract.BCFY_CALLS_MISSING_CALL_IDENTITY_MAX_LENGTH
                ),
            )
        _require_bounded_string(
            self.reason,
            name="reason",
            maximum=slo_contract.BCFY_CALLS_MISSING_CALL_REASON_MAX_LENGTH,
        )
        if (
            isinstance(self.attempt_count, bool)
            or not 0
            <= self.attempt_count
            <= slo_contract.BCFY_CALLS_MISSING_CALL_ATTEMPT_COUNT_MAX
        ):
            message = "attempt_count is outside the bounded event contract"
            raise ValueError(message)


def missing_call_json_fields(event: MissingCallEvent) -> dict[str, object]:
    """Build the exact version-one missing-call JSON payload."""
    return {
        "audio_url": event.audio_url,
        "attempt_count": event.attempt_count,
        "event_type": slo_contract.EVENT_TYPE_BCFY_CALLS_MISSING_CALL,
        "feed_id": event.feed_id,
        "feed_name": event.feed_name,
        "gap_stage": event.gap_stage.value,
        "group_id": event.group_id,
        "provider_ts": event.provider_ts,
        "reason": event.reason,
        "schema_version": (slo_contract.BCFY_CALLS_MISSING_CALL_SCHEMA_VERSION),
        "sid": event.sid,
        "source_feed_id": event.source_feed_id,
        "source_type": event.source_type,
        "status_reason": event.status_reason,
    }


def emit_missing_call(event: MissingCallEvent) -> None:
    """Emit one best-effort event after its owner releases durable state."""
    fields = missing_call_json_fields(event)
    level = (
        logging.WARNING
        if event.gap_stage is MissingCallStage.TERMINAL_ITEM_SKIP
        else logging.ERROR
    )
    try:
        logger.log(
            level,
            "Broadcastify Calls missing call",
            extra={"json_fields": fields},
        )
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        # Never log the logging failure: its arguments include the signed URL.
        return

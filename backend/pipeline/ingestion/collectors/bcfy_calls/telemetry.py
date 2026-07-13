"""Bounded Broadcastify Calls telemetry with one signed-URL exception."""

from __future__ import annotations

import dataclasses
import enum
import logging

from backend.pipeline.ingestion import slo_contract

__all__ = [
    "MissingCallEvent",
    "MissingCallStage",
    "emit_missing_call",
    "missing_call_json_fields",
]

logger = logging.getLogger(__name__)


class MissingCallStage(enum.StrEnum):
    """Closed irreversible stage for one missing call."""

    TERMINAL_ITEM_SKIP = "terminal_item_skip"
    PUBLICATION_EXHAUSTED = "post_bookmark_publication_exhausted"
    PUBLICATION_ABANDONED = "committed_publication_abandonment"


def _require_bounded_string(
    value: object,
    *,
    name: str,
    maximum: int,
) -> str:
    if not isinstance(value, str):
        message = f"{name} must be a string"
        raise TypeError(message)
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
        if not isinstance(self.gap_stage, MissingCallStage):
            message = "gap_stage must be a MissingCallStage"
            raise TypeError(message)
        _require_bounded_string(
            self.reason,
            name="reason",
            maximum=slo_contract.BCFY_CALLS_MISSING_CALL_REASON_MAX_LENGTH,
        )
        if (
            isinstance(self.attempt_count, bool)
            or not isinstance(self.attempt_count, int)
            or not 0
            <= self.attempt_count
            <= slo_contract.BCFY_CALLS_MISSING_CALL_ATTEMPT_COUNT_MAX
        ):
            message = "attempt_count is outside the bounded event contract"
            raise ValueError(message)


def missing_call_json_fields(event: MissingCallEvent) -> dict[str, object]:
    """Build the exact version-one missing-call JSON payload."""
    if type(event) is not MissingCallEvent:
        message = "event must be an exact MissingCallEvent"
        raise TypeError(message)
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

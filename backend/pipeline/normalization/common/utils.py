"""Utility functions for the radio transcription pipeline."""

import uuid
from typing import Self

import pydantic

from backend.pipeline.normalization.common.logging import get_task_logger
from backend.pipeline.schema_types import streaming_state as bp_state

logger = get_task_logger(
    __name__, {"system": "transcription", "component": "utils"}
)


class ConfigBase(pydantic.BaseModel):
    """Base Pydantic model for JSON configuration classes."""

    model_config = pydantic.ConfigDict(extra="ignore", frozen=True)

    @classmethod
    def from_json(cls, json_str: str) -> Self:
        """Creates an instance from a JSON string."""
        if not json_str:
            return cls()
        try:
            return cls.model_validate_json(json_str)
        except pydantic.ValidationError as e:
            logger.exception(
                "Failed to parse config JSON for %s: %s", cls.__name__, json_str
            )
            msg = f"Invalid config JSON for {cls.__name__}: {e}"
            raise ValueError(msg) from e


def generate_segment_id(
    feed_or_session_id: str,
    time_range: bp_state.TimeRangeProto,
    segment_duration_ms: int = 0,
) -> str:
    """Generates a deterministic UUID5 using raw VAD boundaries and segment duration to ensure retry stability and prevent collisions."""
    deterministic_id = f"{feed_or_session_id}_{time_range.start_ms}_{time_range.end_ms}_{segment_duration_ms}"
    return str(uuid.uuid5(uuid.NAMESPACE_OID, deterministic_id))


def get_duration_ms(time_range: bp_state.TimeRangeProto) -> int:
    """Stateless helper to calculate duration of a time range in milliseconds."""
    return time_range.end_ms - time_range.start_ms

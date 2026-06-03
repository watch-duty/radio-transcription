"""Utility functions for the segmentation Apache Beam pipeline."""

from backend.pipeline.common import utils as common_utils
from backend.pipeline.schema_types import streaming_state as bp_state


def generate_segment_id(
    feed_or_session_id: str,
    time_range: bp_state.TimeRangeProto,
    segment_duration_ms: int = 0,
) -> str:
    """Generates a deterministic UUID5 using raw VAD boundaries and segment duration."""
    suffix = f"{time_range.start_ms}_{time_range.end_ms}_{segment_duration_ms}"
    return common_utils.generate_segment_id(feed_or_session_id, suffix)


def get_duration_ms(time_range: bp_state.TimeRangeProto) -> int:
    """Stateless helper to calculate duration of a time range in milliseconds."""
    return time_range.end_ms - time_range.start_ms

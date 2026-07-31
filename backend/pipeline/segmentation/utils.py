"""Utility functions for the segmentation Apache Beam pipeline."""

from collections.abc import Sequence

from backend.pipeline.common import utils as common_utils
from backend.pipeline.schema_types import streaming_state as bp_state
from backend.pipeline.schema_types.segmented_audio_pb2 import SegmentedAudio


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


def is_speech_classification(audio_classification: int) -> bool:
    """Returns True if the audio classification represents speech."""
    return audio_classification == SegmentedAudio.AUDIO_CLASSIFICATION_SPEECH


def compute_end_audio_offset_ms(
    segment_end_ms: int,
    contributing_chunks: Sequence[bp_state.BufferedChunkProto],
    fallback_chunk_start_ms: int,
) -> int:
    """Computes the offset into the LAST contributing raw source file where this segment's audio ends.

    This mirrors start_audio_offset_ms, which is relative to the start of the
    FIRST contributing file: end_audio_offset_ms must be read against the file
    the segment actually ends in, not against buffer_start_time_ms (which
    anchors the first file and, for multi-file segments, is a different file
    entirely). Using buffer_start_time_ms here previously produced a value
    equal to the segment's own duration rather than a real offset into the
    ending file.
    """
    last_chunk_start_ms = (
        contributing_chunks[-1].timestamp_ms
        if contributing_chunks
        else fallback_chunk_start_ms
    )
    return max(0, segment_end_ms - last_chunk_start_ms)

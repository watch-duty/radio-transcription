"""Utility functions for the radio transcription pipeline."""

import logging
from typing import Self

import pydantic

from backend.pipeline.transcription.constants import (
    DEFAULT_VAD_POST_ROLL_MS,
    DEFAULT_VAD_PRE_ROLL_MS,
)
from backend.pipeline.transcription.datatypes import TimeRange

logger = logging.getLogger(__name__)


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


def calculate_padded_ranges(
    audio_duration_ms: int,
    speech_segments: list[TimeRange],
    noise_segments: list[TimeRange],
    file_start_ms: int,
    pre_roll_ms: int = DEFAULT_VAD_PRE_ROLL_MS,
    post_roll_ms: int = DEFAULT_VAD_POST_ROLL_MS,
) -> list[tuple[int, int]]:
    """Calculates padded boundaries for speech segments, bounded by noise."""
    padded_ranges = []
    for reg in speech_segments:
        if reg.start_ms >= reg.end_ms:
            continue
        global_start_ms = reg.start_ms - file_start_ms
        global_end_ms = reg.end_ms - file_start_ms

        # Pre-roll calculation
        append_start = max(0, global_start_ms - pre_roll_ms)

        # Post-roll calculation
        append_end = min(audio_duration_ms, global_end_ms + post_roll_ms)

        # Noise truncation
        # Find the next noise segment that starts after or at the end of speech
        noise_after = [
            n.start_ms - file_start_ms
            for n in noise_segments
            if n.start_ms >= reg.end_ms
        ]
        if noise_after:
            orig_end = append_end
            append_end = min(append_end, min(noise_after))
            if append_end < orig_end:
                logger.info(
                    f"PaddedSegment post-roll truncated by noise from {orig_end} to {append_end}ms"
                )

        # Find the previous noise segment that ends before or at the start of speech
        noise_before = [
            n.end_ms - file_start_ms
            for n in noise_segments
            if n.end_ms <= reg.start_ms
        ]
        if noise_before:
            append_start = max(append_start, max(noise_before))

        padded_ranges.append((append_start, append_end))
    return padded_ranges

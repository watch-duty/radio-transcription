import logging

import numpy as np

from backend.pipeline.transcription.constants import (
    DEFAULT_VAD_POST_ROLL_MS,
    DEFAULT_VAD_PRE_ROLL_MS,
)
from backend.pipeline.transcription.datatypes import TimeRange

SILENCE_THRESHOLD_DB = -40

logger = logging.getLogger(__name__)



def calculate_padded_ranges(
    audio_duration_ms: int,
    speech_segments: list[TimeRange],
    noise_segments: list[TimeRange],
    file_start_ms: int,
    pre_roll_ms: int = DEFAULT_VAD_PRE_ROLL_MS,
    post_roll_ms: int = DEFAULT_VAD_POST_ROLL_MS,
) -> list[tuple[int, int]]:
    """Calculates padded boundaries for speech segments, avoiding noise and other speech."""
    padded_ranges = []

    # Convert speech segments to relative time for easier comparison
    rel_speech = []
    for reg in speech_segments:
        if reg.start_ms >= reg.end_ms:
            continue
        rel_speech.append(
            (reg.start_ms - file_start_ms, reg.end_ms - file_start_ms)
        )

    # Convert noise segments to relative time
    rel_noise = []
    for n in noise_segments:
        rel_noise.append(
            (n.start_ms - file_start_ms, n.end_ms - file_start_ms)
        )

    for i, (global_start_ms, global_end_ms) in enumerate(rel_speech):
        # Standard padding expansion
        append_start = max(0, global_start_ms - pre_roll_ms)
        append_end = min(audio_duration_ms, global_end_ms + post_roll_ms)

        # 1. Bumper: Don't pad into ADJACENT speech segments (Bisect if they overlap)
        # Check previous speech segment
        if i > 0:
            prev_speech_end = rel_speech[i - 1][1]
            if global_start_ms - prev_speech_end < pre_roll_ms + post_roll_ms:
                midpoint = (prev_speech_end + global_start_ms) // 2
                append_start = max(append_start, midpoint)
            else:
                append_start = max(append_start, prev_speech_end)

        # Check next speech segment
        if i + 1 < len(rel_speech):
            next_speech_start = rel_speech[i + 1][0]
            if next_speech_start - global_end_ms < pre_roll_ms + post_roll_ms:
                midpoint = (global_end_ms + next_speech_start) // 2
                append_end = min(append_end, midpoint)
            else:
                append_end = min(append_end, next_speech_start)


        # 2. Bumper: Don't pad into KNOWN NOISE (Sirens, Alert Tones)
        for noise_start, noise_end in rel_noise:
            # Check for pre-roll collision
            if append_start < noise_end <= global_start_ms:
                append_start = max(append_start, noise_end)

            # Check for post-roll collision
            if global_end_ms <= noise_start < append_end:
                append_end = min(append_end, noise_start)

        padded_ranges.append((append_start, append_end))

    return padded_ranges


def detect_silence_segments(
    audio_float: np.ndarray,
    sample_rate: int,
    threshold_db: float,
    window_ms: int = 30,
) -> list[TimeRange]:
    """Detects silence segments in audio based on energy threshold.

    Args:
        audio_float: Audio samples in float32 range [-1.0, 1.0].
        sample_rate: Sample rate of the audio.
        threshold_db: Energy threshold in dB (e.g., -40).
        window_ms: Window size in milliseconds for energy calculation.

    Returns:
        List of TimeRange objects representing silent segments.
    """
    window_size = int((window_ms / 1000.0) * sample_rate)
    num_chunks = len(audio_float) // window_size

    if num_chunks == 0:
        return []

    reshaped = audio_float[: num_chunks * window_size].reshape(
        (num_chunks, window_size)
    )

    # Calculate RMS per chunk
    # Add small epsilon to prevent log10(0)
    chunk_rms = np.sqrt(np.mean(reshaped**2, axis=1) + 1e-10)

    # Convert to dB
    chunk_db = 20 * np.log10(chunk_rms)

    # Find silent chunks
    silent_mask = chunk_db < threshold_db

    silence_segments = []
    in_silence = False
    start_time_ms = 0

    for i, is_silent in enumerate(silent_mask):
        current_time_ms = int((i * window_size * 1000.0) / sample_rate)

        if is_silent and not in_silence:
            in_silence = True
            start_time_ms = current_time_ms
        elif not is_silent and in_silence:
            in_silence = False
            silence_segments.append(
                TimeRange(start_ms=start_time_ms, end_ms=current_time_ms)
            )

    # Handle case where silence ends at the end of audio
    if in_silence:
        end_time_ms = int(
            (num_chunks * window_size * 1000.0) / sample_rate
        )
        silence_segments.append(
            TimeRange(start_ms=start_time_ms, end_ms=end_time_ms)
        )

    return silence_segments


import logging
from dataclasses import dataclass
import os
import numpy as np
import sherpa_onnx

from backend.pipeline.common.constants import SAMPLE_RATE_HZ
from backend.pipeline.transcription.constants import (
    MS_PER_SEC,
)
from backend.pipeline.transcription.datatypes import (
    AudioChunkData,
    TimeRange,
    VadResult,
)
from backend.pipeline.transcription.audio.signal_processing import (
    RadioSignalAnalyzer,
)

logger = logging.getLogger(__name__)


class VadWrapper:
    """Wrapper around Sherpa-ONNX VoiceActivityDetector to allow adding attributes."""

    def __init__(self, detector: sherpa_onnx.VoiceActivityDetector):
        self.detector = detector
        self.processed_samples = 0


def create_sherpa_vad(
    threshold: float = 0.5,
    min_silence_duration: float = 0.25,
    min_speech_duration: float = 0.5,
) -> VadWrapper:
    """Factory for Sherpa-ONNX VoiceActivityDetector."""
    vad_config = sherpa_onnx.VadModelConfig(
        silero_vad=sherpa_onnx.SileroVadModelConfig(
            model=os.environ.get(
                "SHERPA_VAD_MODEL_PATH",
                "backend/pipeline/transcription/resources/silero_vad.onnx",
            ),
            threshold=threshold,
            min_silence_duration=min_silence_duration,
            min_speech_duration=min_speech_duration,
        ),
        sample_rate=SAMPLE_RATE_HZ,
        num_threads=1,
    )
    detector = sherpa_onnx.VoiceActivityDetector(
        vad_config, buffer_size_in_seconds=30
    )
    return VadWrapper(detector)


def process_vad_streaming(
    samples: np.ndarray, start_ms: int, vad: VadWrapper
) -> VadResult:
    """Feeds audio to Sherpa VAD and drains detected segments.

    Args:
        samples: int16 numpy array.
        start_ms: Absolute timeline offset of this chunk.
        vad: VadWrapper instance containing Sherpa VAD and state.
    """
    if len(samples) == 0:
        return VadResult(speech_segments=[], silence_segments=[])

    # Online VAD accepts float32 in range [-1, 1]
    samples_float = samples.astype(np.float32) / 32768.0

    window_size = 512  # SIGNAL_ANALYZER_HOP_LENGTH
    chunk_start_vad_samples = vad.processed_samples

    # Process in chunks of window_size
    for i in range(0, len(samples_float) - window_size + 1, window_size):
        chunk = samples_float[i : i + window_size]
        vad.detector.accept_waveform(chunk)
        vad.processed_samples += window_size

    speech_segments: list[TimeRange] = []
    silence_segments: list[TimeRange] = []

    # Drain any detected segments
    while not vad.detector.empty():
        segment = vad.detector.front
        logger.debug("RAW VAD segment.start: %s", segment.start)
        relative_start_samples = segment.start - chunk_start_vad_samples
        seg_start_ms = start_ms + int(
            (relative_start_samples / SAMPLE_RATE_HZ) * MS_PER_SEC
        )

        # Apply lookback (e.g., 100ms) to catch clipped onsets, clamped to chunk start
        lookback_ms = 100
        seg_start_ms = max(start_ms, seg_start_ms - lookback_ms)

        duration_sec = len(segment.samples) / SAMPLE_RATE_HZ
        seg_end_ms = seg_start_ms + int(duration_sec * MS_PER_SEC)

        speech_segments.append(
            TimeRange(start_ms=seg_start_ms, end_ms=seg_end_ms)
        )
        vad.detector.pop()

    return VadResult(
        speech_segments=speech_segments,
        silence_segments=silence_segments,
    )

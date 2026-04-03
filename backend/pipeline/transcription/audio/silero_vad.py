import logging
from dataclasses import dataclass
import os

import numpy as np
import pydub
import sherpa_onnx

from backend.pipeline.common.constants import SAMPLE_RATE_HZ
from backend.pipeline.transcription.constants import (
    MS_PER_SEC,
)
from backend.pipeline.transcription.datatypes import AudioChunkData, TimeRange
from backend.pipeline.transcription.audio.signal_processing import RadioSignalAnalyzer

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
    detector = sherpa_onnx.VoiceActivityDetector(vad_config, buffer_size_in_seconds=30)
    return VadWrapper(detector)


@dataclass
class VadResult:
    speech_segments: list[TimeRange]
    silence_segments: list[TimeRange]


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
        # segment.start is cumulative samples from VAD start.
        # Offset within current chunk is segment.start - chunk_start_vad_samples
        relative_start_samples = segment.start - chunk_start_vad_samples
        seg_start_ms = start_ms + int((relative_start_samples / SAMPLE_RATE_HZ) * MS_PER_SEC)
        
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


def verify_speech_segment(audio_buffer: pydub.AudioSegment) -> bool:
    """Heuristic check to see if there is tonality (speech) in the buffer.
    
    Uses RadioSignalAnalyzer to evaluate pYIN pitch tracking and HNR.
    """
    samples = np.array(audio_buffer.get_array_of_samples()).astype(np.float32) / 32768.0
    if len(samples) == 0:
        return False

    analyzer = RadioSignalAnalyzer()
    characterization = analyzer.characterize(samples)

    if characterization.label == "deterministic_linear":
        logger.info("verify_speech_segment: Tonal noise (horn/siren) detected. Rejecting.")
        return False

    # Heuristic for speech: high HNR and high confidence (voiced probability)
    # hnr > 3.0 (approx 5dB), confidence > 0.5, and we have voiced frames.
    if (
        characterization.hnr > 3.0
        and characterization.confidence > 0.5
        and characterization.trimmed_duration_ms > 0
    ):
        logger.info(
            "verify_speech_segment: Speech detected (HNR: %.1f, Confidence: %.2f, Duration: %d ms). Approving.",
            characterization.hnr,
            characterization.confidence,
            characterization.trimmed_duration_ms,
        )
        return True

    logger.info(
        "verify_speech_segment: Signal appears to be static or noise (HNR: %.1f, Confidence: %.2f). Rejecting.",
        characterization.hnr,
        characterization.confidence,
    )
    return False

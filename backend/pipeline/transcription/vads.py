"""
Voice Activity Detection (VAD) Plugin System.

This module defines the abstract interface for VAD engines used by the pipeline to
distinguish speech from silence/noise, preventing empty or purely static audio
from being sent to the expensive transcription APIs.
"""

import abc
import logging
from dataclasses import dataclass

import numpy as np
import ten_vad

from backend.pipeline.transcription.constants import (
    DEFAULT_TENVAD_HOP_SIZE,
    DEFAULT_TENVAD_MIN_SPEECH_SEC,
    DEFAULT_TENVAD_THRESHOLD,
)
from backend.pipeline.transcription.enums import VadType
from backend.pipeline.transcription.utils import JsonConfigMixin

logger = logging.getLogger(__name__)


class VoiceActivityDetector(abc.ABC):
    """
    Abstract base class for Voice Activity Detection (VAD) plugins.
    Allows swapping out different VAD models/services without changing the core
    Beam pipeline logic.
    """

    @abc.abstractmethod
    def setup(self, config_json: str) -> None:
        """
        Initializes the VAD model or client.
        Called once per worker during Beam's DoFn setup phase.
        """

    @abc.abstractmethod
    def evaluate(self, audio_data: bytes, sample_rate: int) -> bool:
        """
        Evaluates raw PCM audio data and returns True if speech is detected.
        """


@dataclass(frozen=True)
class TenVadConfig(JsonConfigMixin):
    """Strongly typed configuration for the TenVAD plugin."""

    # VAD tuning parameters
    threshold: float = DEFAULT_TENVAD_THRESHOLD
    hop_size: int = DEFAULT_TENVAD_HOP_SIZE
    min_speech_sec: float = DEFAULT_TENVAD_MIN_SPEECH_SEC


class TenVadPlugin(VoiceActivityDetector):
    """
    VAD Plugin utilizing the local `ten_vad` library.
    """

    config: TenVadConfig
    vad: ten_vad.TenVad

    def setup(self, config_json: str) -> None:

        self.config = TenVadConfig.from_json(config_json)
        self.vad = ten_vad.TenVad(
            threshold=self.config.threshold, hop_size=self.config.hop_size
        )
        logger.info(
            "TenVAD plugin initialized with threshold: %s, hop_size: %s",
            self.config.threshold,
            self.config.hop_size,
        )

    def evaluate(self, audio_data: bytes, sample_rate: int) -> bool:
        # Convert raw PCM bytes to int16 numpy array
        audio_array = np.frombuffer(audio_data, dtype=np.int16)

        speech_frames = 0
        hop_size = self.config.hop_size

        for i in range(0, len(audio_array), hop_size):
            chunk = audio_array[i : i + hop_size]
            if len(chunk) < hop_size:
                # Pad the last chunk with zeros if necessary
                padded_chunk = np.zeros(hop_size, dtype=np.int16)
                padded_chunk[: len(chunk)] = chunk
                chunk = padded_chunk

            prob, _flags = self.vad.process(chunk)

            if prob >= self.config.threshold:
                speech_frames += 1

        total_speech_sec = (speech_frames * hop_size) / sample_rate

        if total_speech_sec < self.config.min_speech_sec:
            logger.info(
                "TenVAD detected %.3fs speech, below %.3fs threshold. Discarding.",
                total_speech_sec,
                self.config.min_speech_sec,
            )
            return False

        return True


def get_vad_plugin(vad_type: VadType, vad_config: str) -> VoiceActivityDetector:
    """Factory function to instantiate the requested VAD plugin."""
    if vad_type == VadType.TEN_VAD:
        plugin = TenVadPlugin()
        plugin.setup(vad_config)
        return plugin
    msg = f"Unknown vad_type: {vad_type}"
    raise ValueError(msg)

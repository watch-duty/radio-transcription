"""
Voice Activity Detection (VAD) Plugin System.

This module defines the abstract interface for VAD engines used by the pipeline to
distinguish speech from silence/noise, preventing empty or purely static audio
from being sent to the expensive transcription APIs.
"""

import abc
import json
import logging
from dataclasses import dataclass
from typing import Any, cast

import ten_vad

from backend.pipeline.transcription.enums import VadType

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
class TenVadConfig:
    """Strongly typed configuration for the TenVAD plugin."""

    # VAD tuning parameters
    threshold: float = 0.5
    hop_size: int = 256

    @classmethod
    def from_json(cls, json_str: str) -> "TenVadConfig":
        if not json_str:
            return cls()
        try:
            config_dict = json.loads(json_str)
            valid_keys = {f.name for f in cls.__dataclass_fields__.values()}
            filtered_dict = {k: v for k, v in config_dict.items() if k in valid_keys}
            return cls(**filtered_dict)
        except json.JSONDecodeError as e:
            logger.exception("Failed to parse vad_config JSON: %s", json_str)
            msg = f"Invalid vad_config JSON: {e}"
            raise ValueError(msg) from e


class TenVadPlugin(VoiceActivityDetector):
    """
    VAD Plugin utilizing the local `ten_vad` library.
    """

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
        # ten_vad evaluate returns a list of dictionaries with speech segments:
        # {'start': 0.1, 'end': 0.5}, ...]
        vad_instance = cast("Any", self.vad)
        results = vad_instance.evaluate(audio_data, sample_rate=sample_rate)
        return bool(results)


def get_vad_plugin(vad_type: VadType, vad_config: str) -> VoiceActivityDetector:
    """Factory function to instantiate the requested VAD plugin."""
    if vad_type == VadType.TEN_VAD:
        plugin = TenVadPlugin()
        plugin.setup(vad_config)
        return plugin
    msg = f"Unknown vad_type: {vad_type}"
    raise ValueError(msg)

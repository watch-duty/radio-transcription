"""Pluggable Transcription API factory.

Exposes get_transcriber using standard statically-typed imports from sibling modules.
"""

from backend.pipeline.transcription.enums import TranscriberType
from backend.pipeline.transcription.transcribers import gemini_3_1_flash_lite
from backend.pipeline.transcription.transcribers.base import Transcriber
from backend.pipeline.transcription.transcribers.chirp import (
    ChirpConfig,
    GoogleChirpV3Transcriber,
)
from backend.pipeline.transcription.transcribers.local_api import (
    LocalApiTranscriber,
)
from backend.pipeline.transcription.transcribers.mock import (
    MockConfig,
    MockTranscriber,
)


def get_transcriber(
    transcriber_type: TranscriberType,
    project_id: str,
    config_json: str,
) -> Transcriber:
    """A factory method instantiating the requested Transcriber implementation.

    Args:
        transcriber_type: The engine type (MOCK, GOOGLE_CHIRP_V3).
        project_id: The GCP Project ID.
        config_json: JSON serialized configuration string matching the engine.

    Returns:
        An instantiated Transcriber implementation.

    Raises:
        ValueError: If the transcriber type is unrecognized.
    """
    if transcriber_type == TranscriberType.GOOGLE_CHIRP_V3:
        return GoogleChirpV3Transcriber(
            project_id, ChirpConfig.from_json(config_json)
        )
    if transcriber_type == TranscriberType.MOCK:
        return MockTranscriber(MockConfig.from_json(config_json))
    if transcriber_type == TranscriberType.LOCAL_WHISPER:
        return LocalApiTranscriber()
    if transcriber_type == TranscriberType.GEMINI:
        return gemini_3_1_flash_lite.GeminiTranscriber(
            project_id,
            gemini_3_1_flash_lite.GeminiConfig.from_json(config_json),
        )
    msg = f"Unknown transcriber type: {transcriber_type}"
    raise ValueError(msg)

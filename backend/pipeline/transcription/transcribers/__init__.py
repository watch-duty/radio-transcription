"""Pluggable Transcription API Architecture.

This package defines the abstract interface for audio transcription services,
allowing the pipeline to dynamically swap between different engines via configuration.
"""

from backend.pipeline.normalization.common.enums import TranscriberType
from backend.pipeline.transcription.transcribers.base import Transcriber


def get_transcriber(
    transcriber_type: TranscriberType,
    project_id: str,
    config_json: str,
) -> Transcriber:
    """A factory method instantiating the requested Transcriber implementation.

    Uses lazy inline imports to prevent heavy cloud client dependencies (like GCP
    Speech-to-Text client library) from being loaded during local/offline mock
    executions.

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
        from backend.pipeline.transcription.transcribers.chirp import (  # noqa: PLC0415
            ChirpConfig,
            GoogleChirpV3Transcriber,
        )

        return GoogleChirpV3Transcriber(
            project_id, ChirpConfig.from_json(config_json)
        )

    if transcriber_type == TranscriberType.MOCK:
        from backend.pipeline.transcription.transcribers.mock import (  # noqa: PLC0415
            MockConfig,
            MockTranscriber,
        )

        return MockTranscriber(MockConfig.from_json(config_json))

    msg = f"Unknown transcriber type: {transcriber_type}"
    raise ValueError(msg)

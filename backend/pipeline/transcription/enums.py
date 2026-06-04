"""Enums for the audio transcription service."""

from enum import StrEnum


class TranscriberType(StrEnum):
    """Identifies the pluggable Speech-to-Text engine implementation."""

    MOCK = "mock"
    GOOGLE_CHIRP_V3 = "google_chirp_v3"
    LOCAL_WHISPER = "local_whisper"

"""Enums for the audio transcription service."""

from enum import StrEnum


class TranscriberType(StrEnum):
    """Identifies the pluggable Speech-to-Text engine implementation."""

    MOCK = "mock"
    GOOGLE_CHIRP_V3 = "google_chirp_v3"
    LOCAL_WHISPER = "local_whisper"
    GEMINI = "gemini"


class TranscriptionStatus(StrEnum):
    """Status categories for transcription pipeline telemetry."""

    ATTEMPTS = "attempts"
    SUCCESS = "success"
    EMPTY = "empty"
    UNINTELLIGIBLE = "unintelligible"
    PARTIAL = "partial"
    POLICY_BLOCKED = "policy_blocked"
    TRANSIENT_ERROR = "transient_error"
    PERMANENT_ERROR = "permanent_error"

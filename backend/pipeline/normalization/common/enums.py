"""Shared enum constants used across pipeline configuration and state machines."""

import enum


class TranscriberType(enum.StrEnum):
    """Supported external API transcriber engines."""

    GOOGLE_CHIRP_V3 = "google_chirp_v3"
    MOCK = "mock"
    LOCAL_WHISPER = "local_whisper"

"""Shared enum constants used across pipeline configuration and state machines."""
import enum


class TranscriberType(enum.StrEnum):
    """Supported external API transcriber engines."""
    GOOGLE_CHIRP_V3 = "google_chirp_v3"

class VadType(enum.StrEnum):
    """Supported Voice Activity Detection models."""
    TEN_VAD = "ten_vad"

class MetricsExporterType(enum.StrEnum):
    """Supported telemetry and observability destinations."""
    NONE = "none"
    GCP = "gcp"

import enum


class TranscriberType(enum.StrEnum):
    GOOGLE_CHIRP_V3 = "google_chirp_v3"


class VadType(enum.StrEnum):
    TEN_VAD = "ten_vad"


class MetricsExporterType(enum.StrEnum):
    NONE = "none"
    GCP = "gcp"

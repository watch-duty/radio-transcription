from enum import StrEnum


class TranscriberType(StrEnum):
    GOOGLE_CHIRP_V3 = "google_chirp_v3"


class VadType(StrEnum):
    TEN_VAD = "ten_vad"


class MetricsExporterType(StrEnum):
    NONE = "none"
    GCP = "gcp"

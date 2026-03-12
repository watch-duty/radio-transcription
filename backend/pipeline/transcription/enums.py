import enum


class TranscriberType(enum.Enum):
    GOOGLE_CHIRP_V3 = enum.auto()


class VadType(enum.Enum):
    TEN_VAD = enum.auto()

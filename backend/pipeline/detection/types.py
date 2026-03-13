from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True, kw_only=True)
class DetectionResult:
    """
    A single detection event produced by a SoundEventDetector.

    Represents one moment where a detector determined the presence or
    absence of its target signal within a sliding analysis window.

    Attributes:
        signal_present: Whether the target signal was detected.
        confidence: Detection confidence in the range [0.0, 1.0].
        timestamp_ns: Monotonic nanoseconds from the start of the
            current analysis window (NOT wall-clock time).
        detector_type: Identifier matching the ``detector_type`` property
            of the detector that produced this result.  Used as the
            tracking key in the combiner, the provenance field in the
            sidecar proto, and the factory registry key.

    """

    signal_present: bool
    confidence: float
    timestamp_ns: int
    detector_type: str

    def __post_init__(self) -> None:
        if not 0.0 <= self.confidence <= 1.0:
            msg = f"confidence must be in [0.0, 1.0], got {self.confidence}"
            raise ValueError(msg)
        if self.timestamp_ns < 0:
            msg = f"timestamp_ns must be non-negative, got {self.timestamp_ns}"
            raise ValueError(msg)

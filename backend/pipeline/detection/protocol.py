from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    import numpy as np

    from backend.pipeline.detection.types import DetectionResult


@runtime_checkable
class SoundEventDetector(Protocol):
    """
    Batch interface for a sound event detector.

    Every concrete detector (Silero VAD, energy squelch, CTCSS tone
    detector, etc.) must satisfy this protocol.  The protocol is
    structural — implementations do **not** inherit from this class;
    they simply provide the required properties and methods.

    Properties
    ----------
    detector_type
        Unique string identifier for this detector kind
        (e.g. ``"silero_vad"``, ``"energy_squelch"``).  Serves as the
        factory registration key, the combiner tracking key, and the
        sidecar provenance field.

    Methods
    -------
    detect(samples)
        Analyze a complete audio window and return all speech regions
        found.  *samples* is an int16 numpy array of shape
        ``(num_samples,)``.  Implementations handle all internal
        chunking, thresholding, hangover smoothing, and min-duration
        filtering.

    """

    @property
    def detector_type(self) -> str: ...

    def detect(self, samples: np.ndarray) -> DetectionResult: ...

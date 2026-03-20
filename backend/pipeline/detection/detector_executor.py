from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import numpy as np

    from backend.pipeline.detection.protocol import SoundEventDetector
    from backend.pipeline.detection.sound_event_signal_combiner import (
        SoundEventSignalCombiner,
    )
    from backend.pipeline.detection.types import CombinedResult, DetectionResult

logger = logging.getLogger(__name__)


class DetectorExecutor:
    """
    Runs a detector ensemble and combines results.

    Each detector is executed independently; if one raises, the error
    is logged and the remaining detectors continue.  Results from all
    successful detectors are merged by the combiner.
    """

    def __init__(
        self,
        detectors: list[SoundEventDetector],
        combiner: SoundEventSignalCombiner,
    ) -> None:
        self._detectors = detectors
        self._combiner = combiner

    def run(self, samples: np.ndarray, *, context: str = "") -> CombinedResult:
        """
        Execute all detectors on *samples* and return combined result.

        Args:
            samples: int16 numpy array of audio samples.
            context: Optional label for log messages (e.g. GCS URI).

        """
        all_results: list[DetectionResult] = []
        for detector in self._detectors:
            try:
                result = detector.detect(samples)
                all_results.append(result)
            except Exception:
                logger.exception(
                    "Detector %s failed on %s, continuing",
                    detector.detector_type,
                    context,
                )
        return self._combiner.combine(all_results)

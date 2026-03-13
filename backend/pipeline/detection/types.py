from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True, kw_only=True)
class SpeechRegion:
    """
    A contiguous time range where speech was detected.

    Attributes:
        start_sec: Start of the region in seconds from the audio window start.
        end_sec: End of the region in seconds from the audio window start.
        detector_type: Identifier of the detector(s) that produced this region.
            For regions produced by a single detector this matches the
            detector's ``detector_type`` property.  After merging by the
            combiner it may be a comma-joined string of multiple detector
            types (e.g. ``"energy_squelch,silero_vad"``).

    """

    start_sec: float
    end_sec: float
    detector_type: str

    def __post_init__(self) -> None:
        if self.start_sec < 0.0:
            msg = f"start_sec must be non-negative, got {self.start_sec}"
            raise ValueError(msg)
        if self.end_sec < self.start_sec:
            msg = f"end_sec ({self.end_sec}) must be >= start_sec ({self.start_sec})"
            raise ValueError(msg)
        if not self.detector_type:
            msg = "detector_type must be non-empty"
            raise ValueError(msg)


@dataclass(frozen=True, slots=True, kw_only=True)
class DetectionResult:
    """
    Output of a single detector's analysis of an audio window.

    Attributes:
        speech_regions: Speech regions found in the window.  Immutable tuple
            because the dataclass is frozen.
        detector_type: Identifier of the detector that produced this result.
            All regions must carry the same ``detector_type``.

    """

    speech_regions: tuple[SpeechRegion, ...]
    detector_type: str

    def __post_init__(self) -> None:
        if not self.detector_type:
            msg = "detector_type must be non-empty"
            raise ValueError(msg)
        for region in self.speech_regions:
            if region.detector_type != self.detector_type:
                msg = (
                    f"Region detector_type {region.detector_type!r} does not match "
                    f"result detector_type {self.detector_type!r}"
                )
                raise ValueError(msg)


@dataclass(frozen=True, slots=True, kw_only=True)
class CombinedResult:
    """
    Merged output from the signal combiner.

    Attributes:
        speech_regions: Merged speech regions, sorted by ``start_sec``.
            Regions may have comma-joined ``detector_type`` strings when
            they were produced by merging regions from multiple detectors.

    """

    speech_regions: tuple[SpeechRegion, ...]

    def __post_init__(self) -> None:
        for i in range(len(self.speech_regions) - 1):
            if self.speech_regions[i + 1].start_sec < self.speech_regions[i].start_sec:
                msg = (
                    f"speech_regions must be sorted by start_sec: "
                    f"region[{i}].start_sec={self.speech_regions[i].start_sec} > "
                    f"region[{i + 1}].start_sec={self.speech_regions[i + 1].start_sec}"
                )
                raise ValueError(msg)

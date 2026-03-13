from __future__ import annotations

from typing import TYPE_CHECKING

from backend.pipeline.detection.types import CombinedResult, SpeechRegion

if TYPE_CHECKING:
    from backend.pipeline.detection.types import DetectionResult


class SoundEventSignalCombiner:
    """
    Merges detection results from multiple detectors using OR logic.

    All speech regions from all detectors are collected, then
    overlapping or touching regions are merged.

    """

    def combine(self, results: list[DetectionResult]) -> CombinedResult:
        """
        Merge speech regions from all detectors.

        Collect every region, sort by ``start_sec``, and merge
        overlapping or touching regions (``next.start_sec <=
        current.end_sec``).

        Merged region confidence is the maximum of its constituents.
        Merged region ``detector_type`` is a comma-joined, sorted string
        of the constituent detector types.

        """
        all_regions: list[SpeechRegion] = []
        for result in results:
            all_regions.extend(result.speech_regions)

        if not all_regions:
            return CombinedResult(speech_regions=())

        all_regions.sort(key=lambda r: (r.start_sec, r.end_sec))

        merged: list[tuple[float, float, float, set[str]]] = []
        first = all_regions[0]
        cur_start = first.start_sec
        cur_end = first.end_sec
        cur_conf = first.confidence
        cur_types: set[str] = {first.detector_type}

        for region in all_regions[1:]:
            if region.start_sec <= cur_end:
                cur_end = max(cur_end, region.end_sec)
                cur_conf = max(cur_conf, region.confidence)
                cur_types.add(region.detector_type)
            else:
                merged.append((cur_start, cur_end, cur_conf, cur_types))
                cur_start = region.start_sec
                cur_end = region.end_sec
                cur_conf = region.confidence
                cur_types = {region.detector_type}

        merged.append((cur_start, cur_end, cur_conf, cur_types))

        speech_regions = tuple(
            SpeechRegion(
                start_sec=start,
                end_sec=end,
                confidence=conf,
                detector_type=",".join(sorted(types)),
            )
            for start, end, conf, types in merged
        )

        return CombinedResult(speech_regions=speech_regions)

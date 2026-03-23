from __future__ import annotations

from typing import TYPE_CHECKING

from google.protobuf.duration_pb2 import Duration  # type: ignore[attr-defined]

from backend.pipeline.common.constants import NANOS_PER_SECOND
from backend.pipeline.schema_types.sed_metadata_pb2 import (
    SedMetadata,
    SoundEvent,
)

if TYPE_CHECKING:
    from backend.pipeline.detection.types import CombinedResult


class SidecarBuilder:
    """Builds SedMetadata protobuf sidecars from detection results."""

    @staticmethod
    def build(
        combined_result: CombinedResult,
        source_chunk_id: str = "",
    ) -> SedMetadata:
        """
        Build a SedMetadata protobuf from a CombinedResult.

        Maps each SpeechRegion to a SoundEvent:
        - start_time = Duration from start_sec
        - duration = Duration from (end_sec - start_sec)

        Args:
            combined_result: The merged detection output.
            source_chunk_id: Full GCS URI of the audio file that the
                transcription pipeline will consume.

        Returns:
            The SedMetadata protobuf message (not serialized).
            The caller is responsible for setting start_timestamp.

        """
        metadata = SedMetadata(source_chunk_id=source_chunk_id)

        for region in combined_result.speech_regions:
            start_secs = int(region.start_sec)
            start_nanos = int(
                (region.start_sec - start_secs) * NANOS_PER_SECOND
            )

            duration_sec = region.end_sec - region.start_sec
            dur_secs = int(duration_sec)
            dur_nanos = int((duration_sec - dur_secs) * NANOS_PER_SECOND)

            event = SoundEvent(
                start_time=Duration(seconds=start_secs, nanos=start_nanos),
                duration=Duration(seconds=dur_secs, nanos=dur_nanos),
            )
            metadata.sound_events.append(event)

        return metadata

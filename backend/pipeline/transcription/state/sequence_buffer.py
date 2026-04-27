"""A framework-agnostic chronological jitter buffer abstracting gap logic away from Beam state."""

import heapq

from backend.pipeline.transcription.common.constants import (
    DEFAULT_FLOAT_TOLERANCE_MS,
)
from backend.pipeline.transcription.common.datatypes import (
    BufferedChunk,
    OrderRestorerConfig,
)
from backend.pipeline.transcription.common.logging import get_logger

logger = get_logger(
    __name__, {"system": "transcription", "component": "sequence-buffer"}
)


class SequenceBuffer:
    """A framework-agnostic domain class for managing chronological audio chunks.

    This encapsulates the epsilon tolerance bounds and sequential yielding,
    making it easily testable independent of Apache Beam state APIs.
    """

    def __init__(self, config: OrderRestorerConfig) -> None:
        """Binds the expected sequence duration and tolerance configuration parameters."""
        self.config = config

    def process_chunk(
        self,
        sequence_number: int,
        current_ts_ms: int,
        gcs_uri: str,
        expected_next_seq: int | None,
        buffer_elements: list[BufferedChunk],
    ) -> tuple[int, list[BufferedChunk], list[BufferedChunk], bool, bool]:
        """Processes a single incoming audio chunk against the expected sequence progression."""
        to_emit = []
        was_late = False
        was_buffered = False

        if expected_next_seq is None:
            expected_next_seq = sequence_number

        if sequence_number == expected_next_seq:
            to_emit.append(
                BufferedChunk(
                    sequence_number=sequence_number,
                    timestamp_ms=current_ts_ms,
                    gcs_uri=gcs_uri,
                )
            )
            expected_next_seq = sequence_number + 1

            expected_next_seq, buffer_elements, drained = (
                self.drain_ready_sequences(expected_next_seq, buffer_elements)
            )
            to_emit.extend(drained)
        elif sequence_number < expected_next_seq:
            was_late = True
            logger.info(
                f"Yielding late chunk {sequence_number} (expected {expected_next_seq}) for isolated transcription."
            )
            to_emit.append(
                BufferedChunk(
                    sequence_number=sequence_number,
                    timestamp_ms=current_ts_ms,
                    gcs_uri=gcs_uri,
                )
            )
        else:
            was_buffered = True
            heapq.heappush(
                buffer_elements,
                BufferedChunk(
                    sequence_number=sequence_number,
                    timestamp_ms=current_ts_ms,
                    gcs_uri=gcs_uri,
                ),
            )

        return (
            expected_next_seq,
            buffer_elements,
            to_emit,
            was_late,
            was_buffered,
        )

    def drain_ready_elements(
        self,
        expected_next_ts: int,
        buffer_elements: list[BufferedChunk],
        epsilon_ms: int = DEFAULT_FLOAT_TOLERANCE_MS,
    ) -> tuple[int, list[BufferedChunk], list[BufferedChunk]]:
        """Recursively scans the active buffer to find any chunks that sequentially match the newly advanced expected_next_ts.

        If found, yields them and steps the timestamp forward.
        """
        to_emit = []
        while buffer_elements:
            smallest = buffer_elements[0]
            difference = smallest.timestamp_ms - expected_next_ts
            if abs(difference) <= epsilon_ms:
                heapq.heappop(buffer_elements)
                to_emit.append(smallest)
                expected_next_ts = (
                    smallest.timestamp_ms + self.config.chunk_duration_ms
                )
            else:
                break

        return expected_next_ts, buffer_elements, to_emit

    def drain_ready_sequences(
        self,
        expected_next_seq: int,
        buffer_elements: list[BufferedChunk],
    ) -> tuple[int, list[BufferedChunk], list[BufferedChunk]]:
        """Recursively scans the active buffer to find any chunks that sequentially match the newly advanced expected_next_seq."""
        to_emit = []
        while buffer_elements:
            smallest = buffer_elements[0]
            if smallest.sequence_number == expected_next_seq:
                heapq.heappop(buffer_elements)
                to_emit.append(smallest)
                expected_next_seq += 1
            else:
                break

        return expected_next_seq, buffer_elements, to_emit

        return expected_next_seq, buffer_elements, to_emit

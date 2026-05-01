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
        current_ts_ms: int,
        gcs_uri: str,
        expected_next_ts: int | None,
        buffer_elements: list[BufferedChunk],
        chunk_duration_ms: int | None = None,
        traceparent: str | None = None,
    ) -> tuple[int, list[BufferedChunk], list[BufferedChunk], bool, bool]:
        """Processes a single incoming audio chunk against the expected sequence progression.

        This method acts as the core traffic cop for the jitter buffer:
        1. Emits the chunk immediately if it perfectly matches our chronological expectation.
        2. Bypasses the buffer (emits immediately) if the chunk arrives after the timeline has advanced past it.
        3. Buffers the chunk if it arrives from the future, awaiting its predecessors.
        """
        to_emit = []
        was_late = False
        was_buffered = False

        # Initialize sequence if this is the very first chunk for this session.
        if expected_next_ts is None:
            expected_next_ts = current_ts_ms

        # We allow a small epsilon to absorb float arithmetic tolerance.
        epsilon_ms = DEFAULT_FLOAT_TOLERANCE_MS
        difference = current_ts_ms - expected_next_ts

        if abs(difference) <= epsilon_ms:
            # HAPP PATH: The chunk matches our mathematical expectation exactly.
            to_emit.append(BufferedChunk(current_ts_ms, gcs_uri, traceparent))
            # Advance the expected timestamp. Use provided duration if available (for varying lengths),
            # otherwise fallback to fixed config duration.
            duration = (
                chunk_duration_ms
                if chunk_duration_ms is not None
                else self.config.chunk_duration_ms
            )
            expected_next_ts = current_ts_ms + duration

            # Now that the sequence advanced, see if we already possess the newly expected chunks
            # that were previously held in the buffer.
            expected_next_ts, buffer_elements, drained = (
                self.drain_ready_elements(
                    expected_next_ts, buffer_elements, epsilon_ms
                )
            )
            to_emit.extend(drained)
        elif difference < -epsilon_ms:
            # LATE PATH: The chunk arrived later than its position in the sequence, meaning
            # the pipeline had already "given up" and moved past it. We emit it in isolation
            # so it still gets transcribed separately as a distinct utterance.
            was_late = True
            logger.info(
                f"Yielding late chunk at {current_ts_ms} (expected {expected_next_ts}) for isolated transcription."
            )
            to_emit.append(BufferedChunk(current_ts_ms, gcs_uri, traceparent))
        else:
            # FUTURE PATH: The difference > epsilon_ms, meaning this chunk arrived before
            # its predecessor. We store it in state, parking it until the missing chunk arrives.
            was_buffered = True
            heapq.heappush(
                buffer_elements,
                BufferedChunk(current_ts_ms, gcs_uri, traceparent),
            )

        return (
            expected_next_ts,
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

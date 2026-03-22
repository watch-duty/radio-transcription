"""A framework-agnostic chronological jitter buffer abstracting gap logic away from Beam state."""
import logging

from backend.pipeline.transcription.constants import DEFAULT_FLOAT_TOLERANCE_MS
from backend.pipeline.transcription.datatypes import BufferedChunk, OrderRestorerConfig

logger = logging.getLogger(__name__)

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
    ) -> tuple[int, list[BufferedChunk], list[str], bool, bool]:
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

        # We allow a small epsilon to absorb float arithmetic truncation (e.g. 14.999s vs 15.000s).
        epsilon_ms = DEFAULT_FLOAT_TOLERANCE_MS
        difference = current_ts_ms - expected_next_ts

        if abs(difference) <= epsilon_ms:
            # HAPP PATH: The chunk matches our mathematical expectation exactly.
            to_emit.append(gcs_uri)
            # Advance the expected timestamp strictly by perfect arithmetic (e.g. +15,000ms) 
            # rather than measuring the audio duration exactly, to prevent drift.
            expected_next_ts = current_ts_ms + self.config.chunk_duration_ms

            # Now that the sequence advanced, see if we already possess the newly expected chunks
            # that were previously held in the buffer.
            expected_next_ts, buffer_elements, drained = self.drain_ready_elements(
                expected_next_ts, buffer_elements, epsilon_ms
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
            to_emit.append(gcs_uri)
        else:
            # FUTURE PATH: The difference > epsilon_ms, meaning this chunk arrived before
            # its predecessor. We store it in state, parking it until the missing chunk arrives.
            was_buffered = True
            buffer_elements.append(BufferedChunk(current_ts_ms, gcs_uri))

        return expected_next_ts, buffer_elements, to_emit, was_late, was_buffered

    def drain_ready_elements(
        self,
        expected_next_ts: int,
        buffer_elements: list[BufferedChunk],
        epsilon_ms: int = DEFAULT_FLOAT_TOLERANCE_MS,
    ) -> tuple[int, list[BufferedChunk], list[str]]:
        """Recursively scans the active buffer to find any chunks that sequentially match the newly advanced expected_next_ts.

        If found, yields them and steps the timestamp forward.
        """
        if not buffer_elements:
            return expected_next_ts, buffer_elements, []

        sorted_elements = sorted(buffer_elements)
        retained = []
        to_emit = []

        for chunk in sorted_elements:
            difference = chunk.timestamp_ms - expected_next_ts
            if abs(difference) <= epsilon_ms:
                to_emit.append(chunk.gcs_uri)
                expected_next_ts = chunk.timestamp_ms + self.config.chunk_duration_ms
            else:
                retained.append(chunk)

        return expected_next_ts, retained, to_emit

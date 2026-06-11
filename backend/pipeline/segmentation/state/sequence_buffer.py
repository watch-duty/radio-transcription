"""A framework-agnostic chronological jitter buffer abstracting gap logic away from Beam state."""

import heapq

from backend.pipeline.common.log_helper import get_task_logger
from backend.pipeline.schema_types import (
    streaming_state as bp_state,
)
from backend.pipeline.segmentation.constants import (
    DEFAULT_FLOAT_TOLERANCE_MS,
)
from backend.pipeline.segmentation.datatypes import (
    BufferedChunk,
    OrderRestorerConfig,
)

logger = get_task_logger(
    __name__, {"system": "transcription", "component": "sequence-buffer"}
)


class ComparableChunk:
    """Static type-safe wrapper to enable binary heap operations on BufferedChunkProto."""

    def __init__(self, chunk: bp_state.BufferedChunkProto) -> None:
        self.chunk = chunk

    def __lt__(self, other: "ComparableChunk") -> bool:
        return self.chunk.timestamp_ms < other.chunk.timestamp_ms


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
        max_emit: int | None = None,
        ingested_at_ms: int | None = None,
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
            # HAPPY PATH: The chunk matches our mathematical expectation exactly.
            to_emit.append(BufferedChunk(current_ts_ms, gcs_uri, traceparent, ingested_at_ms))
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
                    expected_next_ts,
                    buffer_elements,
                    epsilon_ms,
                    max_emit=max_emit,
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
            to_emit.append(BufferedChunk(current_ts_ms, gcs_uri, traceparent, ingested_at_ms))
        else:
            # FUTURE PATH: The difference > epsilon_ms, meaning this chunk arrived before
            # its predecessor. We store it in state, parking it until the missing chunk arrives.
            was_buffered = True
            heap: list[ComparableChunk] = [
                ComparableChunk(c) for c in buffer_elements
            ]
            heapq.heapify(heap)
            heapq.heappush(
                heap,
                ComparableChunk(
                    BufferedChunk(current_ts_ms, gcs_uri, traceparent, ingested_at_ms)
                ),
            )
            buffer_elements[:] = [item.chunk for item in heap]

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
        max_emit: int | None = None,
    ) -> tuple[int, list[BufferedChunk], list[BufferedChunk]]:
        """Drain sequentially-ready chunks from the buffer heap.

        Walks the min-heap in timestamp order and emits every chunk whose
        timestamp falls within epsilon_ms of expected_next_ts, advancing the
        expected pointer after each emission.

        Args:
            expected_next_ts: The timestamp (ms) of the next expected chunk.
            buffer_elements: The current out-of-order buffer (mutated in-place).
            epsilon_ms: Float-arithmetic tolerance for timestamp matching.
            max_emit: Optional hard cap on the number of chunks emitted per call.
                When set and the cap is reached, remaining chunks stay in the
                buffer and the caller is responsible for scheduling follow-up
                processing — in practice, by setting an immediate Dataflow timer
                so the next Windmill bundle drains the next slice. Without this
                cap, a large backlog (e.g. after a pipeline restart or traffic
                spike) would be drained in a single bundle, potentially breaching
                Windmill's 300-second lease and causing the bundle to be aborted
                and replayed. See MAX_CHUNKS_PER_WINDMILL_BUNDLE in constants.py.
        """
        heap = [ComparableChunk(c) for c in buffer_elements]
        heapq.heapify(heap)
        to_emit = []
        while heap:
            # Windmill lease guard: stop early if the per-bundle emit cap is
            # reached. The caller checks len(emitted) >= max_emit and sets an
            # immediate self-chaining timer so the next bundle drains the rest.
            if max_emit is not None and len(to_emit) >= max_emit:
                break
            smallest = heap[0].chunk
            difference = smallest.timestamp_ms - expected_next_ts
            if abs(difference) <= epsilon_ms:
                heapq.heappop(heap)
                to_emit.append(smallest)
                expected_next_ts = (
                    smallest.timestamp_ms + self.config.chunk_duration_ms
                )
            else:
                break
        buffer_elements[:] = [item.chunk for item in heap]
        return expected_next_ts, buffer_elements, to_emit

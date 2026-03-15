import logging
import time
import uuid
from collections.abc import Generator
from typing import Any

import apache_beam as beam
from apache_beam import window
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.metrics import Metrics
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import (
    BagStateSpec,
    ReadModifyWriteStateSpec,
    TimerSpec,
    on_timer,
)
from google.protobuf.message import DecodeError

from backend.pipeline.schema_types.raw_audio_chunk_pb2 import (
    AudioChunk,
)
from backend.pipeline.schema_types.transcribed_audio_pb2 import (
    TranscribedAudio,
)
from backend.pipeline.transcription.constants import (
    DEAD_LETTER_QUEUE_TAG,
    MICROSECONDS_PER_MS,
)
from backend.pipeline.transcription.datatypes import (
    OrderRestorerConfig,
    TranscriptionResult,
)

logger = logging.getLogger(__name__)


class ParseAndKeyFn(beam.DoFn):
    """Extracts the feed_id and builds the GCS URI from Pub/Sub attributes.
    Routes messages missing required attributes to the DLQ.

    Why this matters:
    In Apache Beam streaming, data arrives uncoordinated across many workers.
    By yielding a tuple of `(feed_id, payload)`, we are "Keying" the data. Grouping
    by this key natively guarantees that all subsequent stateful operations (like stitching)
    for a specific radio feed are routed to the exact same worker node instance.
    """

    def process(
        self, element: PubsubMessage, *args: Any, **kwargs: Any
    ) -> Generator[tuple[str, bytes] | beam.pvalue.TaggedOutput, None, None]:
        try:
            feed_id = element.attributes["feed_id"]
            yield (feed_id, element.data)
        except KeyError as e:
            msg = f"Missing required payload attribute: {e}"
            logger.exception(msg)
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER_QUEUE_TAG,
                {
                    "error": msg,
                    "attributes": dict(element.attributes),
                },
            )


class AddEventTimestamp(beam.DoFn):
    """Extracts the event timestamp directly from the `AudioChunk` protobuf
    and assigns it as the Beam windowing `TimestampedValue`, yielding the GCS URI.

    Why this matters:
    Streaming pipelines differentiate between "Processing Time" (when the worker
    sees the data) and "Event Time" (when the data actually occurred). By explicitly
    assigning the hardware Event Time here, we ensure that Beam logic (like Watermarks and
    Timers) accurately respects the true chronological ordering of the audio,
    even if messages arrive out-of-order or are delayed by network partitions.
    """

    def process(
        self, element: tuple[str, bytes], *args: Any, **kwargs: Any
    ) -> Generator[tuple[str, str] | beam.pvalue.TaggedOutput, None, None]:
        feed_id, chunk_data = element

        chunk_proto = AudioChunk()
        try:
            chunk_proto.ParseFromString(chunk_data)
        except DecodeError as e:
            msg = f"Failed to parse AudioChunk proto: {e}"
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER_QUEUE_TAG, {"error": msg, "feed_id": feed_id}
            )
            return

        if not chunk_proto.HasField("start_timestamp"):
            msg = f"AudioChunk missing required start_timestamp: {chunk_proto.gcs_uri}"
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER_QUEUE_TAG, {"error": msg, "feed_id": feed_id}
            )
            return

        # Convert google.protobuf.Timestamp to unix integer timestamp for Beam Windowing
        timestamp_sec = chunk_proto.start_timestamp.seconds

        yield window.TimestampedValue((feed_id, chunk_proto.gcs_uri), timestamp_sec)


class SerializeToPubSubMessageFn(beam.DoFn):
    """Converts a `TranscriptionResult` dataclass into a serialized `TranscribedAudio` Protobuf payload
    and wraps it in a `PubsubMessage` for downstream publishing.
    """

    def process(
        self, element: TranscriptionResult, *args: Any, **kwargs: Any
    ) -> Generator[PubsubMessage, None, None]:
        # Create a deterministic UUID using uuid5 so that Beam retries produce the exact same ID
        deterministic_id_string = f"{element.feed_id}_{element.time_range.start_ms}_{element.time_range.end_ms}"
        deterministic_uuid = uuid.uuid5(uuid.NAMESPACE_OID, deterministic_id_string)

        proto = TranscribedAudio(
            feed_id=element.feed_id,
            source_chunk_ids=[str(u) for u in element.audio_ids],
            transmission_id=str(deterministic_uuid),
            transcript=element.transcript,
            missing_prior_context=element.missing_prior_context,
        )
        proto.start_timestamp.FromMicroseconds(
            element.time_range.start_ms * MICROSECONDS_PER_MS
        )
        proto.end_timestamp.FromMicroseconds(
            element.time_range.end_ms * MICROSECONDS_PER_MS
        )
        yield PubsubMessage(
            data=proto.SerializeToString(),
            attributes={},
        )


class RestoreOrderFn(beam.DoFn):
    """A stateful DoFn that buffers out-of-order chunks and emits them in strict chronological order.

    Why this matters:
    Audio chunks may arrive at the pipeline out of order due to network latency or Pub/Sub delivery variance.
    To avoid creating an endless number of tiny, fragmented transmissions downstream, this function acts as a
    "jitter buffer" (the out-of-order buffer). It holds chunks until the exactly-expected next chunk arrives.

    If a chunk is completely dropped by the network, the buffer would wait forever (Head-of-Line blocking).
    To prevent this, we set a processing-time timer (`OUT_OF_ORDER_TIMER`). If the timer expires before
    the missing chunk arrives, we "accept the gap", flush the buffer chronologically, and move on.
    """

    OUT_OF_ORDER_BUFFER_SPEC = BagStateSpec(
        "out_of_order_buffer",
        beam.coders.TupleCoder((beam.coders.VarIntCoder(), beam.coders.StrUtf8Coder())),
    )
    OUT_OF_ORDER_BUFFER_STATE = beam.DoFn.StateParam(OUT_OF_ORDER_BUFFER_SPEC)

    EXPECTED_NEXT_TS_SPEC = ReadModifyWriteStateSpec(
        "expected_next_ts", beam.coders.VarIntCoder()
    )
    EXPECTED_NEXT_TS_STATE = beam.DoFn.StateParam(EXPECTED_NEXT_TS_SPEC)

    TIMER_ACTIVE_SPEC = ReadModifyWriteStateSpec(
        "timer_active", beam.coders.BooleanCoder()
    )
    TIMER_ACTIVE_STATE = beam.DoFn.StateParam(TIMER_ACTIVE_SPEC)

    OUT_OF_ORDER_TIMER_SPEC = TimerSpec("out_of_order_timer", TimeDomain.REAL_TIME)
    OUT_OF_ORDER_TIMER = beam.DoFn.TimerParam(OUT_OF_ORDER_TIMER_SPEC)

    def __init__(self, config: OrderRestorerConfig) -> None:
        self.config = config
        self.out_of_order_counter = Metrics.counter(
            self.__class__, "chunks_buffered_out_of_order"
        )
        self.chunks_buffered_out_of_order = Metrics.counter(
            self.__class__, "chunks_buffered_out_of_order"
        )
        self.gaps_encountered_counter = Metrics.counter(
            self.__class__, "gaps_encountered"
        )
        self.chunks_dropped_late = Metrics.counter(
            self.__class__, "chunks_dropped_late"
        )

    def process(  # type: ignore[override] # noqa: PLR0913
        self,
        element: tuple[str, str],
        timestamp: beam.utils.timestamp.Timestamp = beam.DoFn.TimestampParam,
        expected_next_ts: beam.transforms.userstate.ReadModifyWriteRuntimeState = beam.DoFn.StateParam(  # noqa: B008
            EXPECTED_NEXT_TS_SPEC
        ),
        out_of_order_buffer: beam.transforms.userstate.BagRuntimeState = beam.DoFn.StateParam(  # noqa: B008
            OUT_OF_ORDER_BUFFER_SPEC
        ),
        timer_active: beam.transforms.userstate.ReadModifyWriteRuntimeState = beam.DoFn.StateParam(  # noqa: B008
            TIMER_ACTIVE_SPEC
        ),
        out_of_order_timer: beam.transforms.userstate.RuntimeTimer = beam.DoFn.TimerParam(  # noqa: B008
            OUT_OF_ORDER_TIMER_SPEC
        ),
    ) -> Generator[tuple[str, str], None, None]:
        """Process incoming audio chunks, buffering those that arrive ahead of their expected order.
        If a chunk is heavily delayed, a real-time expiry timer ensures we don't block downstream
        processing indefinitely.
        """
        feed_id, gcs_path = element
        current_ts_ms = int(float(timestamp) * 1000)

        # Track the timestamp we expect to receive next to maintain perfect sequential order.
        # If uninitialized, this is the very first chunk for this feed, so we initialize it now.
        expected = expected_next_ts.read()
        if expected is None:
            expected = current_ts_ms
            expected_next_ts.write(expected)

        if current_ts_ms == expected:
            # Happy path: Chunk arrived exactly when expected.
            # Yield it immediately downstream without buffering.
            yield (feed_id, gcs_path)

            # Advance our expectation tracker by exactly one chunk duration
            self._advance_expected(current_ts_ms, expected_next_ts)

            # Having advanced the expected sequence, check if this newly expected chunk
            # is already sitting in our out-of-order buffer waiting its turn.
            yield from self._drain_ready_elements(
                feed_id,
                expected_next_ts,
                out_of_order_buffer,
                timer_active,
                out_of_order_timer,
            )
        elif current_ts_ms < expected:
            # This chunk belongs to the past (likely a delayed duplicate or a wildly late arrival).
            # The pipeline has already moved on from this point in time, so we drop it.
            self.chunks_dropped_late.inc()
            logger.warning(
                f"[{feed_id}] Dropping obsolete chunk at {current_ts_ms} (expected {expected})."
            )
        else:
            # This chunk belongs to the future (arrived early, out-of-order).
            # We must buffer it and wait for the missing chunk(s) before it to arrive.
            self.chunks_buffered_out_of_order.inc()
            out_of_order_buffer.add((current_ts_ms, gcs_path))

            # Start a "gap timeout" countdown. If the missing prior chunk doesn't arrive
            # within this real-time window, the timer will fire and force a flush, preventing Head-of-Line blocking.
            if not timer_active.read():
                deadline_s = time.time() + (
                    self.config.out_of_order_timeout_ms / 1000.0
                )
                out_of_order_timer.set(beam.utils.timestamp.Timestamp(deadline_s))
                timer_active.write(True)  # noqa: FBT003

    def _advance_expected(
        self,
        current_ts_ms: int,
        expected_next_ts: beam.transforms.userstate.ReadModifyWriteRuntimeState,
    ) -> None:
        """Helper to advance the sequence tracker by exactly one 15-second chunk duration."""
        expected_next_ts.write(current_ts_ms + self.config.chunk_duration_ms)

    def _drain_ready_elements(
        self,
        feed_id: str,
        expected_next_ts: beam.transforms.userstate.ReadModifyWriteRuntimeState,
        out_of_order_buffer: beam.transforms.userstate.BagRuntimeState,
        timer_active: beam.transforms.userstate.ReadModifyWriteRuntimeState,
        out_of_order_timer: beam.transforms.userstate.RuntimeTimer | None = None,
    ) -> Generator[tuple[str, str], None, None]:
        """Scans the out-of-order buffer to see if the missing chunks we were waiting for
        can now be emitted sequentially.
        """
        buffer_elements = out_of_order_buffer.read()
        if not buffer_elements:
            return

        # Sort chronologically
        sorted_elements = sorted(buffer_elements)

        expected = expected_next_ts.read()
        retained = []
        drained_any = False

        # Attempt to drain as many continuously sequential chunks as possible
        for ts_ms, gcs_path in sorted_elements:
            if ts_ms == expected:
                # We found the one we were waiting for! Yield it instantly.
                yield (feed_id, gcs_path)
                # Advance the sequence line to the next slot
                expected = ts_ms + self.config.chunk_duration_ms
                drained_any = True
            else:
                # This chunk is still in the future; keep it in the buffer.
                retained.append((ts_ms, gcs_path))

        # If we successfully emptied some chunks out of the buffer, we must
        # update the persistent state storage to reflect the new state of the world.
        if drained_any:
            expected_next_ts.write(expected)
            out_of_order_buffer.clear()
            for item in retained:
                out_of_order_buffer.add(item)

        # If the buffer is now completely empty, we can safely kill the timeout countdown.
        if not retained and timer_active.read():
            out_of_order_timer.clear()
            timer_active.clear()

    @on_timer(OUT_OF_ORDER_TIMER_SPEC)
    def handle_gap_timeout(
        self,
        feed_id: str = beam.DoFn.KeyParam,  # type: ignore[assignment]
        expected_next_ts: beam.transforms.userstate.ReadModifyWriteRuntimeState = beam.DoFn.StateParam(  # noqa: B008
            EXPECTED_NEXT_TS_SPEC
        ),
        out_of_order_buffer: beam.transforms.userstate.BagRuntimeState = beam.DoFn.StateParam(  # noqa: B008
            OUT_OF_ORDER_BUFFER_SPEC
        ),
        timer_active: beam.transforms.userstate.ReadModifyWriteRuntimeState = beam.DoFn.StateParam(  # noqa: B008
            TIMER_ACTIVE_SPEC
        ),
    ) -> Generator[tuple[str, str], None, None]:
        """Fires when out-of-order chunks have sat in the buffer for too long.
        This signals that a chunk has been permanently lost in the network.
        We 'Accept the Gap', skip ahead to the earliest chunk we *do* have, and flush.
        """
        self.gaps_encountered_counter.inc()
        timer_active.clear()

        buffer_elements = list(out_of_order_buffer.read())
        if not buffer_elements:
            return

        # Sort chronologically
        sorted_elements = sorted(buffer_elements)

        # Skip our expected sequence over the gap and land squarely
        # on the earliest chunk we currently have stored in our stash.
        new_expected = sorted_elements[0][0]

        logger.warning(
            f"[{feed_id}] Gap timeout! Advancing expected from {expected_next_ts.read()} to {new_expected}."
        )

        expected_next_ts.write(new_expected)

        # Since we've advanced the expectation line to align with the stash,
        # we can just use our standard drain logic to unpack the rest of the chunks.
        yield from self._drain_ready_elements(
            feed_id,
            expected_next_ts,
            out_of_order_buffer,
            timer_active,
            # We cheat slightly here passing None for the timer, because we just explicitly cleared it above
            # and know that timer_active is False, so `_drain_ready_elements` won't attempt to access it.
            None,
        )

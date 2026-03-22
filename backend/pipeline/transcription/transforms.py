"""Apache Beam DoFns for mapping incoming stream messages and downloading audio chunks."""

import logging
import time
import uuid
from collections.abc import Generator
from typing import Any, override

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.metrics import Metrics
from apache_beam.transforms import window
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import (
    BagRuntimeState,
    BagStateSpec,
    ReadModifyWriteRuntimeState,
    ReadModifyWriteStateSpec,
    RuntimeTimer,
    TimerSpec,
    on_timer,
)
from apache_beam.utils.timestamp import Timestamp
from google.protobuf.duration_pb2 import Duration  # type: ignore[attr-defined]
from google.protobuf.message import DecodeError

from backend.pipeline.common.constants import (
    MICROSECONDS_PER_MS,
    MS_PER_SECOND,
    NANOS_PER_MS,
    NANOS_PER_SECOND,
)
from backend.pipeline.schema_types.raw_audio_chunk_pb2 import (
    AudioChunk,
)
from backend.pipeline.schema_types.transcribed_audio_pb2 import (
    TranscribedAudio,
)
from backend.pipeline.transcription.audio_processor import AudioProcessor
from backend.pipeline.transcription.constants import (
    DEAD_LETTER_QUEUE_TAG,
    DEFAULT_FLOAT_TOLERANCE_MS,
)
from backend.pipeline.transcription.datatypes import (
    OrderRestorerConfig,
    StitchAudioConfig,
    TranscriptionResult,
)
from backend.pipeline.transcription.sequence_buffer import SequenceBuffer

logger = logging.getLogger(__name__)


class ParseAndKeyFn(beam.DoFn):
    """Extracts the feed_id and builds the GCS URI from Pub/Sub attributes.

    Routes messages missing required attributes to the DLQ.
    Yields a tuple of `(feed_id, payload)` to establish a deterministic routing key
    for all subsequent stateful operations (like stitching) on that feed.
    """

    @override
    def process(
        self, element: PubsubMessage, *args: Any, **kwargs: Any
    ) -> Generator[tuple[str, bytes] | beam.pvalue.TaggedOutput, None, None]:  # noqa: UP043
        """Extracts the feed_id attribute from the payload to establish a routing key."""
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
    """Extracts the event timestamp directly from the `AudioChunk` protobuf.

    Assigns it as the Beam windowing `TimestampedValue`, yielding the GCS URI.
    This guarantees that all downstream Watermarks and Timers accurately respect
    the chronological ordering of the hardware audio events.
    """

    @override
    def process(
        self, element: tuple[str, bytes], *args: Any, **kwargs: Any
    ) -> Generator[tuple[str, str] | beam.pvalue.TaggedOutput, None, None]:  # noqa: UP043
        """Extracts the original hardware timestamp and assigns it to Beam's event timeline."""
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

        if not chunk_proto.gcs_uri:
            msg = f"AudioChunk missing required gcs_uri (feed_id: {feed_id})"
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER_QUEUE_TAG, {"error": msg, "feed_id": feed_id}
            )
            return

        if not chunk_proto.session_id:
            msg = f"AudioChunk missing required session_id: {chunk_proto.gcs_uri}"
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER_QUEUE_TAG, {"error": msg, "feed_id": feed_id}
            )
            return

        # Convert google.protobuf.Timestamp to unix float timestamp for Beam Windowing
        timestamp_sec = chunk_proto.start_timestamp.seconds + (
            chunk_proto.start_timestamp.nanos / NANOS_PER_SECOND
        )

        yield window.TimestampedValue(
            (feed_id, (chunk_proto.gcs_uri, chunk_proto.session_id)), timestamp_sec
        )


class SerializeToPubSubMessageFn(beam.DoFn):
    """Converts a `TranscriptionResult` dataclass into a serialized `TranscribedAudio` Protobuf payload.

    and wraps it in a `PubsubMessage` for downstream publishing.
    """

    @override
    def process(
        self, element: TranscriptionResult, *args: Any, **kwargs: Any
    ) -> Generator[PubsubMessage, None, None]:  # noqa: UP043
        """Serializes the final domain result into a wire-ready JSON payload."""
        # Create a deterministic UUID using uuid5 so that Beam retries produce the exact same ID
        deterministic_id_string = f"{element.feed_id}_{element.time_range.start_ms}_{element.time_range.end_ms}"
        deterministic_uuid = uuid.uuid5(uuid.NAMESPACE_OID, deterministic_id_string)

        start_offset = None
        if element.start_audio_offset_ms is not None:
            start_offset = Duration(
                seconds=element.start_audio_offset_ms // MICROSECONDS_PER_MS,
                nanos=(element.start_audio_offset_ms % MICROSECONDS_PER_MS)
                * NANOS_PER_MS,
            )

        end_offset = None
        if element.end_audio_offset_ms is not None:
            end_offset = Duration(
                seconds=element.end_audio_offset_ms // MICROSECONDS_PER_MS,
                nanos=(element.end_audio_offset_ms % MICROSECONDS_PER_MS)
                * NANOS_PER_MS,
            )

        proto = TranscribedAudio(
            feed_id=element.feed_id,
            source_audio_uris=element.contributing_audio_uris,
            transmission_id=str(deterministic_uuid),
            transcript=element.transcript,
            missing_prior_context=element.missing_prior_context,
            missing_post_context=element.missing_post_context,
            start_audio_offset=start_offset,
            end_audio_offset=end_offset,
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

    It acts as a thin wrapper around the SequenceBuffer framework-agnostic domain class.
    """

    SESSION_ID_SPEC = ReadModifyWriteStateSpec("session_id", beam.coders.StrUtf8Coder())
    SESSION_ID_STATE = beam.DoFn.StateParam(SESSION_ID_SPEC)

    OUT_OF_ORDER_BUFFER_SPEC = BagStateSpec(
        "out_of_order_buffer",
        beam.coders.PickleCoder(),
    )
    # A bag state holding chunks that have arrived earlier than their expected chronological sequence.
    OUT_OF_ORDER_BUFFER_STATE = beam.DoFn.StateParam(OUT_OF_ORDER_BUFFER_SPEC)

    EXPECTED_NEXT_TS_SPEC = ReadModifyWriteStateSpec(
        "expected_next_ts", beam.coders.VarIntCoder()
    )
    # Tracks the exact chronological timestamp (ms) of the next chunk we must receive before emitting anything downstream.
    EXPECTED_NEXT_TS_STATE = beam.DoFn.StateParam(EXPECTED_NEXT_TS_SPEC)

    TIMER_ACTIVE_SPEC = ReadModifyWriteStateSpec(
        "timer_active", beam.coders.BooleanCoder()
    )
    # Boolean flag ensuring we only ever have a single active processing-time timer scheduled across the buffer.
    TIMER_ACTIVE_STATE = beam.DoFn.StateParam(TIMER_ACTIVE_SPEC)

    OUT_OF_ORDER_TIMER_SPEC = TimerSpec("out_of_order_timer", TimeDomain.REAL_TIME)
    # A real-time (processing time) timer that acts as a maximum allowed wait period for missing chunks.
    OUT_OF_ORDER_TIMER = beam.DoFn.TimerParam(OUT_OF_ORDER_TIMER_SPEC)

    def __init__(self, config: OrderRestorerConfig) -> None:
        """Binds the OrderRestorerConfig and initializes Beam metrics counters."""
        self.config = config
        self.chunks_buffered_out_of_order = Metrics.counter(
            self.__class__, "chunks_buffered_out_of_order"
        )
        self.gaps_encountered_counter = Metrics.counter(
            self.__class__, "gaps_encountered"
        )
        self.session_resets_counter = Metrics.counter(self.__class__, "session_resets")
        self.chunks_dropped_late = Metrics.counter(
            self.__class__, "chunks_dropped_late"
        )

    @override
    def process(  # type: ignore[override]
        self,
        element: tuple[str, tuple[str, str]],
        timestamp: Timestamp = beam.DoFn.TimestampParam,  # type: ignore[assignment]
        session_id_state: ReadModifyWriteRuntimeState = beam.DoFn.StateParam(  # type: ignore[assignment] # noqa: B008
            SESSION_ID_SPEC
        ),
        expected_next_ts_state: ReadModifyWriteRuntimeState = beam.DoFn.StateParam(  # type: ignore[assignment] # noqa: B008
            EXPECTED_NEXT_TS_SPEC
        ),
        out_of_order_buffer_state: BagRuntimeState = beam.DoFn.StateParam(  # type: ignore[assignment] # noqa: B008
            OUT_OF_ORDER_BUFFER_SPEC
        ),
        timer_active_state: ReadModifyWriteRuntimeState = beam.DoFn.StateParam(  # type: ignore[assignment] # noqa: B008
            TIMER_ACTIVE_SPEC
        ),
        out_of_order_timer: RuntimeTimer = beam.DoFn.TimerParam(  # type: ignore[assignment] # noqa: B008
            OUT_OF_ORDER_TIMER_SPEC
        ),
    ) -> Generator[tuple[str, str], None, None]:  # noqa: UP043
        """Ingests out-of-order chunks and orchestrates chronologically sorted yields."""
        feed_id, (gcs_path, incoming_session_id) = element
        current_ts_ms = int(float(timestamp) * MS_PER_SECOND)

        current_session_id = session_id_state.read()
        if current_session_id != incoming_session_id:
            logger.info(
                f"[{feed_id}] Session ID changed from {current_session_id} to {incoming_session_id}. Resetting state."
            )
            self.session_resets_counter.inc()
            session_id_state.write(incoming_session_id)
            expected_next_ts_state.clear()
            out_of_order_buffer_state.clear()
            if timer_active_state.read():
                out_of_order_timer.clear()
            timer_active_state.clear()

        sequence_buffer = SequenceBuffer(self.config)
        buffer_elements = list(out_of_order_buffer_state.read())
        expected_next_ts = expected_next_ts_state.read()

        (
            new_expected_next_ts,
            new_buffer_elements,
            elements_to_emit,
            was_late,
            was_buffered,
        ) = sequence_buffer.process_chunk(
            current_ts_ms=current_ts_ms,
            gcs_uri=gcs_path,
            expected_next_ts=expected_next_ts,
            buffer_elements=buffer_elements,
        )

        if was_late:
            self.chunks_dropped_late.inc()
        if was_buffered:
            self.chunks_buffered_out_of_order.inc()

        expected_next_ts_state.write(new_expected_next_ts)
        out_of_order_buffer_state.clear()
        for chunk in new_buffer_elements:
            out_of_order_buffer_state.add(chunk)

        for gcs_uri in elements_to_emit:
            yield (feed_id, gcs_uri)

        # Handle Timer for Gap Timeout
        if new_buffer_elements and not timer_active_state.read():
            deadline_s = time.time() + (
                self.config.out_of_order_timeout_ms / float(MS_PER_SECOND)
            )
            out_of_order_timer.set(Timestamp(deadline_s))
            timer_active_state.write(True)  # noqa: FBT003
        elif not new_buffer_elements and timer_active_state.read():
            out_of_order_timer.clear()
            timer_active_state.clear()

    @on_timer(OUT_OF_ORDER_TIMER_SPEC)
    def handle_gap_timeout(
        self,
        feed_id: str = beam.DoFn.KeyParam,  # type: ignore[assignment]
        expected_next_ts_state: ReadModifyWriteRuntimeState = beam.DoFn.StateParam(  # type: ignore[assignment] # noqa: B008
            EXPECTED_NEXT_TS_SPEC
        ),
        out_of_order_buffer_state: BagRuntimeState = beam.DoFn.StateParam(  # type: ignore[assignment] # noqa: B008
            OUT_OF_ORDER_BUFFER_SPEC
        ),
        timer_active_state: ReadModifyWriteRuntimeState = beam.DoFn.StateParam(  # type: ignore[assignment] # noqa: B008
            TIMER_ACTIVE_SPEC
        ),
    ) -> Generator[tuple[str, str], None, None]:  # noqa: UP043
        """Handles the gap timeout."""
        self.gaps_encountered_counter.inc()
        timer_active_state.clear()

        buffer_elements = list(out_of_order_buffer_state.read())
        if not buffer_elements:
            return

        sorted_elements = sorted(buffer_elements)
        new_expected = sorted_elements[0].timestamp_ms

        logger.warning(
            f"[{feed_id}] Gap timeout! Advancing expected from {expected_next_ts_state.read()} to {new_expected}."
        )

        expected_next_ts_state.write(new_expected)

        sequence_buffer = SequenceBuffer(self.config)

        new_expected_next_ts, new_buffer_elements, elements_to_emit = (
            sequence_buffer.drain_ready_elements(
                expected_next_ts=new_expected,
                buffer_elements=buffer_elements,
                epsilon_ms=DEFAULT_FLOAT_TOLERANCE_MS,
            )
        )

        expected_next_ts_state.write(new_expected_next_ts)
        out_of_order_buffer_state.clear()
        for chunk in new_buffer_elements:
            out_of_order_buffer_state.add(chunk)

        for gcs_uri in elements_to_emit:
            yield (feed_id, gcs_uri)


class DownloadAudioFn(beam.DoFn):
    """A stateless DoFn that downloads audio chunks from GCS based on the provided GCS URI."""

    def __init__(self, config: StitchAudioConfig) -> None:
        """Binds the runtime configuration parameters and initializes Beam metrics."""
        self.config = config
        self.dlq_count = Metrics.counter("DownloadAudioFn", "dlq_count")

        self.audio_processor = None

    @override
    def setup(self) -> None:
        """Instantiates the Google Cloud Storage client lazily on the executing worker."""
        self.audio_processor = AudioProcessor(
            self.config.vad_type, self.config.vad_config
        )
        self.audio_processor.setup()

    @override
    def process(
        self, element: tuple[str, str], *args: Any, **kwargs: Any
    ) -> Generator[tuple[str, tuple[str, Any]] | beam.pvalue.TaggedOutput, None, None]:  # noqa: UP043
        """Downloads the raw audio bytes from GCS and passes them to the acoustic processor."""
        feed_id, gcs_path = element
        if not self.audio_processor:
            msg = "AudioProcessor not initialized. setup() must be called."
            raise RuntimeError(msg)

        try:
            chunk_data = self.audio_processor.download_audio_and_sed(gcs_path)
            yield (feed_id, (gcs_path, chunk_data))
        except FileNotFoundError:
            logger.info("GCS object not found yet. Re-raising to NACK Pub/Sub message.")
            raise
        except Exception as e:
            if self.config.route_to_dlq:
                self.dlq_count.inc()
                logger.exception("Error downloading %s for feed %s", gcs_path, feed_id)
                msg = str(e)
                yield beam.pvalue.TaggedOutput(
                    DEAD_LETTER_QUEUE_TAG, {"error": msg, "feed_id": feed_id}
                )
            else:
                raise

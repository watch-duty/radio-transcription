"""Apache Beam DoFns for mapping incoming stream messages and downloading audio chunks."""

import logging
from collections.abc import Iterator
from typing import Any, override

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.metrics import Metrics
from apache_beam.transforms import window
from apache_beam.utils.timestamp import Timestamp
from google.protobuf.duration_pb2 import Duration  # type: ignore
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
)
from backend.pipeline.transcription.datatypes import (
    StitchAudioConfig,
    TranscriptionResult,
)

logger = logging.getLogger(__name__)


@beam.typehints.with_input_types(PubsubMessage)
@beam.typehints.with_output_types(tuple[str, bytes])
class ParseAndKeyFn(beam.DoFn):
    """Extracts the feed_id and builds the GCS URI from Pub/Sub attributes.

    Routes messages missing required attributes to the DLQ.
    Yields a tuple of `(feed_id, payload)` to establish a deterministic routing key
    for all subsequent stateful operations (like stitching) on that feed.
    """

    @override
    def process(
        self, element: PubsubMessage, *args: Any, **kwargs: Any
    ) -> Iterator[tuple[str, bytes] | beam.pvalue.TaggedOutput]:
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


@beam.typehints.with_input_types(tuple[str, bytes])
@beam.typehints.with_output_types(tuple[str, str])
class AddEventTimestamp(beam.DoFn):
    """Extracts the event timestamp directly from the `AudioChunk` protobuf.

    Assigns it as the Beam windowing `TimestampedValue`, yielding the GCS URI.
    This guarantees that all downstream Watermarks and Timers accurately respect
    the chronological ordering of the hardware audio events.
    """

    @override
    def process(
        self, element: tuple[str, bytes], *args: Any, **kwargs: Any
    ) -> Iterator[tuple[str, str] | beam.pvalue.TaggedOutput]:
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
        else:
            if not chunk_proto.HasField("start_timestamp"):
                msg = f"AudioChunk missing required start_timestamp: {chunk_proto.gcs_uri}"
                yield beam.pvalue.TaggedOutput(
                    DEAD_LETTER_QUEUE_TAG, {"error": msg, "feed_id": feed_id}
                )
            elif not chunk_proto.gcs_uri:
                msg = (
                    f"AudioChunk missing required gcs_uri (feed_id: {feed_id})"
                )
                yield beam.pvalue.TaggedOutput(
                    DEAD_LETTER_QUEUE_TAG, {"error": msg, "feed_id": feed_id}
                )
            elif not chunk_proto.session_id:
                msg = f"AudioChunk missing required session_id: {chunk_proto.gcs_uri}"
                yield beam.pvalue.TaggedOutput(
                    DEAD_LETTER_QUEUE_TAG, {"error": msg, "feed_id": feed_id}
                )
            else:
                # Convert google.protobuf.Timestamp to unix float timestamp for Beam Windowing
                timestamp_sec = chunk_proto.start_timestamp.seconds + (
                    chunk_proto.start_timestamp.nanos / NANOS_PER_SECOND
                )

                yield window.TimestampedValue(
                    (feed_id, chunk_proto.gcs_uri),
                    timestamp_sec,
                )


@beam.typehints.with_input_types(TranscriptionResult)
@beam.typehints.with_output_types(PubsubMessage)
class SerializeToPubSubMessageFn(beam.DoFn):
    """Converts a `TranscriptionResult` dataclass into a serialized `TranscribedAudio` Protobuf payload.

    and wraps it in a `PubsubMessage` for downstream publishing.
    """

    @override
    def process(
        self, element: TranscriptionResult, *args: Any, **kwargs: Any
    ) -> Iterator[PubsubMessage]:
        """Serializes the final domain result into a wire-ready JSON payload."""
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
            transmission_id=element.transmission_id,
            transcript=element.transcript,
            missing_prior_context=element.missing_prior_context,
            missing_post_context=element.missing_post_context,
            start_audio_offset=start_offset,
            end_audio_offset=end_offset,
            canonical_audio_uri=element.canonical_audio_uri,
            playback_audio_uri=element.playback_audio_uri,
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
            ordering_key=element.feed_id,
        )


@beam.typehints.with_input_types(tuple[str, str])
@beam.typehints.with_output_types(tuple[str, tuple[str, Any]])
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
        from backend.pipeline.transcription.resources import (  # noqa: PLC0415
            SHARED_RESOURCE_HANDLE,
            SharedResources,
        )

        self.shared_resources = SHARED_RESOURCE_HANDLE.acquire(SharedResources)
        self.audio_processor = AudioProcessor(
            self.config.vad_type,
            self.config.vad_config,
            shared_resources=self.shared_resources,
        )
        self.audio_processor.setup()

    @override
    def process(  # type: ignore[override] # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        element: tuple[str, str],
        timestamp: Timestamp = beam.DoFn.TimestampParam,  # type: ignore
    ) -> Iterator[tuple[str, tuple[str, Any]] | beam.pvalue.TaggedOutput]:
        """Downloads the raw audio bytes from GCS and passes them to the acoustic processor."""
        feed_id, gcs_path = element
        if not self.audio_processor:
            msg = "AudioProcessor not initialized. setup() must be called."
            raise RuntimeError(msg)

        start_ms = int(float(timestamp) * MS_PER_SECOND)

        try:
            chunk_data = self.audio_processor.download_audio_and_detect(
                gcs_path, start_ms
            )
            yield (feed_id, (gcs_path, chunk_data))
        except FileNotFoundError:
            logger.info(
                "GCS object not found yet. Re-raising to NACK Pub/Sub message."
            )
            raise
        except Exception as e:
            if self.config.route_to_dlq:
                self.dlq_count.inc()
                logger.exception(
                    "Error downloading %s for feed %s", gcs_path, feed_id
                )
                msg = str(e)
                yield beam.pvalue.TaggedOutput(
                    DEAD_LETTER_QUEUE_TAG, {"error": msg, "feed_id": feed_id}
                )
            else:
                raise

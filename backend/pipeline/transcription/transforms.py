"""Apache Beam DoFns for mapping incoming stream messages and downloading audio chunks."""

import logging
from collections.abc import Iterable, Iterator
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
    ChunkMetadata,
    DownloadedChunkPayload,
    FeedMetadata,
    FlushRequest,
    StitchAudioConfig,
    TimeRange,
    TranscriptionResult,
)
from backend.pipeline.transcription.resources import (
    SHARED_RESOURCE_HANDLE,
    SharedResources,
)
from backend.pipeline.transcription.utils import generate_transmission_id

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
@beam.typehints.with_output_types(tuple[str, FeedMetadata])
class ExtractFeedMetadataFn(beam.DoFn):
    """Extracts feed metadata from the serialized AudioChunk proto."""

    @override
    def process(
        self, element: tuple[str, bytes]
    ) -> Iterator[tuple[str, FeedMetadata]]:
        feed_id, chunk_bytes = element
        chunk_proto = AudioChunk()
        chunk_proto.ParseFromString(chunk_bytes)
        yield (feed_id, FeedMetadata(feed_name=chunk_proto.feed_name))


@beam.typehints.with_input_types(tuple[str, bytes])
@beam.typehints.with_output_types(tuple[str, ChunkMetadata])
class AddEventTimestamp(beam.DoFn):
    """Extracts the event timestamp directly from the `AudioChunk` protobuf.

    Assigns it as the Beam windowing `TimestampedValue`, yielding the GCS URI.
    This guarantees that all downstream Watermarks and Timers accurately respect
    the chronological ordering of the hardware audio events.
    """

    @override
    def process(
        self,
        element: tuple[str, bytes],
        *args: Any,
        **kwargs: Any,
    ) -> Iterator[tuple[str, ChunkMetadata] | beam.pvalue.TaggedOutput]:
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
                timestamp_ms = int(timestamp_sec * MS_PER_SECOND)

                yield window.TimestampedValue(
                    (
                        chunk_proto.session_id,
                        ChunkMetadata(
                            gcs_uri=chunk_proto.gcs_uri,
                            session_id=chunk_proto.session_id,
                            duration_ms=chunk_proto.duration_ms,
                            feed_id=feed_id,
                            timestamp_ms=timestamp_ms,
                            feed_name=chunk_proto.feed_name,
                        ),
                    ),
                    timestamp_sec,
                )


@beam.typehints.with_input_types(tuple[str, Iterable[ChunkMetadata]])
@beam.typehints.with_output_types(tuple[str, ChunkMetadata])
class SortAndEmitFn(beam.DoFn):
    """Sorts buffered chunks by timestamp and emits them in order."""

    @override
    def process(
        self, element: tuple[str, Iterable[ChunkMetadata]]
    ) -> Iterator[tuple[str, ChunkMetadata]]:
        _session_id, chunks = element
        sorted_chunks = sorted(chunks, key=lambda c: c.timestamp_ms)

        last_start_ms = None
        for chunk in sorted_chunks:
            current_start_ms = chunk.timestamp_ms
            if (
                last_start_ms is not None
                and abs(current_start_ms - last_start_ms) < 100
            ):
                logger.warning(
                    "[%s] Potential growing/overlapping transmission detected! "
                    "Starts at nearly the same time (%dms) as previous (%dms).",
                    chunk.feed_id,
                    current_start_ms,
                    last_start_ms,
                )
            last_start_ms = current_start_ms
            yield (chunk.feed_id, chunk)


@beam.typehints.with_input_types(TranscriptionResult)
@beam.typehints.with_output_types(PubsubMessage)
class SerializeAndEnrichFn(beam.DoFn):
    """Enriches the final TranscribedAudio message with feed metadata."""

    @override
    def process(self, element: TranscriptionResult) -> Iterator[PubsubMessage]:
        value = element
        feed_id = value.feed_id
        feed_name = value.feed_name

        if value.start_audio_offset_ms is None:
            msg = f"Missing start_audio_offset_ms for feed_id: {feed_id}"
            raise ValueError(msg)
        start_offset = Duration(
            seconds=value.start_audio_offset_ms // MICROSECONDS_PER_MS,
            nanos=(value.start_audio_offset_ms % MICROSECONDS_PER_MS)
            * NANOS_PER_MS,
        )

        if value.end_audio_offset_ms is None:
            msg = f"Missing end_audio_offset_ms for feed_id: {feed_id}"
            raise ValueError(msg)
        end_offset = Duration(
            seconds=value.end_audio_offset_ms // MICROSECONDS_PER_MS,
            nanos=(value.end_audio_offset_ms % MICROSECONDS_PER_MS)
            * NANOS_PER_MS,
        )

        proto = TranscribedAudio(
            feed_id=value.feed_id,
            source_audio_uris=value.contributing_audio_uris,
            transmission_id=value.transmission_id,
            transcript=value.transcript,
            missing_prior_context=value.missing_prior_context,
            missing_post_context=value.missing_post_context,
            start_audio_offset=start_offset,
            end_audio_offset=end_offset,
            canonical_audio_uri=value.canonical_audio_uri,
            playback_audio_uri=value.playback_audio_uri,
            feed_name=feed_name,
        )
        proto.start_timestamp.FromMicroseconds(
            value.time_range.start_ms * MICROSECONDS_PER_MS
        )
        proto.end_timestamp.FromMicroseconds(
            value.time_range.end_ms * MICROSECONDS_PER_MS
        )
        yield PubsubMessage(
            data=proto.SerializeToString(),
            attributes={},
            ordering_key=value.feed_id,
        )


@beam.typehints.with_input_types(tuple[str, DownloadedChunkPayload])
@beam.typehints.with_output_types(tuple[str, FlushRequest])
class BypassStitchingFn(beam.DoFn):
    """Stateless DoFn that directly forwards pre-segmented audio to transcription.

    It bypasses the stateful stitching logic and treats each audio chunk as a complete,
    independent transmission.
    """

    @override
    def process(
        self,
        element: tuple[str, DownloadedChunkPayload],
        *args: Any,
        **kwargs: Any,
    ) -> Iterator[tuple[str, FlushRequest]]:
        """Maps the downloaded audio chunk directly into a FlushRequest."""
        feed_id, payload = element
        gcs_path = payload.gcs_uri
        chunk_data = payload.chunk_data

        start_ms = chunk_data.start_ms
        duration_ms = chunk_data.duration_ms
        end_ms = start_ms + duration_ms

        transmission_id = generate_transmission_id(feed_id, start_ms, end_ms)

        yield (
            feed_id,
            FlushRequest(
                buffer=chunk_data.audio,
                feed_id=feed_id,
                contributing_audio_uris=[gcs_path],
                time_range=TimeRange(start_ms=start_ms, end_ms=end_ms),
                transmission_id=transmission_id,
                feed_name=payload.feed_name,
                missing_prior_context=False,
                missing_post_context=False,
            ),
        )


@beam.typehints.with_input_types(tuple[str, ChunkMetadata])
@beam.typehints.with_output_types(tuple[str, DownloadedChunkPayload])
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
        element: tuple[str, ChunkMetadata],
        timestamp: Timestamp = beam.DoFn.TimestampParam,  # type: ignore
    ) -> Iterator[
        tuple[str, DownloadedChunkPayload] | beam.pvalue.TaggedOutput
    ]:
        """Downloads the raw audio bytes from GCS and passes them to the acoustic processor."""
        session_id, metadata = element
        gcs_path = metadata.gcs_uri
        if not self.audio_processor:
            msg = "AudioProcessor not initialized. setup() must be called."
            raise RuntimeError(msg)

        start_ms = int(float(timestamp) * MS_PER_SECOND)

        try:
            chunk_data = self.audio_processor.download_audio_and_detect(
                gcs_path, start_ms
            )

            yield (
                session_id,
                DownloadedChunkPayload(
                    gcs_uri=gcs_path,
                    chunk_data=chunk_data,
                    feed_name=metadata.feed_name,
                    feed_id=metadata.feed_id,
                ),
            )
        except FileNotFoundError:
            logger.info(
                "GCS object not found yet. Re-raising to NACK Pub/Sub message."
            )
            raise
        except Exception as e:
            if self.config.route_to_dlq:
                self.dlq_count.inc()
                logger.exception(
                    "Error downloading %s for feed %s",
                    gcs_path,
                    metadata.feed_id,
                )
                msg = str(e)
                yield beam.pvalue.TaggedOutput(
                    DEAD_LETTER_QUEUE_TAG,
                    {"error": msg, "feed_id": metadata.feed_id},
                )
            else:
                raise

"""Apache Beam DoFns for mapping incoming stream messages and downloading audio chunks."""

import logging
from collections.abc import Iterator
from typing import Any, Union, override

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.transforms import window
from apache_beam.transforms.userstate import (
    ReadModifyWriteRuntimeState,
    ReadModifyWriteStateSpec,
)
from google.protobuf.duration_pb2 import Duration  # type: ignore
from google.protobuf.message import DecodeError

from backend.pipeline.common.constants import (
    MICROSECONDS_PER_MS,
    NANOS_PER_MS,
    NANOS_PER_SECOND,
)
from backend.pipeline.schema_types.raw_audio_chunk_pb2 import (
    AudioChunk,
)
from backend.pipeline.schema_types.transcribed_audio_pb2 import (
    TranscribedAudio,
)
from backend.pipeline.transcription.constants import (
    DEAD_LETTER_QUEUE_TAG,
)
from backend.pipeline.transcription.datatypes import (
    ChunkMetadata,
    FeedMetadata,
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
        yield (
            feed_id,
            FeedMetadata(
                feed_name=chunk_proto.feed_name,
                external_id=chunk_proto.external_id,
            ),
        )


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

                yield window.TimestampedValue(
                    (
                        feed_id,
                        ChunkMetadata(
                            chunk_proto.gcs_uri,
                            chunk_proto.session_id,
                            chunk_proto.duration_ms,
                        ),
                    ),
                    timestamp_sec,
                )


@beam.typehints.with_input_types(
    tuple[str, Union[FeedMetadata, TranscriptionResult]]
)
@beam.typehints.with_output_types(PubsubMessage)
class SerializeAndEnrichFn(beam.DoFn):
    """Stores feed metadata in state and enriches the final TranscribedAudio message."""

    FEED_METADATA_SPEC = ReadModifyWriteStateSpec(
        "feed_metadata", beam.coders.PickleCoder()
    )
    LAST_START_MS_SPEC = ReadModifyWriteStateSpec(
        "last_start_ms", beam.coders.VarIntCoder()
    )

    @override
    def process(
        self,
        element: tuple[str, Any],
        feed_metadata_state: ReadModifyWriteRuntimeState = beam.DoFn.StateParam(  # type: ignore # noqa: B008
            FEED_METADATA_SPEC
        ),
        last_start_ms_state: ReadModifyWriteRuntimeState = beam.DoFn.StateParam(  # type: ignore # noqa: B008
            LAST_START_MS_SPEC
        ),
    ) -> Iterator[PubsubMessage]:
        feed_id, value = element

        if isinstance(value, FeedMetadata):
            feed_metadata_state.write(value)
        elif isinstance(value, TranscriptionResult):
            metadata = feed_metadata_state.read()
            if not metadata or not metadata.feed_name:
                msg = f"Missing or incomplete feed metadata for feed_id: {feed_id}"
                raise ValueError(msg)
            feed_name = metadata.feed_name
            external_id = metadata.external_id

            # Duplicate detection based on start time
            last_start_ms = last_start_ms_state.read()
            current_start_ms = value.time_range.start_ms

            if (
                last_start_ms is not None
                and abs(current_start_ms - last_start_ms) < 100
            ):
                logger.warning(
                    "[%s / %s] Potential growing/overlapping transmission detected! "
                    "Starts at nearly the same time (%dms) as previous (%dms).",
                    feed_id,
                    value.session_id,
                    current_start_ms,
                    last_start_ms,
                )

            last_start_ms_state.write(current_start_ms)

            if value.start_audio_offset_ms is None:
                msg = f"Missing start_audio_offset_ms for feed_id: {feed_id} (session: {value.session_id})"
                raise ValueError(msg)
            start_offset = Duration(
                seconds=value.start_audio_offset_ms // MICROSECONDS_PER_MS,
                nanos=(value.start_audio_offset_ms % MICROSECONDS_PER_MS)
                * NANOS_PER_MS,
            )

            if value.end_audio_offset_ms is None:
                msg = f"Missing end_audio_offset_ms for feed_id: {feed_id} (session: {value.session_id})"
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
                external_id=external_id,
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

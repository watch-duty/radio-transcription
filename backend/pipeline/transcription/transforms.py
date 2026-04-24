"""Apache Beam DoFns for mapping incoming stream messages and downloading audio chunks."""

import logging
from collections.abc import Iterator
from typing import Any, Union, override

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.transforms.userstate import (
    ReadModifyWriteRuntimeState,
    ReadModifyWriteStateSpec,
)
from google.protobuf.duration_pb2 import Duration  # type: ignore

from backend.pipeline.common.constants import (
    MICROSECONDS_PER_MS,
    NANOS_PER_MS,
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
@beam.typehints.with_output_types(tuple[str, ChunkMetadata])
class ParseAndKeyFn(beam.DoFn):
    """Extracts the feed_id, parses the protobuf, and builds ChunkMetadata.

    Routes messages missing required attributes or with invalid payload to the DLQ.
    Yields a tuple of `(feed_id, ChunkMetadata)` to establish a deterministic routing key
    for all subsequent stateful operations on that feed.
    """

    @override
    def process(
        self, element: PubsubMessage, *args: Any, **kwargs: Any
    ) -> Iterator[tuple[str, ChunkMetadata] | beam.pvalue.TaggedOutput]:
        """Extracts the feed_id and parses the protobuf payload."""

        def _raise(msg: str) -> None:
            raise ValueError(msg)

        try:
            feed_id = element.attributes["feed_id"]
            chunk_proto = AudioChunk()
            chunk_proto.ParseFromString(element.data)

            if not chunk_proto.gcs_uri:
                msg = "AudioChunk missing required gcs_uri"
                _raise(msg)
            if not chunk_proto.session_id:
                msg = "AudioChunk missing required session_id"
                _raise(msg)
            if not chunk_proto.feed_name:
                msg = "AudioChunk missing required feed_name"
                _raise(msg)

            yield (
                feed_id,
                ChunkMetadata(
                    gcs_uri=chunk_proto.gcs_uri,
                    session_id=chunk_proto.session_id,
                    duration_ms=chunk_proto.duration_ms,
                    feed_name=chunk_proto.feed_name,
                ),
            )
        except Exception as e:
            msg = f"Failed to parse or validate payload: {e}"
            logger.exception(msg)
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER_QUEUE_TAG,
                {
                    "error": msg,
                    "attributes": dict(element.attributes),
                },
            )


@beam.typehints.with_input_types(tuple[str, ChunkMetadata])
@beam.typehints.with_output_types(tuple[str, FeedMetadata])
class ExtractFeedMetadataFn(beam.DoFn):
    """Extracts feed metadata from the ChunkMetadata."""

    @override
    def process(
        self, element: tuple[str, ChunkMetadata]
    ) -> Iterator[tuple[str, FeedMetadata]]:
        feed_id, chunk = element
        yield (feed_id, FeedMetadata(feed_name=chunk.feed_name))


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

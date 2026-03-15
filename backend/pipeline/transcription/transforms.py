import logging
import uuid
from collections.abc import Generator
from typing import Any

import apache_beam as beam
from apache_beam import window  # type: ignore[import-untyped, unresolved-import]
from apache_beam.io.gcp.pubsub import PubsubMessage
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
from backend.pipeline.transcription.datatypes import TranscriptionResult

logger = logging.getLogger(__name__)


class ParseAndKeyFn(beam.DoFn):
    """
    Extracts the feed_id and builds the GCS URI from Pub/Sub attributes.
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
    """
    Extracts the event timestamp directly from the `AudioChunk` protobuf
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
    """
    Converts a `TranscriptionResult` dataclass into a serialized `TranscribedAudio` Protobuf payload
    and wraps it in a `PubsubMessage` for downstream publishing.
    """

    def process(
        self, element: TranscriptionResult, *args: Any, **kwargs: Any
    ) -> Generator[PubsubMessage, None, None]:
        # Create a deterministic UUID using uuid5 so that Beam retries produce the exact same ID
        deterministic_id_string = (
            f"{element.feed_id}_{element.start_ms}_{element.end_ms}"
        )
        deterministic_uuid = uuid.uuid5(uuid.NAMESPACE_OID, deterministic_id_string)

        proto = TranscribedAudio(
            feed_id=element.feed_id,
            source_chunk_ids=[str(u) for u in element.audio_ids],
            transmission_id=str(deterministic_uuid),
            transcript=element.transcript,
        )
        proto.start_timestamp.FromMicroseconds(element.start_ms * MICROSECONDS_PER_MS)
        proto.end_timestamp.FromMicroseconds(element.end_ms * MICROSECONDS_PER_MS)
        yield PubsubMessage(
            data=proto.SerializeToString(),
            attributes={},
            ordering_key=element.feed_id,
        )

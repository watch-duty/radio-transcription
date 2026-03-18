from __future__ import annotations

import asyncio
import base64
import datetime
from typing import TYPE_CHECKING

from backend.pipeline.schema_types.raw_audio_chunk_pb2 import AudioChunk

if TYPE_CHECKING:
    from backend.pipeline.common.clients.gcs_client import GcsClient
    from backend.pipeline.common.clients.pubsub_client import PubSubClient
    from backend.pipeline.schema_types.sed_metadata_pb2 import SedMetadata
    from backend.pipeline.storage.feed_store import LeasedFeed

_GCS_METADATA_SIZE_LIMIT = 8 * 1024  # 8 KiB in bytes


async def upload_audio(  # noqa: PLR0913
    gcs_client: GcsClient,
    audio_chunk: bytes,
    feed: LeasedFeed,
    bucket: str,
    chunk_seq: int,
    sed_metadata: SedMetadata | None = None,
) -> str:
    """
    Upload an audio chunk to GCS and return the object path.

    The object path follows the convention:
    ``{source_type}/{feed_id}/{timestamp}_{seq}.flac``

    Args:
        gcs_client: Shared GCS client manager used for upload.
        audio_chunk: Raw audio bytes to upload.
        feed: The leased feed this chunk belongs to.
        bucket: GCS bucket name.
        chunk_seq: Monotonically increasing sequence number for this feed.
        sed_metadata: Optional SED metadata serialized into object metadata.

    Returns:
        The full GCS path (``gs://bucket/object``).

    Raises:
        ValueError: If encoded metadata size exceeds GCS metadata limits.

    """
    storage = gcs_client.get_storage()
    timestamp = datetime.datetime.now(tz=datetime.UTC).strftime(
        "%Y%m%dT%H%M%SZ",
    )
    object_name = f"{feed['source_type']}/{feed['id']}/{timestamp}_{chunk_seq}.flac"
    metadata = None
    if sed_metadata:
        sed_metadata_bytes = sed_metadata.SerializeToString()
        encoded_metadata = base64.b64encode(sed_metadata_bytes).decode("ascii")
        metadata = {"sed_metadata": encoded_metadata}

        metadata_size = sum(
            len(key.encode()) + len(value.encode()) for key, value in metadata.items()
        )
        if metadata_size > _GCS_METADATA_SIZE_LIMIT:
            msg = (
                f"Metadata size ({metadata_size} bytes) exceeds GCS limit "
                f"({_GCS_METADATA_SIZE_LIMIT} bytes) for object "
                f"'{object_name}'"
            )
            raise ValueError(msg)

    await storage.upload(
        bucket,
        object_name,
        audio_chunk,
        metadata=metadata,
    )
    return f"gs://{bucket}/{object_name}"


async def publish_audio_chunk(
    pubsub_client: PubSubClient,
    topic_path: str,
    feed_id: str,
    gcs_uri: str,
) -> str:
    """Publish a GCS audio chunk URI to Pub/Sub and return message ID."""
    publisher = pubsub_client.get_publisher()

    audio_chunk_msg = AudioChunk(gcs_uri=gcs_uri)
    now = datetime.datetime.now(tz=datetime.UTC)
    audio_chunk_msg.start_timestamp.FromDatetime(now)

    future = publisher.publish(
        topic_path,
        audio_chunk_msg.SerializeToString(),
        feed_id=feed_id,
        ordering_key=feed_id,
    )
    return await asyncio.to_thread(future.result)

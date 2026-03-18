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
    start_timestamp: datetime.datetime | None = None,
) -> str:
    """
    Publish a GCS audio chunk URI to Pub/Sub and return message ID.

    Args:
        pubsub_client: Shared Pub/Sub client manager.
        topic_path: Full Pub/Sub topic path.
        feed_id: Feed identifier, used as ordering key.
        gcs_uri: GCS URI of the audio chunk.
        start_timestamp: Original capture timestamp to preserve.
            Defaults to ``datetime.now(UTC)`` if not provided.

    """
    publisher = pubsub_client.get_publisher()

    audio_chunk_msg = AudioChunk(gcs_uri=gcs_uri)
    ts = start_timestamp or datetime.datetime.now(tz=datetime.UTC)
    audio_chunk_msg.start_timestamp.FromDatetime(ts)

    future = publisher.publish(
        topic_path,
        audio_chunk_msg.SerializeToString(),
        feed_id=feed_id,
        ordering_key=feed_id,
    )
    return await asyncio.to_thread(future.result)


def parse_gcs_uri(gcs_uri: str) -> tuple[str, str]:
    """
    Parse a ``gs://bucket/object`` URI into (bucket, object_name).

    Raises ``ValueError`` if the URI does not start with ``gs://``.
    """
    if not gcs_uri.startswith("gs://"):
        msg = f"Expected gs:// URI, got: {gcs_uri!r}"
        raise ValueError(msg)
    parts = gcs_uri.removeprefix("gs://").split("/", 1)
    bucket = parts[0]
    object_name = parts[1] if len(parts) > 1 else ""
    return bucket, object_name


async def download_audio(gcs_client: GcsClient, gcs_uri: str) -> bytes:
    """
    Download an audio file from GCS using the shared async client.

    Args:
        gcs_client: Shared GCS client manager.
        gcs_uri: Full GCS URI (e.g. ``gs://bucket/path/to/file.flac``).

    Returns:
        The file contents as bytes.

    """
    storage = gcs_client.get_storage()
    bucket, object_name = parse_gcs_uri(gcs_uri)
    return await storage.download(bucket, object_name)


async def upload_normalized_audio(
    gcs_client: GcsClient,
    audio_chunk: bytes,
    bucket: str,
    object_name: str,
    sed_metadata: SedMetadata | None = None,
) -> str:
    """
    Upload normalized audio to GCS with optional SED metadata.

    Unlike ``upload_audio``, this accepts an explicit *object_name*
    instead of deriving one from a ``LeasedFeed``.  The SED metadata
    protobuf is serialized, base64-encoded, and attached as GCS custom
    metadata in the same write operation as the audio bytes.

    Args:
        gcs_client: Shared GCS client manager.
        audio_chunk: Raw audio bytes to upload.
        bucket: Destination GCS bucket name.
        object_name: Object path within the bucket.
        sed_metadata: Optional SED metadata attached as custom metadata.

    Returns:
        The full GCS path (``gs://bucket/object``).

    """
    storage = gcs_client.get_storage()
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
        content_type="audio/flac",
    )
    return f"gs://{bucket}/{object_name}"

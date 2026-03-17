from __future__ import annotations

import asyncio
import base64
import datetime
import logging
from typing import TYPE_CHECKING

import aiohttp
from gcloud.aio.storage import Storage
from google.cloud import pubsub_v1

from backend.pipeline.schema_types.raw_audio_chunk_pb2 import AudioChunk

_GCS_METADATA_SIZE_LIMIT = 8 * 1024  # 8 KiB in bytes

if TYPE_CHECKING:
    from backend.pipeline.schema_types.sed_metadata_pb2 import SedMetadata
    from backend.pipeline.storage.feed_store import LeasedFeed

logger = logging.getLogger(__name__)

_session: aiohttp.ClientSession | None = None
_storage: Storage | None = None
_publisher: pubsub_v1.PublisherClient | None = None


def _get_publisher() -> pubsub_v1.PublisherClient:
    """Return a shared Pub/Sub publisher client, creating one lazily."""
    global _publisher  # noqa: PLW0603
    if _publisher is None:
        publisher_options = pubsub_v1.types.PublisherOptions(
            enable_message_ordering=True,
        )
        _publisher = pubsub_v1.PublisherClient(publisher_options=publisher_options)
    return _publisher


def _get_storage() -> Storage:
    """Return a shared ``Storage`` client, creating one lazily."""
    global _session, _storage  # noqa: PLW0603
    if _storage is None:
        _session = aiohttp.ClientSession()
        _storage = Storage(session=_session)
    return _storage


async def upload_audio(
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
        audio_chunk: Raw audio bytes to upload.
        feed: The leased feed this chunk belongs to.
        bucket: GCS bucket name.
        chunk_seq: Monotonically increasing sequence number for this feed.
        sed_metadata: Optional SED metadata serialized into object metadata.

    Returns:
        The full GCS path (``gs://bucket/object``).

    """
    storage = _get_storage()
    timestamp = datetime.datetime.now(tz=datetime.UTC).strftime(
        "%Y%m%dT%H%M%SZ",
    )
    object_name = f"{feed['source_type']}/{feed['id']}/{timestamp}_{chunk_seq}.flac"
    metadata = None
    if sed_metadata:
        # Serialize the SED metadata proto and encode it as a base64 string.
        sed_metadata_bytes = sed_metadata.SerializeToString()
        # Decode to string because GCS metadata values must be strings
        encoded_metadata = base64.b64encode(sed_metadata_bytes).decode("ascii")
        metadata = {"sed_metadata": encoded_metadata}

        # Validate metadata size doesn't exceed GCS limit (8 KiB)
        metadata_size = sum(
            len(k.encode()) + len(v.encode()) for k, v in metadata.items()
        )
        if metadata_size > _GCS_METADATA_SIZE_LIMIT:
            msg = (
                f"Metadata size ({metadata_size} bytes) exceeds GCS limit "
                f"({_GCS_METADATA_SIZE_LIMIT} bytes) for object '{object_name}'"
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
    topic_path: str,
    feed_id: str,
    gcs_uri: str,
) -> str:
    """Publish a GCS audio chunk URI to Pub/Sub and return message ID."""
    publisher = _get_publisher()

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


async def close_client() -> None:
    """Close shared GCS, Pub/Sub, and aiohttp clients."""
    global _session, _storage, _publisher  # noqa: PLW0603
    if _publisher is not None:
        _publisher.stop()
        _publisher = None
    if _storage is not None:
        await _storage.close()
        _storage = None
    if _session is not None:
        await _session.close()
        _session = None

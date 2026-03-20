from __future__ import annotations

import asyncio
import base64
import datetime
import logging
from typing import TYPE_CHECKING, Any

import aiohttp

from backend.pipeline.common.constants import GCS_METADATA_SIZE_LIMIT
from backend.pipeline.schema_types.raw_audio_chunk_pb2 import AudioChunk

if TYPE_CHECKING:
    from backend.pipeline.common.clients.gcs_client import GcsClient
    from backend.pipeline.common.clients.pubsub_client import PubSubClient
    from backend.pipeline.schema_types.sed_metadata_pb2 import SedMetadata
    from backend.pipeline.storage.feed_store import LeasedFeed

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Private helper functions
# -----------------------------------------------------------------------------


def _build_sed_metadata(
    object_name: str,
    sed_metadata: SedMetadata | None,
) -> dict[str, str] | None:
    """Encode and validate optional SED metadata for GCS object metadata."""
    if not sed_metadata:
        return None

    sed_metadata_bytes = sed_metadata.SerializeToString()
    encoded_metadata = base64.b64encode(sed_metadata_bytes).decode("ascii")
    metadata = {"sed_metadata": encoded_metadata}

    metadata_size = sum(
        len(key.encode()) + len(value.encode()) for key, value in metadata.items()
    )
    if metadata_size > GCS_METADATA_SIZE_LIMIT:
        msg = (
            f"Metadata size ({metadata_size} bytes) exceeds GCS limit "
            f"({GCS_METADATA_SIZE_LIMIT} bytes) for object "
            f"'{object_name}'"
        )
        logger.error(msg)
        raise ValueError(msg)

    return metadata


# -----------------------------------------------------------------------------
# Google Cloud Storage helpers
# -----------------------------------------------------------------------------


async def upload_staged_audio(
    gcs_client: GcsClient,
    audio_chunk: bytes,
    feed: LeasedFeed,
    bucket: str,
    chunk_seq: int,
    fencing_token: int | None = None,
    sed_metadata: SedMetadata | None = None,
) -> str:
    """
    Upload an unnormalized audio chunk to GCS and return the object path.

    When *fencing_token* is provided, the object path includes a
    token-qualified segment for split-brain prevention:
    ``{source_type}/{feed_id}/token-{N}/{timestamp}_{seq}.flac``

    Without a fencing token the legacy path is used:
    ``{source_type}/{feed_id}/{timestamp}_{seq}.flac``

    Args:
        gcs_client: Shared GCS client manager used for upload.
        audio_chunk: Raw audio bytes to upload.
        feed: The leased feed this chunk belongs to.
        bucket: GCS bucket name.
        chunk_seq: Monotonically increasing sequence number for this feed.
        fencing_token: Fencing token from lease acquisition. When set,
            produces a token-qualified path and uses ``ifGenerationMatch=0``
            to guarantee create-only semantics.
        sed_metadata: Optional SED metadata serialized into object metadata.

    Returns:
        The full GCS path (``gs://bucket/object``).

    Raises:
        ValueError: If encoded metadata size exceeds GCS metadata limits.

    """
    timestamp = datetime.datetime.now(tz=datetime.UTC).strftime(
        "%Y%m%dT%H%M%SZ",
    )
    # Token-qualified paths give each lease holder a unique namespace.
    # GCS ifGenerationMatch is per-object OCC — it cannot enforce "reject
    # if token < max_seen."  Putting the token in the path sidesteps this:
    # different lease holders write to different paths, so a zombie can
    # never overwrite the current holder's objects.
    if fencing_token is not None:
        object_name = (
            f"{feed['source_type']}/{feed['id']}/"
            f"token-{fencing_token}/{timestamp}_{chunk_seq}.flac"
        )
    else:
        object_name = f"{feed['source_type']}/{feed['id']}/{timestamp}_{chunk_seq}.flac"

    return await upload_audio(
        gcs_client,
        audio_chunk,
        bucket,
        object_name,
        sed_metadata,
        if_generation_match=0 if fencing_token is not None else None,
    )


async def upload_audio(
    gcs_client: GcsClient,
    audio_chunk: bytes,
    bucket: str,
    object_name: str,
    sed_metadata: SedMetadata | None = None,
    if_generation_match: int | None = None,
) -> str:
    """
    Upload audio to GCS with optional SED metadata.

    Unlike ``upload_staged_audio``, this accepts an explicit *object_name*
    instead of deriving one from a ``LeasedFeed``.  Also does not need a
    chunk_seq to generate the object name, since it's provided directly.

    Args:
        gcs_client: Shared GCS client manager.
        audio_chunk: Raw audio bytes to upload.
        bucket: Destination GCS bucket name.
        object_name: Object path within the bucket.
        sed_metadata: Optional SED metadata attached as custom metadata.
        if_generation_match: GCS generation precondition. Set to ``0`` for
            create-only semantics (fails with 412 if the object exists).
            When set, a 412 is treated as success (idempotent retry).

    Returns:
        The full GCS path (``gs://bucket/object``).

    """
    storage = gcs_client.get_storage()
    metadata = _build_sed_metadata(object_name, sed_metadata)

    # Build kwargs conditionally: passing parameters=None would break
    # existing assert_called_once_with assertions that do exact matching.
    upload_kwargs: dict[str, Any] = {
        "metadata": metadata,
        "content_type": "audio/flac",
    }
    if if_generation_match is not None:
        upload_kwargs["parameters"] = {
            "ifGenerationMatch": str(if_generation_match),
        }

    try:
        await storage.upload(bucket, object_name, audio_chunk, **upload_kwargs)
    except aiohttp.ClientResponseError as exc:
        # Catch 412 here rather than in the caller because
        # aiohttp.ClientResponseError is a subclass of ClientError, which
        # retry_with_lease_check treats as retryable — without this catch,
        # a 412 would be retried indefinitely instead of treated as success.
        if if_generation_match is not None and exc.status == 412:
            logger.info(
                "GCS 412 (object exists): %s/%s -- treating as success",
                bucket,
                object_name,
            )
        else:
            raise
    return f"gs://{bucket}/{object_name}"


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


# -----------------------------------------------------------------------------
# Pub/Sub helpers
# -----------------------------------------------------------------------------


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

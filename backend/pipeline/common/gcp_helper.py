from __future__ import annotations

import asyncio
import datetime
import logging
import uuid
from http import HTTPStatus
from typing import TYPE_CHECKING, TypedDict

import aiohttp
from google.cloud.pubsub_v1.publisher.exceptions import (
    PublishToPausedOrderingKeyException,
)
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from backend.pipeline.common import tracing_utils
from backend.pipeline.common.constants import (
    CONTINUOUS_SOURCE_TYPES,
    GCS_DOWNLOAD_TIMEOUT_SEC,
    GCS_RETRY_MAX_ATTEMPTS,
    GCS_RETRY_MAX_WAIT_SEC,
    GCS_RETRY_MIN_WAIT_SEC,
    GCS_UPLOAD_TIMEOUT_SEC,
)
from backend.pipeline.common.utils import generate_segment_id
from backend.pipeline.schema_types.continuous_audio_pb2 import ContinuousAudio
from backend.pipeline.schema_types.segmented_audio_pb2 import SegmentedAudio

if TYPE_CHECKING:
    from google.cloud import pubsub_v1

    from backend.pipeline.common.clients.gcs_client import GcsClient
    from backend.pipeline.common.clients.pubsub_client import PubSubClient
    from backend.pipeline.storage.feed_store import LeasedFeed


class UploadKwargs(TypedDict, total=False):
    metadata: dict[str, str]
    content_type: str
    timeout: int
    parameters: dict[str, str]


logger = logging.getLogger(__name__)

# Ordered publishing otherwise expands the Pub/Sub SDK retry deadline and RPC
# timeout to effectively infinite values. The ingestion pipeline owns retries,
# so each physical publish attempt must remain finite.
_ORDERED_PUBLISH_ATTEMPT_TIMEOUT_SEC = 5.0

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
    extension: str = "flac",
    content_type: str = "audio/flac",
) -> str:
    """
    Upload an unnormalized audio chunk to GCS and return the object path.

    When *fencing_token* is provided, the object path includes a
    token-qualified segment for split-brain prevention:
    ``{source_type}/{feed_id}/token-{N}/{timestamp}_{seq}.{extension}``

    Without a fencing token the legacy path is used:
    ``{source_type}/{feed_id}/{timestamp}_{seq}.{extension}``

    Note: the timestamp is the timestamp of upload, not the original capture time.

    Args:
        gcs_client: Shared GCS client manager used for upload.
        audio_chunk: Raw audio bytes to upload.
        feed: The leased feed this chunk belongs to.
        bucket: GCS bucket name.
        chunk_seq: Monotonically increasing sequence number for this feed.
        fencing_token: Fencing token from lease acquisition. When set,
            produces a token-qualified path and uses ``ifGenerationMatch=0``
            to guarantee create-only semantics.
        extension: File extension to use (default: "flac").
        content_type: Content-Type header for upload (default: "audio/flac").

    Returns:
        The full GCS path (``gs://bucket/object``).

    Raises:
        ValueError: If encoded metadata size exceeds GCS metadata limits.

    """
    with tracing_utils.get_tracer(__name__).start_as_current_span(
        "upload_staged_audio"
    ):
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
                f"token-{fencing_token}/{timestamp}_{chunk_seq}.{extension}"
            )
        else:
            object_name = f"{feed['source_type']}/{feed['id']}/{timestamp}_{chunk_seq}.{extension}"

        return await upload_audio(
            gcs_client,
            audio_chunk,
            bucket,
            object_name,
            if_generation_match=0 if fencing_token is not None else None,
            content_type=content_type,
        )


async def upload_audio(
    gcs_client: GcsClient,
    audio_chunk: bytes,
    bucket: str,
    object_name: str,
    if_generation_match: int | None = None,
    content_type: str = "audio/flac",
) -> str:
    """
    Upload audio to GCS.

    Unlike ``upload_staged_audio``, this accepts an explicit *object_name*
    instead of deriving one from a ``LeasedFeed``.  Also does not need a
    chunk_seq to generate the object name, since it's provided directly.

    Args:
        gcs_client: Shared GCS client manager.
        audio_chunk: Raw audio bytes to upload.
        bucket: Destination GCS bucket name.
        object_name: Object path within the bucket.
        if_generation_match: GCS generation precondition. Set to ``0`` for
            create-only semantics (fails with 412 if the object exists).
            When set, a 412 is treated as success (idempotent retry).
        content_type: Content-Type header for upload (default: "audio/flac").

    Returns:
        The full GCS path (``gs://bucket/object``).

    """
    storage = gcs_client.get_storage()
    traceparent = tracing_utils.get_current_traceparent()
    upload_kwargs: UploadKwargs = {
        "metadata": {"traceparent": traceparent},
        "content_type": content_type,
        "timeout": GCS_UPLOAD_TIMEOUT_SEC,
    }
    if if_generation_match is not None:
        upload_kwargs["parameters"] = {
            "ifGenerationMatch": str(if_generation_match),
        }

    logger.debug(
        "GCS upload",
        extra={
            "bucket": bucket,
            "object": object_name,
            "has_metadata": False,
        },
    )

    @gcs_async_retry
    async def _execute_upload() -> None:
        await storage.upload(bucket, object_name, audio_chunk, **upload_kwargs)

    try:
        await _execute_upload()
    except aiohttp.ClientResponseError as exc:
        # Catch 412 here rather than in the caller because
        # aiohttp.ClientResponseError is a subclass of ClientError, which
        # retry_with_lease_check treats as retryable — without this catch,
        # a 412 would be retried indefinitely instead of treated as success.
        # Other ClientResponseError statuses (incl. transient 5xx) propagate
        # so retry_with_lease_check can match them via the retryable tuple.
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


_NON_RETRYABLE_HTTP_STATUSES = {
    HTTPStatus.NOT_FOUND,  # 404
    HTTPStatus.FORBIDDEN,  # 403
    HTTPStatus.PRECONDITION_FAILED,  # 412
}


def _should_retry_gcs(exc: BaseException) -> bool:
    if isinstance(exc, aiohttp.ClientResponseError):
        # Do not retry on missing objects, authentication issues, or existing objects
        return exc.status not in _NON_RETRYABLE_HTTP_STATUSES
    return isinstance(exc, (aiohttp.ClientError, asyncio.TimeoutError))


def _log_gcs_retry(retry_state: RetryCallState) -> None:
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    logger.warning(
        "GCS operation failed (attempt %d/%d): %s. Retrying...",
        retry_state.attempt_number,
        GCS_RETRY_MAX_ATTEMPTS,
        exc,
    )


gcs_async_retry = retry(
    retry=retry_if_exception(_should_retry_gcs),
    stop=stop_after_attempt(GCS_RETRY_MAX_ATTEMPTS),
    wait=wait_exponential(
        multiplier=GCS_RETRY_MIN_WAIT_SEC,
        min=GCS_RETRY_MIN_WAIT_SEC,
        max=GCS_RETRY_MAX_WAIT_SEC,
    ),
    before_sleep=_log_gcs_retry,
    reraise=True,
)


async def download_audio(gcs_client: GcsClient, gcs_uri: str) -> bytes:
    """Download an audio file from GCS using the shared async client.

    Configures highly resilient asynchronous exponential backoff retries via Tenacity
    policies to entirely absorb transient network drops and Colossus load-shedding (`429` / `503`).
    Missing 404 objects are short-circuited and translated to standard Python FileNotFoundError.

    Args:
        gcs_client: Shared GCS client manager.
        gcs_uri: Full GCS URI (e.g. ``gs://bucket/path/to/file.flac``).

    Returns:
        The file contents as bytes.

    Raises:
        FileNotFoundError: If the GCS object does not exist (HTTP 404).
        aiohttp.ClientError: If a transient error persists after all retries.
    """
    storage = gcs_client.get_storage()
    bucket, object_name = parse_gcs_uri(gcs_uri)

    @gcs_async_retry
    async def _execute_download() -> bytes:
        return await storage.download(
            bucket, object_name, timeout=GCS_DOWNLOAD_TIMEOUT_SEC
        )

    try:
        return await _execute_download()
    except aiohttp.ClientResponseError as exc:
        if exc.status == 404:
            err_msg = f"GCS object not found: {gcs_uri}"
            logger.exception(err_msg)
            raise FileNotFoundError(err_msg) from exc
        raise
    except Exception:
        logger.exception(
            "Persistent failure downloading audio after all retries: %s",
            gcs_uri,
        )
        raise


# -----------------------------------------------------------------------------
# Pub/Sub helpers
# -----------------------------------------------------------------------------


def _build_audio_chunk_payload(
    topic_path: str,
    feed_id: str,
    feed_name: str,
    gcs_uri: str,
    session_id: str | None,
    start_timestamp: datetime.datetime,
    duration_ms: int,
    source_type: str | None = None,
    external_audio_segment_id: str | None = None,
    receipt_time: datetime.datetime | None = None,
) -> tuple[bytes, dict[str, str]]:
    if "segmented" in topic_path or (
        source_type and source_type.lower() not in CONTINUOUS_SOURCE_TYPES
    ):
        resolved_segment_id = (
            generate_segment_id(feed_id, external_audio_segment_id)
            if external_audio_segment_id
            else str(uuid.uuid4())
        )
        s_msg = SegmentedAudio(
            segment_id=resolved_segment_id,
            feed_id=feed_id,
            feed_name=feed_name,
            raw_audio_uri=gcs_uri,
            audio_classification=SegmentedAudio.AUDIO_CLASSIFICATION_SPEECH,
        )
        s_msg.source_audio_uris.append(gcs_uri)
        s_msg.start_timestamp.FromDatetime(start_timestamp)
        end_ts = start_timestamp + datetime.timedelta(milliseconds=duration_ms)
        s_msg.end_timestamp.FromDatetime(end_ts)
        if external_audio_segment_id is not None:
            s_msg.external_audio_segment_id = external_audio_segment_id
        if receipt_time is not None:
            s_msg.receipt_timestamp.FromDatetime(receipt_time)
        serialized_data = s_msg.SerializeToString()
    else:
        c_msg = ContinuousAudio(
            gcs_uri=gcs_uri,
            feed_id=feed_id,
            feed_name=feed_name,
            duration_ms=duration_ms,
        )
        if session_id is not None:
            c_msg.session_id = session_id
        c_msg.start_timestamp.FromDatetime(start_timestamp)
        if receipt_time is not None:
            c_msg.receipt_timestamp.FromDatetime(receipt_time)
        serialized_data = c_msg.SerializeToString()

    attrs: dict[str, str] = {
        "feed_id": feed_id,
        "gcs_uri": gcs_uri,
        "timestamp_ms": str(int(start_timestamp.timestamp() * 1000)),
    }
    if session_id is not None:
        attrs["session_id"] = session_id
    if source_type is not None:
        attrs["source_type"] = source_type

    tracing_utils.inject_otel_context(attrs)
    return serialized_data, attrs


def publish_audio_chunk_sync(
    publisher: pubsub_v1.PublisherClient,
    topic_path: str,
    feed_id: str,
    feed_name: str,
    gcs_uri: str,
    session_id: str | None,
    start_timestamp: datetime.datetime,
    duration_ms: int,
    source_type: str | None = None,
    external_audio_segment_id: str | None = None,
    receipt_time: datetime.datetime | None = None,
) -> str:
    """Publish an AudioChunk to Pub/Sub and return the message ID.

    This is the synchronous core used by both sync callers (e.g. Echo
    ingestion) and the async wrapper below.
    """
    with tracing_utils.get_tracer(__name__).start_as_current_span(
        "publish_raw_audio_chunk"
    ):
        serialized_data, attrs = _build_audio_chunk_payload(
            topic_path,
            feed_id,
            feed_name,
            gcs_uri,
            session_id,
            start_timestamp,
            duration_ms,
            source_type,
            external_audio_segment_id,
            receipt_time,
        )

        max_retries = 1
        for attempt in range(max_retries + 1):
            future = publisher.publish(
                topic_path,
                serialized_data,
                ordering_key=feed_id,
                **attrs,
            )
            try:
                return future.result()
            except PublishToPausedOrderingKeyException:
                if attempt >= max_retries:
                    raise
                resume_succeeded = False
                try:
                    publisher.resume_publish(topic_path, ordering_key=feed_id)
                    resume_succeeded = True
                except (RuntimeError, ValueError):
                    logger.exception(
                        "resume_publish failed for feed=%s topic=%s",
                        feed_id,
                        topic_path,
                    )
                if not resume_succeeded:
                    raise
        msg = "Unreachable"
        raise RuntimeError(msg)


async def publish_audio_chunk(
    pubsub_client: PubSubClient,
    topic_path: str,
    feed_id: str,
    feed_name: str,
    gcs_uri: str,
    session_id: str | None,
    start_timestamp: datetime.datetime,
    duration_ms: int,
    source_type: str | None = None,
    external_audio_segment_id: str | None = None,
    receipt_time: datetime.datetime | None = None,
) -> str:
    """Asynchronously publish an AudioChunk to Pub/Sub.

    Leverages asyncio.wrap_future to await the background gRPC thread pool
    non-blockingly, ensuring other asyncio tasks remain responsive.
    """
    with tracing_utils.get_tracer(__name__).start_as_current_span(
        "publish_raw_audio_chunk"
    ):
        publisher = pubsub_client.get_publisher()
        serialized_data, attrs = _build_audio_chunk_payload(
            topic_path,
            feed_id,
            feed_name,
            gcs_uri,
            session_id,
            start_timestamp,
            duration_ms,
            source_type,
            external_audio_segment_id,
            receipt_time,
        )

        max_retries = 1
        for attempt in range(max_retries + 1):
            future = publisher.publish(
                topic_path,
                serialized_data,
                ordering_key=feed_id,
                retry=None,
                timeout=_ORDERED_PUBLISH_ATTEMPT_TIMEOUT_SEC,
                **attrs,
            )
            try:
                return await asyncio.wrap_future(future)
            except PublishToPausedOrderingKeyException:
                # Clear the local Publisher pause flag and retry publishing the
                # message once so a transient pause doesn't discard valid audio
                # or cause unnecessary strikes against the feed.
                if attempt >= max_retries:
                    raise
                resume_succeeded = False
                try:
                    publisher.resume_publish(topic_path, ordering_key=feed_id)
                    resume_succeeded = True
                except (RuntimeError, ValueError):
                    logger.exception(
                        "resume_publish failed for feed=%s topic=%s",
                        feed_id,
                        topic_path,
                    )
                if not resume_succeeded:
                    raise
        msg = "Unreachable"
        raise RuntimeError(msg)

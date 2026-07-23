"""Shared helpers for the Feed and SID-owned audio pipelines."""

from __future__ import annotations

import asyncio
import collections.abc  # noqa: TC003 - public hints resolve at runtime.
import datetime
import logging  # noqa: TC003 - public hints resolve at runtime.
import typing
import uuid  # noqa: TC003 - public hints resolve at runtime.

import aiohttp
from google.api_core import exceptions as google_exceptions
from google.cloud.pubsub_v1.publisher import exceptions as pubsub_exceptions

from backend.pipeline.common.clients import (  # noqa: TC001
    gcs_client,
    pubsub_client,
)
from backend.pipeline.ingestion import models, retry, settings, slo_contract
from backend.pipeline.storage import feed_store  # noqa: TC001

_GCS_RETRYABLE = (
    aiohttp.ClientError,
    asyncio.TimeoutError,
    OSError,
)
_PUBSUB_RETRYABLE = (
    google_exceptions.Aborted,
    google_exceptions.DeadlineExceeded,
    google_exceptions.InternalServerError,
    google_exceptions.ResourceExhausted,
    google_exceptions.ServiceUnavailable,
    google_exceptions.Unknown,
    google_exceptions.Cancelled,
    pubsub_exceptions.PublishToPausedOrderingKeyException,
)


def _raise_error(error: BaseException) -> typing.NoReturn:
    raise error


def staging_parameters(
    mime_type: models.AudioMimeType | None,
) -> tuple[str, str]:
    """Map an audio MIME type to its staged-object representation.

    Args:
        mime_type: Detected audio MIME type, or ``None`` when unavailable.

    Returns:
        The file extension and HTTP content type used for staged audio.
    """
    mime_map = {
        models.AudioMimeType.MPEG: ("mp3", "audio/mpeg"),
        models.AudioMimeType.AAC: ("aac", "audio/aac"),
        models.AudioMimeType.WAV: ("wav", "audio/x-wav"),
        models.AudioMimeType.FLAC: ("flac", "audio/flac"),
        models.AudioMimeType.MP4: ("m4a", "audio/mp4"),
        models.AudioMimeType.OGG: ("ogg", "audio/ogg"),
    }
    if mime_type is None:
        return ("flac", "audio/flac")
    return mime_map.get(mime_type, ("flac", "audio/flac"))


async def settle_post_bookmark_publish[ResultT](
    operation: collections.abc.Coroutine[object, object, ResultT],
    *,
    event_logger: logging.Logger,
) -> ResultT:
    """Settle an accepted post-bookmark publication before cancellation.

    Args:
        operation: Publication coroutine accepted after durable progress.
        event_logger: Logger that owns failure evidence for the caller.

    Returns:
        The downstream publication result.

    Raises:
        asyncio.CancelledError: The caller was cancelled and publication
            succeeded, or publication itself was cancelled.
        BaseException: Publication failed. If caller cancellation was also
            received, the publication failure remains primary and is chained
            from that cancellation.
    """
    settlement = await retry.settle_issued_operation(operation)
    failure = settlement.failure
    cancellation = settlement.cancellation
    if failure is not None:
        if cancellation is not None:
            try:
                _raise_error(failure)
            except BaseException as error:
                event_logger.exception(
                    "Post-bookmark publication failed during cancellation"
                )
                raise error from cancellation
        raise failure
    if cancellation is not None:
        raise cancellation
    return typing.cast("ResultT", settlement.result)


async def upload_staged_audio_with_retry(
    operation: collections.abc.Callable[..., collections.abc.Awaitable[str]],
    *,
    gcs_client: gcs_client.GcsClient,
    chunk: models.CapturedChunk,
    feed: feed_store.LeasedFeed,
    settings: settings.CollectorSettings,
    sequence: int,
    fencing_token: int,
    extension: str,
    content_type: str,
    lease_lost: asyncio.Event,
    shutdown: asyncio.Event,
) -> str:
    """Upload one staged chunk through the shared bounded retry policy.

    Args:
        operation: Physical GCS upload callable.
        gcs_client: Shared GCS client manager.
        chunk: Captured audio and timing metadata.
        feed: Feed-shaped object metadata used for the staged path.
        settings: Runtime retry and bucket settings.
        sequence: Feed-local create-only object sequence.
        fencing_token: Exact owning generation included in the object path.
        extension: Staged object file extension.
        content_type: Staged object HTTP content type.
        lease_lost: Event that interrupts retries after confirmed authority
            loss.
        shutdown: Event that interrupts retries during cooperative shutdown.

    Returns:
        The uploaded ``gs://`` object URI.
    """
    return await retry.retry_with_lease_check(
        operation,
        gcs_client,
        chunk.audio_bytes,
        feed,
        settings.audio_staging_bucket,
        sequence,
        fencing_token,
        extension,
        content_type,
        lease_lost=lease_lost,
        shutdown=shutdown,
        max_retries=settings.gcs_upload_max_retries,
        base_delay_sec=settings.gcs_upload_retry_base_delay_sec,
        max_delay_sec=settings.gcs_upload_retry_max_delay_sec,
        retryable=_GCS_RETRYABLE,
        operation_name="GCS upload",
    )


async def publish_audio_chunk_after_bookmark(
    operation: collections.abc.Callable[..., collections.abc.Awaitable[str]],
    *,
    pubsub_client: pubsub_client.PubSubClient,
    topic_path: str,
    feed_id: uuid.UUID,
    feed_name: str,
    source_type: feed_store.SourceType,
    gcs_uri: str,
    chunk: models.CapturedChunk,
    settings: settings.CollectorSettings,
    lease_lost: asyncio.Event,
    shutdown: asyncio.Event,
    event_logger: logging.Logger,
) -> str:
    """Publish committed audio and emit success evidence before cancellation.

    Args:
        operation: Physical Pub/Sub publication callable.
        pubsub_client: Shared publisher client manager.
        topic_path: Fully qualified downstream topic.
        feed_id: Permanent Feed UUID.
        feed_name: Human-readable Feed name.
        source_type: Feed source family.
        gcs_uri: Committed staged audio URI.
        chunk: Captured audio and timing metadata.
        settings: Runtime publication retry settings.
        lease_lost: Event that interrupts retries after confirmed authority
            loss.
        shutdown: Event that interrupts retries during cooperative shutdown.
        event_logger: Logger that owns post-commit success and failure
            evidence.

    Returns:
        The downstream Pub/Sub message identifier.
    """
    duration_ms = int(
        (chunk.chunk_end_time - chunk.chunk_start_time).total_seconds() * 1000
    )

    async def publish_and_observe() -> str:
        message_id = await retry.retry_with_lease_check(
            operation,
            pubsub_client,
            topic_path,
            str(feed_id),
            feed_name,
            gcs_uri,
            chunk.session_id,
            chunk.chunk_start_time,
            duration_ms,
            source_type,
            chunk.external_audio_segment_id,
            chunk.receipt_time,
            lease_lost=lease_lost,
            shutdown=shutdown,
            max_retries=settings.pubsub_publish_max_retries,
            base_delay_sec=settings.pubsub_publish_retry_base_delay_sec,
            max_delay_sec=settings.pubsub_publish_retry_max_delay_sec,
            retryable=_PUBSUB_RETRYABLE,
            operation_name="Pub/Sub publish",
        )
        event_logger.info(
            "Published message %s for feed %s",
            message_id,
            feed_name,
        )
        log_chunk_ingested(
            event_logger,
            feed_id=feed_id,
            source_type=source_type,
            chunk=chunk,
        )
        return message_id

    return await settle_post_bookmark_publish(
        publish_and_observe(),
        event_logger=event_logger,
    )


def log_chunk_ingested(
    event_logger: logging.Logger,
    *,
    feed_id: uuid.UUID,
    source_type: feed_store.SourceType,
    chunk: models.CapturedChunk,
) -> None:
    """Emit common ingestion-SLO evidence after successful publication.

    Args:
        event_logger: Logger associated with the physical pipeline.
        feed_id: Feed whose audio completed the physical pipeline.
        source_type: Feed source family.
        chunk: Published audio and its optional latency observations.

    Returns:
        None.
    """
    payload: dict[str, object] = {
        "event_type": slo_contract.EVENT_TYPE_CHUNK_INGESTED,
        "feed_id": str(feed_id),
        "source_type": source_type,
    }
    if chunk.receipt_time is not None:
        raw_latency_sec = (
            datetime.datetime.now(datetime.UTC) - chunk.receipt_time
        ).total_seconds()
        payload["processing_latency_sec"] = max(
            0.0,
            round(raw_latency_sec, 2),
        )
        if raw_latency_sec < 0:
            payload["latency_clamped"] = True
    if chunk.stream_interval_lag_sec is not None:
        payload["stream_interval_lag_sec"] = round(
            chunk.stream_interval_lag_sec,
            2,
        )
    # SLO: chunk_ingested emit -- shared by Feed and SID-owned pipelines.
    event_logger.info("Chunk ingested", extra={"json_fields": payload})

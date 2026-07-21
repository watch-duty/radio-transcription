"""Shared helpers for the Feed and SID-owned audio pipelines."""

from __future__ import annotations

import asyncio
import datetime
import typing

from backend.pipeline.ingestion import models, slo_contract

if typing.TYPE_CHECKING:
    import collections.abc
    import logging
    import uuid

    from backend.pipeline.storage import feed_store


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


async def settle_accepted_operation[ResultT](
    operation: collections.abc.Coroutine[object, object, ResultT],
    *,
    event_logger: logging.Logger,
    failure_message: str,
) -> ResultT:
    """Settle an accepted side effect before propagating cancellation.

    Args:
        operation: Side-effect coroutine that must settle after acceptance.
        event_logger: Logger that owns failure evidence for the caller.
        failure_message: Message emitted if settlement fails during caller
            cancellation.

    Returns:
        The settled operation result.

    Raises:
        asyncio.CancelledError: The caller was cancelled while the accepted
            operation was settling.
        BaseException: The accepted operation failed before cancellation.
    """
    task = asyncio.create_task(operation)
    cancellation: asyncio.CancelledError | None = None

    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            owner = asyncio.current_task()
            if owner is None or owner.cancelling() == 0:
                raise
            if cancellation is None:
                cancellation = error
            if task.done():
                break
        except Exception:
            break

    try:
        result = task.result()
    except BaseException as error:
        if cancellation is not None:
            event_logger.exception(failure_message)
            raise error from cancellation
        raise
    if cancellation is not None:
        raise cancellation
    return result


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

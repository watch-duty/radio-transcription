from __future__ import annotations

import asyncio
import collections
import datetime
import logging
import random
import uuid
from typing import TYPE_CHECKING

from backend.pipeline.common.audio import get_audio_duration
from backend.pipeline.ingestion import quarantine_reason
from backend.pipeline.ingestion.collectors import (
    control_flow,
    failure_classification,
    telemetry,
)
from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemBatchOutcome,
    ItemFailure,
    collector_failure,
    missing_source_feed_id_failure,
)
from backend.pipeline.ingestion.collectors.fire_notifications.client import (
    FireNotificationsClient,
    FireNotificationsFile,
    FireNotificationsRestClient,
)
from backend.pipeline.ingestion.failure_classifiers import (
    ffmpeg as ffmpeg_classifier,
)
from backend.pipeline.ingestion.models import (
    AudioMimeType,
    CapturedChunk,
    CaptureEvent,
    FeedFailure,
    SourceObservation,
)
from backend.pipeline.ingestion.settings import _require_env
from backend.pipeline.storage import feed_store

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from backend.pipeline.ingestion.models import CaptureResources
    from backend.pipeline.storage.feed_store import LeasedFeed

logger = logging.getLogger(__name__)

_POLL_INTERVAL_SEC = 30.0
_MAX_CONSECUTIVE_FAILURES = 10


async def _process_file_list(
    files: list[FireNotificationsFile],
    client: FireNotificationsClient,
    shutdown_event: asyncio.Event,
    connection_session_id: str,
    feed: LeasedFeed,
    processed_uuids: collections.deque[str],
    source_feed_id: str,
    outcome: ItemBatchOutcome,
) -> AsyncIterator[CaptureEvent]:
    """Filter, sort and process audio files, yielding CapturedChunks."""
    # A Fire Notifications file-list response is the observation boundary:
    # all eligible attempted MP3s failing is meaningful, but isolated stale or
    # corrupt files should not mark the feed unhealthy.
    for f in files:
        if shutdown_event.is_set():
            break

        if f.uuid in processed_uuids:
            continue

        last_bookmark_time = feed.get("last_bookmark_time")
        if (
            last_bookmark_time is not None
            and f.start_time <= last_bookmark_time
        ):
            continue

        # Download the actual audio
        receipt_time = datetime.datetime.now(datetime.UTC)

        outcome.record_attempt()
        audio_result = await client.download_audio(
            f"{f.uuid}.mp3", shutdown_event
        )
        if isinstance(audio_result, ItemFailure):
            outcome.record_failure(audio_result)
            if not shutdown_event.is_set():
                telemetry.emit_call_download_failed(
                    logger,
                    feed_id=feed["id"],
                    source_type=feed["source_type"],
                )
            continue
        mp3_bytes = audio_result

        try:
            # to_thread: get_audio_duration shells out to ffprobe — keep it off the event loop.
            duration_ms = await asyncio.to_thread(
                get_audio_duration, mp3_bytes, input_format="mp3"
            )
        except Exception as exc:
            reason = ffmpeg_classifier.ffprobe_exception_failure_reason(exc)
            logger.warning(
                "Failed to compute duration for corrupt audio file in feed %s (%s): "
                "uuid=%s filename=%s size=%d start_time=%s reason=%s. "
                "Permanently skipping corrupt audio file.",
                feed["id"],
                feed.get("name", "Unknown"),
                f.uuid,
                f.filename,
                f.size,
                f.start_time,
                reason,
            )
            outcome.record_failure(
                ItemFailure(
                    feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
                    reason,
                )
            )
            yield SourceObservation(resume_position=f.start_time)
            continue

        end_time = f.start_time + datetime.timedelta(milliseconds=duration_ms)

        logger.debug(
            "FN Audio ready: source_feed_id=%s uuid=%s size=%d duration_ms=%d",
            source_feed_id,
            f.uuid,
            len(mp3_bytes),
            duration_ms,
        )
        yield CapturedChunk(
            audio_bytes=mp3_bytes,
            chunk_start_time=f.start_time,
            chunk_end_time=end_time,
            session_id=connection_session_id,
            receipt_time=receipt_time,
            mime_type=AudioMimeType.MPEG,
            resume_position=f.start_time,
            external_audio_segment_id=f"{f.uuid}|{f.filename}",
        )
        # Only mark as processed after a successful yield, confirming
        # the chunk was handed off to the pipeline.
        processed_uuids.append(f.uuid)
        feed["last_bookmark_time"] = f.start_time
        outcome.record_chunk_produced()

    if shutdown_event.is_set():
        return

    promoted = outcome.promoted_failure()
    if promoted is not None:
        raise collector_failure(
            promoted.status_reason,
            promoted.reason,
        )


def _init_client(
    url_base: str,
    resources: CaptureResources,
) -> FireNotificationsClient:
    try:
        s3_base_url = _require_env("FIRE_NOTIFICATIONS_S3_BASE")
    except ValueError as e:
        raise collector_failure(
            feed_store.FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID,
            "missing_fire_notifications_s3_base",
        ) from e
    try:
        user = _require_env("FIRE_NOTIFICATIONS_USER")
        password = _require_env("FIRE_NOTIFICATIONS_PASSWORD")
    except ValueError as e:
        raise collector_failure(
            feed_store.FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID,
            "missing_fire_notifications_auth_config",
        ) from e

    return FireNotificationsRestClient(
        session=resources.http_session,
        url_base=url_base,
        s3_base_url=s3_base_url,
        user=user,
        password=password,
    )


async def fire_notifications_collector(  # noqa: PLR0912
    feed: LeasedFeed,
    shutdown_event: asyncio.Event,
    url_base: str,
    resources: CaptureResources,
) -> AsyncIterator[CaptureEvent]:
    """Capture Fire Notifications audio via HTTP Polling.

    Yields :class:`CapturedChunk` for each new MP3 file found, and
    :class:`SourceObservation` for successful empty/skipped-only file listings.
    """
    source_feed_id = feed.get("source_feed_id")
    if not source_feed_id:
        logger.error(
            "Feed %s (%s) missing source_feed_id",
            feed["id"],
            feed["name"],
        )
        raise missing_source_feed_id_failure()

    # Construct the Fire Notifications API REST client helper
    client = _init_client(url_base, resources)

    # Track UUIDs we've already ingested to prevent duplicates.
    # We use a deque with maxlen to prevent unbounded memory growth.
    processed_uuids: collections.deque[str] = collections.deque(maxlen=1000)
    consecutive_failures = 0
    last_poll_failure = failure_classification.FailureInfo(
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        "source_unreachable",
    )
    connection_session_id = str(uuid.uuid4())

    # Stagger initial polling requests to prevent thundering herd
    startup_delay = random.uniform(0, _POLL_INTERVAL_SEC)  # noqa: S311
    await control_flow.sleep_or_cancel(shutdown_event, startup_delay)

    while not shutdown_event.is_set():
        poll_ok = False

        try:
            files = await client.fetch_file_list(
                source_feed_id,
                str(feed["id"]),
                shutdown_event,
            )
        except FeedFailure as exc:
            if exc.status_reason in {
                feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                feed_store.FeedStatusReason.SOURCE_RATE_LIMITED,
            }:
                last_poll_failure = failure_classification.FailureInfo(
                    exc.status_reason,
                    exc.reason or "unknown_failure",
                )
                logger.warning(
                    "FN API transient error feed %s: %s",
                    feed["id"],
                    exc,
                )
            else:
                raise
        except Exception as exc:
            last_poll_failure = failure_classification.FailureInfo(
                feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                f"source_unreachable: {quarantine_reason.exception_text(exc)}",
            )
            logger.warning(
                "FN API unexpected poll error feed %s",
                feed["id"],
                exc_info=True,
            )
        else:
            if files == []:
                yield SourceObservation()
            else:
                outcome = ItemBatchOutcome()
                async for chunk in _process_file_list(
                    files,
                    client,
                    shutdown_event,
                    connection_session_id,
                    feed,
                    processed_uuids,
                    source_feed_id,
                    outcome,
                ):
                    yield chunk
                is_skipped_only_listing_while_running = (
                    outcome.attempted_count == 0
                    and not outcome.chunk_produced
                    and not shutdown_event.is_set()
                )
                if is_skipped_only_listing_while_running:
                    yield SourceObservation()
            poll_ok = True

        if poll_ok:
            consecutive_failures = 0
        else:
            consecutive_failures += 1
            if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                raise collector_failure(
                    last_poll_failure.status_reason,
                    last_poll_failure.reason,
                )

        # Sleep before next poll, with a small jitter
        if shutdown_event.is_set():
            return
        jitter = random.uniform(0, 5.0)  # noqa: S311
        await control_flow.sleep_or_cancel(
            shutdown_event, _POLL_INTERVAL_SEC + jitter
        )

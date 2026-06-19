"""Echo Audio Ingestion Cloud Run Service.

Triggered by Eventarc on GCS OBJECT_FINALIZE events from the Echo recordings
bucket. Resolves feed metadata from AlloyDB, yields raw audio bytes to the
staging bucket, and publishes an AudioChunk to the appropriate Pub/Sub topic
for downstream transcription.
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import functions_framework
from google.api_core.exceptions import NotFound, PreconditionFailed
from google.cloud import storage
from opentelemetry import trace

from backend.pipeline.common.audio import get_audio_duration
from backend.pipeline.common.clients.pubsub_client import PubSubClient
from backend.pipeline.common.gcp_helper import publish_audio_chunk_sync
from backend.pipeline.common.log_helper import setup_logging
from backend.pipeline.ingestion import failure_policy
from backend.pipeline.ingestion.collectors import failure_classification
from backend.pipeline.ingestion.failure_classifiers import (
    ffmpeg as ffmpeg_classifier,
)
from backend.pipeline.ingestion.settings import _require_env
from backend.pipeline.storage.feed_store import FeedStatusReason
from backend.pipeline.storage.sync_connection import connect_db
from backend.pipeline.storage.sync_feed_store import SyncFeedStore

if TYPE_CHECKING:
    from cloudevents.http import event as cloudevent

# ---------------------------------------------------------------------------
# Configuration
#
# Required env vars are validated at module import via `_require_env(...)`.
# These calls run BEFORE `setup_logging()` so that contract-drift
# (a missing or empty required env var) surfaces as the first error,
# not masked behind a logging-init traceback (e.g., DefaultCredentialsError
# in environments where K_SERVICE is set but ADC is unavailable).
# ---------------------------------------------------------------------------
STAGING_BUCKET = _require_env("AUDIO_STAGING_BUCKET")
SEGMENTED_PUBSUB_TOPIC_PATH = _require_env("SEGMENTED_PUBSUB_TOPIC_PATH")
# Optional: set only on prod to also mirror the source MP3 into the dev
# recordings bucket so dev's pipeline runs E2E against real prod traffic.
# Best-effort — failures are logged and do not affect prod ingestion.
DEV_RECORDINGS_BUCKET = os.environ.get("DEV_RECORDINGS_BUCKET")

setup_logging()
logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

_RECORDING_DOWNLOAD_FAILED = "echo_recording_download_failed"
_STAGING_UPLOAD_FAILED = "echo_staging_upload_failed"
_PUBSUB_PUBLISH_FAILED = "echo_pubsub_publish_failed"
_HEARTBEAT_WRITE_FAILED = "echo_heartbeat_write_failed"
# Returning success prevents the same object notification from being retried.
# This is separate from feed-budget routing, which is handled by failure_policy.
_RETURN_SUCCESS_AFTER_FAILURE_RECORD_ATTEMPT_STATUS_REASONS = {
    FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
    FeedStatusReason.SYSTEM_PIPELINE_ERROR,
}

# ---------------------------------------------------------------------------
# Global state (persisted across warm invocations)
# ---------------------------------------------------------------------------

# Lazily initialized on first invocation so importing this module in unit
# tests does not require GCP credentials.
gcs_client: storage.Client | None = None
pubsub_client: PubSubClient | None = None
feed_store: SyncFeedStore | None = None


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
@functions_framework.cloud_event
def handle_notification(cloud_event: cloudevent.CloudEvent) -> None:
    """Sync entry point for Eventarc GCS OBJECT_FINALIZE events."""
    global gcs_client, pubsub_client, feed_store  # noqa: PLW0603

    with tracer.start_as_current_span("echo_ingestion"):
        if gcs_client is None:
            gcs_client = storage.Client()
            logger.info(
                "Echo ingestion initialized (bucket=%s)", STAGING_BUCKET
            )
        if pubsub_client is None:
            pubsub_client = PubSubClient()
        if feed_store is None:
            feed_store = SyncFeedStore(connect_db)
        _handle(cloud_event)


def _handle(cloud_event: cloudevent.CloudEvent) -> None:  # noqa: PLR0911, PLR0912, PLR0915
    """Core handler — fully synchronous."""
    if gcs_client is None or pubsub_client is None or feed_store is None:
        msg = (
            "Clients not initialized — handle_notification must be called first"
        )
        raise RuntimeError(msg)

    data = cloud_event.data
    name = data["name"]
    bucket = data["bucket"]

    if not name.endswith(".mp3"):
        return

    parts = name.split("/")
    if len(parts) != 3:
        logger.warning("Unexpected path structure, skipping: %s", name)
        return

    channel_name = parts[0]

    # Resolve feed from DB
    feed = feed_store.resolve_echo_feed(channel_name)

    if not feed:
        return
    if feed["status"] == "deactivated":
        logger.info(
            "Draining deactivated feed %s (channel: %s)",
            feed["id"],
            channel_name,
        )
        return
    if feed["status"] == "quarantined":
        logger.warning(
            "Feed %s is quarantined (channel: %s), dropping event",
            feed["id"],
            channel_name,
        )
        return

    failure: failure_classification.FailureInfo | None = None

    try:
        # Download MP3.  A NotFound means the object was deleted between the
        # OBJECT_FINALIZE event and our download — not the feed's fault.
        try:
            mp3_bytes = gcs_client.bucket(bucket).blob(name).download_as_bytes()
        except NotFound:
            logger.warning("Object deleted before download, skipping: %s", name)
            return
        except Exception:
            failure = _pipeline_failure(_RECORDING_DOWNLOAD_FAILED)
            raise

        # Calculate duration of audio bytes using shared helper
        try:
            duration_ms = get_audio_duration(mp3_bytes, input_format="mp3")
        except Exception as exc:
            reason = ffmpeg_classifier.ffprobe_exception_failure_reason(exc)
            logger.warning(
                "Echo duration probe failed: %s",
                reason,
            )
            failure = _collector_failure(reason)
            raise

        # RTL-Airband appends the filename timestamp when opening a split
        # transmission file; GCS timeCreated is only upload finalization time.
        try:
            start_ts = _parse_timestamp(name)
        except ValueError:
            time_created_str = data.get("timeCreated")
            if not time_created_str:
                logger.warning("Unparseable filename, skipping: %s", name)
                return
            try:
                end_ts = datetime.fromisoformat(time_created_str)
                start_ts = end_ts - timedelta(milliseconds=duration_ms)
            except ValueError:
                logger.warning(
                    "Unparseable filename and GCS timeCreated, skipping: "
                    "name=%s timeCreated=%s",
                    name,
                    time_created_str,
                )
                return

        # Upload MP3 directly to staging bucket.
        # if_generation_match=0 skips redundant writes but we
        # always proceed to publish (prior invocation may have crashed after upload).
        date_dir = parts[1]
        mp3_path = f"echo/{feed['id']}/{date_dir}/{Path(name).name}"
        staging_uri = f"gs://{STAGING_BUCKET}/{mp3_path}"
        blob = gcs_client.bucket(STAGING_BUCKET).blob(mp3_path)
        with tracer.start_as_current_span("upload_echo_staged_audio"):
            try:
                blob.upload_from_string(
                    mp3_bytes,
                    content_type="audio/mpeg",
                    if_generation_match=0,
                )
            except PreconditionFailed:
                logger.info(
                    "MP3 already exists, skipping upload: %s", staging_uri
                )
            except Exception:
                failure = _pipeline_failure(_STAGING_UPLOAD_FAILED)
                raise

        # Publish AudioChunk with deterministic session_id for dedup.
        feed_id_str = str(feed["id"])
        session_id = str(uuid.uuid5(uuid.NAMESPACE_URL, staging_uri))

        try:
            publisher = pubsub_client.get_publisher()
            publish_audio_chunk_sync(
                publisher,
                SEGMENTED_PUBSUB_TOPIC_PATH,
                feed_id_str,
                feed["name"],
                staging_uri,
                session_id,
                start_ts,
                duration_ms=duration_ms,
                source_type="echo",
                external_audio_segment_id=f"{bucket}/{name}",
            )
        except Exception:
            failure = _pipeline_failure(_PUBSUB_PUBLISH_FAILED)
            raise

        # Unconditional heartbeat — also resets failure_count if recovering.
        try:
            feed_store.record_heartbeat(feed["id"])
        except Exception:
            failure = _pipeline_failure(_HEARTBEAT_WRITE_FAILED)
            raise

        _mirror_to_dev_best_effort(bucket, name)

    except Exception as exc:
        classification = failure or failure_classification.FailureInfo(
            FeedStatusReason.SYSTEM_UNEXPECTED_ERROR,
            _unexpected_failure_reason(exc),
        )
        should_return_success = (
            _should_return_success_after_failure_record_attempt(
                classification.status_reason
            )
        )
        if should_return_success:
            logger.exception(
                "Echo processing failure will return success for "
                "object notification: "
                "feed=%s status_reason=%s reason=%s",
                feed["id"],
                classification.status_reason.value,
                classification.reason,
            )
        _record_failure_by_policy(feed["id"], classification)
        if should_return_success:
            return
        raise


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _collector_failure(
    reason: str,
) -> failure_classification.FailureInfo:
    return failure_classification.FailureInfo(
        FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        reason,
    )


def _pipeline_failure(
    reason: str,
) -> failure_classification.FailureInfo:
    return failure_classification.FailureInfo(
        FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        reason,
    )


def _record_failure_by_policy(
    feed_id: uuid.UUID,
    classification: failure_classification.FailureInfo,
) -> None:
    if feed_store is None:
        msg = "Feed store is not initialized"
        raise RuntimeError(msg)

    action = failure_policy.classify_failure_policy(
        classification.status_reason,
    )
    try:
        if (
            action
            is failure_policy.ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET
        ):
            feed_store.record_failure(
                feed_id,
                reason=classification.reason,
                status_reason=classification.status_reason,
            )
        else:
            feed_store.record_non_budgeted_failure(
                feed_id,
                status_reason=classification.status_reason,
            )
    except Exception:
        logger.exception("Failed to record failure for feed %s", feed_id)


def _should_return_success_after_failure_record_attempt(
    status_reason: FeedStatusReason,
) -> bool:
    return (
        status_reason
        in _RETURN_SUCCESS_AFTER_FAILURE_RECORD_ATTEMPT_STATUS_REASONS
    )


def _unexpected_failure_reason(exc: Exception) -> str:
    reason = str(exc)
    return reason or type(exc).__name__


def _mirror_to_dev_best_effort(bucket: str, name: str) -> None:
    """Copy the source MP3 into the dev recordings bucket, best-effort.

    No-op when ``DEV_RECORDINGS_BUCKET`` is unset (i.e. outside prod).
    Any failure is logged and swallowed — a dev mirror issue is not the
    feed's fault and must not propagate up to the outer except that
    records a failure against the feed.
    """
    if not DEV_RECORDINGS_BUCKET or gcs_client is None:
        return
    try:
        with tracer.start_as_current_span("mirror_to_dev"):
            source_bucket = gcs_client.bucket(bucket)
            source_bucket.copy_blob(
                source_bucket.blob(name),
                gcs_client.bucket(DEV_RECORDINGS_BUCKET),
                name,
                timeout=30,
            )
    except Exception:
        logger.exception(
            "Dev mirror failed (best-effort): src=%s/%s dst=%s",
            bucket,
            name,
            DEV_RECORDINGS_BUCKET,
        )


# ---------------------------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------------------------
def _parse_timestamp(name: str) -> datetime:
    """Extract the UTC recording start time from an Echo filename.

    Expected path: {channel}-{location}/{YYYYMMDD}/{channel}_{YYYYMMDD}_{HHMMSS}.mp3
    Example: fire-ca_almaden_valley/20260326/fire_20260326_143022.mp3

    RTL-Airband writes the suffix when opening the split transmission file, and
    current Echo configs set ``localtime = False`` so that suffix is UTC.
    """
    filename = name.rsplit("/", 1)[-1]
    stem = Path(filename).stem
    parts = stem.rsplit("_", 2)
    if len(parts) < 3:
        msg = f"Cannot parse timestamp from filename: {name}"
        raise ValueError(msg)
    date_str, time_str = parts[-2], parts[-1]
    return datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S").replace(
        tzinfo=UTC
    )

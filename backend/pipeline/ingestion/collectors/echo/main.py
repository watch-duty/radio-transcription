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
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import functions_framework
from google.api_core.exceptions import NotFound, PreconditionFailed
from google.cloud import storage
from opentelemetry import trace

from backend.pipeline.common.audio import get_audio_duration
from backend.pipeline.common.clients.pubsub_client import PubSubClient
from backend.pipeline.common.gcp_helper import publish_audio_chunk_sync
from backend.pipeline.common.logging import setup_logging
from backend.pipeline.ingestion.settings import _require_env
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
RAW_AUDIO_TOPIC = _require_env("RAW_AUDIO_TOPIC")
# Optional: set only on prod to also mirror the source MP3 into the dev
# recordings bucket so dev's pipeline runs E2E against real prod traffic.
# Best-effort — failures are logged and do not affect prod ingestion.
DEV_RECORDINGS_BUCKET = os.environ.get("DEV_RECORDINGS_BUCKET")

setup_logging()
logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

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


def _handle(cloud_event: cloudevent.CloudEvent) -> None:  # noqa: PLR0911
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
        logger.warning("No feed found for channel: %s", channel_name)
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

    # Bad filenames are not the feed's fault — skip without failure increment.
    try:
        start_ts = _parse_timestamp(name)
    except ValueError:
        logger.warning("Unparseable filename, skipping: %s", name)
        return

    try:
        # Download MP3.  A NotFound means the object was deleted between the
        # OBJECT_FINALIZE event and our download — not the feed's fault.
        try:
            mp3_bytes = gcs_client.bucket(bucket).blob(name).download_as_bytes()
        except NotFound:
            logger.warning("Object deleted before download, skipping: %s", name)
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

        # Publish AudioChunk with deterministic session_id for dedup.
        feed_id_str = str(feed["id"])
        session_id = str(uuid.uuid5(uuid.NAMESPACE_URL, staging_uri))
        publisher = pubsub_client.get_publisher()

        # Calculate duration of audio bytes using shared helper
        duration_ms = get_audio_duration(mp3_bytes)

        publish_audio_chunk_sync(
            publisher,
            RAW_AUDIO_TOPIC,
            feed_id_str,
            feed["name"],
            feed["external_id"],
            staging_uri,
            session_id,
            start_ts,
            duration_ms=duration_ms,
            source_type="echo",
        )

        # Unconditional heartbeat — also resets failure_count if recovering.
        feed_store.record_heartbeat(feed["id"])

        _mirror_to_dev_best_effort(bucket, name)

    except Exception:
        try:
            feed_store.record_failure(feed["id"])
        except Exception:
            logger.exception("Failed to record failure for feed %s", feed["id"])
        raise


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
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
    """Extract UTC timestamp from an Echo recording filename.

    Expected path: {channel}-{location}/{YYYYMMDD}/{channel}_{YYYYMMDD}_{HHMMSS}.mp3
    Example: fire-ca_almaden_valley/20260326/fire_20260326_143022.mp3
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

"""Echo Audio Ingestion Cloud Run Service.

Triggered by Eventarc on GCS OBJECT_FINALIZE events from the Echo recordings
bucket. Resolves feed metadata from AlloyDB, converts MP3 to FLAC, writes to
canonical bucket, and publishes an AudioChunk to the raw-audio topic for
downstream transcription.
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

from backend.pipeline.common.audio import convert_to_flac
from backend.pipeline.common.clients.pubsub_client import PubSubClient
from backend.pipeline.common.gcp_helper import publish_audio_chunk_sync
from backend.pipeline.storage.connection import connect_db
from backend.pipeline.storage.sync_feed_store import SyncFeedStore

if TYPE_CHECKING:
    from cloudevents.http import event as cloudevent

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CANONICAL_BUCKET = os.environ.get("CANONICAL_BUCKET", "")
RAW_AUDIO_TOPIC = os.environ.get("RAW_AUDIO_TOPIC", "")

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
    if not CANONICAL_BUCKET:
        msg = "CANONICAL_BUCKET environment variable is not set"
        raise RuntimeError(msg)
    if not RAW_AUDIO_TOPIC:
        msg = "RAW_AUDIO_TOPIC environment variable is not set"
        raise RuntimeError(msg)
    if gcs_client is None:
        gcs_client = storage.Client()
    if pubsub_client is None:
        pubsub_client = PubSubClient()
    if feed_store is None:
        feed_store = SyncFeedStore(connect_db)
    _handle(cloud_event)


def _handle(cloud_event: cloudevent.CloudEvent) -> None:  # noqa: PLR0911, PLR0915
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

        # Convert MP3 → FLAC. Corrupt audio is a per-file issue — skip it.
        try:
            flac_bytes = convert_to_flac(mp3_bytes, "mp3")
        except Exception:
            logger.warning(
                "Failed to decode audio, skipping corrupt file: %s", name
            )
            return

        # Upload FLAC. if_generation_match=0 skips redundant writes but we
        # always proceed to publish (prior invocation may have crashed after upload).
        date_dir = parts[1]
        flac_path = f"echo/{feed['id']}/{date_dir}/{Path(name).stem}.flac"
        canonical_uri = f"gs://{CANONICAL_BUCKET}/{flac_path}"
        blob = gcs_client.bucket(CANONICAL_BUCKET).blob(flac_path)
        try:
            blob.upload_from_string(
                flac_bytes,
                content_type="audio/flac",
                if_generation_match=0,
            )
        except PreconditionFailed:
            logger.info(
                "FLAC already exists, skipping upload: %s", canonical_uri
            )

        # Publish AudioChunk with deterministic session_id for dedup.
        feed_id_str = str(feed["id"])
        session_id = str(uuid.uuid5(uuid.NAMESPACE_URL, canonical_uri))
        publisher = pubsub_client.get_publisher()
        publish_audio_chunk_sync(
            publisher,
            RAW_AUDIO_TOPIC,
            feed_id_str,
            canonical_uri,
            session_id,
            start_ts,
            source_type="echo",
        )

        # Unconditional heartbeat — also resets failure_count if recovering.
        feed_store.record_heartbeat(feed["id"])

    except Exception:
        try:
            feed_store.record_failure(feed["id"])
        except Exception:
            logger.exception("Failed to record failure for feed %s", feed["id"])
        raise


# ---------------------------------------------------------------------------
# Helpers
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

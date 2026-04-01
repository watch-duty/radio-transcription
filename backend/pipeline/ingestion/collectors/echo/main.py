"""Echo Audio Ingestion Cloud Run Service.

Triggered by Eventarc on GCS OBJECT_FINALIZE events from the Echo recordings
bucket. Resolves feed metadata from AlloyDB, converts MP3 to FLAC, writes to
canonical bucket, and publishes an AudioChunk to the raw-audio topic for
downstream transcription.
"""

from __future__ import annotations

import io
import logging
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import functions_framework
import psycopg
from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage
from psycopg.rows import dict_row
from pydub import AudioSegment

from backend.pipeline.common.clients.pubsub_client import PubSubClient
from backend.pipeline.common.constants import (
    AUDIO_SAMPLE_RATE,
    NUM_AUDIO_CHANNELS,
)
from backend.pipeline.schema_types.raw_audio_chunk_pb2 import AudioChunk

if TYPE_CHECKING:
    from concurrent.futures import Future

    from cloudevents.http import event as cloudevent

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ALLOYDB_HOST = os.environ.get("ALLOYDB_HOST", "")
ALLOYDB_PORT = int(os.environ.get("ALLOYDB_PORT", "6432"))
ALLOYDB_USER = os.environ.get("ALLOYDB_USER", "worker")
ALLOYDB_DB = os.environ.get("ALLOYDB_DB", "postgres")
ALLOYDB_PASSWORD = os.environ.get("ALLOYDB_PASSWORD", "")
CANONICAL_BUCKET = os.environ.get("CANONICAL_BUCKET", "")
RAW_AUDIO_TOPIC = os.environ.get("RAW_AUDIO_TOPIC", "")
FAILURE_THRESHOLD = int(os.environ.get("FAILURE_THRESHOLD", "5"))
BASE_BACKOFF_SEC = int(os.environ.get("BASE_BACKOFF_SEC", "15"))
MAX_BACKOFF_SEC = int(os.environ.get("MAX_BACKOFF_SEC", "600"))

# 16-bit PCM sample width (no shared constant; matches BYTES_PER_SECOND formula)
TARGET_SAMPLE_WIDTH = 2

# ---------------------------------------------------------------------------
# Global state (persisted across warm invocations)
# ---------------------------------------------------------------------------

# Lazily initialized on first invocation so importing this module in unit
# tests does not require GCP credentials.
gcs_client: storage.Client | None = None
pubsub_client: PubSubClient | None = None

# ---------------------------------------------------------------------------
# SQL (psycopg v3 uses %s params instead of asyncpg's $1)
# ---------------------------------------------------------------------------
_RESOLVE_FEED_SQL = """\
SELECT f.id, f.status, f.failure_count
FROM feeds f
JOIN feed_properties_echo fpe ON fpe.feed_id = f.id
WHERE fpe.channel_name = %s
"""

_HEARTBEAT_SQL = """\
UPDATE feeds
SET last_heartbeat = NOW(),
    failure_count = CASE WHEN failure_count > 0 THEN 0 ELSE failure_count END,
    status = CASE WHEN failure_count > 0 THEN 'active'::feed_status ELSE status END
WHERE id = %s
"""

# Backoff formula: base * 2^(failure_count), capped at max, plus 0-10s jitter.
# Matches _REPORT_FAILURE_SQL in feed_store.py (minus worker_id/fencing_token).
_RECORD_FAILURE_SQL = """\
UPDATE feeds
SET status = CASE WHEN failure_count + 1 >= %s
                  THEN 'quarantined'::feed_status
                  ELSE 'failing'::feed_status END,
    failure_count = failure_count + 1,
    last_heartbeat = NOW(),
    retry_after = CASE WHEN failure_count + 1 < %s
                       THEN NOW() + LEAST(
                            %s * INTERVAL '1 second',
                            %s * INTERVAL '1 second' * POWER(2, failure_count)
                       ) + (RANDOM() * INTERVAL '10 seconds')
                       ELSE NULL END
WHERE id = %s
"""


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
@functions_framework.cloud_event
def handle_notification(cloud_event: cloudevent.CloudEvent) -> None:
    """Sync entry point for Eventarc GCS OBJECT_FINALIZE events."""
    global gcs_client, pubsub_client  # noqa: PLW0603
    if gcs_client is None:
        gcs_client = storage.Client()
    if pubsub_client is None:
        pubsub_client = PubSubClient()
    _handle(cloud_event)


def _handle(cloud_event: cloudevent.CloudEvent) -> None:
    """Core handler — fully synchronous."""
    if gcs_client is None or pubsub_client is None:
        msg = (
            "Clients not initialized — handle_notification must be called first"
        )
        raise RuntimeError(msg)

    data = cloud_event.data
    name = data["name"]
    bucket = data["bucket"]

    if not name.endswith(".mp3"):
        return

    channel_name = name.split("/")[0]

    # Resolve feed from DB
    with _connect_db() as conn:
        feed = conn.execute(_RESOLVE_FEED_SQL, (channel_name,)).fetchone()

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
        msg = f"Feed {feed['id']} is quarantined (channel: {channel_name})"
        raise RuntimeError(msg)

    # Bad filenames are not the feed's fault — skip without failure increment.
    try:
        start_ts = _parse_timestamp(name)
    except ValueError:
        logger.warning("Unparseable filename, skipping: %s", name)
        return

    try:
        # Download MP3
        mp3_bytes = gcs_client.bucket(bucket).blob(name).download_as_bytes()

        # Convert MP3 → FLAC. Corrupt audio is a per-file issue — skip it.
        try:
            flac_bytes = _convert_to_flac(mp3_bytes)
        except Exception:
            logger.warning(
                "Failed to decode audio, skipping corrupt file: %s", name
            )
            return

        # Upload FLAC. if_generation_match=0 skips redundant writes but we
        # always proceed to publish (prior invocation may have crashed after upload).
        date_dir = name.split("/")[1]
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
        _publish_audio_chunk(canonical_uri, start_ts, feed_id_str)

        # Unconditional heartbeat — also resets failure_count if recovering.
        with _connect_db() as conn:
            conn.execute(_HEARTBEAT_SQL, (feed["id"],))

    except Exception:
        try:
            with _connect_db() as conn:
                conn.execute(
                    _RECORD_FAILURE_SQL,
                    (
                        FAILURE_THRESHOLD,
                        FAILURE_THRESHOLD,
                        MAX_BACKOFF_SEC,
                        BASE_BACKOFF_SEC,
                        feed["id"],
                    ),
                )
        except Exception:
            logger.exception("Failed to record failure for feed %s", feed["id"])
        raise


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _connect_db() -> psycopg.Connection[dict[str, Any]]:
    """Open a fresh connection to AlloyDB via pgBouncer.

    No pool needed — pgBouncer handles server-side pooling, and Cloud Run
    concurrency=1 means at most one connection per instance at a time.
    """
    return cast(
        psycopg.Connection[dict[str, Any]],
        psycopg.connect(
            host=ALLOYDB_HOST,
            port=ALLOYDB_PORT,
            user=ALLOYDB_USER,
            password=ALLOYDB_PASSWORD,
            dbname=ALLOYDB_DB,
            autocommit=True,
            row_factory=cast(Any, dict_row),
        ),
    )


def _publish_audio_chunk(
    canonical_uri: str, start_ts: datetime, feed_id: str
) -> None:
    """Build and publish an AudioChunk to the raw-audio topic."""
    if pubsub_client is None:
        msg = "PubSubClient not initialized"
        raise RuntimeError(msg)
    publisher = pubsub_client.get_publisher()

    chunk = AudioChunk(gcs_uri=canonical_uri)
    chunk.start_timestamp.FromDatetime(start_ts)
    chunk.session_id = str(uuid.uuid5(uuid.NAMESPACE_URL, canonical_uri))

    future = publisher.publish(
        RAW_AUDIO_TOPIC,
        chunk.SerializeToString(),
        ordering_key=feed_id,
        feed_id=feed_id,
        chunk_uri=canonical_uri,
        source_type="echo",
    )

    # Done-callback runs on Pub/Sub's background thread — fires even if the
    # request thread is terminated by Cloud Run, preventing permanently paused
    # ordering keys.
    def _resume_on_err(f: Future) -> None:
        if f.exception() is not None:
            publisher.resume_publish(RAW_AUDIO_TOPIC, feed_id)

    future.add_done_callback(_resume_on_err)
    future.result()


def _convert_to_flac(mp3_bytes: bytes) -> bytes:
    """Convert MP3 to FLAC (16kHz, 16-bit, mono)."""
    audio = AudioSegment.from_mp3(io.BytesIO(mp3_bytes))
    audio = audio.set_frame_rate(AUDIO_SAMPLE_RATE)
    audio = audio.set_channels(NUM_AUDIO_CHANNELS)
    audio = audio.set_sample_width(TARGET_SAMPLE_WIDTH)
    buf = io.BytesIO()
    audio.export(buf, format="flac")
    return buf.getvalue()


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

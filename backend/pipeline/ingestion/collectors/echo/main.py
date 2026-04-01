"""Echo Audio Ingestion Cloud Function.

Triggered by Eventarc on GCS OBJECT_FINALIZE events from the Echo recordings bucket.
Resolves feed metadata from AlloyDB, converts MP3 to FLAC, writes to canonical bucket,
and publishes an AudioChunk to the raw-audio topic for downstream transcription.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import io
import logging
import os
import threading
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import functions_framework
from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage
from pydub import AudioSegment

from backend.pipeline.common.clients.pubsub_client import PubSubClient
from backend.pipeline.common.constants import (
    AUDIO_SAMPLE_RATE,
    NUM_AUDIO_CHANNELS,
)
from backend.pipeline.common.gcp_helper import publish_audio_chunk
from backend.pipeline.storage.connection import create_pool

if TYPE_CHECKING:
    import asyncpg
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

# GCS and Pub/Sub clients are initialized lazily on first invocation so that
# importing this module in unit tests does not require GCP credentials.
gcs_client: storage.Client | None = None
pubsub_client: PubSubClient | None = None

# Persistent event loop for asyncpg — shared across concurrent request threads.
# CF v2 with concurrency=10 dispatches requests across threads. asyncpg pools
# are bound to one event loop, so we use a single background loop for all DB work.
_loop = asyncio.new_event_loop()
# Match Cloud Run max_instance_request_concurrency (10) so every concurrent
# request can run its asyncio.to_thread calls without queuing on the default
# executor (which caps at min(32, cpu_count+4) = 5 on 1 vCPU).
_loop.set_default_executor(
    concurrent.futures.ThreadPoolExecutor(max_workers=10)
)
_loop_thread = threading.Thread(target=_loop.run_forever, daemon=True)
_loop_thread.start()

_db_pool: asyncpg.Pool | None = None
# Lazily created on _loop inside _get_pool() to avoid cross-loop RuntimeError.
# Safe without a threading guard because all callers run on the single _loop.
_pool_lock: asyncio.Lock | None = None

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------
_RESOLVE_FEED_SQL = """\
SELECT f.id, f.status, f.failure_count
FROM feeds f
JOIN feed_properties_echo fpe ON fpe.feed_id = f.id
WHERE fpe.channel_name = $1
"""

_HEARTBEAT_SQL = """\
UPDATE feeds
SET last_heartbeat = NOW(),
    failure_count = CASE WHEN failure_count > 0 THEN 0 ELSE failure_count END,
    status = CASE WHEN failure_count > 0 THEN 'active'::feed_status ELSE status END
WHERE id = $1
"""

# NOTE: $2 = failure_threshold, $3 = backoff_base_sec, $4 = backoff_max_sec.
# Backoff formula: base * 2^(failure_count), capped at max, plus 0-10s jitter.
# Matches _REPORT_FAILURE_SQL in feed_store.py (minus worker_id/fencing_token).
_RECORD_FAILURE_SQL = """\
UPDATE feeds
SET status = CASE WHEN failure_count + 1 >= $2
                  THEN 'quarantined'::feed_status
                  ELSE 'failing'::feed_status END,
    failure_count = failure_count + 1,
    last_heartbeat = NOW(),
    retry_after = CASE WHEN failure_count + 1 < $2
                       THEN NOW() + LEAST(
                            $4 * INTERVAL '1 second',
                            $3 * INTERVAL '1 second' * POWER(2, failure_count)
                       ) + (RANDOM() * INTERVAL '10 seconds')
                       ELSE NULL END
WHERE id = $1
"""


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
@functions_framework.cloud_event
def handle_notification(cloud_event: cloudevent.CloudEvent) -> None:
    """Sync entry point — submits async work to the shared event loop."""
    global gcs_client, pubsub_client  # noqa: PLW0603
    if gcs_client is None:
        gcs_client = storage.Client()
    if pubsub_client is None:
        pubsub_client = PubSubClient()
    future = asyncio.run_coroutine_threadsafe(_handle(cloud_event), _loop)
    try:
        future.result(timeout=30)
    except TimeoutError:
        future.cancel()
        raise


async def _handle(cloud_event: cloudevent.CloudEvent) -> None:
    """Core async handler for a single GCS OBJECT_FINALIZE event."""
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

    pool = await _get_pool()
    feed = await pool.fetchrow(_RESOLVE_FEED_SQL, channel_name)
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
        # Raise so Eventarc retries until the feed is un-quarantined,
        # rather than silently discarding recordings during a transient outage.
        msg = f"Feed {feed['id']} is quarantined (channel: {channel_name})"
        raise RuntimeError(msg)

    # Catch unparseable filenames early — bad filenames are not the feed's
    # fault and must not increment failure_count or trigger Eventarc retries.
    try:
        start_ts = _parse_timestamp(name)
    except ValueError:
        logger.warning("Unparseable filename, skipping: %s", name)
        return

    try:
        # Download MP3 — run in thread pool to avoid blocking the event loop
        mp3_bytes = await asyncio.to_thread(
            gcs_client.bucket(bucket).blob(name).download_as_bytes
        )

        # Convert MP3 → FLAC (16kHz, 16-bit, mono).
        # Corrupt audio is a per-file issue, not an infrastructure failure —
        # isolate it so it doesn't increment failure_count or trigger retries.
        try:
            flac_bytes = await asyncio.to_thread(_convert_to_flac, mp3_bytes)
        except Exception:
            logger.warning(
                "Failed to decode audio, skipping corrupt file: %s", name
            )
            return

        # Upload FLAC to canonical bucket. if_generation_match=0 skips a
        # redundant write when the object already exists (concurrent retry
        # or Eventarc redelivery), but we always proceed to publish — a
        # prior invocation may have crashed between upload and publish.
        date_dir = name.split("/")[1]
        flac_path = f"echo/{feed['id']}/{date_dir}/{Path(name).stem}.flac"
        canonical_uri = f"gs://{CANONICAL_BUCKET}/{flac_path}"
        blob = gcs_client.bucket(CANONICAL_BUCKET).blob(flac_path)
        try:
            await asyncio.to_thread(
                blob.upload_from_string,
                flac_bytes,
                content_type="audio/flac",
                if_generation_match=0,
            )
        except PreconditionFailed:
            logger.info(
                "FLAC already exists, skipping upload: %s",
                canonical_uri,
            )

        # Deterministic session_id so Eventarc redeliveries produce the same
        # ID and downstream Stitcher dedup recognises the duplicate.
        feed_id_str = str(feed["id"])
        session_id = str(uuid.uuid5(uuid.NAMESPACE_URL, canonical_uri))
        await publish_audio_chunk(
            pubsub_client,
            RAW_AUDIO_TOPIC,
            feed_id_str,
            canonical_uri,
            session_id,
            start_ts,
            source_type="echo",
        )

        # Unconditional heartbeat — also resets failure_count if recovering
        await pool.execute(_HEARTBEAT_SQL, feed["id"])

    except Exception:
        try:
            await pool.execute(
                _RECORD_FAILURE_SQL,
                feed["id"],
                FAILURE_THRESHOLD,
                BASE_BACKOFF_SEC,
                MAX_BACKOFF_SEC,
            )
        except Exception:
            logger.exception("Failed to record failure for feed %s", feed["id"])
        raise


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _convert_to_flac(mp3_bytes: bytes) -> bytes:
    """Convert MP3 to FLAC (16kHz, 16-bit, mono). CPU-bound — called via to_thread."""
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
    stem = Path(filename).stem  # fire_20260326_143022
    parts = stem.rsplit("_", 2)
    if len(parts) < 3:
        msg = f"Cannot parse timestamp from filename: {name}"
        raise ValueError(msg)
    date_str, time_str = parts[-2], parts[-1]
    return datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S").replace(
        tzinfo=UTC
    )


async def _get_pool() -> asyncpg.Pool:
    """Return the shared asyncpg pool, creating it lazily with a lock."""
    global _db_pool, _pool_lock  # noqa: PLW0603
    if _pool_lock is None:
        _pool_lock = asyncio.Lock()
    async with _pool_lock:
        if _db_pool is None:
            _db_pool = await create_pool(
                host=ALLOYDB_HOST,
                user=ALLOYDB_USER,
                db_name=ALLOYDB_DB,
                password=ALLOYDB_PASSWORD,
                port=ALLOYDB_PORT,
                min_size=2,
                max_size=5,
                max_inactive_connection_lifetime=60.0,
            )
    return _db_pool

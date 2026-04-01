"""Echo Audio Ingestion Cloud Function.

Triggered by Eventarc on GCS OBJECT_FINALIZE events from the Echo recordings bucket.
Resolves feed metadata from AlloyDB, converts MP3 to FLAC, writes to canonical bucket,
and publishes an AudioChunk to the raw-audio topic for downstream transcription.
"""

from __future__ import annotations

import asyncio
import io
import logging
import os
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import asyncpg
import functions_framework
from google.cloud import pubsub_v1, storage
from pydub import AudioSegment
from schema_types.raw_audio_chunk_pb2 import AudioChunk

if TYPE_CHECKING:
    from cloudevents.http import event as cloudevent

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ALLOYDB_HOST = os.environ["ALLOYDB_HOST"]
ALLOYDB_PORT = int(os.environ.get("ALLOYDB_PORT", "6432"))
ALLOYDB_USER = os.environ.get("ALLOYDB_USER", "worker")
ALLOYDB_DB = os.environ.get("ALLOYDB_DB", "postgres")
ALLOYDB_PASSWORD = os.environ["ALLOYDB_PASSWORD"]
CANONICAL_BUCKET = os.environ["CANONICAL_BUCKET"]
RAW_AUDIO_TOPIC = os.environ["RAW_AUDIO_TOPIC"]
FAILURE_THRESHOLD = int(os.environ.get("FAILURE_THRESHOLD", "5"))
BASE_BACKOFF_SEC = int(os.environ.get("BASE_BACKOFF_SEC", "15"))
MAX_BACKOFF_SEC = int(os.environ.get("MAX_BACKOFF_SEC", "600"))

# Target audio format (matches Icecast collector output)
TARGET_SAMPLE_RATE = 16_000
TARGET_CHANNELS = 1
TARGET_SAMPLE_WIDTH = 2  # 16-bit

# ---------------------------------------------------------------------------
# Global state (persisted across warm invocations)
# ---------------------------------------------------------------------------
gcs_client = storage.Client()
publisher = pubsub_v1.PublisherClient(
    publisher_options=pubsub_v1.types.PublisherOptions(
        enable_message_ordering=True,
    ),
)

# Persistent event loop for asyncpg — shared across concurrent request threads.
# CF v2 with concurrency=10 dispatches requests across threads. asyncpg pools
# are bound to one event loop, so we use a single background loop for all DB work.
_loop = asyncio.new_event_loop()
_loop_thread = threading.Thread(target=_loop.run_forever, daemon=True)
_loop_thread.start()

_db_pool: asyncpg.Pool | None = None
_pool_lock = asyncio.Lock()

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------
_RESOLVE_FEED_SQL = """\
SELECT f.id, f.status, f.failure_count
FROM feeds f
JOIN feed_properties_echo fpe ON fpe.feed_id = f.id
WHERE fpe.channel_name = $1
"""

_RESET_FAILURE_SQL = """\
UPDATE feeds
SET failure_count = 0, status = 'active'::feed_status, last_heartbeat = NOW()
WHERE id = $1 AND failure_count > 0
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
    future = asyncio.run_coroutine_threadsafe(_handle(cloud_event), _loop)
    future.result(timeout=30)


async def _handle(cloud_event: cloudevent.CloudEvent) -> None:
    """Core async handler for a single GCS OBJECT_FINALIZE event."""
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
    if feed["status"] in ("deactivated", "quarantined"):
        logger.info(
            "Skipping %s feed %s (channel: %s)",
            feed["status"],
            feed["id"],
            channel_name,
        )
        return

    try:
        start_ts = _parse_timestamp(name)

        # Download MP3 — run in thread pool to avoid blocking the event loop
        mp3_bytes = await asyncio.to_thread(
            gcs_client.bucket(bucket).blob(name).download_as_bytes
        )

        # Convert MP3 → FLAC (16kHz, 16-bit, mono)
        flac_bytes = await asyncio.to_thread(_convert_to_flac, mp3_bytes)

        # Upload FLAC to canonical bucket
        date_dir = name.split("/")[1]
        flac_path = f"echo/{feed['id']}/{date_dir}/{Path(name).stem}.flac"
        canonical_uri = f"gs://{CANONICAL_BUCKET}/{flac_path}"
        await asyncio.to_thread(
            gcs_client.bucket(CANONICAL_BUCKET)
            .blob(flac_path)
            .upload_from_string,
            flac_bytes,
            "audio/flac",
        )

        # Publish AudioChunk (matches gcp_helper.publish_audio_chunk pattern)
        chunk = AudioChunk(gcs_uri=canonical_uri)
        chunk.start_timestamp.FromDatetime(start_ts)
        chunk.session_id = str(uuid.uuid4())
        feed_id_str = str(feed["id"])
        publisher.publish(
            RAW_AUDIO_TOPIC,
            chunk.SerializeToString(),
            feed_id=feed_id_str,
            ordering_key=feed_id_str,
            chunk_uri=canonical_uri,
            source_type="echo",
        )

        # Conditional reset — only writes if recovering from a previous failure
        await pool.execute(_RESET_FAILURE_SQL, feed["id"])

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
            logger.exception(
                "Failed to record failure for feed %s", feed["id"]
            )
        raise


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _convert_to_flac(mp3_bytes: bytes) -> bytes:
    """Convert MP3 to FLAC (16kHz, 16-bit, mono). CPU-bound — called via to_thread."""
    audio = AudioSegment.from_mp3(io.BytesIO(mp3_bytes))
    audio = audio.set_frame_rate(TARGET_SAMPLE_RATE)
    audio = audio.set_channels(TARGET_CHANNELS)
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
    return datetime.strptime(
        f"{date_str}{time_str}", "%Y%m%d%H%M%S"
    ).replace(tzinfo=datetime.UTC)


async def _get_pool() -> asyncpg.Pool:
    """Return the shared asyncpg pool, creating it lazily with a lock."""
    global _db_pool  # noqa: PLW0603
    async with _pool_lock:
        if _db_pool is None or _db_pool._closed:
            _db_pool = await asyncpg.create_pool(
                host=ALLOYDB_HOST,
                port=ALLOYDB_PORT,
                user=ALLOYDB_USER,
                password=ALLOYDB_PASSWORD,
                database=ALLOYDB_DB,
                min_size=2,
                max_size=5,
                statement_cache_size=0,  # Required for pgBouncer transaction mode
            )
    return _db_pool

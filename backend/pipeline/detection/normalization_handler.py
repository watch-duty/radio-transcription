from __future__ import annotations

import asyncio
import base64
import datetime
import json
import logging
import os

import aiohttp
import functions_framework.aio
import numpy as np
from cloudevents.http.event import (
    CloudEvent,  # noqa: TC002 (runtime: functions_framework)
)

from backend.pipeline.common.clients.gcs_client import GcsClient
from backend.pipeline.common.clients.pubsub_client import PubSubClient
from backend.pipeline.common.constants import AUDIO_SAMPLE_RATE
from backend.pipeline.common.gcp_helper import (
    download_audio,
    parse_gcs_uri,
    publish_audio_chunk,
    upload_audio,
)
from backend.pipeline.common.logging import setup_logging
from backend.pipeline.detection.detector_executor import DetectorExecutor
from backend.pipeline.detection.detector_factory import DetectorFactory
from backend.pipeline.detection.sidecar_builder import SidecarBuilder
from backend.pipeline.schema_types.raw_audio_chunk_pb2 import AudioChunk
from backend.pipeline.schema_types.sed_metadata_pb2 import SedMetadata

logger = logging.getLogger(__name__)

# --- Module-level initialization (matches evaluation/transcription pattern) ---
setup_logging()

_detector_config = json.loads(
    os.environ.get("DETECTOR_CONFIG", '{"detectors": []}')
)
_detectors, _combiner = DetectorFactory.create_ensemble(_detector_config)
_executor = DetectorExecutor(_detectors, _combiner)

_gcs_client = GcsClient()
_pubsub_client = PubSubClient()

logger.info(
    "Detection normalization handler initialized with %d detector(s)",
    len(_detectors),
)


# --- Helpers ------------------------------------------------------------------


def _parse_cloud_event(
    cloud_event: CloudEvent,
) -> tuple[AudioChunk, str] | None:
    """Parse CloudEvent, returning (AudioChunk, feed_id) or None."""
    message = cloud_event.data.get("message", {})
    pubsub_data = message.get("data")
    if not pubsub_data:
        logger.warning("No data in Pub/Sub message, acknowledging")
        return None

    try:
        decoded_data = base64.b64decode(pubsub_data)
        audio_chunk_msg = AudioChunk()
        audio_chunk_msg.ParseFromString(decoded_data)
    except Exception:
        logger.exception("Failed to parse Pub/Sub message data (permanent)")
        return None

    if not audio_chunk_msg.gcs_uri:
        logger.warning("Empty gcs_uri in AudioChunk, acknowledging")
        return None

    attributes = message.get("attributes", {})
    feed_id = attributes.get("feed_id", "unknown")
    return audio_chunk_msg, feed_id


async def _decode_flac(flac_bytes: bytes) -> tuple[np.ndarray, int]:
    """Decode FLAC bytes to int16 numpy array and sample rate.

    Uses async ffmpeg subprocess instead of soundfile because libsndfile
    cannot decode streaming FLAC (total_samples=0 in STREAMINFO) produced
    by ffmpeg's segment muxer. ffmpeg's own FLAC decoder handles this
    correctly. Async avoids blocking the event loop under concurrency.
    """
    process = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-i", "pipe:0",
        "-f", "s16le",
        "-acodec", "pcm_s16le",
        "-ar", str(AUDIO_SAMPLE_RATE),
        "-ac", "1",
        "pipe:1",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )  # fmt: skip
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(input=flac_bytes), timeout=30
        )
    except TimeoutError:
        msg = f"ffmpeg FLAC decode timed out after 30s (input={len(flac_bytes)} bytes)"
        raise RuntimeError(msg)
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    if process.returncode != 0:
        msg = (
            f"ffmpeg FLAC decode failed (exit {process.returncode}, "
            f"input={len(flac_bytes)} bytes): {stderr.decode(errors='replace')}"
        )
        raise RuntimeError(msg)
    samples = np.frombuffer(stdout, dtype=np.int16)
    if len(samples) == 0:
        msg = (
            f"ffmpeg produced no audio samples (input={len(flac_bytes)} bytes)"
        )
        raise RuntimeError(msg)
    logger.info(
        "Decoded FLAC: %d bytes -> %d samples (%.3fs)",
        len(flac_bytes),
        len(samples),
        len(samples) / AUDIO_SAMPLE_RATE,
    )
    return samples, AUDIO_SAMPLE_RATE


async def _get_existing_metadata(
    gcs_client: GcsClient,
    bucket: str,
    object_name: str,
) -> SedMetadata | None:
    """Check if canonical object exists and return its SED metadata.

    Returns deserialized SedMetadata if the object exists, None if not (404).
    If the object exists but sed_metadata is missing or corrupt, returns
    an empty SedMetadata (no speech events).
    """
    storage = gcs_client.get_storage()
    try:
        response = await storage.download_metadata(bucket, object_name)
    except aiohttp.ClientResponseError as exc:
        if exc.status == 404:
            return None
        raise

    raw = response.get("metadata", {}).get("sed_metadata")
    if not raw:
        logger.warning(
            "Object exists but missing sed_metadata, treating as no-speech: "
            "gs://%s/%s",
            bucket,
            object_name,
        )
        return SedMetadata()

    try:
        sed = SedMetadata()
        sed.ParseFromString(base64.b64decode(raw))
    except Exception:
        logger.exception(
            "Object exists but invalid sed_metadata, treating as no-speech: "
            "gs://%s/%s",
            bucket,
            object_name,
        )
        return SedMetadata()
    else:
        return sed


async def _maybe_publish_transcription(
    sed_metadata: SedMetadata,
    audio_chunk_msg: AudioChunk,
    feed_id: str,
    output_gcs_uri: str,
    source_gcs_uri: str,
    *,
    propagate_errors: bool = False,
) -> None:
    """Publish to transcription topic if speech was detected.

    Reused by both the early-exit (redelivery) and normal processing paths.

    Args:
        sed_metadata: SED metadata with speech detection results.
        audio_chunk_msg: Original AudioChunk protobuf from Pub/Sub message.
        feed_id: Feed identifier for the audio source.
        output_gcs_uri: Canonical bucket GCS URI for the processed audio.
        source_gcs_uri: Original staging bucket GCS URI (for error logs).
        propagate_errors: If True, let publish exceptions propagate (500 →
            Pub/Sub retry). Used on the redelivery path where retrying is
            cheap since no heavy processing is at risk. If False (default),
            catch and log publish failures (original behavior for normal path
            where re-downloading/decoding would be expensive).
    """
    if not sed_metadata.sound_events:
        return

    if not audio_chunk_msg.HasField("start_timestamp"):
        logger.error(
            "Missing start_timestamp in audio chunk for %s (permanent)",
            source_gcs_uri,
        )
        return

    if not audio_chunk_msg.session_id:
        logger.error(
            "Missing session_id in audio chunk for %s (permanent)",
            source_gcs_uri,
        )
        return

    start_ts = audio_chunk_msg.start_timestamp.ToDatetime(tzinfo=datetime.UTC)

    topic_path = os.environ.get("TRANSCRIPTION_TOPIC_PATH", "")
    if topic_path:
        try:
            message_id = await publish_audio_chunk(
                _pubsub_client,
                topic_path,
                feed_id,
                output_gcs_uri,
                session_id=audio_chunk_msg.session_id,
                start_timestamp=start_ts,
            )
            logger.info(
                "Published to transcription topic: message_id=%s feed=%s",
                message_id,
                feed_id,
            )
        except Exception:
            if propagate_errors:
                raise
            logger.exception(
                "Failed to publish to transcription topic for %s",
                source_gcs_uri,
            )
    else:
        logger.warning("TRANSCRIPTION_TOPIC_PATH not configured")


# --- Entry point --------------------------------------------------------------


@functions_framework.aio.cloud_event
async def normalize(cloud_event: CloudEvent) -> None:
    """
    Cloud Function entry point triggered by Pub/Sub.

    Returning normally signals success (200 ack). Raising an exception
    signals retriable failure (functions_framework returns 500).
    """
    parsed = _parse_cloud_event(cloud_event)
    if parsed is None:
        return

    audio_chunk_msg, feed_id = parsed
    gcs_uri = audio_chunk_msg.gcs_uri
    logger.info("Processing detection for feed=%s gcs_uri=%s", feed_id, gcs_uri)

    # 1. Compute canonical path (needed for both early-exit and normal paths)
    canonical_bucket = os.environ.get("INGESTION_CANONICAL_BUCKET", "")
    _, object_name = parse_gcs_uri(gcs_uri)
    output_gcs_uri = f"gs://{canonical_bucket}/{object_name}"

    # 2. Check if already processed (idempotency for Pub/Sub redelivery)
    existing = await _get_existing_metadata(
        _gcs_client, canonical_bucket, object_name
    )
    if existing is not None:
        logger.info(
            "Already processed, skipping reprocessing feed=%s gcs_uri=%s",
            feed_id,
            gcs_uri,
        )
        await _maybe_publish_transcription(
            existing,
            audio_chunk_msg,
            feed_id,
            output_gcs_uri,
            gcs_uri,
            propagate_errors=True,
        )
        return

    # 3. Download FLAC from GCS (raises on failure → 500 retry)
    flac_bytes = await download_audio(_gcs_client, gcs_uri)

    # 4. Decode FLAC → int16 numpy array
    try:
        samples, sample_rate = await _decode_flac(flac_bytes)
    except Exception:
        logger.exception("Failed to decode FLAC from %s (permanent)", gcs_uri)
        return

    if sample_rate != AUDIO_SAMPLE_RATE:
        logger.error(
            "Unexpected sample rate %d (expected %d) for %s (permanent)",
            sample_rate,
            AUDIO_SAMPLE_RATE,
            gcs_uri,
        )
        return

    # 5. Run detectors and combine via DetectorExecutor
    combined_result = _executor.run(samples, context=gcs_uri)

    # 6. Build sidecar
    sidecar = SidecarBuilder.build(
        combined_result, source_chunk_id=output_gcs_uri
    )
    if audio_chunk_msg.HasField("start_timestamp"):
        sidecar.start_timestamp.CopyFrom(audio_chunk_msg.start_timestamp)

    # 7. Upload audio+sidecar (if_generation_match=0 for idempotency)
    await upload_audio(
        _gcs_client,
        flac_bytes,
        canonical_bucket,
        object_name,
        sed_metadata=sidecar,
        if_generation_match=0,
    )
    logger.info(
        "Uploaded audio+sidecar to %s (first processing)", output_gcs_uri
    )

    # 8. Publish to transcription topic if speech detected
    # (sidecar.sound_events mirrors combined_result.speech_regions via SidecarBuilder)
    await _maybe_publish_transcription(
        sidecar, audio_chunk_msg, feed_id, output_gcs_uri, gcs_uri
    )

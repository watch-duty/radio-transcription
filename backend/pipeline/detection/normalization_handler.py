from __future__ import annotations

import base64
import datetime
import io
import json
import logging
import os
from typing import TYPE_CHECKING

import functions_framework.aio
import google.cloud.logging
import soundfile as sf
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
from backend.pipeline.detection.detector_executor import DetectorExecutor
from backend.pipeline.detection.detector_factory import DetectorFactory
from backend.pipeline.detection.sidecar_builder import SidecarBuilder
from backend.pipeline.schema_types.raw_audio_chunk_pb2 import AudioChunk

if TYPE_CHECKING:
    import numpy as np

logger = logging.getLogger(__name__)

# --- Module-level initialization (matches evaluation/transcription pattern) ---

if not os.environ.get("LOCAL_DEV"):
    _log_client = google.cloud.logging.Client()
    _log_client.setup_logging()
else:
    logger.setLevel(logging.INFO)
    _handler = logging.StreamHandler()
    logger.addHandler(_handler)
    logger.info("Running in LOCAL_DEV mode. Logs will print here.")

_detector_config = json.loads(os.environ.get("DETECTOR_CONFIG", '{"detectors": []}'))
_detectors, _combiner = DetectorFactory.create_ensemble(_detector_config)
_executor = DetectorExecutor(_detectors, _combiner)

_gcs_client = GcsClient()
_pubsub_client = PubSubClient()

logger.info(
    "Detection normalization handler initialized with %d detector(s)",
    len(_detectors),
)


# --- Helpers ------------------------------------------------------------------


def _parse_cloud_event(cloud_event: CloudEvent) -> tuple[AudioChunk, str] | None:
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


def _decode_flac(flac_bytes: bytes) -> tuple[np.ndarray, int]:
    """Decode FLAC bytes to int16 numpy array and sample rate."""
    samples, sample_rate = sf.read(io.BytesIO(flac_bytes), dtype="int16")
    if samples.ndim > 1:
        samples = samples[:, 0]
    return samples, sample_rate


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

    # 1. Download FLAC from GCS (raises on failure → 500 retry)
    flac_bytes = await download_audio(_gcs_client, gcs_uri)

    # 2. Decode FLAC → int16 numpy array
    try:
        samples, sample_rate = _decode_flac(flac_bytes)
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

    # 3. Run detectors and combine via DetectorExecutor
    combined_result = _executor.run(samples, context=gcs_uri)

    # 4. Compute output path (mirrors source path in canonical bucket)
    canonical_bucket = os.environ.get("INGESTION_CANONICAL_BUCKET", "")
    _, object_name = parse_gcs_uri(gcs_uri)
    output_gcs_uri = f"gs://{canonical_bucket}/{object_name}"

    # 5. Build sidecar
    sidecar = SidecarBuilder.build(combined_result, source_chunk_id=output_gcs_uri)
    if audio_chunk_msg.HasField("start_timestamp"):
        sidecar.start_timestamp.CopyFrom(audio_chunk_msg.start_timestamp)

    # 6. Upload audio+sidecar via shared gcp_helper (raises on failure → 500 retry)
    await upload_audio(
        _gcs_client,
        flac_bytes,
        canonical_bucket,
        object_name,
        sed_metadata=sidecar,
    )
    logger.info("Uploaded audio+sidecar to %s", output_gcs_uri)

    # 7. Publish to transcription topic if speech detected
    if combined_result.speech_regions:
        if not audio_chunk_msg.HasField("start_timestamp"):
            logger.error(
                "Missing start_timestamp in audio chunk for %s (permanent)", gcs_uri
            )
            return

        if not audio_chunk_msg.session_id:
            logger.error(
                "Missing session_id in audio chunk for %s (permanent)", gcs_uri
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
                logger.exception(
                    "Failed to publish to transcription topic for %s",
                    gcs_uri,
                )
        else:
            logger.warning("TRANSCRIPTION_TOPIC_PATH not configured")

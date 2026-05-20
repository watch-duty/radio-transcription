"""Serverless Cloud Run / Cloud Function transcription entry point.

Triggered by Pub/Sub push events containing serialized AudioReadyForTranscription
claim-check metadata. Retrieves references, transcribes them using the pluggable
transcriber, and publishes the final TranscribedAudio egress message.
"""

import base64
import logging
import os
from typing import Any

import functions_framework
from google.cloud import pubsub_v1

from backend.pipeline.normalization.common.enums import TranscriberType
from backend.pipeline.schema_types.normalized_audio_pb2 import (
    NormalizedAudio,
)
from backend.pipeline.schema_types.transcribed_audio_pb2 import (
    TranscribedAudio,
)
from backend.pipeline.transcription import transcribers

# Setup system logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("transcription-function")

# Warm start singleton client cache
_transcriber_instance: transcribers.Transcriber | None = None
_publisher_client: pubsub_v1.PublisherClient | None = None


def get_lazy_transcriber(project_id: str) -> transcribers.Transcriber:
    """Warms up and caches the transcriber instance."""
    global _transcriber_instance  # noqa: PLW0603
    if _transcriber_instance is None:
        t_type_str = os.environ.get("TRANSCRIBER_TYPE", "GOOGLE_CHIRP_V3")
        t_config_json = os.environ.get("TRANSCRIBER_CONFIG", "{}")
        t_type = TranscriberType[t_type_str]
        logger.info("Initializing transcriber type %s", t_type.name)
        _transcriber_instance = transcribers.get_transcriber(
            t_type, project_id, t_config_json
        )
        _transcriber_instance.setup()
    return _transcriber_instance


def get_lazy_publisher() -> pubsub_v1.PublisherClient:
    """Warms up and caches the Pub/Sub publisher client."""
    global _publisher_client  # noqa: PLW0603
    if _publisher_client is None:
        logger.info("Initializing Pub/Sub PublisherClient")
        # Enable publisher side ordering keys
        publisher_options = pubsub_v1.types.PublisherOptions(
            enable_message_ordering=True
        )
        _publisher_client = pubsub_v1.PublisherClient(
            publisher_options=publisher_options
        )
    return _publisher_client


@functions_framework.http
def transcribe_claim_check(request: Any) -> Any:
    """Entry point for Cloud Function push events."""
    project_id = os.environ.get("PROJECT_ID", "watch-duty-dev")
    output_topic = os.environ.get("OUTPUT_TOPIC")

    if not output_topic:
        msg = "OUTPUT_TOPIC environment variable must be set"
        logger.error(msg)
        return "Internal Server Error: Missing OUTPUT_TOPIC", 500

    envelope = request.get_json()
    if not envelope or "message" not in envelope:
        msg = "Bad Request: Missing Pub/Sub envelope message"
        logger.error(msg)
        return msg, 400

    pubsub_message = envelope["message"]

    # Parse claim-check payload
    try:
        data_bytes = base64.b64decode(pubsub_message["data"])
        claim = NormalizedAudio()
        claim.ParseFromString(data_bytes)
    except Exception as e:
        msg = f"Bad Request: Failed to parse NormalizedAudio: {e}"
        logger.exception(msg)
        return msg, 400

    feed_id = claim.feed_id
    transmission_id = claim.transmission_id

    logger.info(
        "Received claim for transmission %s (feed %s, uri: %s)",
        transmission_id,
        feed_id,
        claim.canonical_audio_uri,
    )

    try:
        # Determine audio duration from start and end timestamps
        start_ms = (
            claim.start_timestamp.seconds * 1000
            + claim.start_timestamp.nanos // 1000000
        )
        end_ms = (
            claim.end_timestamp.seconds * 1000
            + claim.end_timestamp.nanos // 1000000
        )
        duration_ms = max(0, int(end_ms - start_ms))

        # Retrieve active transcriber and run Speech API
        transcriber = get_lazy_transcriber(project_id)
        transcript = transcriber.transcribe(
            uri=claim.canonical_audio_uri,
            duration_ms=duration_ms,
        )

        if not transcript:
            logger.info(
                "Speech API returned empty transcription. Using fallback unintelligible marker."
            )
            transcript = "[UNINTELLIGIBLE]"

        # Build TranscribedAudio egress protobuf message
        out_proto = TranscribedAudio(
            transmission_id=claim.transmission_id,
            feed_id=claim.feed_id,
            transcript=transcript,
            start_timestamp=claim.start_timestamp,
            end_timestamp=claim.end_timestamp,
            missing_prior_context=claim.missing_prior_context,
            missing_post_context=claim.missing_post_context,
            source_audio_uris=claim.source_audio_uris,
            start_audio_offset=claim.start_audio_offset,
            end_audio_offset=claim.end_audio_offset,
            canonical_audio_uri=claim.canonical_audio_uri,
            playback_audio_uri=claim.playback_audio_uri,
            feed_name=claim.feed_name,
            external_id=claim.external_id,
        )

        # Egress to final output topic, strictly ordered by feed_id
        publisher = get_lazy_publisher()
        topic_path = publisher.topic_path(
            project_id, output_topic.split("/")[-1]
        )

        attrs: dict[str, str] = {}
        # Forward traceparent metadata if present in trigger attributes
        traceparent = pubsub_message.get("attributes", {}).get("traceparent")
        if traceparent:
            attrs["traceparent"] = traceparent

        future = publisher.publish(
            topic=topic_path,
            data=out_proto.SerializeToString(),
            ordering_key=feed_id,
            **attrs,
        )
        message_id = future.result()
        logger.info(
            "Successfully transcribed and published egress message %s for transmission %s (feed %s)",
            message_id,
            transmission_id,
            feed_id,
        )
    except Exception as e:
        logger.exception(
            "Failed to process transcription claim for transmission %s (feed %s): %s",
            transmission_id,
            feed_id,
            e,
        )
        # Return a 500 error so Pub/Sub retries push delivery (highly resilient!)
        return f"Internal Server Error: {e}", 500
    else:
        return (
            f"Success: Published transcribed transmission {transmission_id}",
            200,
        )

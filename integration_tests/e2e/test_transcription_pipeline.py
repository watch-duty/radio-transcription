import logging
import os
import time
import uuid

from google.cloud import pubsub_v1

from backend.pipeline.schema_types.continuous_audio_pb2 import ContinuousAudio
from integration_tests.feed_utils import create_test_bcfy_feed  # noqa: F401
from integration_tests.test_utils import verify_audio_segments_via_api

logger = logging.getLogger(__name__)


def _publish_and_verify(
    topic: str,
    feed_id: str,
    feed_name: str,
    audio_filename: str,
) -> bool:
    # Construct ContinuousAudio message
    staging_bucket = os.environ["AUDIO_STAGING_BUCKET"]
    chunk = ContinuousAudio(
        feed_id=feed_id,
        session_id=str(uuid.uuid4()),
        gcs_uri=f"gs://{staging_bucket}/{audio_filename}",
        feed_name=feed_name,
        duration_ms=1000,
    )

    publisher = pubsub_v1.PublisherClient()

    logger.info(f"Publishing to {topic}...")
    future = publisher.publish(
        topic,
        data=chunk.SerializeToString(),
        gcs_uri=chunk.gcs_uri,
        timestamp_ms=str(int(time.time() * 1000)),
    )
    future.result()

    return verify_audio_segments_via_api(
        feed_id,
        lambda s: (
            any(ann["type"] == "TRANSCRIPT" for ann in s.get("annotations", []))
            and any(ann["type"] == "EVALUATION" for ann in s.get("annotations", []))
        ),
    )


def test_continuous_pipeline_flow(
    test_bcfy_feed: tuple[str, str],
) -> None:
    """Tests that a message published to continuous topic reaches evaluation."""
    continuous_topic = os.environ["CONTINUOUS_TOPIC"]
    feed_id, feed_name = test_bcfy_feed
    _publish_and_verify(
        continuous_topic,
        feed_id,
        feed_name,
        audio_filename="test_bcfy.flac",
    )


def test_segmented_pipeline_flow(
    test_bcfy_feed: tuple[str, str],
) -> None:
    """Tests that a message published to segmented topic reaches evaluation."""
    segmented_topic = os.environ["SEGMENTED_TOPIC"]
    feed_id, feed_name = test_bcfy_feed
    _publish_and_verify(
        segmented_topic,
        feed_id,
        feed_name,
        audio_filename="test_joined.flac",
    )

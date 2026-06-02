import logging
import os
import time
import uuid

from google.cloud import pubsub_v1
from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp

from backend.pipeline.schema_types.continuous_audio_pb2 import ContinuousAudio
from backend.pipeline.schema_types.segmented_audio_pb2 import SegmentedAudio
from integration_tests.feed_utils import create_test_bcfy_feed  # noqa: F401
from integration_tests.test_utils import verify_transcript_in_db

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

    return verify_transcript_in_db(feed_id)


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
    staging_bucket = os.environ["AUDIO_STAGING_BUCKET"]
    audio_filename = "test_joined.flac"
    gcs_uri = f"gs://{staging_bucket}/{audio_filename}"

    start_timestamp = Timestamp()
    start_timestamp.FromMicroseconds(int(time.time() * 1000000))

    end_timestamp = Timestamp()
    end_timestamp.FromMicroseconds(int((time.time() + 1) * 1000000))

    start_offset = Duration(seconds=0, nanos=0)
    end_offset = Duration(seconds=1, nanos=0)

    # Construct SegmentedAudio message
    segmented_msg = SegmentedAudio(
        segment_id=str(uuid.uuid4()),
        feed_id=feed_id,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        missing_prior_context=False,
        missing_post_context=False,
        source_audio_uris=[gcs_uri],
        start_audio_offset=start_offset,
        end_audio_offset=end_offset,
        feed_name=feed_name,
        external_id="mock-external-id",
        audio_classification=SegmentedAudio.AUDIO_CLASSIFICATION_SPEECH,
        raw_audio_uri=gcs_uri,
    )

    publisher = pubsub_v1.PublisherClient()
    logger.info(f"Publishing SegmentedAudio to {segmented_topic}...")
    future = publisher.publish(
        segmented_topic,
        data=segmented_msg.SerializeToString(),
        feed_id=feed_id,
        gcs_uri=gcs_uri,
        timestamp_ms=str(int(time.time() * 1000)),
    )
    future.result()

    assert verify_transcript_in_db(feed_id)

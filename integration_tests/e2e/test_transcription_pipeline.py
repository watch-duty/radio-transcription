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
from integration_tests.test_utils import verify_audio_segments_via_api

logger = logging.getLogger(__name__)


def _publish_and_verify(
    topic: str,
    feed_id: str,
    feed_name: str,
    audio_filename: str,
) -> bool:
    staging_bucket = os.environ["AUDIO_STAGING_BUCKET"]
    is_segmented = "segmented" in topic

    if is_segmented:
        now = time.time()
        start_ts = Timestamp()
        start_ts.FromSeconds(int(now))
        end_ts = Timestamp()
        end_ts.FromSeconds(int(now + 15))

        zero_dur = Duration()
        zero_dur.FromSeconds(0)

        chunk = SegmentedAudio(
            segment_id=str(uuid.uuid4()),
            feed_id=feed_id,
            feed_name=feed_name,
            raw_audio_uri=f"gs://{staging_bucket}/{audio_filename}",
            audio_classification=SegmentedAudio.AUDIO_CLASSIFICATION_SPEECH,
            source_audio_uris=[f"gs://{staging_bucket}/{audio_filename}"],
            start_timestamp=start_ts,
            end_timestamp=end_ts,
            start_audio_offset=zero_dur,
            end_audio_offset=zero_dur,
            missing_prior_context=False,
            missing_post_context=False,
        )
        data_payload = chunk.SerializeToString()
        gcs_uri_attr = chunk.raw_audio_uri
    else:
        chunk = ContinuousAudio(
            feed_id=feed_id,
            session_id=str(uuid.uuid4()),
            gcs_uri=f"gs://{staging_bucket}/{audio_filename}",
            feed_name=feed_name,
            duration_ms=1000,
        )
        data_payload = chunk.SerializeToString()
        gcs_uri_attr = chunk.gcs_uri

    publisher = pubsub_v1.PublisherClient()

    logger.info(f"Publishing to {topic}...")
    future = publisher.publish(
        topic,
        data=data_payload,
        gcs_uri=gcs_uri_attr,
        timestamp_ms=str(int(time.time() * 1000)),
    )
    future.result()

    return verify_audio_segments_via_api(
        feed_id,
        lambda s: (
            any(ann["type"] == "TRANSCRIPT" for ann in s.get("annotations", []))
            and any(
                ann["type"] == "EVALUATION" for ann in s.get("annotations", [])
            )
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

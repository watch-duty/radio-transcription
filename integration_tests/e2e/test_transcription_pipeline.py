import logging
import time

import pytest
from google.cloud import pubsub_v1

from backend.pipeline.schema_types.evaluated_transcribed_audio_pb2 import (
    EvaluatedTranscribedAudio,
)
from backend.pipeline.schema_types.raw_audio_chunk_pb2 import AudioChunk

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = "local-project"
CONTINUOUS_TOPIC = "projects/local-project/topics/continuous-audio"
SEGMENTED_TOPIC = "projects/local-project/topics/segmented-audio"
RESULTS_TOPIC = "projects/local-project/topics/rules-evaluation-results-topic"


@pytest.fixture(scope="module")
def publisher() -> pubsub_v1.PublisherClient:
    return pubsub_v1.PublisherClient()


@pytest.fixture(scope="module")
def subscriber() -> pubsub_v1.SubscriberClient:
    return pubsub_v1.SubscriberClient()


def _publish_and_verify(
    publisher: pubsub_v1.PublisherClient,
    subscriber: pubsub_v1.SubscriberClient,
    topic: str,
    feed_id: str,
) -> bool:
    # Create a temporary subscription to the results topic to verify output
    sub_id = f"test-sub-{feed_id}-{int(time.time())}"
    sub_path = f"projects/{PROJECT_ID}/subscriptions/{sub_id}"

    subscriber.create_subscription(
        request={"name": sub_path, "topic": RESULTS_TOPIC}
    )

    try:
        # Construct AudioChunk message
        chunk = AudioChunk(
            feed_id=feed_id,
            session_id="session-123",
            gcs_uri="gs://test-bucket/test-audio.flac",  # Note: Beam will try to download this
            feed_name="Test Feed",
            duration_ms=1000,
            trace_id="test-trace-id",
        )

        logger.info(f"Publishing to {topic}...")
        publisher.publish(topic, chunk.SerializeToString())

        # Pull from results subscription
        logger.info(f"Waiting for message on {sub_path}...")
        # Wait up to 30 seconds for Beam to process and Evaluation to run
        for _ in range(30):
            response = subscriber.pull(
                request={"subscription": sub_path, "max_messages": 1},
                timeout=1,
            )
            if response.received_messages:
                msg = response.received_messages[0]
                logger.info(f"Received message: {msg.message.data}")

                # Verify it's the expected message type
                evaluated_audio = EvaluatedTranscribedAudio()
                evaluated_audio.ParseFromString(msg.message.data)

                assert evaluated_audio.feed_id == feed_id

                # Acknowledge message
                subscriber.acknowledge(
                    request={"subscription": sub_path, "ack_ids": [msg.ack_id]}
                )
                return True
            time.sleep(1)

        pytest.fail("Timed out waiting for message on results topic")

    finally:
        # Clean up subscription
        try:
            subscriber.delete_subscription(request={"subscription": sub_path})
        except Exception as e:
            logger.warning(f"Failed to delete subscription {sub_path}: {e}")


def test_continuous_pipeline_flow(
    publisher: pubsub_v1.PublisherClient,
    subscriber: pubsub_v1.SubscriberClient,
) -> None:
    """Tests that a message published to continuous topic reaches evaluation."""
    _publish_and_verify(
        publisher, subscriber, CONTINUOUS_TOPIC, "test-continuous-feed"
    )


def test_segmented_pipeline_flow(
    publisher: pubsub_v1.PublisherClient,
    subscriber: pubsub_v1.SubscriberClient,
) -> None:
    """Tests that a message published to segmented topic reaches evaluation."""
    _publish_and_verify(
        publisher, subscriber, SEGMENTED_TOPIC, "test-segmented-feed"
    )

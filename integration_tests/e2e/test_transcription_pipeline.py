import asyncio
import logging
import os

import asyncpg
import pytest
from google.cloud import pubsub_v1

from backend.pipeline.schema_types.raw_audio_chunk_pb2 import AudioChunk
from integration_tests.feed_utils import create_test_feed  # noqa: F401
from integration_tests.utils import assert_eventually

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


def _verify_transcript_in_db(feed_id: str) -> bool:
    """Polls the database until a transcript for the given feed_id appears."""
    _conn_kwargs = {
        "host": os.environ.get("ALLOYDB_HOST", "postgres"),
        "port": int(os.environ.get("ALLOYDB_PORT", "5432")),
        "user": os.environ.get("ALLOYDB_USER", "postgres"),
        "password": os.environ.get("ALLOYDB_PASSWORD", "postgres"),
        "database": os.environ.get("ALLOYDB_DB", "postgres"),
    }

    async def _check_db():
        conn = await asyncpg.connect(**_conn_kwargs)
        row = await conn.fetchrow(
            "SELECT * FROM transcripts WHERE feed_id = $1::uuid", feed_id
        )
        await conn.close()
        return row is not None

    def condition():
        return asyncio.run(_check_db())

    logger.info(f"Waiting for transcript in DB for feed {feed_id}...")

    assert_eventually(
        condition, timeout_sec=60, error_msg="Transcript not found in DB"
    )
    return True


def _publish_and_verify(
    publisher: pubsub_v1.PublisherClient,
    subscriber: pubsub_v1.SubscriberClient,
    topic: str,
    feed_id: str,
    feed_name: str,
) -> bool:
    # Construct AudioChunk message
    staging_bucket = os.environ.get("AUDIO_STAGING_BUCKET", "test-bucket")
    chunk = AudioChunk(
        feed_id=feed_id,
        session_id="session-123",
        gcs_uri=f"gs://{staging_bucket}/test_fire_audio.flac",
        feed_name=feed_name,
        duration_ms=1000,
    )

    logger.info(f"Publishing to {topic}...")
    logger.info(f"Publishing chunk with URI: {chunk.gcs_uri}")
    publisher.publish(topic, chunk.SerializeToString(), gcs_uri=chunk.gcs_uri)

    return _verify_transcript_in_db(feed_id)


def test_continuous_pipeline_flow(
    publisher: pubsub_v1.PublisherClient,
    subscriber: pubsub_v1.SubscriberClient,
    test_feed: tuple[str, str],
) -> None:
    """Tests that a message published to continuous topic reaches evaluation."""
    feed_id, feed_name = test_feed
    _publish_and_verify(
        publisher, subscriber, CONTINUOUS_TOPIC, feed_id, feed_name
    )


def test_segmented_pipeline_flow(
    publisher: pubsub_v1.PublisherClient,
    subscriber: pubsub_v1.SubscriberClient,
    test_feed: tuple[str, str],
) -> None:
    """Tests that a message published to segmented topic reaches evaluation."""
    feed_id, feed_name = test_feed
    _publish_and_verify(
        publisher, subscriber, SEGMENTED_TOPIC, feed_id, feed_name
    )

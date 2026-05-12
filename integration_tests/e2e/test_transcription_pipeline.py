import asyncio
import logging
import os
import uuid

import asyncpg
import pytest
from google.cloud import pubsub_v1

from backend.pipeline.schema_types.raw_audio_chunk_pb2 import AudioChunk
from integration_tests.feed_utils import create_test_feed  # noqa: F401
from integration_tests.utils import assert_eventually

logger = logging.getLogger(__name__)

PROJECT_ID = os.environ["PROJECT_ID"]
CONTINUOUS_TOPIC = os.environ["CONTINUOUS_TOPIC"]
SEGMENTED_TOPIC = os.environ["SEGMENTED_TOPIC"]
RESULTS_TOPIC = os.environ["RULES_EVALUATION_RESULTS_TOPIC"]


@pytest.fixture(scope="module")
def publisher() -> pubsub_v1.PublisherClient:
    return pubsub_v1.PublisherClient()


@pytest.fixture(scope="module")
def subscriber() -> pubsub_v1.SubscriberClient:
    return pubsub_v1.SubscriberClient()


def _verify_transcript_in_db(feed_id: str) -> bool:
    """Polls the database until a transcript for the given feed_id appears."""
    _conn_kwargs = {
        "host": os.environ["ALLOYDB_HOST"],
        "port": int(os.environ["ALLOYDB_PORT"]),
        "user": os.environ["ALLOYDB_USER"],
        "password": os.environ["ALLOYDB_PASSWORD"],
        "database": os.environ["ALLOYDB_DB"],
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
        condition, timeout_sec=120, error_msg="Transcript not found in DB"
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
    staging_bucket = os.environ["AUDIO_STAGING_BUCKET"]
    chunk = AudioChunk(
        feed_id=feed_id,
        session_id=str(uuid.uuid4()),
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

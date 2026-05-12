import asyncio
import base64
import logging
import os
import time
import uuid

import asyncpg
import pytest
import requests
from google.cloud import pubsub_v1

from backend.pipeline.schema_types.raw_audio_chunk_pb2 import AudioChunk
from integration_tests.feed_utils import create_test_feed  # noqa: F401
from integration_tests.utils import assert_eventually

logger = logging.getLogger(__name__)


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
        condition, timeout_sec=60, error_msg="Transcript not found in DB"
    )
    return True


def _publish_and_verify(
    publisher: pubsub_v1.PublisherClient,
    subscriber: pubsub_v1.SubscriberClient,
    topic: str,
    feed_id: str,
    feed_name: str,
    audio_filename: str,
) -> bool:
    # Construct AudioChunk message
    staging_bucket = os.environ["AUDIO_STAGING_BUCKET"]
    chunk = AudioChunk(
        feed_id=feed_id,
        session_id=str(uuid.uuid4()),
        gcs_uri=f"gs://{staging_bucket}/{audio_filename}",
        feed_name=feed_name,
        duration_ms=1000,
    )

    payload = {
        "messages": [
            {
                "data": base64.b64encode(chunk.SerializeToString()).decode(
                    "utf-8"
                ),
                "attributes": {
                    "gcs_uri": chunk.gcs_uri,
                    "timestamp_ms": str(int(time.time() * 1000)),
                },
            }
        ]
    }

    pubsub_emulator_host = os.environ["PUBSUB_EMULATOR_HOST"]
    url = f"http://{pubsub_emulator_host}/v1/{topic}:publish"

    logger.info(f"Publishing to {url}...")
    response = requests.post(url, json=payload, timeout=10)
    response.raise_for_status()

    return _verify_transcript_in_db(feed_id)


def test_continuous_pipeline_flow(
    publisher: pubsub_v1.PublisherClient,
    subscriber: pubsub_v1.SubscriberClient,
    test_feed: tuple[str, str],
) -> None:
    """Tests that a message published to continuous topic reaches evaluation."""
    continuous_topic = os.environ["CONTINUOUS_TOPIC"]
    feed_id, feed_name = test_feed
    _publish_and_verify(
        publisher,
        subscriber,
        continuous_topic,
        feed_id,
        feed_name,
        audio_filename="test_bcfy.flac",
    )


def test_segmented_pipeline_flow(
    publisher: pubsub_v1.PublisherClient,
    subscriber: pubsub_v1.SubscriberClient,
    test_feed: tuple[str, str],
) -> None:
    """Tests that a message published to segmented topic reaches evaluation."""
    segmented_topic = os.environ["SEGMENTED_TOPIC"]
    feed_id, feed_name = test_feed
    _publish_and_verify(
        publisher,
        subscriber,
        segmented_topic,
        feed_id,
        feed_name,
        audio_filename="test_dispatch_amador.flac",
    )

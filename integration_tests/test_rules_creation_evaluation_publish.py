"""Publishes test messages and verifies the end-to-end Rules Evaluation flow."""

import asyncio
import base64
import os
import time
import uuid
from collections.abc import Generator

import asyncpg
import pytest
import requests

from backend.pipeline.schema_types.transcribed_audio_pb2 import TranscribedAudio
from integration_tests.utils import assert_eventually

# Constants from environment with sensible defaults for local development
PUBSUB_EMULATOR_HOST = os.environ.get("PUBSUB_EMULATOR_HOST", "localhost:8085")
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "local-project")
RULES_API_HOST = os.environ.get("RULES_API_HOST", "localhost:8086")
TRANSCRIPTION_TOPIC = os.environ.get(
    "TRANSCRIPTION_TOPIC", "transcription-text-topic"
)
MOCK_SERVER_HOST = os.environ.get("MOCK_SERVER_HOST", "localhost:8082")
TRANSCRIPTS_API_HOST = os.environ.get("TRANSCRIPTS_API_HOST", "localhost:8087")


def create_test_rule(test_keyword: str) -> None:
    """Creates a temporary rule for testing with a specific keyword."""
    rule_payload = {
        "rule_name": f"Integration Test Rule - {test_keyword}",
        "description": f"Triggers on {test_keyword} mentions.",
        "is_active": True,
        "scope": {"level": "GLOBAL"},
        "conditions": {
            "evaluation_type": "KEYWORD_MATCH",
            "operator": "ANY",
            "keywords": [test_keyword],
            "case_sensitive": False,
        },
    }

    url = f"http://{RULES_API_HOST}/v1/rules"
    response = requests.post(url, json=rule_payload, timeout=10)
    response.raise_for_status()

    rule_id = response.json().get("rule_id", "")
    assert rule_id != "", "Rule ID not returned by API"


@pytest.fixture(name="test_feed")
def create_test_feed() -> Generator[str]:
    """Fixture to create a temporary feed for testing."""
    _conn_kwargs = {
        "host": os.environ.get("ALLOYDB_HOST", "postgres"),
        "port": int(os.environ.get("ALLOYDB_PORT", "5432")),
        "user": os.environ.get("ALLOYDB_USER", "postgres"),
        "password": os.environ.get("ALLOYDB_PASSWORD", "postgres"),
        "database": os.environ.get("ALLOYDB_DB", "postgres"),
    }

    async def _setup():
        conn = await asyncpg.connect(**_conn_kwargs)
        feed_name = f"integration-test-feed-{uuid.uuid4()}"
        feed_id = await conn.fetchval(
            "INSERT INTO feeds (name, source_type) VALUES ($1, $2) RETURNING id",
            feed_name,
            "bcfy_feeds",
        )
        await conn.close()
        return str(feed_id)

    feed_id = asyncio.run(_setup())
    try:
        yield feed_id
    finally:

        async def _cleanup():
            conn = await asyncpg.connect(**_conn_kwargs)
            await conn.execute(
                "DELETE FROM transcripts WHERE feed_id = $1::uuid", feed_id
            )
            await conn.execute("DELETE FROM feeds WHERE id = $1::uuid", feed_id)
            await conn.close()

        asyncio.run(_cleanup())


def publish_test_message(
    transmission_id: str, transcript: str, feed_id: str
) -> None:
    """Publishes a test message to the transcription topic."""
    message = TranscribedAudio(
        transmission_id=transmission_id,
        transcript=transcript,
        source_audio_uris=["chunk1", "chunk2"],
        feed_id=feed_id,
        start_timestamp={"seconds": int(time.time()), "nanos": 0},
        end_timestamp={"seconds": int(time.time()) + 10, "nanos": 0},
    )

    payload = {
        "messages": [
            {
                "data": base64.b64encode(message.SerializeToString()).decode(
                    "utf-8"
                )
            }
        ]
    }

    url = f"http://{PUBSUB_EMULATOR_HOST}/v1/{TRANSCRIPTION_TOPIC}:publish"

    response = requests.post(url, json=payload, timeout=10)
    response.raise_for_status()


def test_rules_creation_evaluation_publish(test_feed: str) -> None:
    test_uuid = str(uuid.uuid4())[:8]
    unique_keyword = f"evacuation-{test_uuid}"
    unique_trans_id = str(uuid.uuid4())
    unique_transcript = f"Attention: {unique_keyword} is required for Sector 7."

    create_test_rule(unique_keyword)
    publish_test_message(unique_trans_id, unique_transcript, test_feed)

    def transcript_and_notification_received() -> bool:
        try:
            # 1. Check Notification (via Mock Server)
            url = f"http://{MOCK_SERVER_HOST}"
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            data = response.json()

            notification_found = False
            if data:
                for item in data:
                    if unique_trans_id in str(item):
                        notification_found = True

            # 2. Check Transcript (via Real Transcripts API)
            transcript_found = False
            trans_url = f"http://{TRANSCRIPTS_API_HOST}/v1/transcripts/{unique_trans_id}"
            trans_response = requests.get(trans_url, timeout=5)
            if trans_response.status_code == 200:
                transcript_found = True
        except requests.RequestException:
            pass
        else:
            return notification_found and transcript_found
        return False

    assert_eventually(
        transcript_and_notification_received,
        timeout_sec=30.0,
        error_msg=f"Did not receive expected notification or transcript matching {unique_trans_id}.",
    )

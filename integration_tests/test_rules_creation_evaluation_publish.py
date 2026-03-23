"""Publishes test messages and verifies the end-to-end Rules Evaluation flow."""

import base64
import os
import time
import uuid

import requests

from backend.pipeline.schema_types.transcribed_audio_pb2 import TranscribedAudio
from integration_tests.utils import assert_eventually

# Constants from environment with sensible defaults for local development
PUBSUB_EMULATOR_HOST = os.environ.get("PUBSUB_EMULATOR_HOST", "localhost:8085")
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "local-project")
RULES_API_HOST = os.environ.get("RULES_API_HOST", "localhost:8086")
TRANSCRIPTION_TOPIC = os.environ.get("TRANSCRIPTION_TOPIC", "transcription-text-topic")
MOCK_SERVER_HOST = os.environ.get("MOCK_SERVER_HOST", "localhost:8082")


def wait_for_services() -> None:
    """Wait for all required services to be up."""
    services = [
        ("Pub/Sub Emulator", f"http://{PUBSUB_EMULATOR_HOST}/"),
        ("Rules Management", f"http://{RULES_API_HOST}/v1/rules"),
    ]
    for name, url in services:
        logger.info(f"Waiting for {name} at {url}...")
        for i in range(60):
            try:
                response = requests.get(url, timeout=2)
                if response.status_code < 500:
                    logger.info(f"{name} is ready.")
                    break
            except requests.exceptions.RequestException:
                pass
            time.sleep(1)
        else:
            logger.error(f"Timed out waiting for {name}.")
            sys.exit(1)

    topic_url = (
        f"http://{PUBSUB_EMULATOR_HOST}/v1/projects/{PROJECT_ID}/topics/{TRANSCRIPTION_TOPIC}"
    )

    logger.info(f"Waiting for topic {TRANSCRIPTION_TOPIC} at {topic_url}...")
    for i in range(30):
        try:
            response = requests.get(topic_url, timeout=2)
            if response.status_code == 200:
                logger.info(f"Topic {TRANSCRIPTION_TOPIC} is ready.")
                break
        except requests.exceptions.RequestException:
            pass
        time.sleep(1)
    else:
        logger.error(f"Timed out waiting for topic {TRANSCRIPTION_TOPIC}.")
        sys.exit(1)


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


def publish_test_message(transmission_id: str, transcript: str) -> None:
    """Publishes a test message to the transcription topic."""
    message = TranscribedAudio(
        transmission_id=transmission_id,
        transcript=transcript,
        source_audio_uris=["chunk1", "chunk2"],
        feed_id="test-feed",
        start_timestamp={"seconds": int(time.time()), "nanos": 0},
        end_timestamp={"seconds": int(time.time()) + 10, "nanos": 0},
    )

    payload = {
        "messages": [
            {"data": base64.b64encode(message.SerializeToString()).decode("utf-8")}
        ]
    }

    url = (
        f"http://{PUBSUB_EMULATOR_HOST}/v1/projects/"
        f"{PROJECT_ID}/topics/{TRANSCRIPTION_TOPIC}:publish"
    )

    response = requests.post(url, json=payload, timeout=10)
    response.raise_for_status()


def test_rules_creation_evaluation_publish() -> None:
    test_uuid = str(uuid.uuid4())[:8]
    unique_keyword = f"evacuation-{test_uuid}"
    unique_trans_id = f"trans-{test_uuid}"
    unique_transcript = f"Attention: {unique_keyword} is required for Sector 7."

    create_test_rule(unique_keyword)
    publish_test_message(unique_trans_id, unique_transcript)

    def notification_received() -> bool:
        try:
            url = f"http://{MOCK_SERVER_HOST}"
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            data = response.json()

            if data:
                for item in data:
                    if unique_trans_id in str(item):
                        return True
        except requests.RequestException:
            pass
        return False

    assert_eventually(
        notification_received,
        timeout_sec=30.0,
        error_msg=f"Did not receive expected notification matching {unique_trans_id}.",
    )

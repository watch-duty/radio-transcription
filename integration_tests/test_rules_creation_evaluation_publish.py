"""Publishes test messages and verifies the end-to-end Rules Evaluation flow."""

import base64
import logging
import os
import sys
import time
import uuid

import requests

from backend.pipeline.schema_types.transcribed_audio_pb2 import TranscribedAudio

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants from environment with sensible defaults for local development
PUBSUB_EMULATOR_HOST = os.environ.get("PUBSUB_EMULATOR_HOST", "localhost:8085")
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "local-project")
RULES_API_URL = os.environ.get("RULES_API_URL", "http://localhost:8086")
TRANSCRIPTION_TOPIC = os.environ.get("TRANSCRIPTION_TOPIC", "transcription-text-topic")
MOCK_SERVER_URL = os.environ.get(
    "NOTIFICATION_ENDPOINT", "http://localhost:8082/post"
).replace("/post", "")


def wait_for_services() -> None:
    """Wait for all required services to be up."""
    services = [
        ("Pub/Sub Emulator", f"http://{PUBSUB_EMULATOR_HOST}/"),
        ("Rules Management", f"{RULES_API_URL}/v1/rules"),
        ("Mock Server", MOCK_SERVER_URL),
    ]
    for name, url in services:
        logger.info(f"Waiting for {name} at {url}...")
        for i in range(30):
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

    topic_url = f"http://{PUBSUB_EMULATOR_HOST}/v1/projects/{PROJECT_ID}/topics/{TRANSCRIPTION_TOPIC}"

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


def create_test_rule(test_keyword: str) -> str | None:
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
    logger.info("Step 1: Creating a test rule with keyword: %s", test_keyword)
    try:
        response = requests.post(f"{RULES_API_URL}/v1/rules", json=rule_payload)
        response.raise_for_status()
    except Exception as e:
        logger.exception("Failed to create rule: %s", e)
        return None
    else:
        rule_id = response.json().get("rule_id", "unknown")
        logger.info(f"Rule created successfully (ID: {rule_id})")
        return rule_id


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

    logger.info("Step 2: Publishing message to Pub/Sub...")
    logger.info("Transmission ID: %s", transmission_id)
    logger.info("Transcript: %s", transcript)

    response = requests.post(url, json=payload)
    if response.status_code == 200:
        logger.info("Message published successfully.")
    else:
        logger.error(
            "Failed to publish message. Status: %s, Response: %s",
            response.status_code,
            response.text,
        )
        sys.exit(1)


def verify_notification(expected_transmission_id: str) -> bool:
    """Polls the mock server for a notification matching the transmission ID."""
    logger.info("Step 3: Verifying notification on mock server...")
    max_retries = 15
    for i in range(max_retries):
        logger.info(f"Checking mock server (attempt {i + 1}/{max_retries})...")
        try:
            response = requests.get(MOCK_SERVER_URL)
            data = response.json()

            if data:
                for item in data:
                    if expected_transmission_id in str(item):
                        logger.info("SUCCESS: Notification received and verified!")
                        return True
                logger.info("Received notification, but it doesn't match our test ID.")
        except Exception as e:
            logger.debug(f"Error polling mock server: {e}")

        time.sleep(2)

    logger.error(
        "FAILED: Did not receive expected notification matching %s.",
        expected_transmission_id,
    )
    return False


if __name__ == "__main__":
    logger.info("Starting Rules Evaluation Integration Test")

    # Generate unique values for each run to ensure test isolation
    test_uuid = str(uuid.uuid4())[:8]
    unique_keyword = f"evacuation-{test_uuid}"
    unique_trans_id = f"trans-{test_uuid}"
    unique_transcript = f"Attention: {unique_keyword} is required for Sector 7."

    # 0. Wait for environment
    wait_for_services()

    # 1. Setup - Create rule
    create_test_rule(unique_keyword)

    # 2. Trigger - Publish message
    publish_test_message(unique_trans_id, unique_transcript)

    # 3. Verify - Check results
    if verify_notification(unique_trans_id):
        logger.info("Integration test PASSED.")
    else:
        logger.error("Integration test FAILED.")
        sys.exit(1)

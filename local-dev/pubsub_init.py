"""Initializes the Pub/Sub emulator with required topics and subscriptions."""

import logging
import os
import sys
import time

import requests


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PUBSUB_EMULATOR_HOST = os.environ["PUBSUB_EMULATOR_HOST"]
PROJECT_ID = os.environ["GOOGLE_CLOUD_PROJECT"]
PUBSUB_ENDPOINT = f"http://{PUBSUB_EMULATOR_HOST}/v1/projects/{PROJECT_ID}"


def wait_for_emulator() -> None:
    """Waits for the Pub/Sub emulator to become ready."""
    logger.info("Waiting for Pub/Sub emulator...")
    for _ in range(30):
        try:
            response = requests.get(f"http://{PUBSUB_EMULATOR_HOST}/", timeout=10)
            if response.status_code == 200:
                logger.info("Pub/Sub emulator is ready.")
                return
        except requests.exceptions.RequestException:
            pass
        time.sleep(1)
    logger.error("Timed out waiting for Pub/Sub emulator.")
    sys.exit(1)


def create_topic(topic_id) -> None:
    """Creates a topic in the Pub/Sub emulator.

    Ignores pre-existing topics.

    Args:
        topic_id: The ID of the topic to create.
    """
    url = f"{PUBSUB_ENDPOINT}/topics/{topic_id}"
    response = requests.put(url, json={}, timeout=10)
    # 200 Created, 409 Already exists
    if response.status_code in (200, 409):
        logger.info("Topic '%s' ready.", topic_id)
    else:
        logger.error(
            "Failed to create topic '%s': %s", topic_id, response.text
        )


def create_push_subscription(subscription_id, topic_id, push_endpoint) -> None:
    """Creates a push subscription in the Pub/Sub emulator.

    Ignores pre-existing subscriptions.

    Args:
        subscription_id: The ID of the subscription to create.
        topic_id: The ID of the topic to subscribe to.
        push_endpoint: The HTTP endpoint to push messages to.
    """
    url = f"{PUBSUB_ENDPOINT}/subscriptions/{subscription_id}"
    payload = {
        "topic": f"projects/{PROJECT_ID}/topics/{topic_id}",
        "pushConfig": {
            "pushEndpoint": push_endpoint
        }
    }
    response = requests.put(url, json=payload, timeout=10)
    if response.status_code in (200, 409):
        logger.info(
            "Subscription '%s' ready, pushing to %s.",
            subscription_id,
            push_endpoint,
        )
    else:
        logger.error(
            "Failed to create subscription '%s': %s",
            subscription_id,
            response.text,
        )


if __name__ == "__main__":
    wait_for_emulator()

    # Set up Pub/Sub between Capturer and Normalizer in Audio Ingestion Service
    STAGING_TOPIC = os.environ["STAGING_TOPIC"]
    create_topic(STAGING_TOPIC)
    create_push_subscription(
        "normalizer-sub",
        STAGING_TOPIC,
        f"http://{os.environ['NORMALIZER_SERVICE_HOST']}/",
    )

    # Pub/Sub between Audio Ingestion and Transcription Services
    CANONICAL_TOPIC = os.environ["CANONICAL_TOPIC"]
    create_topic(CANONICAL_TOPIC)
    create_push_subscription(
        "transcription-sub",
        CANONICAL_TOPIC,
        f"http://{os.environ['TRANSCRIPTION_SERVICE_HOST']}/",
    )

    # Pub/Sub between Transcription and Rules Evaluation Services
    TRANSCRIPTION_TOPIC = os.environ["TRANSCRIPTION_TOPIC"]
    create_topic(TRANSCRIPTION_TOPIC)
    create_push_subscription(
        "rules-evaluation-sub",
        TRANSCRIPTION_TOPIC,
        f"http://{os.environ['RULES_EVALUATION_SERVICE_HOST']}/",
    )

    # Pub/Sub between Rules Evaluation and Notification Services
    RULES_EVALUATION_RESULTS_TOPIC = os.environ[
        "RULES_EVALUATION_RESULTS_TOPIC"
    ]
    create_topic(RULES_EVALUATION_RESULTS_TOPIC)
    create_push_subscription(
        "notification-sub",
        RULES_EVALUATION_RESULTS_TOPIC,
        f"http://{os.environ['NOTIFICATION_SERVICE_HOST']}/",
    )

    logger.info("Pub/Sub initialization complete.")

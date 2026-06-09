"""Initializes the Pub/Sub emulator with required topics and subscriptions."""

import logging
import os
import sys
import time

from google.cloud import pubsub_v1

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


PROJECT_ID = os.environ["GOOGLE_CLOUD_PROJECT"]


def wait_for_emulator() -> None:
    """Waits for the Pub/Sub emulator to become ready."""
    logger.info("Waiting for Pub/Sub emulator...")

    client = pubsub_v1.PublisherClient()
    project_path = f"projects/{PROJECT_ID}"

    for _ in range(30):
        try:
            client.list_topics(project=project_path)
        except Exception as e:
            # Failures expected if Pub/Sub emulator isn't ready yet.
            logger.info(f"Pub/Sub emulator not ready yet: {e}")
        else:
            logger.info("Pub/Sub emulator is ready.")
            return
        time.sleep(1)

    logger.error("Timed out waiting for Pub/Sub emulator.")
    sys.exit(1)


def create_topic(publisher: pubsub_v1.PublisherClient, topic_path: str) -> None:
    try:
        publisher.create_topic(name=topic_path)
        logger.info(f"Topic '{topic_path}' created.")
    except Exception:
        logger.exception(f"Failed to create topic '{topic_path}'")


def create_pull_subscription(
    subscriber: pubsub_v1.SubscriberClient,
    subscription_id: str,
    topic_path: str,
) -> None:
    subscription_path = subscriber.subscription_path(
        PROJECT_ID, subscription_id
    )
    try:
        subscriber.create_subscription(name=subscription_path, topic=topic_path)
        logger.info(f"Pull subscription '{subscription_id}' ready.")
    except Exception:
        logger.exception(
            f"Failed to create pull subscription '{subscription_id}'"
        )


def create_push_subscription(
    subscriber: pubsub_v1.SubscriberClient,
    subscription_id: str,
    topic_path: str,
    push_endpoint: str,
) -> None:
    subscription_path = subscriber.subscription_path(
        PROJECT_ID, subscription_id
    )
    push_config = pubsub_v1.types.PushConfig(push_endpoint=push_endpoint)  # ty: ignore[unresolved-attribute]
    try:
        subscriber.create_subscription(
            name=subscription_path, topic=topic_path, push_config=push_config
        )
        logger.info(
            f"Push subscription '{subscription_id}' ready, pushing to {push_endpoint}."
        )
    except Exception:
        logger.exception(
            f"Failed to create push subscription '{subscription_id}'"
        )


if __name__ == "__main__":
    wait_for_emulator()

    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    # Pub/Sub for Continuous Audio
    continuous_topic = os.environ["CONTINUOUS_TOPIC"]
    create_topic(publisher, continuous_topic)
    create_pull_subscription(
        subscriber,
        os.environ["CONTINUOUS_AUDIO_SUBSCRIPTION"],
        continuous_topic,
    )

    # Pub/Sub for Segmented Audio
    segmented_topic = os.environ["SEGMENTED_TOPIC"]
    create_topic(publisher, segmented_topic)
    create_push_subscription(
        subscriber,
        "normalization-sub",
        segmented_topic,
        f"http://{os.environ['NORMALIZER_SERVICE_HOST']}/",
    )

    # Pub/Sub between Normalization and Transcription Services
    normalized_audio_topic = "projects/local-project/topics/normalized-audio"
    create_topic(publisher, normalized_audio_topic)
    create_push_subscription(
        subscriber,
        "transcription-sub",
        normalized_audio_topic,
        f"http://{os.environ['TRANSCRIPTION_SERVICE_HOST']}/",
    )

    # Pub/Sub between Transcription and Rules Evaluation Services
    transcription_topic = os.environ["TRANSCRIPTION_TOPIC"]
    create_topic(publisher, transcription_topic)
    create_push_subscription(
        subscriber,
        "rules-evaluation-sub",
        transcription_topic,
        f"http://{os.environ['RULES_EVALUATION_SERVICE_HOST']}/",
    )

    # DLQ for Transcription Pipeline failures
    create_topic(publisher, os.environ["TRANSCRIPTION_DLQ_TOPIC"])

    # Pub/Sub between Rules Evaluation and Notification Services
    rules_evaluation_results_topic = os.environ[
        "RULES_EVALUATION_RESULTS_TOPIC"
    ]
    create_topic(publisher, rules_evaluation_results_topic)
    create_push_subscription(
        subscriber,
        "notification-sub",
        rules_evaluation_results_topic,
        f"http://{os.environ['NOTIFICATION_SERVICE_HOST']}/",
    )

    logger.info("Pub/Sub initialization complete.")

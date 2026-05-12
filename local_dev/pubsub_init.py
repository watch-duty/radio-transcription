import logging
import os
import sys
import time

from google.api_core.exceptions import AlreadyExists, GoogleAPICallError
from google.cloud import pubsub_v1

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def wait_for_emulator():
    """Waits for the Pub/Sub emulator to become ready."""
    logger.info("Waiting for Pub/Sub emulator...")
    emulator_host = os.environ["PUBSUB_EMULATOR_HOST"]

    # We can use a simple socket check or try to list topics
    # Let's use the client library to list topics, which will fail until ready
    client = pubsub_v1.PublisherClient()
    project_path = f"projects/{os.environ['GOOGLE_CLOUD_PROJECT']}"

    for _ in range(30):
        try:
            client.list_topics(project=project_path)
            logger.info("Pub/Sub emulator is ready.")
            return
        except GoogleAPICallError:
            # Expected if service is not ready yet
            pass
        except Exception:
            # Other errors might happen if connection fails
            pass
        time.sleep(1)

    logger.error("Timed out waiting for Pub/Sub emulator.")
    sys.exit(1)


def create_topic(publisher, topic_path):
    try:
        publisher.create_topic(name=topic_path)
        logger.info(f"Topic '{topic_path}' created.")
    except AlreadyExists:
        logger.info(f"Topic '{topic_path}' already exists.")
    except Exception as e:
        logger.error(f"Failed to create topic '{topic_path}': {e}")


def create_pull_subscription(subscriber, subscription_id, topic_path):
    project_id = os.environ["GOOGLE_CLOUD_PROJECT"]
    subscription_path = subscriber.subscription_path(
        project_id, subscription_id
    )
    try:
        subscriber.create_subscription(name=subscription_path, topic=topic_path)
        logger.info(f"Pull subscription '{subscription_id}' ready.")
    except AlreadyExists:
        logger.info(f"Pull subscription '{subscription_id}' already exists.")
    except Exception as e:
        logger.error(
            f"Failed to create pull subscription '{subscription_id}': {e}"
        )


def create_push_subscription(
    subscriber, subscription_id, topic_path, push_endpoint
):
    project_id = os.environ["GOOGLE_CLOUD_PROJECT"]
    subscription_path = subscriber.subscription_path(
        project_id, subscription_id
    )
    push_config = pubsub_v1.types.PushConfig(push_endpoint=push_endpoint)  # ty: ignore[unresolved-attribute]
    try:
        subscriber.create_subscription(
            name=subscription_path, topic=topic_path, push_config=push_config
        )
        logger.info(
            f"Push subscription '{subscription_id}' ready, pushing to {push_endpoint}."
        )
    except AlreadyExists:
        logger.info(f"Push subscription '{subscription_id}' already exists.")
    except Exception as e:
        logger.error(
            f"Failed to create push subscription '{subscription_id}': {e}"
        )


if __name__ == "__main__":
    wait_for_emulator()

    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    # Pub/Sub for Continuous Audio
    CONTINUOUS_TOPIC = os.environ["CONTINUOUS_TOPIC"]
    create_topic(publisher, CONTINUOUS_TOPIC)
    create_pull_subscription(
        subscriber,
        os.environ["CONTINUOUS_AUDIO_SUBSCRIPTION"],
        CONTINUOUS_TOPIC,
    )

    # Pub/Sub for Segmented Audio
    SEGMENTED_TOPIC = os.environ["SEGMENTED_TOPIC"]
    create_topic(publisher, SEGMENTED_TOPIC)
    create_pull_subscription(
        subscriber, os.environ["SEGMENTED_AUDIO_SUBSCRIPTION"], SEGMENTED_TOPIC
    )

    # Pub/Sub between Transcription and Rules Evaluation Services
    TRANSCRIPTION_TOPIC = os.environ["TRANSCRIPTION_TOPIC"]
    create_topic(publisher, TRANSCRIPTION_TOPIC)
    create_push_subscription(
        subscriber,
        "rules-evaluation-sub",
        TRANSCRIPTION_TOPIC,
        f"http://{os.environ['RULES_EVALUATION_SERVICE_HOST']}/",
    )

    # DLQ for Transcription Pipeline failures
    create_topic(publisher, os.environ["TRANSCRIPTION_DLQ_TOPIC"])

    # Pub/Sub between Rules Evaluation and Notification Services
    RULES_EVALUATION_RESULTS_TOPIC = os.environ[
        "RULES_EVALUATION_RESULTS_TOPIC"
    ]
    create_topic(publisher, RULES_EVALUATION_RESULTS_TOPIC)
    create_push_subscription(
        subscriber,
        "notification-sub",
        RULES_EVALUATION_RESULTS_TOPIC,
        f"http://{os.environ['NOTIFICATION_SERVICE_HOST']}/",
    )

    logger.info("Pub/Sub initialization complete.")

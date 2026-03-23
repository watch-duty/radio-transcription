import logging
import os

import functions_framework
import google.cloud.logging
from cloudevents.http import event as cloudevent
from google.cloud import pubsub_v1

from backend.pipeline.evaluation import service
from backend.pipeline.evaluation.rules_evaluation import evaluator

# 1. Setup Logging
logger = logging.getLogger(__name__)

if not os.environ.get("LOCAL_DEV"):
    client = google.cloud.logging.Client()
    client.setup_logging()
else:
    # If local, just print normally to the Docker console
    logging.basicConfig(level=logging.INFO)
    logger.info("Running in LOCAL_DEV mode. Logs will print here.")

# 2. Global Initialization (for performance on warm starts)
publisher = pubsub_v1.PublisherClient()
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
OUTPUT_TOPIC_ID = os.environ.get("RULES_EVALUATION_RESULTS_TOPIC")

# 3. Initialize Evaluator
RULES_API_URL = os.environ.get("RULES_API_URL")
if RULES_API_URL:
    logger.info("Using RemoteTextEvaluator with API: %s", RULES_API_URL)
    text_evaluator = evaluator.RemoteTextEvaluator(api_url=RULES_API_URL)
else:
    logger.info("Using StaticTextEvaluator (no RULES_API_URL set)")
    text_evaluator = evaluator.StaticTextEvaluator()

evaluation_service = service.EvaluationService(
    publisher=publisher,
    project_id=PROJECT_ID,
    output_topic_id=OUTPUT_TOPIC_ID,
    text_evaluator=text_evaluator,
)


@functions_framework.cloud_event
def evaluate_transcribed_audio_segment(
    cloud_event: cloudevent.CloudEvent,
) -> None:
    """
    Triggered from a message on a Cloud Pub/Sub topic.

    Args:
        cloud_event: The CloudEvent triggered by Pub/Sub.
    """
    evaluation_service.handle_event(cloud_event)

import logging
import os

import functions_framework
from cloudevents.http import event as cloudevent
from backend.pipeline.common.clients import pubsub_client
from backend.pipeline.common.logging import setup_logging
from backend.pipeline.evaluation import service
from backend.pipeline.evaluation.rules_evaluation import evaluator

# 1. Setup Logging
setup_logging()
logger = logging.getLogger(__name__)

# 2. Global Initialization (for performance on warm starts)
pubsub_client_instance = pubsub_client.PubSubClient()
publisher = pubsub_client_instance.get_publisher()
OUTPUT_TOPIC_PATH = os.environ.get("RULES_EVALUATION_RESULTS_TOPIC")

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
    output_topic_path=OUTPUT_TOPIC_PATH,
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

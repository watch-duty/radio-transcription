import logging
import os

import functions_framework
from cloudevents.http import event as cloudevent

from backend.pipeline.common.clients import pubsub_client
from backend.pipeline.common.clients.transcripts_client import TranscriptsClient
from backend.pipeline.common.logging import setup_logging
from backend.pipeline.evaluation import service
from backend.pipeline.evaluation.processor import EvaluationEventProcessor
from backend.pipeline.evaluation.rules_evaluation import evaluator

# 1. Setup Logging
setup_logging()
logger = logging.getLogger(__name__)

# 2. Global Initialization (for performance on warm starts)
pubsub_client_instance = pubsub_client.PubSubClient()
OUTPUT_TOPIC_PATH = os.environ.get("RULES_EVALUATION_RESULTS_TOPIC")
if OUTPUT_TOPIC_PATH is None:
    msg = "RULES_EVALUATION_RESULTS_TOPIC environment variable is not set."
    raise ValueError(msg)
TRANSCRIPTS_API_URL = os.environ.get("TRANSCRIPTS_API_URL")
if TRANSCRIPTS_API_URL is None:
    msg = "TRANSCRIPTS_API_URL environment variable is not set."
    raise ValueError(msg)
transcripts_client = TranscriptsClient(api_url=TRANSCRIPTS_API_URL)

# 3. Initialize Evaluator
RULES_API_URL = os.environ.get("RULES_API_URL")
if RULES_API_URL:
    logger.info("Using RemoteTextEvaluator with API: %s", RULES_API_URL)
    text_evaluator = evaluator.RemoteTextEvaluator(api_url=RULES_API_URL)
else:
    logger.info("Using StaticTextEvaluator (no RULES_API_URL set)")
    text_evaluator = evaluator.StaticTextEvaluator()

evaluation_service = service.EvaluationService(
    text_evaluator=text_evaluator,
)

processor = EvaluationEventProcessor(
    evaluation_service=evaluation_service,
    transcripts_client=transcripts_client,
    publisher=pubsub_client_instance,
    output_topic_path=OUTPUT_TOPIC_PATH,
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
    processor.process_event(cloud_event)

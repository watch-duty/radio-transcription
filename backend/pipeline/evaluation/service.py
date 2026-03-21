import base64
import logging

from cloudevents.http import event as cloudevent
from google.cloud import pubsub_v1

from backend.pipeline.evaluation.rules_evaluation import evaluator
from backend.pipeline.schema_types import (
    evaluated_transcribed_audio_pb2 as evaluated_pb2,
)
from backend.pipeline.schema_types import transcribed_audio_pb2 as transcribed_pb2

logger = logging.getLogger(__name__)


class EvaluationService:
    """
    Business logic for evaluating transcribed audio segments against rules.

    Attributes:
        publisher: Google Cloud Pub/Sub publisher client.
        project_id: The GCP project ID.
        output_topic_id: ID of the topic to publish results to.
        output_topic_path: Full path to the output topic.
        text_evaluator: The evaluator instance used to check transcripts.
    """

    def __init__(
        self,
        publisher: pubsub_v1.PublisherClient,
        project_id: str | None,
        output_topic_id: str | None,
        text_evaluator: evaluator.BaseTextEvaluator,
    ) -> None:
        """
        Initializes the EvaluationService.

        Args:
            publisher: Instance of pubsub_v1.PublisherClient.
            project_id: GCP Project ID for the output topic.
            output_topic_id: Topic ID to publish evaluation results.
            text_evaluator: An instance of a text evaluator.
        """
        self.publisher = publisher
        self.project_id = project_id
        self.output_topic_id = output_topic_id
        self.text_evaluator = text_evaluator
        if project_id and output_topic_id:
            self.output_topic_path = publisher.topic_path(project_id, output_topic_id)
        else:
            logger.warning("OUTPUT_TOPIC or PROJECT_ID env var not set.")
            self.output_topic_path = None

    def handle_event(self, cloud_event: cloudevent.CloudEvent) -> None:
        """
        Coordinates parsing, evaluation, and publishing of the result.

        Args:
            cloud_event: The CloudEvent triggered by Pub/Sub.
        """
        try:
            # 1. Decode the Incoming Message
            new_audio = self._parse_cloud_event(cloud_event)
            if new_audio is None:
                logger.warning("Transcribed audio could not be parsed. Skipping.")
                return

            transmission_id = new_audio.transmission_id
            logger.info("Processing transmission ID: %s", transmission_id)

            if not new_audio.transcript.strip():
                logger.info(
                    "No transcript for ID: %s. Skipping evaluation.",
                    transmission_id,
                )
                return

            # 2. Call the evaluator
            evaluation_result = self.text_evaluator.evaluate(new_audio.transcript)

            logger.info(
                "Decision for ID: %s is: %s",
                transmission_id,
                evaluation_result.get("is_flagged"),
            )

            # 3. If not flagged, skip publishing
            if not evaluation_result.get("is_flagged"):
                logger.info(
                    "No rules triggered for ID: %s. Skipping publish.",
                    transmission_id,
                )
                return

            # 4. Create Evaluation Result Payload
            evaluated_payload = evaluated_pb2.EvaluatedTranscribedAudio(
                feed_id=new_audio.feed_id,
                transmission_id=new_audio.transmission_id,
                source_audio_uris=new_audio.source_audio_uris,
                transcript=new_audio.transcript,
                evaluation_decisions=evaluation_result.get("triggered_rules", []),
            )
            evaluated_payload.start_timestamp.CopyFrom(new_audio.start_timestamp)
            evaluated_payload.end_timestamp.CopyFrom(new_audio.end_timestamp)
            evaluated_payload.start_audio_offset.CopyFrom(new_audio.start_audio_offset)
            evaluated_payload.end_audio_offset.CopyFrom(new_audio.end_audio_offset)

            # 5. Publish to Downstream Topic
            self._publish_evaluation_result(evaluated_payload)

        except Exception:
            logger.exception("Error processing new audio message")
            raise

    def _parse_cloud_event(
        self, cloud_event: cloudevent.CloudEvent
    ) -> transcribed_pb2.TranscribedAudio | None:
        """
        Parses the CloudEvent into a TranscribedAudio proto.

        Args:
            cloud_event: The raw CloudEvent data.

        Returns:
            A TranscribedAudio object or None if parsing fails.
        """
        pubsub_message = cloud_event.data.get("message", {})
        transcribed_audio = transcribed_pb2.TranscribedAudio()
        raw_data = pubsub_message.get("data", "")
        if not raw_data:
            logger.warning("No data provided in CloudEvent")
            return None
        decoded_data = base64.b64decode(raw_data)
        transcribed_audio.ParseFromString(decoded_data)
        return transcribed_audio

    def _publish_evaluation_result(
        self, evaluated_payload: evaluated_pb2.EvaluatedTranscribedAudio
    ) -> None:
        """
        Publishes the evaluation result to the configured Pub/Sub topic.

        Args:
            evaluated_payload: The enriched evaluation payload.
        """
        if self.output_topic_path:
            encoded_data = evaluated_payload.SerializeToString()
            future = self.publisher.publish(self.output_topic_path, encoded_data)
            message_id = future.result()

            logger.info(
                "Success! Published enriched message %s to %s",
                message_id,
                self.output_topic_id,
            )
        else:
            logger.warning("Skipping publish: Output topic not configured.")

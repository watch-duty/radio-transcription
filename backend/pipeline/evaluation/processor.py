from __future__ import annotations

import base64
import logging
from typing import TYPE_CHECKING

from backend.pipeline.common.exceptions import AlreadyExistsError
from backend.pipeline.common.tracing_utils import (
    get_current_trace_id,
    with_tracer_context,
)
from backend.pipeline.schema_types import (
    transcribed_audio_pb2 as transcribed_pb2,
)

if TYPE_CHECKING:
    from cloudevents.http import event as cloudevent

    from backend.pipeline.common.clients.pubsub_client import PubSubClient
    from backend.pipeline.common.clients.transcripts_client import (
        TranscriptsClient,
    )
    from backend.pipeline.evaluation.service import EvaluationService

logger = logging.getLogger(__name__)


class EvaluationEventProcessor:
    """
    Orchestrates the evaluation flow: parsing events, invoking the service,
    writing results to Transcripts API, and publishing alerts.
    """

    def __init__(
        self,
        evaluation_service: EvaluationService,
        transcripts_client: TranscriptsClient,
        publisher: PubSubClient,
        output_topic_path: str,
    ) -> None:
        """
        Initializes the EvaluationEventProcessor.

        Args:
            evaluation_service: The service to perform evaluations.
            transcripts_client: Client to write to Transcripts API.
            publisher: Pub/Sub publisher client.
            output_topic_path: Topic path to publish alerts to.
        """
        self.evaluation_service = evaluation_service
        self.transcripts_client = transcripts_client
        self.publisher = publisher
        self.output_topic_path = output_topic_path

    def process_event(self, cloud_event: cloudevent.CloudEvent) -> None:
        """
        Processes a CloudEvent containing transcribed audio.

        Args:
            cloud_event: The CloudEvent triggered by Pub/Sub.
        """
        pubsub_message = cloud_event.data.get("message", {})
        attributes = pubsub_message.get("attributes", {}) or {}
        traceparent = attributes.get("traceparent", "")

        with with_tracer_context(traceparent, "evaluate_rules", __name__):
            # 1. Decode the Incoming Message
            # TODO (https://linear.app/watchduty/issue/GOO-245/): Handle parse failure.
            new_audio = self._parse_cloud_event(cloud_event)
            if new_audio is None:
                logger.error(
                    "Transcribed audio could not be parsed for cloud event %s. Skipping.",
                    cloud_event,
                )
                return

            def _raise(msg: str) -> None:
                raise ValueError(msg)

            if not new_audio.transmission_id:
                _raise("transmission_id is required")
            if not new_audio.feed_id:
                _raise("feed_id is required")
            if not new_audio.transcript:
                _raise("transcript is required")
            if not new_audio.source_audio_uris:
                msg = f"TranscribedAudio missing source_audio_uris for feed_id: {new_audio.feed_id} (transmission: {new_audio.transmission_id})"
                _raise(msg)

            # 2. Evaluate
            # TODO (https://linear.app/watchduty/issue/GOO-245/): Handle evaluation failure.
            evaluated_payload = self.evaluation_service.evaluate(new_audio)
            if not evaluated_payload:
                logger.error(
                    "Evaluation returned no payload for feed %s and transmission %s. Skipping.",
                    new_audio.feed_id,
                    new_audio.transmission_id,
                )
                return

            # 3. Always write to Transcripts API
            # TODO (https://linear.app/watchduty/issue/GOO-245/): Handle write failure.
            try:
                self.transcripts_client.create_transcript(evaluated_payload)
            except AlreadyExistsError:
                logger.warning(
                    "Transcript already exists for transmission %s which indicates we already processed this transmission. Continuing.",
                    evaluated_payload.transmission_id,
                )

            # 4. Publish to Downstream Topic if flagged or has errors
            if (
                len(evaluated_payload.evaluation_decisions) > 0
                or len(evaluated_payload.evaluation_errors) > 0
            ):
                encoded_data = evaluated_payload.SerializeToString()
                # TODO (https://linear.app/watchduty/issue/GOO-245/): Handle publish failure.
                future = self.publisher.get_publisher().publish(
                    self.output_topic_path,
                    encoded_data,
                    ordering_key=evaluated_payload.feed_id,
                    traceparent=get_current_trace_id(),
                )
                message_id = future.result()
                logger.info(
                    "Success! Published enriched message %s to %s",
                    message_id,
                    self.output_topic_path,
                )

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
            logger.error("No data provided in CloudEvent")
            return None
        decoded_data = base64.b64decode(raw_data)
        transcribed_audio.ParseFromString(decoded_data)
        return transcribed_audio

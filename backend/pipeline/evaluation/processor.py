from __future__ import annotations

import base64
import logging
from typing import TYPE_CHECKING

from backend.pipeline.common.exceptions import AlreadyExistsError
from backend.pipeline.common.tracing_utils import (
    inject_otel_context,
    with_tracer_context,
)
from backend.pipeline.schema_types import (
    transcribed_audio_pb2 as transcribed_pb2,
)
from backend.services.audio_segments.models import AnnotationType

if TYPE_CHECKING:
    from cloudevents.http import event as cloudevent

    from backend.pipeline.common.clients.audio_segments_client import (
        AudioSegmentsClient,
    )
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
        audio_segments_client: AudioSegmentsClient | None,
    ) -> None:
        """
        Initializes the EvaluationEventProcessor.

        Args:
            evaluation_service: The service to perform evaluations.
            transcripts_client: Client to write to Transcripts API.
            publisher: Pub/Sub publisher client.
            output_topic_path: Topic path to publish alerts to.
            audio_segments_client: Client for the Audio Segments API (optional).
        """
        self.evaluation_service = evaluation_service
        self.transcripts_client = transcripts_client
        self.publisher = publisher
        self.output_topic_path = output_topic_path
        self.audio_segments_client = audio_segments_client

    def process_event(self, cloud_event: cloudevent.CloudEvent) -> None:
        """
        Processes a CloudEvent containing transcribed audio.

        Args:
            cloud_event: The CloudEvent triggered by Pub/Sub.
        """
        pubsub_message = cloud_event.data.get("message", {})
        attributes = pubsub_message.get("attributes", {}) or {}

        with with_tracer_context(attributes, "evaluate_rules", __name__):
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

            if not new_audio.segment_id:
                _raise("segment_id is required")
            if not new_audio.feed_id:
                _raise("feed_id is required")
            if not new_audio.transcript:
                _raise("transcript is required")
            if not new_audio.source_audio_uris:
                msg = f"TranscribedAudio missing source_audio_uris for feed_id: {new_audio.feed_id} (segment: {new_audio.segment_id})"
                _raise(msg)

            # 2. Evaluate
            # TODO (https://linear.app/watchduty/issue/GOO-245/): Handle evaluation failure.
            evaluated_payload = self.evaluation_service.evaluate(new_audio)
            if not evaluated_payload:
                logger.error(
                    "Evaluation returned no payload for feed %s and transmission %s. Skipping.",
                    new_audio.feed_id,
                    new_audio.segment_id,
                )
                return

            # 3. Always write to Transcripts API
            # TODO (https://linear.app/watchduty/issue/GOO-245/): Handle write failure.
            # TODO (https://linear.app/watchduty/issue/GOO-458/remaining-legacy-cleanup): cleanup after migration
            try:
                self.transcripts_client.create_transcript(evaluated_payload)
            except AlreadyExistsError:
                logger.warning(
                    "Transcript already exists for transmission %s which indicates we already processed this transmission. Continuing.",
                    evaluated_payload.segment_id,
                )

            # 3.5 Write to Annotation Segments table
            if self.audio_segments_client is not None:
                # TODO (https://linear.app/watchduty/issue/GOO-449/ui-uibff-cutover-and-legacy-cleanup): Make this client required and call unconditional once the migration is complete.
                try:
                    annotation_data = {
                        "decisions": list(
                            evaluated_payload.evaluation_decisions
                        ),
                        "errors": list(evaluated_payload.errors),
                    }
                    self.audio_segments_client.add_audio_segment_annotation(
                        audio_segment_id=new_audio.segment_id,
                        annotation_type=AnnotationType.EVALUATION,
                        data=annotation_data,
                    )
                    logger.info(
                        "Successfully added evaluation annotation for segment %s",
                        new_audio.segment_id,
                    )
                except Exception as e:
                    logger.exception(
                        "Failed to add evaluation annotation for segment %s: %s",
                        new_audio.segment_id,
                        e,
                    )

            # 4. Publish to Downstream Topic if flagged or has errors
            if (
                len(evaluated_payload.evaluation_decisions) > 0
                or len(evaluated_payload.errors) > 0
            ):
                encoded_data = evaluated_payload.SerializeToString()
                attrs: dict[str, str] = {}
                inject_otel_context(attrs)
                # TODO (https://linear.app/watchduty/issue/GOO-245/): Handle publish failure.
                future = self.publisher.get_publisher().publish(
                    self.output_topic_path,
                    encoded_data,
                    ordering_key=evaluated_payload.feed_id,
                    **attrs,
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

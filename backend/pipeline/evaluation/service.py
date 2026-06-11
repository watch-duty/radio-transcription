import datetime
import logging

from google.protobuf.duration_pb2 import Duration  # type: ignore

from backend.pipeline.common.tracing_utils import get_current_traceparent
from backend.pipeline.evaluation.rules_evaluation import evaluator
from backend.pipeline.schema_types import (
    evaluated_transcribed_audio_pb2 as evaluated_pb2,
)
from backend.pipeline.schema_types import (
    transcribed_audio_pb2 as transcribed_pb2,
)

logger = logging.getLogger(__name__)


def _sanitize_duration(duration: Duration, context: str = "") -> None:
    """Safeguards protobuf Duration from sign mismatch or negative offsets."""
    if (
        duration.seconds < 0
        or duration.nanos < 0
        or (duration.seconds > 0 and duration.nanos < 0)
        or (duration.seconds < 0 and duration.nanos > 0)
    ):
        logger.warning(
            "Sanitizing invalid or sign-mismatched Duration%s: seconds=%d, nanos=%d",
            f" ({context})" if context else "",
            duration.seconds,
            duration.nanos,
        )
        duration.seconds = 0
        duration.nanos = 0


class EvaluationService:
    """
    Business logic for evaluating transcribed audio segments against rules.

    Attributes:
        text_evaluator: The evaluator instance used to check transcripts.
    """

    def __init__(
        self,
        text_evaluator: evaluator.BaseTextEvaluator,
    ) -> None:
        """
        Initializes the EvaluationService.

        Args:
            text_evaluator: An instance of a text evaluator.
        """
        self.text_evaluator = text_evaluator

    def evaluate(
        self, new_audio: transcribed_pb2.TranscribedAudio
    ) -> evaluated_pb2.EvaluatedTranscribedAudio | None:
        """
        Evaluates the transcript.

        Args:
            new_audio: The transcribed audio object.

        Returns:
            The evaluated payload or None if processing was skipped.
        """
        try:
            segment_id = new_audio.segment_id
            # Safeguard offset durations against sign mismatches (e.g. from negative timedeltas)
            _sanitize_duration(
                new_audio.start_audio_offset,
                f"start_audio_offset for segment {segment_id}",
            )
            _sanitize_duration(
                new_audio.end_audio_offset,
                f"end_audio_offset for segment {segment_id}",
            )

            logger.info("Processing transmission ID: %s", segment_id)

            if not new_audio.transcript.strip():
                logger.info(
                    "No transcript for ID: %s. Skipping evaluation.",
                    segment_id,
                )
                return None

            # 2. Call the evaluator
            evaluation_result = self.text_evaluator.evaluate(
                new_audio.transcript, new_audio.feed_id
            )

            logger.info(
                "Decision for ID: %s is: %s",
                segment_id,
                evaluation_result.get("is_flagged"),
            )

            # 3. Handle Errors
            errors = evaluation_result.get("errors", [])
            if errors:
                logger.warning(
                    "Evaluation encountered errors for transmission %s: %s",
                    segment_id,
                    [str(e) for e in errors],
                )

            # 4. Create Evaluation Result Payload
            evaluated_payload = evaluated_pb2.EvaluatedTranscribedAudio(
                feed_id=new_audio.feed_id,
                segment_id=new_audio.segment_id,
                source_audio_uris=new_audio.source_audio_uris,
                transcript=new_audio.transcript,
                missing_prior_context=new_audio.missing_prior_context,
                missing_post_context=new_audio.missing_post_context,
                evaluation_decisions=evaluation_result.get(
                    "triggered_rules", []
                ),
                errors=errors,
                canonical_audio_uri=new_audio.canonical_audio_uri,
                playback_audio_uri=new_audio.playback_audio_uri,
                feed_name=new_audio.feed_name,
            )
            evaluated_payload.start_timestamp.CopyFrom(
                new_audio.start_timestamp
            )
            evaluated_payload.end_timestamp.CopyFrom(new_audio.end_timestamp)
            evaluated_payload.start_audio_offset.CopyFrom(
                new_audio.start_audio_offset
            )
            evaluated_payload.end_audio_offset.CopyFrom(
                new_audio.end_audio_offset
            )

            logger.info(
                "evaluation_completed",
                extra={
                    "json_fields": {
                        "structured_event": True,
                        "event_type": "evaluation_completed",
                        "event_time_ms": int(
                            datetime.datetime.now(datetime.UTC).timestamp() * 1000
                        ),
                        "feed_id": new_audio.feed_id,
                        "segment_id": new_audio.segment_id,
                        # Log source_audio_uris so Log Analytics can unnest and join them
                        # against the raw_audio_ingested events to find the earliest ingestion
                        # timestamp, enabling true end-to-end latency calculation for fan-in segments.
                        "source_audio_uris": list(new_audio.source_audio_uris),
                        "trace_id": get_current_traceparent(),
                    }
                },
            )

        except Exception:
            logger.exception("Error processing new audio message")
            raise
        else:
            return evaluated_payload

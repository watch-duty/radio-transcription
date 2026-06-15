import base64
import unittest
from unittest.mock import MagicMock, patch

from backend.pipeline.common.clients.pubsub_client import PubSubClient
from backend.pipeline.common.exceptions import AlreadyExistsError
from backend.pipeline.evaluation.processor import EvaluationEventProcessor
from backend.pipeline.schema_types import (
    EvaluationErrorType,
)
from backend.pipeline.schema_types import (
    evaluated_transcribed_audio_pb2 as evaluated_pb2,
)
from backend.pipeline.schema_types import (
    transcribed_audio_pb2 as transcribed_pb2,
)
from backend.services.audio_segments.models import AnnotationType


class TestEvaluationEventProcessor(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_service = MagicMock()
        self.mock_publisher = MagicMock(spec=PubSubClient)
        self.mock_raw_publisher = MagicMock()
        self.mock_publisher.get_publisher.return_value = self.mock_raw_publisher
        self.output_topic_path = "projects/test-project/topics/test-topic"
        self.mock_audio_segments_client = MagicMock()

        self.processor = EvaluationEventProcessor(
            evaluation_service=self.mock_service,
            publisher=self.mock_publisher,
            output_topic_path=self.output_topic_path,
            audio_segments_client=self.mock_audio_segments_client,
        )

        # Create a sample TranscribedAudio proto
        self.transcribed_audio = transcribed_pb2.TranscribedAudio()
        self.transcribed_audio.segment_id = "12345"
        self.transcribed_audio.feed_id = "1234"
        self.transcribed_audio.transcript = "Test transcript"
        self.transcribed_audio.source_audio_uris.append(
            "gs://bucket/audio.flac"
        )

        # Create a sample EvaluatedTranscribedAudio proto
        self.evaluated_payload = evaluated_pb2.EvaluatedTranscribedAudio()
        self.evaluated_payload.segment_id = "12345"
        self.evaluated_payload.feed_id = "1234"
        self.evaluated_payload.transcript = "Test transcript"

    def _create_mock_event(self, data) -> MagicMock:
        mock_event = MagicMock()
        mock_event.data = data
        return mock_event

    def test_process_event_flagged_publishes(self) -> None:
        # Setup
        self.evaluated_payload.evaluation_decisions.append("test_rule")
        self.mock_service.evaluate.return_value = self.evaluated_payload

        # Encode proto to base64
        serialized_audio = self.transcribed_audio.SerializeToString()
        base64_audio = base64.b64encode(serialized_audio).decode("utf-8")

        cloud_event = self._create_mock_event(
            {"message": {"data": base64_audio}}
        )

        # Mock publisher build future
        mock_future = MagicMock()
        mock_future.result.return_value = "msg-123"
        self.mock_raw_publisher.publish.return_value = mock_future

        # Execute
        self.processor.process_event(cloud_event)

        # Verify
        self.mock_service.evaluate.assert_called_once()
        self.mock_audio_segments_client.add_audio_segment_annotation.assert_called_once_with(
            audio_segment_id="12345",
            annotation_type=AnnotationType.EVALUATION,
            data={
                "decisions": ["test_rule"],
                "errors": [],
            },
        )
        self.mock_raw_publisher.publish.assert_called_once()
        call_args = self.mock_raw_publisher.publish.call_args
        self.assertEqual(call_args.args[0], self.output_topic_path)
        self.assertEqual(
            call_args.args[1], self.evaluated_payload.SerializeToString()
        )
        self.assertEqual(call_args.kwargs["ordering_key"], "1234")

    def test_process_event_not_flagged_skips_publish(self) -> None:
        # Setup
        # No decisions or errors
        self.mock_service.evaluate.return_value = self.evaluated_payload

        serialized_audio = self.transcribed_audio.SerializeToString()
        base64_audio = base64.b64encode(serialized_audio).decode("utf-8")

        cloud_event = self._create_mock_event(
            {"message": {"data": base64_audio}}
        )

        # Execute
        self.processor.process_event(cloud_event)

        # Verify
        self.mock_service.evaluate.assert_called_once()
        self.mock_audio_segments_client.add_audio_segment_annotation.assert_called_once_with(
            audio_segment_id="12345",
            annotation_type=AnnotationType.EVALUATION,
            data={
                "decisions": [],
                "errors": [],
            },
        )
        self.mock_raw_publisher.publish.assert_not_called()

    def test_process_event_has_errors_publishes(self) -> None:
        # Setup
        self.evaluated_payload.errors.append(
            EvaluationErrorType.ERROR_FEED_ID_MISSING
        )
        self.mock_service.evaluate.return_value = self.evaluated_payload

        serialized_audio = self.transcribed_audio.SerializeToString()
        base64_audio = base64.b64encode(serialized_audio).decode("utf-8")

        cloud_event = self._create_mock_event(
            {"message": {"data": base64_audio}}
        )

        mock_future = MagicMock()
        mock_future.result.return_value = "msg-123"
        self.mock_raw_publisher.publish.return_value = mock_future

        # Execute
        self.processor.process_event(cloud_event)

        # Verify
        self.mock_raw_publisher.publish.assert_called_once()

    def test_process_event_parse_failure_skips(self) -> None:
        # Setup
        # Missing "data" field
        cloud_event = self._create_mock_event({"message": {}})

        # Execute
        self.processor.process_event(cloud_event)

        # Verify
        self.mock_service.evaluate.assert_not_called()
        self.mock_raw_publisher.publish.assert_not_called()

    def test_process_event_evaluation_none_skips(self) -> None:
        # Setup
        self.mock_service.evaluate.return_value = None

        serialized_audio = self.transcribed_audio.SerializeToString()
        base64_audio = base64.b64encode(serialized_audio).decode("utf-8")

        cloud_event = self._create_mock_event(
            {"message": {"data": base64_audio}}
        )

        # Execute
        self.processor.process_event(cloud_event)

        # Verify
        self.mock_service.evaluate.assert_called_once()
        self.mock_raw_publisher.publish.assert_not_called()


    @patch("backend.pipeline.evaluation.processor.with_tracer_context")
    def test_process_event_uses_traceparent(
        self, mock_with_tracer_context
    ) -> None:
        """Verifies that process_event extracts traceparent and uses with_tracer_context."""
        # Setup
        traceparent_val = (
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        )
        attrs = {"traceparent": traceparent_val}

        # Encode proto to base64
        serialized_audio = self.transcribed_audio.SerializeToString()
        base64_audio = base64.b64encode(serialized_audio).decode("utf-8")

        cloud_event = self._create_mock_event(
            {"message": {"data": base64_audio, "attributes": attrs}}
        )

        # Mock context manager
        mock_cm = MagicMock()
        mock_with_tracer_context.return_value = mock_cm

        # Execute
        self.processor.process_event(cloud_event)

        # Verify
        mock_with_tracer_context.assert_called_once_with(
            attrs,
            "evaluate_rules",
            "backend.pipeline.evaluation.processor",
        )

    @patch("backend.pipeline.evaluation.processor.with_tracer_context")
    def test_process_event_missing_traceparent_uses_empty_string(
        self, mock_with_tracer_context
    ) -> None:
        """Verifies that process_event uses empty string when traceparent is missing."""
        # Setup
        serialized_audio = self.transcribed_audio.SerializeToString()
        base64_audio = base64.b64encode(serialized_audio).decode("utf-8")

        cloud_event = self._create_mock_event(
            {"message": {"data": base64_audio}}
        )

        # Mock context manager
        mock_cm = MagicMock()
        mock_with_tracer_context.return_value = mock_cm

        # Execute
        self.processor.process_event(cloud_event)

        # Verify
        mock_with_tracer_context.assert_called_once_with(
            {}, "evaluate_rules", "backend.pipeline.evaluation.processor"
        )

    def test_process_event_add_annotation_failure_continues(self) -> None:
        # Setup
        self.evaluated_payload.evaluation_decisions.append("test_rule")
        self.mock_service.evaluate.return_value = self.evaluated_payload

        self.mock_audio_segments_client.add_audio_segment_annotation.side_effect = Exception(
            "DB Error"
        )

        serialized_audio = self.transcribed_audio.SerializeToString()
        base64_audio = base64.b64encode(serialized_audio).decode("utf-8")

        cloud_event = self._create_mock_event(
            {"message": {"data": base64_audio}}
        )

        mock_future = MagicMock()
        mock_future.result.return_value = "msg-123"
        self.mock_raw_publisher.publish.return_value = mock_future

        # Execute
        self.processor.process_event(cloud_event)

        # Verify
        self.mock_service.evaluate.assert_called_once()
        # Should still publish because it was flagged, despite DB error
        self.mock_raw_publisher.publish.assert_called_once()



if __name__ == "__main__":
    unittest.main()

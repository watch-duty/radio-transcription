import base64
import unittest
from unittest.mock import MagicMock, patch

from backend.pipeline.evaluation import service
from backend.pipeline.schema_types import (
    evaluated_transcribed_audio_pb2 as evaluated_pb2,
)
from backend.pipeline.schema_types import transcribed_audio_pb2 as transcribed_pb2


class TestEvaluationService(unittest.TestCase):
    """Tests for the EvaluationService class."""

    def setUp(self) -> None:
        """Sets up test fixtures."""
        self.mock_publisher = MagicMock()
        self.project_id = "test-project"
        self.output_topic_id = "test-topic"

        self.service = service.EvaluationService(
            publisher=self.mock_publisher,
            project_id=self.project_id,
            output_topic_id=self.output_topic_id,
        )

        self.transcribed_audio = transcribed_pb2.TranscribedAudio()
        self.transcribed_audio.transmission_id = "12345"
        self.transcribed_audio.transcript = "There is a fire"
        self.transcribed_audio.feed_id = "1234"
        self.transcribed_audio.source_chunk_ids.append("chunk_1")
        self.transcribed_audio.start_timestamp.seconds = 1234567890
        self.transcribed_audio.start_timestamp.nanos = 0
        self.transcribed_audio.end_timestamp.seconds = 1234567999
        self.transcribed_audio.end_timestamp.nanos = 0

        self.data_bytes = self.transcribed_audio.SerializeToString()
        self.b64_encoded_data = base64.b64encode(self.data_bytes).decode("utf-8")

        self.mock_event = MagicMock()
        self.mock_event.data = {
            "message": {"data": self.b64_encoded_data},
            "publish_time": "2023-01-01T12:00:00Z",
        }

    @patch("backend.pipeline.evaluation.service.evaluator.StaticTextEvaluator.evaluate")
    def test_successful_flow(self, mock_evaluate: MagicMock) -> None:
        """Tests a basic successful evaluation and publication flow."""
        mock_evaluate.return_value = {
            "is_flagged": True,
            "triggered_rules": ["basic_fire_terms"],
        }

        mock_future = MagicMock()
        mock_future.result.return_value = "msg_id_999"
        self.mock_publisher.publish.return_value = mock_future

        self.service.handle_event(self.mock_event)

        mock_evaluate.assert_called_with("There is a fire")
        self.assertTrue(self.mock_publisher.publish.called)
        args, _ = self.mock_publisher.publish.call_args
        sent_proto_bytes = args[1]

        result_proto = evaluated_pb2.EvaluatedTranscribedAudio()
        result_proto.ParseFromString(sent_proto_bytes)

        self.assertEqual(result_proto.transmission_id, "12345")
        self.assertEqual(result_proto.transcript, "There is a fire")
        self.assertEqual(result_proto.evaluation_decisions, ["basic_fire_terms"])

    @patch("backend.pipeline.evaluation.service.evaluator.StaticTextEvaluator.evaluate")
    def test_no_publish_if_not_flagged(self, mock_evaluate: MagicMock) -> None:
        """Ensures no publication occurs if the text is not flagged."""
        mock_evaluate.return_value = {
            "is_flagged": False,
            "triggered_rules": [],
        }

        self.service.handle_event(self.mock_event)

        mock_evaluate.assert_called()
        self.mock_publisher.publish.assert_not_called()

    @patch("backend.pipeline.evaluation.service.evaluator.StaticTextEvaluator.evaluate")
    def test_no_publish_if_no_topic(self, mock_evaluate: MagicMock) -> None:
        """Ensures no publication occurs if no output topic is configured."""
        # Create service without topic
        service_no_topic = service.EvaluationService(
            publisher=self.mock_publisher,
            project_id=None,
            output_topic_id=None,
        )

        mock_evaluate.return_value = {
            "is_flagged": True,
            "triggered_rules": ["basic_fire_terms"],
        }

        service_no_topic.handle_event(self.mock_event)

        mock_evaluate.assert_called()
        self.mock_publisher.publish.assert_not_called()


if __name__ == "__main__":
    unittest.main()

import base64
import unittest
from unittest.mock import MagicMock

from backend.pipeline.common.clients.pubsub_client import PubSubClient
from backend.pipeline.common.exceptions import AlreadyExistsError
from backend.pipeline.evaluation.processor import EvaluationEventProcessor
from backend.pipeline.schema_types import (
    evaluated_transcribed_audio_pb2 as evaluated_pb2,
)
from backend.pipeline.schema_types import (
    transcribed_audio_pb2 as transcribed_pb2,
)


class TestEvaluationEventProcessor(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_service = MagicMock()
        self.mock_transcripts_client = MagicMock()
        self.mock_publisher = MagicMock(spec=PubSubClient)
        self.mock_raw_publisher = MagicMock()
        self.mock_publisher.get_publisher.return_value = self.mock_raw_publisher
        self.output_topic_path = "projects/test-project/topics/test-topic"

        self.processor = EvaluationEventProcessor(
            evaluation_service=self.mock_service,
            transcripts_client=self.mock_transcripts_client,
            publisher=self.mock_publisher,
            output_topic_path=self.output_topic_path,
        )

        # Create a sample TranscribedAudio proto
        self.transcribed_audio = transcribed_pb2.TranscribedAudio()
        self.transcribed_audio.transmission_id = "12345"
        self.transcribed_audio.feed_id = "1234"
        self.transcribed_audio.transcript = "Test transcript"

        # Create a sample EvaluatedTranscribedAudio proto
        self.evaluated_payload = evaluated_pb2.EvaluatedTranscribedAudio()
        self.evaluated_payload.transmission_id = "12345"
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
        self.mock_transcripts_client.create_transcript.assert_called_once_with(
            self.evaluated_payload
        )
        self.mock_raw_publisher.publish.assert_called_once_with(
            self.output_topic_path,
            self.evaluated_payload.SerializeToString(),
            ordering_key="1234",
        )

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
        self.mock_transcripts_client.create_transcript.assert_called_once_with(
            self.evaluated_payload
        )
        self.mock_raw_publisher.publish.assert_not_called()

    def test_process_event_has_errors_publishes(self) -> None:
        # Setup
        self.evaluated_payload.evaluation_errors.append(
            evaluated_pb2.EvaluatedTranscribedAudio.EvaluationErrorType.ERROR_FEED_ID_MISSING
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

    def test_process_event_create_transcript_exists_continues(self) -> None:
        # Setup
        self.evaluated_payload.evaluation_decisions.append("test_rule")
        self.mock_service.evaluate.return_value = self.evaluated_payload

        # Mock create_transcript to raise AlreadyExistsError

        self.mock_transcripts_client.create_transcript.side_effect = (
            AlreadyExistsError("12345")
        )

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
        self.mock_transcripts_client.create_transcript.assert_called_once_with(
            self.evaluated_payload
        )
        # Should still publish because it was flagged
        self.mock_raw_publisher.publish.assert_called_once_with(
            self.output_topic_path,
            self.evaluated_payload.SerializeToString(),
            ordering_key="1234",
        )


if __name__ == "__main__":
    unittest.main()

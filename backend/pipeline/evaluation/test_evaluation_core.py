import base64
import unittest
from unittest.mock import MagicMock, patch

from backend.pipeline.schema_types.evaluated_transcribed_audio_pb2 import (
    EvaluatedTranscribedAudio,
)
from backend.pipeline.schema_types.transcribed_audio_pb2 import TranscribedAudio

with (
    patch("google.cloud.pubsub_v1.PublisherClient"),
    patch("google.cloud.logging.Client"),
):
    from backend.pipeline.evaluation import evaluation_core


class TestCloudFunction(unittest.TestCase):
    def setUp(self) -> None:
        self.transcribed_audio = TranscribedAudio()
        self.transcribed_audio.audio_id = "12345"
        self.transcribed_audio.transcript = "There is a fire"
        self.transcribed_audio.file_path = "test/path.wav"
        self.transcribed_audio.location = "test_location"
        self.transcribed_audio.feed = "test_feed"
        ts_start = TranscribedAudio.Timestamp()
        ts_start.seconds = 1234567890
        ts_start.nanos = 0
        self.transcribed_audio.start_timestamp.CopyFrom(ts_start)

        ts_end = TranscribedAudio.Timestamp()
        ts_end.seconds = 1234567999
        ts_end.nanos = 0
        self.transcribed_audio.end_timestamp.CopyFrom(ts_end)

        self.data_bytes = self.transcribed_audio.SerializeToString()
        self.b64_encoded_data = base64.b64encode(self.data_bytes).decode("utf-8")

        self.mock_event = MagicMock()
        self.mock_event.data = {
            "message": {"data": self.b64_encoded_data},
            "publish_time": "2023-01-01T12:00:00Z",
        }

    @patch("backend.pipeline.evaluation.evaluation_core.StaticTextEvaluator.evaluate")
    @patch("backend.pipeline.evaluation.evaluation_core.publisher")
    def test_successful_flow(
        self, mock_publisher: MagicMock, mock_evaluate: MagicMock
    ) -> None:
        mock_evaluate.return_value = {
            "is_flagged": True,
            "triggered_rules": ["basic_fire_terms"],
        }

        mock_future = MagicMock()
        mock_future.result.return_value = "msg_id_999"
        mock_publisher.publish.return_value = mock_future

        evaluation_core.output_topic_path = "projects/test/topics/test-output"

        evaluation_core.evaluate_transcribed_audio_segment(self.mock_event)

        mock_evaluate.assert_called_with("There is a fire")
        self.assertTrue(mock_publisher.publish.called)
        args, _ = mock_publisher.publish.call_args
        sent_proto_bytes = args[1]

        result_proto = EvaluatedTranscribedAudio()
        result_proto.ParseFromString(sent_proto_bytes)

        self.assertEqual(result_proto.audio_id, "12345")
        self.assertEqual(result_proto.transcript, "There is a fire")
        self.assertEqual(result_proto.evaluation_decisions, ["basic_fire_terms"])

    @patch("backend.pipeline.evaluation.evaluation_core.StaticTextEvaluator.evaluate")
    @patch("backend.pipeline.evaluation.evaluation_core.publisher")
    def test_no_publish_if_no_topic(
        self, mock_publisher: MagicMock, mock_evaluate: MagicMock
    ) -> None:
        mock_evaluate.return_value = {
            "is_flagged": True,
            "triggered_rules": ["basic_fire_terms"],
        }
        evaluation_core.output_topic_path = None
        evaluation_core.evaluate_transcribed_audio_segment(self.mock_event)
        mock_evaluate.assert_called()
        mock_publisher.publish.assert_not_called()


if __name__ == "__main__":
    unittest.main()

import base64
import json
import unittest
from unittest.mock import MagicMock, patch

with patch("google.cloud.pubsub_v1.PublisherClient"):
    from backend.pipeline.evaluation import evaluation_core


class TestCloudFunction(unittest.TestCase):
    def setUp(self) -> None:
        self.sample_payload = {
            "id": "12345",
            "transcript": "There is a fire",
        }
        json_str = json.dumps(self.sample_payload)
        self.data_bytes = base64.b64encode(json_str.encode("utf-8"))

        self.mock_event = MagicMock()
        self.mock_event.data = {
            "message": {"data": self.data_bytes},
            "publish_time": "2023-01-01T12:00:00Z",
        }

    @patch("backend.pipeline.evaluation.evaluation_core.StaticTextEvaluator.evaluate")
    @patch("backend.pipeline.evaluation.evaluation_core.publisher")
    def test_successful_flow(
        self, mock_publisher: MagicMock, mock_evaluate: MagicMock
    ) -> None:
        """Test the happy path: Decode -> Evaluate -> Publish"""
        # 1. Setup Mock Return (TypedDict format)
        mock_evaluate.return_value = {
            "is_flagged": True,
            "triggered_rules": ["basic_fire_terms"],
        }

        # 2. Setup Publisher Future
        mock_future = MagicMock()
        mock_future.result.return_value = "msg_id_999"
        mock_publisher.publish.return_value = mock_future

        # 3. Ensure topic path exists for the test
        evaluation_core.output_topic_path = "projects/test/topics/test-output"

        # --- RUN ---
        evaluation_core.evaluate_transcribed_audio_segment(self.mock_event)

        # --- ASSERTIONS ---
        mock_evaluate.assert_called_with("There is a fire")

        # Verify message sent to Pub/Sub
        self.assertTrue(mock_publisher.publish.called)
        args, _ = mock_publisher.publish.call_args
        sent_payload = json.loads(args[1].decode("utf-8"))

        self.assertEqual(sent_payload["evaluation_result"]["is_flagged"], True)
        self.assertEqual(sent_payload["id"], "12345")

    @patch("backend.pipeline.evaluation.evaluation_core.StaticTextEvaluator.evaluate")
    @patch("backend.pipeline.evaluation.evaluation_core.publisher")
    def test_no_publish_if_no_topic(
        self, mock_publisher: MagicMock, mock_evaluate: MagicMock
    ) -> None:
        """Logic runs, but publish is skipped if topic path is None."""
        evaluation_core.output_topic_path = None
        evaluation_core.evaluate_transcribed_audio_segment(self.mock_event)
        mock_evaluate.assert_called()
        mock_publisher.publish.assert_not_called()


if __name__ == "__main__":
    unittest.main()

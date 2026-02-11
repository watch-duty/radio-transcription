import base64
import json
import sys
import unittest
from unittest.mock import MagicMock, patch

# ==========================================
# 1. MOCKING EXTERNAL LIBRARIES (PRE-IMPORT)
# ==========================================

# Mock 'functions_framework'
mock_ff = MagicMock()
# Define a dummy decorator that just returns the function as-is
mock_ff.cloud_event = lambda func: func 
sys.modules["functions_framework"] = mock_ff

# Mock 'google.cloud' and 'pubsub_v1'
mock_pubsub_lib = MagicMock()

# Create a mock for the 'google.cloud' parent package
mock_cloud_pkg = MagicMock()
mock_cloud_pkg.__path__ = [] 
mock_cloud_pkg.pubsub_v1 = mock_pubsub_lib 

sys.modules["google.cloud"] = mock_cloud_pkg
sys.modules["google.cloud.pubsub_v1"] = mock_pubsub_lib

# ==========================================
# 2. IMPORT MODULE UNDER TEST
# ==========================================
import evaluation_core 

class TestCloudFunction(unittest.TestCase):

    def setUp(self) -> None:
        self.sample_payload = {
            "id": "12345",
            "transcript": "There is a fire",
            "publish_time": "2023-01-01T12:00:00Z"
        }
        json_str = json.dumps(self.sample_payload)
        data_bytes = base64.b64encode(json_str.encode("utf-8"))

        self.mock_event = MagicMock()
        self.mock_event.data = {
            "message": {
                "data": data_bytes,
                "publish_time": "2023-01-01T12:00:00Z"
            }
        }

    # Patch 'evaluation_core' since that is where the objects are imported/used
    @patch("evaluation_core.evaluator.evaluate_text")
    @patch("evaluation_core.publisher") 
    def test_successful_flow(self, mock_publisher, mock_evaluate) -> None:
        """Test the happy path: Decode -> Evaluate -> Publish"""
        # 1. Setup the Logic Mock
        mock_evaluate.return_value = {"is_flagged": True, "triggered_rules": ["fire"]}

        # 2. Setup the Publisher Mock
        mock_future = MagicMock()
        mock_future.result.return_value = "msg_id_999"
        mock_publisher.publish.return_value = mock_future

        # 3. Force the global topic path variable (since Env var is missing)
        evaluation_core.output_topic_path = "projects/test/topics/test-output"

        # --- RUN ---
        evaluation_core.evaluate_transcribed_audio_segment(self.mock_event)

        # --- ASSERTIONS ---
        mock_evaluate.assert_called_with("There is a fire")
        self.assertTrue(mock_publisher.publish.called)
        # Check arguments passed to publish
        args, _ = mock_publisher.publish.call_args
        topic_arg, data_arg = args
        self.assertEqual(topic_arg, "projects/test/topics/test-output")
        sent_payload = json.loads(data_arg.decode("utf-8"))
        self.assertEqual(sent_payload["id"], "12345")
        self.assertEqual(sent_payload["analysis"]["is_flagged"], True)

    @patch("evaluation_core.evaluator.evaluate_text")
    @patch("evaluation_core.publisher")
    def test_missing_topic_env_var(self, mock_publisher, mock_evaluate) -> None:
        # Simulate missing output topic
        evaluation_core.output_topic_path = None
        evaluation_core.evaluate_transcribed_audio_segment(self.mock_event)
        # Logic should run, but Publish should NOT run
        mock_evaluate.assert_called()
        mock_publisher.publish.assert_not_called()

    def test_malformed_json(self) -> None:
        # Create bad JSON data
        bad_bytes = base64.b64encode(b"{not valid json").decode("utf-8")
        bad_event = MagicMock()
        bad_event.data = {"message": {"data": bad_bytes}}

        with self.assertRaises(json.JSONDecodeError):
            evaluation_core.evaluate_transcribed_audio_segment(bad_event)

if __name__ == "__main__":
    unittest.main()
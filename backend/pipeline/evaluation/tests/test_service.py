import unittest
from unittest.mock import MagicMock

from backend.pipeline.evaluation import service
from backend.pipeline.schema_types import (
    EvaluationErrorType,
)
from backend.pipeline.schema_types import (
    transcribed_audio_pb2 as transcribed_pb2,
)


class TestEvaluationService(unittest.TestCase):
    """Tests for the EvaluationService class."""

    def setUp(self) -> None:
        """Sets up test fixtures."""
        self.mock_evaluator = MagicMock()
        self.service = service.EvaluationService(
            text_evaluator=self.mock_evaluator,
        )

        self.transcribed_audio = transcribed_pb2.TranscribedAudio()
        self.transcribed_audio.segment_id = "12345"
        self.transcribed_audio.transcript = "There is a fire"
        self.transcribed_audio.feed_id = "1234"
        self.transcribed_audio.feed_name = "Central Fire"
        self.transcribed_audio.source_audio_uris.append("chunk_1")
        self.transcribed_audio.start_timestamp.seconds = 1234567890
        self.transcribed_audio.start_timestamp.nanos = 0
        self.transcribed_audio.end_timestamp.seconds = 1234567999
        self.transcribed_audio.end_timestamp.nanos = 0

    def test_successful_flow(self) -> None:
        """Tests a basic successful evaluation flow returning payload."""
        self.mock_evaluator.evaluate.return_value = {
            "is_flagged": True,
            "triggered_rules": ["basic_fire_terms"],
        }

        result_proto = self.service.evaluate(self.transcribed_audio)

        self.mock_evaluator.evaluate.assert_called_with(
            "There is a fire", "1234"
        )
        self.assertIsNotNone(result_proto)
        assert result_proto is not None
        self.assertEqual(result_proto.feed_name, "Central Fire")

        self.assertEqual(result_proto.segment_id, "12345")
        self.assertEqual(result_proto.transcript, "There is a fire")
        self.assertEqual(
            list(result_proto.evaluation_decisions), ["basic_fire_terms"]
        )

    def test_return_payload_if_not_flagged(self) -> None:
        """Ensures payload is returned even if the text is not flagged."""
        self.mock_evaluator.evaluate.return_value = {
            "is_flagged": False,
            "triggered_rules": [],
        }

        result_proto = self.service.evaluate(self.transcribed_audio)

        self.mock_evaluator.evaluate.assert_called()
        self.assertIsNotNone(result_proto)
        assert result_proto is not None
        self.assertEqual(result_proto.segment_id, "12345")
        self.assertEqual(len(result_proto.evaluation_decisions), 0)

    def test_return_payload_on_proto_error(self) -> None:
        """Ensures payload is returned if evaluating returns a proto error."""
        self.mock_evaluator.evaluate.return_value = {
            "is_flagged": False,
            "triggered_rules": [],
            "errors": [EvaluationErrorType.ERROR_FEED_ID_MISSING],
        }

        result_proto = self.service.evaluate(self.transcribed_audio)

        self.assertIsNotNone(result_proto)
        assert result_proto is not None
        self.assertEqual(
            list(result_proto.errors),
            [EvaluationErrorType.ERROR_FEED_ID_MISSING],
        )

    def test_evaluates_sanitizes_negative_or_invalid_durations(self) -> None:
        """Asserts that negative or sign-mismatched durations are sanitized to 0."""
        self.mock_evaluator.evaluate.return_value = {
            "is_flagged": False,
            "triggered_rules": [],
        }
        # Simulate a sign-mismatched duration (e.g. from negative timedelta serialization)
        self.transcribed_audio.start_audio_offset.seconds = -1
        self.transcribed_audio.start_audio_offset.nanos = 990000000
        self.transcribed_audio.end_audio_offset.seconds = 5
        self.transcribed_audio.end_audio_offset.nanos = -100

        result_proto = self.service.evaluate(self.transcribed_audio)

        self.assertIsNotNone(result_proto)
        assert result_proto is not None
        self.assertEqual(result_proto.start_audio_offset.seconds, 0)
        self.assertEqual(result_proto.start_audio_offset.nanos, 0)
        self.assertEqual(result_proto.end_audio_offset.seconds, 0)
        self.assertEqual(result_proto.end_audio_offset.nanos, 0)


if __name__ == "__main__":
    unittest.main()

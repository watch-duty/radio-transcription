import unittest
from unittest.mock import MagicMock

from backend.pipeline.evaluation import service
from backend.pipeline.schema_types import (
    evaluated_transcribed_audio_pb2 as evaluated_pb2,
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
        self.transcribed_audio.transmission_id = "12345"
        self.transcribed_audio.transcript = "There is a fire"
        self.transcribed_audio.feed_id = "1234"
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
        self.assertEqual(result_proto.transmission_id, "12345")
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
        self.assertEqual(result_proto.transmission_id, "12345")
        self.assertEqual(len(result_proto.evaluation_decisions), 0)

    def test_return_payload_on_proto_error(self) -> None:
        """Ensures payload is returned if evaluating returns a proto error."""
        self.mock_evaluator.evaluate.return_value = {
            "is_flagged": False,
            "triggered_rules": [],
            "errors": [
                evaluated_pb2.EvaluatedTranscribedAudio.EvaluationErrorType.ERROR_FEED_ID_MISSING
            ],
        }

        result_proto = self.service.evaluate(self.transcribed_audio)

        self.assertIsNotNone(result_proto)
        assert result_proto is not None
        self.assertEqual(
            list(result_proto.evaluation_errors),
            [
                evaluated_pb2.EvaluatedTranscribedAudio.EvaluationErrorType.ERROR_FEED_ID_MISSING
            ],
        )


    def test_feed_name_is_propagated(self) -> None:
        """Verifies that feed_name from the TranscribedAudio proto is copied into EvaluatedTranscribedAudio."""
        self.transcribed_audio.feed_name = "Downtown Scanner"
        self.mock_evaluator.evaluate.return_value = {
            "is_flagged": True,
            "triggered_rules": ["basic_fire_terms"],
        }

        result_proto = self.service.evaluate(self.transcribed_audio)

        self.assertIsNotNone(result_proto)
        assert result_proto is not None
        self.assertEqual(result_proto.feed_name, "Downtown Scanner")

    def test_feed_name_defaults_to_empty_string(self) -> None:
        """Verifies that when feed_name is absent in the source proto, the evaluation output has an empty feed_name."""
        # self.transcribed_audio has no feed_name set, so it defaults to ""
        self.mock_evaluator.evaluate.return_value = {
            "is_flagged": False,
            "triggered_rules": [],
        }

        result_proto = self.service.evaluate(self.transcribed_audio)

        self.assertIsNotNone(result_proto)
        assert result_proto is not None
        self.assertEqual(result_proto.feed_name, "")


if __name__ == "__main__":
    unittest.main()

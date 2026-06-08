import unittest
from unittest.mock import MagicMock, patch

import requests

from backend.pipeline.common.clients.transcripts_client import TranscriptsClient
from backend.pipeline.common.exceptions import AlreadyExistsError
from backend.pipeline.schema_types import (
    evaluated_transcribed_audio_pb2 as evaluated_pb2,
)


class TestTranscriptsClient(unittest.TestCase):
    def setUp(self) -> None:
        self.api_url = "http://test-api.com"
        self.client = TranscriptsClient(self.api_url)
        self.mock_session = MagicMock()
        self.client.session = self.mock_session

        self.payload = evaluated_pb2.EvaluatedTranscribedAudio()
        self.payload.segment_id = "12345"
        self.payload.transcript = "Test transcript"
        self.payload.feed_id = "1234"
        self.payload.source_audio_uris.append("gs://bucket/audio.flac")

    def test_create_transcript_success(self) -> None:
        # Setup
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        self.mock_session.post.return_value = mock_response

        # Execute
        self.client.create_transcript(self.payload)

        # Verify
        self.mock_session.post.assert_called_once()
        args, kwargs = self.mock_session.post.call_args
        self.assertEqual(args[0], "http://test-api.com/v1/transcripts")
        self.assertIn("json", kwargs)
        self.assertEqual(kwargs["json"]["transmission_id"], "12345")

    @patch("backend.pipeline.common.clients.transcripts_client.is_gcp_env")
    @patch("backend.pipeline.common.clients.transcripts_client.get_id_token")
    def test_create_transcript_adds_auth_in_gcp(
        self, mock_get_id_token, mock_is_gcp_env
    ) -> None:
        # Setup
        mock_is_gcp_env.return_value = True
        mock_get_id_token.return_value = "fake-token"

        mock_response = MagicMock()
        self.mock_session.post.return_value = mock_response

        # Execute
        self.client.create_transcript(self.payload)

        # Verify
        self.mock_session.headers.update.assert_called_with(
            {"Authorization": "Bearer fake-token"}
        )
        self.mock_session.post.assert_called_once()

    def test_create_transcript_propagates_exception(self) -> None:
        # Setup
        self.mock_session.post.side_effect = Exception("Network error")

        # Execute & Verify
        with self.assertRaises(Exception):
            self.client.create_transcript(self.payload)

    def test_create_transcript_raises_already_exists(self) -> None:
        # Setup
        mock_response = MagicMock()
        mock_response.status_code = 409
        mock_response.raise_for_status.side_effect = (
            requests.exceptions.HTTPError(response=mock_response)
        )
        self.mock_session.post.return_value = mock_response

        # Execute & Verify
        with self.assertRaises(AlreadyExistsError) as cm:
            self.client.create_transcript(self.payload)

        self.assertEqual(cm.exception.transmission_id, "12345")


if __name__ == "__main__":
    unittest.main()

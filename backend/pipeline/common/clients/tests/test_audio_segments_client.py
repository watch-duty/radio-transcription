import unittest
from unittest.mock import MagicMock, patch

from backend.pipeline.common.clients.audio_segments_client import (
    AudioSegmentsClient,
)


class TestAudioSegmentsClient(unittest.TestCase):
    def setUp(self) -> None:
        self.api_url = "http://test-api.com"
        self.client = AudioSegmentsClient(self.api_url)
        self.mock_session = MagicMock()
        self.client.session = self.mock_session

        self.segment_payload = {
            "id": "segment-id-123",
            "feed_id": "feed-id-456",
            "classification": "SPEECH_DETECTED",
            "start_timestamp": "2026-01-01T00:00:00Z",
            "end_timestamp": "2026-01-01T00:01:00Z",
            "missing_prior_context": False,
            "missing_post_context": False,
            "source_audio_uris": ["gs://bucket/audio1.ogg"],
        }

    def test_init_with_custom_max_retries(self) -> None:
        # Execute
        client = AudioSegmentsClient(self.api_url, max_retries=5)

        # Verify
        adapter = client.session.adapters.get("http://")
        self.assertIsNotNone(adapter)
        if adapter is not None:
            self.assertEqual(adapter.max_retries.total, 5)

    def test_init_with_zero_max_retries(self) -> None:
        # Execute
        client = AudioSegmentsClient(self.api_url, max_retries=0)

        # Verify
        adapter = client.session.adapters.get("http://")
        self.assertIsNotNone(adapter)
        if adapter is not None:
            self.assertEqual(
                getattr(adapter.max_retries, "total", adapter.max_retries), 0
            )

    def test_add_audio_segment_annotation_success(self) -> None:
        # Setup
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        self.mock_session.post.return_value = mock_response

        annotation_data = {"decisions": ["ALERT"], "errors": []}

        # Execute
        self.client.add_audio_segment_annotation(
            audio_segment_id="segment-id-123",
            annotation_type="EVALUATION",
            data=annotation_data,
        )

        # Verify
        self.mock_session.post.assert_called_once()
        args, kwargs = self.mock_session.post.call_args
        self.assertEqual(
            args[0],
            "http://test-api.com/v1/audio_segments/segment-id-123/annotations",
        )
        self.assertIn("json", kwargs)
        self.assertEqual(kwargs["json"]["type"], "EVALUATION")
        self.assertEqual(kwargs["json"]["data"], annotation_data)

    @patch("backend.pipeline.common.clients.audio_segments_client.is_gcp_env")
    @patch("backend.pipeline.common.clients.audio_segments_client.get_id_token")
    def test_add_audio_segment_annotation_adds_auth_in_gcp(
        self, mock_get_id_token, mock_is_gcp_env
    ) -> None:
        # Setup
        mock_is_gcp_env.return_value = True
        mock_get_id_token.return_value = "fake-token"

        mock_response = MagicMock()
        self.mock_session.post.return_value = mock_response

        # Execute
        self.client.add_audio_segment_annotation(
            audio_segment_id="segment-id-123",
            annotation_type="EVALUATION",
            data={"key": "val"},
        )

        # Verify
        self.mock_session.headers.update.assert_called_with(
            {"Authorization": "Bearer fake-token"}
        )
        self.mock_session.post.assert_called_once()

    def test_add_audio_segment_annotation_propagates_exception(self) -> None:
        # Setup
        self.mock_session.post.side_effect = Exception("Network error")

        # Execute & Verify
        with self.assertRaises(Exception):
            self.client.add_audio_segment_annotation(
                audio_segment_id="segment-id-123",
                annotation_type="EVALUATION",
                data={"key": "val"},
            )

    def test_add_audio_segment_annotation_empty_id_raises_value_error(
        self,
    ) -> None:
        # Execute & Verify
        with self.assertRaises(ValueError) as cm:
            self.client.add_audio_segment_annotation(
                audio_segment_id="  ",
                annotation_type="EVALUATION",
                data={"key": "value"},
            )
        self.assertEqual(
            str(cm.exception), "audio_segment_id cannot be empty or whitespace"
        )
        self.mock_session.post.assert_not_called()

    def test_add_audio_segment_annotation_empty_type_raises_value_error(
        self,
    ) -> None:
        # Execute & Verify
        with self.assertRaises(ValueError) as cm:
            self.client.add_audio_segment_annotation(
                audio_segment_id="segment-123",
                annotation_type="",
                data={"key": "value"},
            )
        self.assertEqual(str(cm.exception), "annotation_type cannot be empty")
        self.mock_session.post.assert_not_called()

    def test_add_audio_segment_annotation_empty_payload_raises_value_error(
        self,
    ) -> None:
        # Execute & Verify
        with self.assertRaises(ValueError) as cm:
            self.client.add_audio_segment_annotation(
                audio_segment_id="segment-123",
                annotation_type="EVALUATION",
                data={},
            )
        self.assertEqual(
            str(cm.exception), "annotation data payload cannot be empty"
        )
        self.mock_session.post.assert_not_called()

    def test_add_audio_segment_success(self) -> None:
        # Setup
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        self.mock_session.post.return_value = mock_response

        # Execute
        self.client.add_audio_segment(self.segment_payload)

        # Verify
        self.mock_session.post.assert_called_once()
        args, kwargs = self.mock_session.post.call_args
        self.assertEqual(args[0], "http://test-api.com/v1/audio_segments")
        self.assertIn("json", kwargs)
        self.assertEqual(kwargs["json"], self.segment_payload)

    def test_add_audio_segment_propagates_exception(self) -> None:
        # Setup
        self.mock_session.post.side_effect = Exception("Network error")

        # Execute & Verify
        with self.assertRaises(Exception):
            self.client.add_audio_segment(self.segment_payload)

    def test_add_audio_segment_empty_payload_raises_value_error(self) -> None:
        # Execute & Verify
        with self.assertRaises(ValueError) as cm:
            self.client.add_audio_segment({})
        self.assertEqual(str(cm.exception), "segment data cannot be empty")
        self.mock_session.post.assert_not_called()

    def test_add_audio_segment_missing_fields_raises_value_error(self) -> None:
        # Execute & Verify (missing id)
        with self.assertRaises(ValueError) as cm:
            self.client.add_audio_segment({"feed_id": "feed-123"})
        self.assertEqual(str(cm.exception), "segment id is required")

        # Execute & Verify (missing feed_id)
        with self.assertRaises(ValueError) as cm:
            self.client.add_audio_segment({"id": "segment-123"})
        self.assertEqual(str(cm.exception), "segment feed_id is required")
        self.mock_session.post.assert_not_called()


if __name__ == "__main__":
    unittest.main()

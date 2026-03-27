import unittest
from unittest.mock import MagicMock

from backend.pipeline.transcription.audio_uploader import AudioUploader
from backend.pipeline.transcription.datatypes import FlushRequest, TimeRange


class AudioUploaderTest(unittest.TestCase):
    """Tests the AudioUploader class in isolation."""

    def test_upload_derivatives_success(self) -> None:
        """Tests that both FLAC and M4A are uploaded successfully when configured."""
        mock_gcs = MagicMock()
        mock_bucket = MagicMock()
        mock_gcs.bucket.return_value = mock_bucket
        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob

        mock_export_m4a = MagicMock(return_value=b"m4a-bytes")

        uploader = AudioUploader(
            gcs_client=mock_gcs,
            stitched_audio_bucket="test-bucket",
            export_m4a_fn=mock_export_m4a,
        )

        request = FlushRequest(
            feed_id="feed1",
            time_range=TimeRange(start_ms=1000, end_ms=2000),
            buffer=MagicMock(),
            contributing_audio_uris=[],
        )

        processed_audio = MagicMock()
        flac_bytes = b"flac-bytes"

        canonical_uri, playback_uri = uploader.upload_derivatives(
            request, processed_audio, flac_bytes
        )

        self.assertEqual(
            canonical_uri,
            "gs://test-bucket/stitched/lossless/feed1/1970/01/01/19700101T000001Z.flac",
        )
        self.assertEqual(
            playback_uri,
            "gs://test-bucket/stitched/playback/feed1/1970/01/01/19700101T000001Z.m4a",
        )

        mock_gcs.bucket.assert_called_with("test-bucket")
        self.assertEqual(mock_bucket.blob.call_count, 2)
        self.assertEqual(mock_blob.upload_from_string.call_count, 2)
        mock_export_m4a.assert_called_with(processed_audio)

    def test_upload_derivatives_disabled(self) -> None:
        """Tests that uploading is skipped if bucket name is empty."""
        uploader = AudioUploader(
            gcs_client=None,
            stitched_audio_bucket=None,
            export_m4a_fn=MagicMock(),
        )

        request = MagicMock(spec=FlushRequest)
        canonical_uri, playback_uri = uploader.upload_derivatives(
            request, MagicMock(), b"flac"
        )

        self.assertIsNone(canonical_uri)
        self.assertIsNone(playback_uri)

    def test_upload_derivatives_error_no_client(self) -> None:
        """Tests that an error is raised if bucket is configured but client is missing."""
        uploader = AudioUploader(
            gcs_client=None,
            stitched_audio_bucket="test-bucket",
            export_m4a_fn=MagicMock(),
        )

        request = FlushRequest(
            feed_id="feed1",
            time_range=TimeRange(start_ms=1000, end_ms=2000),
            buffer=MagicMock(),
            contributing_audio_uris=[],
        )

        with self.assertRaises(RuntimeError):
            uploader.upload_derivatives(request, MagicMock(), b"flac")

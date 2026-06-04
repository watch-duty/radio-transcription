import unittest
from unittest.mock import MagicMock

from google.api_core.exceptions import PreconditionFailed

from backend.pipeline.common.storage.gcs_uploader import GCSAudioUploader


class GCSAudioUploaderTest(unittest.TestCase):
    """Tests the GCSAudioUploader class in isolation."""

    def test_upload_bytes_success(self) -> None:
        """Tests that raw bytes are uploaded correctly to GCS."""
        mock_gcs = MagicMock()
        mock_bucket = MagicMock()
        mock_gcs.bucket.return_value = mock_bucket
        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob

        uploader = GCSAudioUploader(gcs_client=mock_gcs)

        uri = uploader.upload_bytes(
            data=b"test-data",
            bucket_name="test-bucket",
            destination_path="path/to/obj.txt",
            content_type="text/plain",
        )

        self.assertEqual(uri, "gs://test-bucket/path/to/obj.txt")
        mock_gcs.bucket.assert_called_with("test-bucket")
        mock_bucket.blob.assert_called_with("path/to/obj.txt")
        mock_blob.upload_from_string.assert_called_once_with(
            b"test-data", content_type="text/plain", if_generation_match=0
        )

    def test_upload_bytes_precondition_failed(self) -> None:
        """Tests that PreconditionFailed (412) is caught and treated as success."""
        mock_gcs = MagicMock()
        mock_bucket = MagicMock()
        mock_gcs.bucket.return_value = mock_bucket
        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob

        # Mock upload_from_string to raise PreconditionFailed
        mock_blob.upload_from_string.side_effect = PreconditionFailed(
            "412 Precondition Failed"
        )

        uploader = GCSAudioUploader(gcs_client=mock_gcs)

        uri = uploader.upload_bytes(
            data=b"test-data",
            bucket_name="test-bucket",
            destination_path="path/to/obj.txt",
            content_type="text/plain",
        )

        self.assertEqual(uri, "gs://test-bucket/path/to/obj.txt")
        mock_gcs.bucket.assert_called_with("test-bucket")
        mock_bucket.blob.assert_called_with("path/to/obj.txt")
        mock_blob.upload_from_string.assert_called_once_with(
            b"test-data", content_type="text/plain", if_generation_match=0
        )

    def test_upload_audio_derivatives_success(self) -> None:
        """Tests that both FLAC and M4A audio derivatives are uploaded successfully."""
        mock_gcs = MagicMock()
        mock_bucket = MagicMock()
        mock_gcs.bucket.return_value = mock_bucket
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_bucket.blob.return_value = mock_blob

        mock_export_m4a = MagicMock(return_value=b"m4a-bytes")

        uploader = GCSAudioUploader(gcs_client=mock_gcs)

        processed_audio = MagicMock()
        flac_bytes = b"flac-bytes"

        canonical_uri, playback_uri = uploader.upload_audio_derivatives(
            bucket_name="test-bucket",
            flac_path="stitched/lossless/f1.flac",
            m4a_path="stitched/playback/f1.m4a",
            flac_bytes=flac_bytes,
            processed_audio=processed_audio,
            export_m4a_fn=mock_export_m4a,
        )

        self.assertEqual(
            canonical_uri, "gs://test-bucket/stitched/lossless/f1.flac"
        )
        self.assertEqual(
            playback_uri, "gs://test-bucket/stitched/playback/f1.m4a"
        )

        # 1 call for exists() check, and 2 calls inside upload_bytes
        self.assertEqual(mock_bucket.blob.call_count, 3)
        mock_blob.upload_from_string.assert_any_call(
            flac_bytes, content_type="audio/flac", if_generation_match=0
        )
        mock_blob.upload_from_string.assert_any_call(
            b"m4a-bytes", content_type="audio/mp4", if_generation_match=0
        )
        mock_export_m4a.assert_called_with(processed_audio)

    def test_upload_audio_derivatives_skipped_if_exists(self) -> None:
        """Tests that both export and uploads are skipped if FLAC already exists."""
        mock_gcs = MagicMock()
        mock_bucket = MagicMock()
        mock_gcs.bucket.return_value = mock_bucket
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_bucket.blob.return_value = mock_blob

        mock_export_m4a = MagicMock()

        uploader = GCSAudioUploader(gcs_client=mock_gcs)

        processed_audio = MagicMock()
        flac_bytes = b"flac-bytes"

        canonical_uri, playback_uri = uploader.upload_audio_derivatives(
            bucket_name="test-bucket",
            flac_path="stitched/lossless/f1.flac",
            m4a_path="stitched/playback/f1.m4a",
            flac_bytes=flac_bytes,
            processed_audio=processed_audio,
            export_m4a_fn=mock_export_m4a,
        )

        self.assertEqual(
            canonical_uri, "gs://test-bucket/stitched/lossless/f1.flac"
        )
        self.assertEqual(
            playback_uri, "gs://test-bucket/stitched/playback/f1.m4a"
        )

        # 1 call for exists() check, none for upload_bytes
        self.assertEqual(mock_bucket.blob.call_count, 1)
        mock_blob.upload_from_string.assert_not_called()
        mock_export_m4a.assert_not_called()

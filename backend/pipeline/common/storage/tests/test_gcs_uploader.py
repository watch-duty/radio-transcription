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

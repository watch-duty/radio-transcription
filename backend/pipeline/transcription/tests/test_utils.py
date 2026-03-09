import unittest
from unittest.mock import MagicMock, patch

from utils import get_gcs_client, read_sad_segments_from_gcs

from backend.pipeline.schema_types.sad_metadata_pb2 import (
    SadMetadata,
)


class TestUtils(unittest.TestCase):
    @patch("utils.storage.Client")
    def test_get_gcs_client(self, mock_client: MagicMock) -> None:
        client = get_gcs_client()
        self.assertIsNotNone(client)
        mock_client.assert_called_once()

    @patch("utils.get_gcs_client")
    def test_read_sad_segments_from_gcs_success(
        self, mock_get_client: MagicMock
    ) -> None:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_blob = MagicMock()
        mock_bucket.get_blob.return_value = mock_blob

        sad_metadata = SadMetadata()
        seg1 = sad_metadata.segments.add()
        seg1.start_sec = 0.5
        seg1.end_sec = 1.5

        mock_blob.download_as_bytes.return_value = sad_metadata.SerializeToString()

        segments = read_sad_segments_from_gcs("gs://test-bucket/test.sad.pb")
        self.assertEqual(len(segments), 1)
        self.assertEqual(segments[0], (0.5, 1.5))
        mock_client.bucket.assert_called_once_with("test-bucket")
        mock_bucket.get_blob.assert_called_once_with("test.sad.pb")

    @patch("utils.get_gcs_client")
    def test_read_sad_segments_from_gcs_not_found(
        self, mock_get_client: MagicMock
    ) -> None:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.get_blob.return_value = None

        with self.assertRaises(FileNotFoundError):
            read_sad_segments_from_gcs("gs://test-bucket/missing.sad.pb")


if __name__ == "__main__":
    unittest.main()

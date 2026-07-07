import unittest
from unittest.mock import MagicMock

from backend.services.echo.storage import EchoStorageBackend


class TestEchoStorageBackend(unittest.TestCase):
    def test_directory_prefix_exists(self) -> None:
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        # list_blobs returns a non-empty list for prefix checking
        mock_client.list_blobs.side_effect = [["dummy_blob"]]

        backend = EchoStorageBackend(mock_client, "my-bucket")
        result = backend.verify_directory_exists("my-feed")

        self.assertTrue(result)
        mock_client.list_blobs.assert_called_once_with(
            mock_bucket, prefix="my-feed/", max_results=1
        )

    def test_exact_blob_exists(self) -> None:
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        # first list_blobs returns empty, second returns exact_blob
        mock_client.list_blobs.side_effect = [[], ["dummy_blob"]]

        backend = EchoStorageBackend(mock_client, "my-bucket")
        result = backend.verify_directory_exists("my-feed")

        self.assertTrue(result)
        self.assertEqual(mock_client.list_blobs.call_count, 2)

    def test_not_exists(self) -> None:
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        # both return empty
        mock_client.list_blobs.side_effect = [[], []]

        backend = EchoStorageBackend(mock_client, "my-bucket")
        result = backend.verify_directory_exists("my-feed")

        self.assertFalse(result)

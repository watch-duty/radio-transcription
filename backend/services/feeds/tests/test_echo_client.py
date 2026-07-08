from __future__ import annotations

import unittest
from unittest import mock

from backend.services.feeds.echo_client import EchoClient


class TestEchoClient(unittest.IsolatedAsyncioTestCase):
    """Unit tests for EchoClient."""

    def setUp(self) -> None:
        self.mock_storage_client = mock.MagicMock()

    def test_is_configured_true(self) -> None:
        client = EchoClient(self.mock_storage_client, bucket_name="test-bucket")
        self.assertTrue(client.is_configured)

    def test_is_configured_false_empty(self) -> None:
        client = EchoClient(self.mock_storage_client, bucket_name="")
        self.assertFalse(client.is_configured)

    def test_is_configured_false_none(self) -> None:
        with mock.patch.dict("os.environ", {}, clear=True):
            client = EchoClient(self.mock_storage_client, bucket_name=None)
            self.assertFalse(client.is_configured)

    async def test_verify_directory_exists_by_prefix(self) -> None:
        mock_bucket = mock.MagicMock()
        self.mock_storage_client.bucket.return_value = mock_bucket
        # first list_blobs returns a blob
        self.mock_storage_client.list_blobs.side_effect = [["dummy-blob"]]

        client = EchoClient(self.mock_storage_client, bucket_name="test-bucket")
        result = await client.verify_directory_exists("my-feed")

        self.assertTrue(result)
        self.mock_storage_client.bucket.assert_called_once_with("test-bucket")
        self.assertEqual(self.mock_storage_client.list_blobs.call_count, 1)

    async def test_verify_directory_not_exists(self) -> None:
        mock_bucket = mock.MagicMock()
        self.mock_storage_client.bucket.return_value = mock_bucket
        # list_blobs returns empty
        self.mock_storage_client.list_blobs.return_value = []

        client = EchoClient(self.mock_storage_client, bucket_name="test-bucket")
        result = await client.verify_directory_exists("my-feed")

        self.assertFalse(result)
        self.assertEqual(self.mock_storage_client.list_blobs.call_count, 1)

    async def test_verify_directory_raises_if_not_configured(self) -> None:
        with mock.patch.dict("os.environ", {}, clear=True):
            client = EchoClient(self.mock_storage_client, bucket_name=None)
            with self.assertRaises(ValueError) as ctx:
                await client.verify_directory_exists("my-feed")
            self.assertIn("bucket name is not configured", str(ctx.exception))

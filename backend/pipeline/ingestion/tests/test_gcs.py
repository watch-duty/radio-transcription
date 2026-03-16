import base64
import unittest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

from google.protobuf.duration_pb2 import Duration  # type: ignore

from backend.pipeline.ingestion import gcs
from backend.pipeline.schema_types.sed_metadata_pb2 import SedMetadata, SoundEvent
from backend.pipeline.storage.feed_store import LeasedFeed


def _make_feed(source_type: str, feed_id: int) -> LeasedFeed:
    return LeasedFeed(
        id=uuid.UUID(int=feed_id),
        name=f"test-{source_type}-{feed_id}",
        source_type=source_type,
        last_processed_filename=None,
        stream_url=None,
    )


class TestUploadAudio(unittest.IsolatedAsyncioTestCase):
    """Test suite for the upload_audio function."""

    def setUp(self) -> None:
        """Reset module-level globals before each test."""
        gcs._session = None
        gcs._storage = None

    def tearDown(self) -> None:
        """Cleanup after each test."""
        # Reset module-level globals
        gcs._session = None
        gcs._storage = None

    @patch("backend.pipeline.ingestion.gcs.Storage")
    @patch("backend.pipeline.ingestion.gcs.aiohttp.ClientSession")
    @patch("backend.pipeline.ingestion.gcs.datetime")
    async def test_upload_audio_with_sed_metadata(
        self,
        mock_datetime: MagicMock,
        mock_session_class: MagicMock,
        mock_storage_class: MagicMock,
    ) -> None:
        """Test upload includes serialized SED metadata when provided."""
        # Arrange
        mock_datetime.datetime.now.return_value.strftime.return_value = (
            "20260305T120000Z"
        )
        mock_storage = AsyncMock()
        mock_storage_class.return_value = mock_storage

        audio_chunk = b"\x00\x01" * 100
        feed_id = uuid.UUID(int=1234)
        feed = _make_feed("bcfy_feeds", 1234)
        bucket = "test-bucket"
        chunk_seq = 42
        sound_event = SoundEvent(
            start_time=Duration(seconds=1, nanos=500_000_000),
            duration=Duration(seconds=2),
        )
        sed_metadata = SedMetadata(source_chunk_id="chunk-123")
        sed_metadata.sound_events.append(sound_event)

        # Act
        result = await gcs.upload_audio(
            audio_chunk,
            feed,
            bucket,
            chunk_seq,
            sed_metadata=sed_metadata,
        )

        # Assert
        expected_object_name = f"bcfy_feeds/{feed_id}/20260305T120000Z_42.flac"
        expected_path = f"gs://{bucket}/{expected_object_name}"

        mock_storage.upload.assert_called_once_with(
            bucket,
            expected_object_name,
            audio_chunk,
            metadata={
                "sed_metadata": base64.b64encode(
                    sed_metadata.SerializeToString()
                ).decode("ascii"),
            },
        )
        self.assertEqual(result, expected_path)

    @patch("backend.pipeline.ingestion.gcs.Storage")
    @patch("backend.pipeline.ingestion.gcs.aiohttp.ClientSession")
    @patch("backend.pipeline.ingestion.gcs.datetime")
    async def test_upload_audio_success(
        self,
        mock_datetime: MagicMock,
        mock_session_class: MagicMock,
        mock_storage_class: MagicMock,
    ) -> None:
        """Test successful upload of audio chunk to GCS."""
        # Arrange
        mock_datetime.datetime.now.return_value.strftime.return_value = (
            "20260305T120000Z"
        )
        mock_storage = AsyncMock()
        mock_storage_class.return_value = mock_storage

        audio_chunk = b"\x00\x01" * 1000
        feed_id = uuid.UUID(int=1234)
        feed = _make_feed("bcfy_feeds", 1234)
        bucket = "test-bucket"
        chunk_seq = 5

        # Act
        result = await gcs.upload_audio(audio_chunk, feed, bucket, chunk_seq)

        # Assert
        expected_object_name = f"bcfy_feeds/{feed_id}/20260305T120000Z_5.flac"
        expected_path = f"gs://{bucket}/{expected_object_name}"

        mock_storage.upload.assert_called_once_with(
            bucket, expected_object_name, audio_chunk
        )
        self.assertEqual(result, expected_path)

    @patch("backend.pipeline.ingestion.gcs.Storage")
    @patch("backend.pipeline.ingestion.gcs.aiohttp.ClientSession")
    @patch("backend.pipeline.ingestion.gcs.datetime")
    async def test_upload_audio_empty_chunk(
        self,
        mock_datetime: MagicMock,
        mock_session_class: MagicMock,
        mock_storage_class: MagicMock,
    ) -> None:
        """Test upload with empty audio chunk (edge case)."""
        # Arrange
        mock_datetime.datetime.now.return_value.strftime.return_value = (
            "20260305T120000Z"
        )
        mock_storage = AsyncMock()
        mock_storage_class.return_value = mock_storage

        audio_chunk = b""
        feed_id = uuid.UUID(int=5678)
        feed = _make_feed("echo_feeds", 5678)
        bucket = "test-bucket"
        chunk_seq = 0

        # Act
        result = await gcs.upload_audio(audio_chunk, feed, bucket, chunk_seq)

        # Assert
        expected_object_name = f"echo_feeds/{feed_id}/20260305T120000Z_0.flac"
        expected_path = f"gs://{bucket}/{expected_object_name}"

        mock_storage.upload.assert_called_once_with(
            bucket, expected_object_name, audio_chunk
        )
        self.assertEqual(result, expected_path)

    @patch("backend.pipeline.ingestion.gcs.Storage")
    @patch("backend.pipeline.ingestion.gcs.aiohttp.ClientSession")
    @patch("backend.pipeline.ingestion.gcs.datetime")
    async def test_upload_audio_storage_exception(
        self,
        mock_datetime: MagicMock,
        mock_session_class: MagicMock,
        mock_storage_class: MagicMock,
    ) -> None:
        """Test handling of GCS upload failure."""
        # Arrange
        mock_datetime.datetime.now.return_value.strftime.return_value = (
            "20260305T120000Z"
        )
        mock_storage = AsyncMock()
        mock_storage.upload.side_effect = Exception("GCS upload failed")
        mock_storage_class.return_value = mock_storage

        audio_chunk = b"\x00\x01" * 1000
        feed = _make_feed("bcfy_feeds", 1234)
        bucket = "test-bucket"
        chunk_seq = 5

        # Act & Assert
        with self.assertRaises(Exception) as context:
            await gcs.upload_audio(audio_chunk, feed, bucket, chunk_seq)

        self.assertIn("GCS upload failed", str(context.exception))
        mock_storage.upload.assert_called_once()

    @patch("backend.pipeline.ingestion.gcs.Storage")
    @patch("backend.pipeline.ingestion.gcs.aiohttp.ClientSession")
    async def test_upload_audio_reuses_storage_client(
        self,
        mock_session_class: MagicMock,
        mock_storage_class: MagicMock,
    ) -> None:
        """Test that subsequent uploads reuse the same Storage client."""
        # Arrange
        mock_storage = AsyncMock()
        mock_storage_class.return_value = mock_storage

        audio_chunk = b"\x00\x01" * 100
        feed = _make_feed("bcfy_feeds", 1234)
        bucket = "test-bucket"

        # Act - Upload twice
        await gcs.upload_audio(audio_chunk, feed, bucket, 1)
        await gcs.upload_audio(audio_chunk, feed, bucket, 2)

        # Assert - Storage client should only be created once
        self.assertEqual(mock_storage_class.call_count, 1)
        self.assertEqual(mock_storage.upload.call_count, 2)

    @patch("backend.pipeline.ingestion.gcs.Storage")
    @patch("backend.pipeline.ingestion.gcs.aiohttp.ClientSession")
    @patch("backend.pipeline.ingestion.gcs.datetime")
    async def test_upload_audio_high_sequence_number(
        self,
        mock_datetime: MagicMock,
        mock_session_class: MagicMock,
        mock_storage_class: MagicMock,
    ) -> None:
        """Test upload with very high sequence number (edge case)."""
        # Arrange
        mock_datetime.datetime.now.return_value.strftime.return_value = (
            "20260305T120000Z"
        )
        mock_storage = AsyncMock()
        mock_storage_class.return_value = mock_storage

        audio_chunk = b"\x00\x01" * 100
        feed_id = uuid.UUID(int=1234)
        feed = _make_feed("bcfy_feeds", 1234)
        bucket = "test-bucket"
        chunk_seq = 999999999

        # Act
        result = await gcs.upload_audio(audio_chunk, feed, bucket, chunk_seq)

        # Assert
        expected_object_name = f"bcfy_feeds/{feed_id}/20260305T120000Z_999999999.flac"
        expected_path = f"gs://{bucket}/{expected_object_name}"

        self.assertEqual(result, expected_path)


class TestCloseClient(unittest.IsolatedAsyncioTestCase):
    """Test suite for the close_client function."""

    def setUp(self) -> None:
        """Reset module-level globals before each test."""
        gcs._session = None
        gcs._storage = None

    def tearDown(self) -> None:
        """Cleanup after each test."""
        gcs._session = None
        gcs._storage = None

    async def test_close_client_when_clients_exist(self) -> None:
        """Test closing clients when both storage and session exist."""
        # Arrange
        mock_session = AsyncMock()
        mock_storage = AsyncMock()

        gcs._session = mock_session
        gcs._storage = mock_storage

        # Act
        await gcs.close_client()

        # Assert
        mock_storage.close.assert_called_once()
        mock_session.close.assert_called_once()
        self.assertIsNone(gcs._storage)
        self.assertIsNone(gcs._session)

    async def test_close_client_when_clients_are_none(self) -> None:
        """Test closing clients when they are already None (edge case)."""
        # Arrange
        gcs._session = None
        gcs._storage = None

        # Act
        await gcs.close_client()

        # Assert - Should not raise any exceptions
        self.assertIsNone(gcs._storage)
        self.assertIsNone(gcs._session)

    async def test_close_client_only_storage_exists(self) -> None:
        """Test closing when only storage exists (edge case)."""
        # Arrange
        mock_storage = AsyncMock()
        gcs._storage = mock_storage
        gcs._session = None

        # Act
        await gcs.close_client()

        # Assert
        mock_storage.close.assert_called_once()
        self.assertIsNone(gcs._storage)
        self.assertIsNone(gcs._session)

    async def test_close_client_only_session_exists(self) -> None:
        """Test closing when only session exists (edge case)."""
        # Arrange
        mock_session = AsyncMock()
        gcs._session = mock_session
        gcs._storage = None

        # Act
        await gcs.close_client()

        # Assert
        mock_session.close.assert_called_once()
        self.assertIsNone(gcs._storage)
        self.assertIsNone(gcs._session)

    async def test_close_client_storage_close_exception(self) -> None:
        """Test handling of exception during storage close."""
        # Arrange
        mock_session = AsyncMock()
        mock_storage = AsyncMock()
        mock_storage.close.side_effect = Exception("Storage close failed")

        gcs._session = mock_session
        gcs._storage = mock_storage

        # Act & Assert
        with self.assertRaises(Exception) as context:
            await gcs.close_client()

        self.assertIn("Storage close failed", str(context.exception))
        mock_storage.close.assert_called_once()
        # Session close should not be called due to exception
        mock_session.close.assert_not_called()

    async def test_close_client_session_close_exception(self) -> None:
        """Test handling of exception during session close."""
        # Arrange
        mock_session = AsyncMock()
        mock_session.close.side_effect = Exception("Session close failed")
        mock_storage = AsyncMock()

        gcs._session = mock_session
        gcs._storage = mock_storage

        # Act & Assert
        with self.assertRaises(Exception) as context:
            await gcs.close_client()

        self.assertIn("Session close failed", str(context.exception))
        mock_storage.close.assert_called_once()
        mock_session.close.assert_called_once()

    async def test_close_client_multiple_calls(self) -> None:
        """Test that calling close_client multiple times is safe."""
        # Arrange
        mock_session = AsyncMock()
        mock_storage = AsyncMock()

        gcs._session = mock_session
        gcs._storage = mock_storage

        # Act
        await gcs.close_client()
        await gcs.close_client()  # Second call with None clients

        # Assert
        mock_storage.close.assert_called_once()
        mock_session.close.assert_called_once()
        self.assertIsNone(gcs._storage)
        self.assertIsNone(gcs._session)


if __name__ == "__main__":
    unittest.main()

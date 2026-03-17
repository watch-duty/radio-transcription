import base64
import datetime
import unittest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

from google.protobuf.duration_pb2 import Duration  # type: ignore

from backend.pipeline.ingestion import gcp_helper
from backend.pipeline.schema_types.raw_audio_chunk_pb2 import AudioChunk
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
        gcp_helper._session = None
        gcp_helper._storage = None

    def tearDown(self) -> None:
        """Cleanup after each test."""
        # Reset module-level globals
        gcp_helper._session = None
        gcp_helper._storage = None

    @patch("backend.pipeline.ingestion.gcp_helper.Storage")
    @patch("backend.pipeline.ingestion.gcp_helper.aiohttp.ClientSession")
    @patch("backend.pipeline.ingestion.gcp_helper.datetime")
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
        result = await gcp_helper.upload_audio(
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

    @patch("backend.pipeline.ingestion.gcp_helper.Storage")
    @patch("backend.pipeline.ingestion.gcp_helper.aiohttp.ClientSession")
    @patch("backend.pipeline.ingestion.gcp_helper.datetime")
    async def test_upload_audio_success(
        self,
        mock_datetime: MagicMock,
        mock_session_class: MagicMock,
        mock_storage_class: MagicMock,
    ) -> None:
        """Test successful upload of audio chunk to gcp_helper."""
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
        result = await gcp_helper.upload_audio(audio_chunk, feed, bucket, chunk_seq)

        # Assert
        expected_object_name = f"bcfy_feeds/{feed_id}/20260305T120000Z_5.flac"
        expected_path = f"gs://{bucket}/{expected_object_name}"

        mock_storage.upload.assert_called_once_with(
            bucket, expected_object_name, audio_chunk, metadata=None
        )
        self.assertEqual(result, expected_path)

    @patch("backend.pipeline.ingestion.gcp_helper.Storage")
    @patch("backend.pipeline.ingestion.gcp_helper.aiohttp.ClientSession")
    @patch("backend.pipeline.ingestion.gcp_helper.datetime")
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
        result = await gcp_helper.upload_audio(audio_chunk, feed, bucket, chunk_seq)

        # Assert
        expected_object_name = f"echo_feeds/{feed_id}/20260305T120000Z_0.flac"
        expected_path = f"gs://{bucket}/{expected_object_name}"

        mock_storage.upload.assert_called_once_with(
            bucket, expected_object_name, audio_chunk, metadata=None
        )
        self.assertEqual(result, expected_path)

    @patch("backend.pipeline.ingestion.gcp_helper.Storage")
    @patch("backend.pipeline.ingestion.gcp_helper.aiohttp.ClientSession")
    @patch("backend.pipeline.ingestion.gcp_helper.datetime")
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
            await gcp_helper.upload_audio(audio_chunk, feed, bucket, chunk_seq)

        self.assertIn("GCS upload failed", str(context.exception))
        mock_storage.upload.assert_called_once()

    @patch("backend.pipeline.ingestion.gcp_helper.Storage")
    @patch("backend.pipeline.ingestion.gcp_helper.aiohttp.ClientSession")
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
        await gcp_helper.upload_audio(audio_chunk, feed, bucket, 1)
        await gcp_helper.upload_audio(audio_chunk, feed, bucket, 2)

        # Assert - Storage client should only be created once
        self.assertEqual(mock_storage_class.call_count, 1)
        self.assertEqual(mock_storage.upload.call_count, 2)

    @patch("backend.pipeline.ingestion.gcp_helper.Storage")
    @patch("backend.pipeline.ingestion.gcp_helper.aiohttp.ClientSession")
    @patch("backend.pipeline.ingestion.gcp_helper.datetime")
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
        result = await gcp_helper.upload_audio(audio_chunk, feed, bucket, chunk_seq)

        # Assert
        expected_object_name = f"bcfy_feeds/{feed_id}/20260305T120000Z_999999999.flac"
        expected_path = f"gs://{bucket}/{expected_object_name}"

        self.assertEqual(result, expected_path)

    @patch("backend.pipeline.ingestion.gcp_helper.Storage")
    @patch("backend.pipeline.ingestion.gcp_helper.aiohttp.ClientSession")
    @patch("backend.pipeline.ingestion.gcp_helper.datetime")
    async def test_upload_audio_metadata_too_large_raises(
        self,
        mock_datetime: MagicMock,
        mock_session_class: MagicMock,
        mock_storage_class: MagicMock,
    ) -> None:
        """Test upload raises when serialized metadata exceeds GCS metadata size limit."""
        # Arrange
        mock_datetime.datetime.now.return_value.strftime.return_value = (
            "20260305T120000Z"
        )
        mock_storage = AsyncMock()
        mock_storage_class.return_value = mock_storage

        audio_chunk = b"\x00\x01" * 100
        feed = _make_feed("bcfy_feeds", 1234)
        bucket = "test-bucket"
        chunk_seq = 9
        # A large proto payload that guarantees the base64 metadata exceeds 8 KiB.
        sed_metadata = SedMetadata(source_chunk_id="x" * 10000)

        # Act & Assert
        with self.assertRaises(ValueError) as context:
            await gcp_helper.upload_audio(
                audio_chunk,
                feed,
                bucket,
                chunk_seq,
                sed_metadata=sed_metadata,
            )

        self.assertIn("Metadata size", str(context.exception))
        self.assertIn("exceeds GCS limit", str(context.exception))
        mock_storage.upload.assert_not_called()


class TestPublishAudioChunk(unittest.IsolatedAsyncioTestCase):
    """Test suite for the publish_audio_chunk function."""

    def setUp(self) -> None:
        """Reset module-level publisher before each test."""
        gcp_helper._publisher = None

    def tearDown(self) -> None:
        """Cleanup after each test."""
        gcp_helper._publisher = None

    @patch(
        "backend.pipeline.ingestion.gcp_helper.asyncio.to_thread",
        new_callable=AsyncMock,
    )
    @patch("backend.pipeline.ingestion.gcp_helper.pubsub_v1.PublisherClient")
    @patch("backend.pipeline.ingestion.gcp_helper.datetime")
    async def test_publish_audio_chunk_sets_timestamp_and_ordering_key(
        self,
        mock_datetime: MagicMock,
        mock_publisher_class: MagicMock,
        mock_to_thread: AsyncMock,
    ) -> None:
        """Publish serializes AudioChunk, sets timestamp, and uses feed ordering."""
        mock_now = datetime.datetime(2026, 3, 5, 12, 0, tzinfo=datetime.UTC)
        mock_datetime.datetime.now.return_value = mock_now
        mock_future = MagicMock()
        mock_future.result.return_value = "message-123"
        mock_publisher = MagicMock()
        mock_publisher.publish.return_value = mock_future
        mock_publisher_class.return_value = mock_publisher
        mock_to_thread.return_value = "message-123"

        result = await gcp_helper.publish_audio_chunk(
            topic_path="projects/test/topics/audio",
            feed_id="feed-42",
            gcs_uri="gs://bucket/audio.flac",
        )

        self.assertEqual(result, "message-123")
        mock_publisher.publish.assert_called_once()
        publish_args, publish_kwargs = mock_publisher.publish.call_args
        self.assertEqual(publish_args[0], "projects/test/topics/audio")
        self.assertEqual(publish_kwargs["feed_id"], "feed-42")
        self.assertEqual(publish_kwargs["ordering_key"], "feed-42")

        chunk = AudioChunk()
        chunk.ParseFromString(publish_args[1])
        self.assertEqual(chunk.gcs_uri, "gs://bucket/audio.flac")
        self.assertTrue(chunk.HasField("start_timestamp"))
        self.assertEqual(chunk.start_timestamp.seconds, int(mock_now.timestamp()))
        mock_to_thread.assert_awaited_once_with(mock_future.result)

    @patch(
        "backend.pipeline.ingestion.gcp_helper.asyncio.to_thread",
        new_callable=AsyncMock,
    )
    @patch("backend.pipeline.ingestion.gcp_helper.pubsub_v1.PublisherClient")
    async def test_publish_audio_chunk_reuses_shared_publisher(
        self,
        mock_publisher_class: MagicMock,
        mock_to_thread: AsyncMock,
    ) -> None:
        """Subsequent publishes reuse the same PublisherClient instance."""
        mock_publisher = MagicMock()
        mock_publisher.publish.return_value = MagicMock()
        mock_publisher_class.return_value = mock_publisher
        mock_to_thread.side_effect = ["message-1", "message-2"]

        first_result = await gcp_helper.publish_audio_chunk(
            topic_path="projects/test/topics/audio",
            feed_id="feed-1",
            gcs_uri="gs://bucket/one.flac",
        )
        second_result = await gcp_helper.publish_audio_chunk(
            topic_path="projects/test/topics/audio",
            feed_id="feed-2",
            gcs_uri="gs://bucket/two.flac",
        )

        self.assertEqual(first_result, "message-1")
        self.assertEqual(second_result, "message-2")
        self.assertEqual(mock_publisher_class.call_count, 1)
        self.assertEqual(mock_publisher.publish.call_count, 2)


class TestCloseClient(unittest.IsolatedAsyncioTestCase):
    """Test suite for the close_client function."""

    def setUp(self) -> None:
        """Reset module-level globals before each test."""
        gcp_helper._session = None
        gcp_helper._storage = None
        gcp_helper._publisher = None

    def tearDown(self) -> None:
        """Cleanup after each test."""
        gcp_helper._session = None
        gcp_helper._storage = None
        gcp_helper._publisher = None

    async def test_close_client_when_clients_exist(self) -> None:
        """Test closing clients when both storage and session exist."""
        # Arrange
        mock_session = AsyncMock()
        mock_storage = AsyncMock()
        mock_publisher = MagicMock()

        gcp_helper._session = mock_session
        gcp_helper._storage = mock_storage
        gcp_helper._publisher = mock_publisher

        # Act
        await gcp_helper.close_client()

        # Assert
        mock_publisher.stop.assert_called_once()
        mock_storage.close.assert_called_once()
        mock_session.close.assert_called_once()
        self.assertIsNone(gcp_helper._publisher)
        self.assertIsNone(gcp_helper._storage)
        self.assertIsNone(gcp_helper._session)

    async def test_close_client_only_publisher_exists(self) -> None:
        """Test closing when only publisher exists."""
        mock_publisher = MagicMock()
        gcp_helper._publisher = mock_publisher

        await gcp_helper.close_client()

        mock_publisher.stop.assert_called_once()
        self.assertIsNone(gcp_helper._publisher)

    async def test_close_client_when_clients_are_none(self) -> None:
        """Test closing clients when they are already None (edge case)."""
        # Arrange
        gcp_helper._session = None
        gcp_helper._storage = None

        # Act
        await gcp_helper.close_client()

        # Assert - Should not raise any exceptions
        self.assertIsNone(gcp_helper._storage)
        self.assertIsNone(gcp_helper._session)

    async def test_close_client_only_storage_exists(self) -> None:
        """Test closing when only storage exists (edge case)."""
        # Arrange
        mock_storage = AsyncMock()
        gcp_helper._storage = mock_storage
        gcp_helper._session = None

        # Act
        await gcp_helper.close_client()

        # Assert
        mock_storage.close.assert_called_once()
        self.assertIsNone(gcp_helper._storage)
        self.assertIsNone(gcp_helper._session)

    async def test_close_client_only_session_exists(self) -> None:
        """Test closing when only session exists (edge case)."""
        # Arrange
        mock_session = AsyncMock()
        gcp_helper._session = mock_session
        gcp_helper._storage = None

        # Act
        await gcp_helper.close_client()

        # Assert
        mock_session.close.assert_called_once()
        self.assertIsNone(gcp_helper._storage)
        self.assertIsNone(gcp_helper._session)

    async def test_close_client_storage_close_exception(self) -> None:
        """Test handling of exception during storage close."""
        # Arrange
        mock_session = AsyncMock()
        mock_storage = AsyncMock()
        mock_storage.close.side_effect = Exception("Storage close failed")

        gcp_helper._session = mock_session
        gcp_helper._storage = mock_storage

        # Act & Assert
        with self.assertRaises(Exception) as context:
            await gcp_helper.close_client()

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

        gcp_helper._session = mock_session
        gcp_helper._storage = mock_storage

        # Act & Assert
        with self.assertRaises(Exception) as context:
            await gcp_helper.close_client()

        self.assertIn("Session close failed", str(context.exception))
        mock_storage.close.assert_called_once()
        mock_session.close.assert_called_once()

    async def test_close_client_multiple_calls(self) -> None:
        """Test that calling close_client multiple times is safe."""
        # Arrange
        mock_session = AsyncMock()
        mock_storage = AsyncMock()

        gcp_helper._session = mock_session
        gcp_helper._storage = mock_storage

        # Act
        await gcp_helper.close_client()
        await gcp_helper.close_client()  # Second call with None clients

        # Assert
        mock_storage.close.assert_called_once()
        mock_session.close.assert_called_once()
        self.assertIsNone(gcp_helper._storage)
        self.assertIsNone(gcp_helper._session)


if __name__ == "__main__":
    unittest.main()

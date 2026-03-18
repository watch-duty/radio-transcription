import base64
import datetime
import unittest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

from google.protobuf.duration_pb2 import Duration  # type: ignore

from backend.pipeline.common.clients.gcs_client import GcsClient
from backend.pipeline.common.clients.pubsub_client import PubSubClient
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


def _make_gcs_client() -> tuple[MagicMock, AsyncMock]:
    """Create a mock GcsClient and its underlying Storage."""
    mock_gcs_client = MagicMock(spec=GcsClient)
    mock_storage = AsyncMock()
    mock_gcs_client.get_storage.return_value = mock_storage
    return mock_gcs_client, mock_storage


def _make_pubsub_client() -> tuple[MagicMock, MagicMock]:
    """Create a mock PubSubClient and its underlying publisher."""
    mock_pubsub_client = MagicMock(spec=PubSubClient)
    mock_publisher = MagicMock()
    mock_pubsub_client.get_publisher.return_value = mock_publisher
    return mock_pubsub_client, mock_publisher


class TestUploadAudio(unittest.IsolatedAsyncioTestCase):
    """Test suite for the upload_audio function."""

    @patch("backend.pipeline.ingestion.gcp_helper.datetime")
    async def test_upload_audio_with_sed_metadata(
        self,
        mock_datetime: MagicMock,
    ) -> None:
        """Test upload includes serialized SED metadata when provided."""
        # Arrange
        mock_datetime.datetime.now.return_value.strftime.return_value = (
            "20260305T120000Z"
        )
        mock_gcs_client, mock_storage = _make_gcs_client()

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
            mock_gcs_client,
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

    @patch("backend.pipeline.ingestion.gcp_helper.datetime")
    async def test_upload_audio_success(
        self,
        mock_datetime: MagicMock,
    ) -> None:
        """Test successful upload of audio chunk to GCS."""
        # Arrange
        mock_datetime.datetime.now.return_value.strftime.return_value = (
            "20260305T120000Z"
        )
        mock_gcs_client, mock_storage = _make_gcs_client()

        audio_chunk = b"\x00\x01" * 1000
        feed_id = uuid.UUID(int=1234)
        feed = _make_feed("bcfy_feeds", 1234)
        bucket = "test-bucket"
        chunk_seq = 5

        # Act
        result = await gcp_helper.upload_audio(
            mock_gcs_client, audio_chunk, feed, bucket, chunk_seq
        )

        # Assert
        expected_object_name = f"bcfy_feeds/{feed_id}/20260305T120000Z_5.flac"
        expected_path = f"gs://{bucket}/{expected_object_name}"

        mock_storage.upload.assert_called_once_with(
            bucket, expected_object_name, audio_chunk, metadata=None
        )
        self.assertEqual(result, expected_path)

    @patch("backend.pipeline.ingestion.gcp_helper.datetime")
    async def test_upload_audio_empty_chunk(
        self,
        mock_datetime: MagicMock,
    ) -> None:
        """Test upload with empty audio chunk (edge case)."""
        # Arrange
        mock_datetime.datetime.now.return_value.strftime.return_value = (
            "20260305T120000Z"
        )
        mock_gcs_client, mock_storage = _make_gcs_client()

        audio_chunk = b""
        feed_id = uuid.UUID(int=5678)
        feed = _make_feed("echo_feeds", 5678)
        bucket = "test-bucket"
        chunk_seq = 0

        # Act
        result = await gcp_helper.upload_audio(
            mock_gcs_client, audio_chunk, feed, bucket, chunk_seq
        )

        # Assert
        expected_object_name = f"echo_feeds/{feed_id}/20260305T120000Z_0.flac"
        expected_path = f"gs://{bucket}/{expected_object_name}"

        mock_storage.upload.assert_called_once_with(
            bucket, expected_object_name, audio_chunk, metadata=None
        )
        self.assertEqual(result, expected_path)

    @patch("backend.pipeline.ingestion.gcp_helper.datetime")
    async def test_upload_audio_storage_exception(
        self,
        mock_datetime: MagicMock,
    ) -> None:
        """Test handling of GCS upload failure."""
        # Arrange
        mock_datetime.datetime.now.return_value.strftime.return_value = (
            "20260305T120000Z"
        )
        mock_gcs_client, mock_storage = _make_gcs_client()
        mock_storage.upload.side_effect = Exception("GCS upload failed")

        audio_chunk = b"\x00\x01" * 1000
        feed = _make_feed("bcfy_feeds", 1234)
        bucket = "test-bucket"
        chunk_seq = 5

        # Act & Assert
        with self.assertRaises(Exception) as context:
            await gcp_helper.upload_audio(
                mock_gcs_client, audio_chunk, feed, bucket, chunk_seq
            )

        self.assertIn("GCS upload failed", str(context.exception))
        mock_storage.upload.assert_called_once()

    async def test_upload_audio_calls_get_storage_for_each_upload(
        self,
    ) -> None:
        """Test that upload_audio calls get_storage on the provided client."""
        mock_gcs_client, mock_storage = _make_gcs_client()

        audio_chunk = b"\x00\x01" * 100
        feed = _make_feed("bcfy_feeds", 1234)
        bucket = "test-bucket"

        # Act - Upload twice with the same client
        await gcp_helper.upload_audio(mock_gcs_client, audio_chunk, feed, bucket, 1)
        await gcp_helper.upload_audio(mock_gcs_client, audio_chunk, feed, bucket, 2)

        # Assert - Both uploads went through the storage returned by the client
        self.assertEqual(mock_storage.upload.call_count, 2)

    @patch("backend.pipeline.ingestion.gcp_helper.datetime")
    async def test_upload_audio_high_sequence_number(
        self,
        mock_datetime: MagicMock,
    ) -> None:
        """Test upload with very high sequence number (edge case)."""
        # Arrange
        mock_datetime.datetime.now.return_value.strftime.return_value = (
            "20260305T120000Z"
        )
        mock_gcs_client, _ = _make_gcs_client()

        audio_chunk = b"\x00\x01" * 100
        feed_id = uuid.UUID(int=1234)
        feed = _make_feed("bcfy_feeds", 1234)
        bucket = "test-bucket"
        chunk_seq = 999999999

        # Act
        result = await gcp_helper.upload_audio(
            mock_gcs_client, audio_chunk, feed, bucket, chunk_seq
        )

        # Assert
        expected_object_name = f"bcfy_feeds/{feed_id}/20260305T120000Z_999999999.flac"
        expected_path = f"gs://{bucket}/{expected_object_name}"

        self.assertEqual(result, expected_path)

    @patch("backend.pipeline.ingestion.gcp_helper.datetime")
    async def test_upload_audio_metadata_too_large_raises(
        self,
        mock_datetime: MagicMock,
    ) -> None:
        """Test upload raises when serialized metadata exceeds GCS metadata size limit."""
        # Arrange
        mock_datetime.datetime.now.return_value.strftime.return_value = (
            "20260305T120000Z"
        )
        mock_gcs_client, mock_storage = _make_gcs_client()

        audio_chunk = b"\x00\x01" * 100
        feed = _make_feed("bcfy_feeds", 1234)
        bucket = "test-bucket"
        chunk_seq = 9
        # A large proto payload that guarantees the base64 metadata exceeds 8 KiB.
        sed_metadata = SedMetadata(source_chunk_id="x" * 10000)

        # Act & Assert
        with self.assertRaises(ValueError) as context:
            await gcp_helper.upload_audio(
                mock_gcs_client,
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

    @patch(
        "backend.pipeline.ingestion.gcp_helper.asyncio.to_thread",
        new_callable=AsyncMock,
    )
    @patch("backend.pipeline.ingestion.gcp_helper.datetime")
    async def test_publish_audio_chunk_sets_timestamp_and_ordering_key(
        self,
        mock_datetime: MagicMock,
        mock_to_thread: AsyncMock,
    ) -> None:
        """Publish serializes AudioChunk, sets timestamp, and uses feed ordering."""
        mock_now = datetime.datetime(2026, 3, 5, 12, 0, tzinfo=datetime.UTC)
        mock_datetime.datetime.now.return_value = mock_now
        mock_datetime.UTC = datetime.UTC
        mock_future = MagicMock()
        mock_future.result.return_value = "message-123"
        mock_pubsub_client, mock_publisher = _make_pubsub_client()
        mock_publisher.publish.return_value = mock_future
        mock_to_thread.return_value = "message-123"

        result = await gcp_helper.publish_audio_chunk(
            mock_pubsub_client,
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
    async def test_publish_audio_chunk_returns_message_id(
        self,
        mock_to_thread: AsyncMock,
    ) -> None:
        """publish_audio_chunk returns the message ID from the publisher future."""
        mock_pubsub_client, mock_publisher = _make_pubsub_client()
        mock_publisher.publish.return_value = MagicMock()
        mock_to_thread.side_effect = ["message-1", "message-2"]

        first_result = await gcp_helper.publish_audio_chunk(
            mock_pubsub_client,
            topic_path="projects/test/topics/audio",
            feed_id="feed-1",
            gcs_uri="gs://bucket/one.flac",
        )
        second_result = await gcp_helper.publish_audio_chunk(
            mock_pubsub_client,
            topic_path="projects/test/topics/audio",
            feed_id="feed-2",
            gcs_uri="gs://bucket/two.flac",
        )

        self.assertEqual(first_result, "message-1")
        self.assertEqual(second_result, "message-2")
        self.assertEqual(mock_publisher.publish.call_count, 2)


if __name__ == "__main__":
    unittest.main()

import concurrent.futures
import datetime
import unittest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
from google.cloud.pubsub_v1.publisher.exceptions import (
    PublishToPausedOrderingKeyException,
)
from multidict import CIMultiDict, CIMultiDictProxy
from yarl import URL

from backend.pipeline.common import gcp_helper
from backend.pipeline.common.clients import gcs_client, pubsub_client
from backend.pipeline.schema_types.continuous_audio_pb2 import ContinuousAudio
from backend.pipeline.storage.feed_store import LeasedFeed, SourceType

_DUMMY_REQUEST_INFO = aiohttp.RequestInfo(
    url=URL("http://example.com"),
    method="POST",
    headers=CIMultiDictProxy(CIMultiDict()),
    real_url=URL("http://example.com"),
)


def _make_feed(
    source_type: SourceType, feed_id: int, fencing_token: int = 0
) -> LeasedFeed:
    return LeasedFeed(
        id=uuid.UUID(int=feed_id),
        name=f"test-{source_type}-{feed_id}",
        source_type=source_type,
        last_processed_filename=None,
        last_bookmark_time=None,
        fencing_token=fencing_token,
        failure_count=0,
        status_reason=None,
        source_feed_id=None,
    )


def _make_gcs_client() -> tuple[MagicMock, AsyncMock]:
    """Create a mock GcsClient and its underlying Storage."""
    mock_gcs_client = MagicMock(spec=gcs_client.GcsClient)
    mock_storage = AsyncMock()
    mock_gcs_client.get_storage.return_value = mock_storage
    return mock_gcs_client, mock_storage


def _make_pubsub_client() -> tuple[MagicMock, MagicMock]:
    """Create a mock PubSubClient and its underlying publisher."""
    mock_pubsub_client = MagicMock(spec=pubsub_client.PubSubClient)
    mock_publisher = MagicMock()
    mock_pubsub_client.get_publisher.return_value = mock_publisher
    return mock_pubsub_client, mock_publisher


class TestUploadStagedAudio(unittest.IsolatedAsyncioTestCase):
    """Test suite for the upload_staged_audio function."""

    @patch("backend.pipeline.common.gcp_helper.datetime")
    async def test_upload_staged_audio_success(
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
        feed = _make_feed(SourceType.BCFY_FEEDS, 1234)
        bucket = "test-bucket"
        chunk_seq = 5

        # Act
        result = await gcp_helper.upload_staged_audio(
            mock_gcs_client,
            audio_chunk,
            feed,
            bucket,
            chunk_seq,
        )

        # Assert
        expected_object_name = f"bcfy_feeds/{feed_id}/20260305T120000Z_5.flac"
        expected_path = f"gs://{bucket}/{expected_object_name}"

        mock_storage.upload.assert_called_once_with(
            bucket,
            expected_object_name,
            audio_chunk,
            metadata={"traceparent": ""},
            content_type="audio/flac",
        )
        self.assertEqual(result, expected_path)

    @patch("backend.pipeline.common.gcp_helper.datetime")
    async def test_upload_staged_audio_empty_chunk(
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
        feed = _make_feed(SourceType.BCFY_CALLS, 5678)
        bucket = "test-bucket"
        chunk_seq = 0

        # Act
        result = await gcp_helper.upload_staged_audio(
            mock_gcs_client,
            audio_chunk,
            feed,
            bucket,
            chunk_seq,
        )

        # Assert
        expected_object_name = f"bcfy_calls/{feed_id}/20260305T120000Z_0.flac"
        expected_path = f"gs://{bucket}/{expected_object_name}"

        mock_storage.upload.assert_called_once_with(
            bucket,
            expected_object_name,
            audio_chunk,
            metadata={"traceparent": ""},
            content_type="audio/flac",
        )
        self.assertEqual(result, expected_path)

    @patch("backend.pipeline.common.gcp_helper.datetime")
    async def test_upload_staged_audio_storage_exception(
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
        feed = _make_feed(SourceType.BCFY_FEEDS, 1234)
        bucket = "test-bucket"
        chunk_seq = 5

        # Act & Assert
        with self.assertRaises(Exception) as context:
            await gcp_helper.upload_staged_audio(
                mock_gcs_client,
                audio_chunk,
                feed,
                bucket,
                chunk_seq,
            )

        self.assertIn("GCS upload failed", str(context.exception))
        mock_storage.upload.assert_called_once()

    async def test_upload_staged_audio_calls_get_storage_for_each_upload(
        self,
    ) -> None:
        """Test that upload_staged_audio calls get_storage on the provided client."""
        mock_gcs_client, mock_storage = _make_gcs_client()

        audio_chunk = b"\x00\x01" * 100
        feed = _make_feed(SourceType.BCFY_FEEDS, 1234)
        bucket = "test-bucket"

        # Act - Upload twice with the same client
        await gcp_helper.upload_staged_audio(
            mock_gcs_client,
            audio_chunk,
            feed,
            bucket,
            1,
        )
        await gcp_helper.upload_staged_audio(
            mock_gcs_client,
            audio_chunk,
            feed,
            bucket,
            2,
        )

        # Assert - Both uploads went through the storage returned by the client
        self.assertEqual(mock_storage.upload.call_count, 2)

    @patch("backend.pipeline.common.gcp_helper.datetime")
    async def test_upload_staged_audio_high_sequence_number(
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
        feed = _make_feed(SourceType.BCFY_FEEDS, 1234)
        bucket = "test-bucket"
        chunk_seq = 999999999

        # Act
        result = await gcp_helper.upload_staged_audio(
            mock_gcs_client,
            audio_chunk,
            feed,
            bucket,
            chunk_seq,
        )

        # Assert
        expected_object_name = (
            f"bcfy_feeds/{feed_id}/20260305T120000Z_999999999.flac"
        )
        expected_path = f"gs://{bucket}/{expected_object_name}"

        self.assertEqual(result, expected_path)

    @patch("backend.pipeline.common.gcp_helper.datetime")
    async def test_upload_staged_audio_with_fencing_token(
        self,
        mock_datetime: MagicMock,
    ) -> None:
        """Fencing token produces a token-qualified path."""
        mock_datetime.datetime.now.return_value.strftime.return_value = (
            "20260305T120000Z"
        )
        mock_gcs_client, mock_storage = _make_gcs_client()
        feed_id = uuid.UUID(int=1234)
        feed = _make_feed(SourceType.BCFY_FEEDS, 1234, fencing_token=7)

        result = await gcp_helper.upload_staged_audio(
            mock_gcs_client,
            b"\x00\x01" * 100,
            feed,
            "test-bucket",
            42,
            fencing_token=7,
        )

        expected_object_name = (
            f"bcfy_feeds/{feed_id}/token-7/20260305T120000Z_42.flac"
        )
        expected_path = f"gs://test-bucket/{expected_object_name}"

        mock_storage.upload.assert_called_once_with(
            "test-bucket",
            expected_object_name,
            b"\x00\x01" * 100,
            metadata={"traceparent": ""},
            content_type="audio/flac",
            parameters={"ifGenerationMatch": "0"},
        )
        self.assertEqual(result, expected_path)


class TestUploadAudio(unittest.IsolatedAsyncioTestCase):
    """Tests for the upload_audio function."""

    async def test_upload_with_correct_bucket_and_object_name(self) -> None:
        mock_gcs_client, mock_storage = _make_gcs_client()

        audio = b"flac-audio-bytes"
        bucket = "canonical-bucket"
        object_name = "bcfy_feeds/abc/20260305T120000Z_0.flac"

        result = await gcp_helper.upload_audio(
            mock_gcs_client,
            audio,
            bucket,
            object_name,
        )

        mock_storage.upload.assert_called_once_with(
            bucket,
            object_name,
            audio,
            metadata={"traceparent": ""},
            content_type="audio/flac",
        )
        self.assertEqual(result, f"gs://{bucket}/{object_name}")

    async def test_upload_no_metadata_when_none(self) -> None:
        mock_gcs_client, mock_storage = _make_gcs_client()

        await gcp_helper.upload_audio(
            mock_gcs_client,
            b"audio",
            "bucket",
            "obj.flac",
        )

        call_kwargs = mock_storage.upload.call_args
        metadata = call_kwargs.kwargs.get("metadata") or call_kwargs[1].get(
            "metadata"
        )
        self.assertEqual(metadata, {"traceparent": ""})

    async def test_upload_with_if_generation_match(self) -> None:
        """ifGenerationMatch=0 is passed as parameters to storage.upload."""
        mock_gcs_client, mock_storage = _make_gcs_client()

        result = await gcp_helper.upload_audio(
            mock_gcs_client,
            b"audio",
            "bucket",
            "obj.flac",
            if_generation_match=0,
        )

        mock_storage.upload.assert_called_once_with(
            "bucket",
            "obj.flac",
            b"audio",
            metadata={"traceparent": ""},
            content_type="audio/flac",
            parameters={"ifGenerationMatch": "0"},
        )
        self.assertEqual(result, "gs://bucket/obj.flac")

    async def test_upload_412_treated_as_success(self) -> None:
        """412 Precondition Failed is treated as success when precondition set."""
        mock_gcs_client, mock_storage = _make_gcs_client()
        mock_storage.upload.side_effect = aiohttp.ClientResponseError(
            request_info=_DUMMY_REQUEST_INFO,
            history=(),
            status=412,
            message="Precondition Failed",
        )

        result = await gcp_helper.upload_audio(
            mock_gcs_client,
            b"audio",
            "bucket",
            "obj.flac",
            if_generation_match=0,
        )

        self.assertEqual(result, "gs://bucket/obj.flac")

    async def test_upload_412_without_precondition_raises(self) -> None:
        """412 propagates when if_generation_match is not set."""
        mock_gcs_client, mock_storage = _make_gcs_client()
        mock_storage.upload.side_effect = aiohttp.ClientResponseError(
            request_info=_DUMMY_REQUEST_INFO,
            history=(),
            status=412,
            message="Precondition Failed",
        )

        with self.assertRaises(aiohttp.ClientResponseError):
            await gcp_helper.upload_audio(
                mock_gcs_client,
                b"audio",
                "bucket",
                "obj.flac",
            )

    async def test_upload_non_412_error_raises_with_precondition(self) -> None:
        """Non-412 errors propagate even when if_generation_match is set."""
        mock_gcs_client, mock_storage = _make_gcs_client()
        mock_storage.upload.side_effect = aiohttp.ClientResponseError(
            request_info=_DUMMY_REQUEST_INFO,
            history=(),
            status=500,
            message="Internal Server Error",
        )

        with self.assertRaises(aiohttp.ClientResponseError) as ctx:
            await gcp_helper.upload_audio(
                mock_gcs_client,
                b"audio",
                "bucket",
                "obj.flac",
                if_generation_match=0,
            )

        self.assertEqual(ctx.exception.status, 500)


class TestPublishAudioChunkSync(unittest.TestCase):
    """Test suite for the synchronous publish_audio_chunk_sync function."""

    def test_sets_timestamp_ordering_key_and_attributes(self) -> None:
        mock_now = datetime.datetime(2026, 3, 5, 12, 0, tzinfo=datetime.UTC)
        mock_future = MagicMock()
        mock_future.result.return_value = "message-123"
        _, mock_publisher = _make_pubsub_client()
        mock_publisher.publish.return_value = mock_future

        result = gcp_helper.publish_audio_chunk_sync(
            mock_publisher,
            topic_path="projects/test/topics/audio",
            feed_id="feed-42",
            feed_name="Central Fire",
            gcs_uri="gs://bucket/audio.flac",
            session_id="test-session-1",
            start_timestamp=mock_now,
            duration_ms=15000,
            source_type="echo",
        )

        self.assertEqual(result, "message-123")
        mock_publisher.publish.assert_called_once()
        publish_args, publish_kwargs = mock_publisher.publish.call_args
        self.assertEqual(publish_args[0], "projects/test/topics/audio")
        self.assertEqual(publish_kwargs["source_type"], "echo")

        self.assertEqual(publish_kwargs["ordering_key"], "feed-42")

        chunk = ContinuousAudio()
        chunk.ParseFromString(publish_args[1])
        self.assertEqual(chunk.gcs_uri, "gs://bucket/audio.flac")
        self.assertEqual(chunk.feed_id, "feed-42")
        self.assertTrue(chunk.HasField("start_timestamp"))
        self.assertEqual(
            chunk.start_timestamp.seconds, int(mock_now.timestamp())
        )
        self.assertEqual(chunk.feed_name, "Central Fire")

    def test_omits_source_type_when_none(self) -> None:
        mock_future = MagicMock()
        mock_future.result.return_value = "msg-1"
        _, mock_publisher = _make_pubsub_client()
        mock_publisher.publish.return_value = mock_future

        gcp_helper.publish_audio_chunk_sync(
            mock_publisher,
            topic_path="projects/test/topics/audio",
            feed_id="feed-1",
            feed_name="Central Fire",
            gcs_uri="gs://bucket/audio.flac",
            session_id="sess-1",
            start_timestamp=datetime.datetime(
                2026, 3, 5, 12, 0, tzinfo=datetime.UTC
            ),
            duration_ms=15000,
        )

        publish_kwargs = mock_publisher.publish.call_args.kwargs
        self.assertNotIn("source_type", publish_kwargs)

    def test_injects_traceparent_attribute_sync(self) -> None:
        """Verifies that sync publish logic serializes active traceparent context."""
        mock_future = MagicMock()
        mock_future.result.return_value = "msg-1"
        _, mock_publisher = _make_pubsub_client()
        mock_publisher.publish.return_value = mock_future

        def mock_inject(carrier, context=None):
            carrier["traceparent"] = (
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
            )

        with patch(
            "opentelemetry.trace.propagation.tracecontext.TraceContextTextMapPropagator.inject",
            side_effect=mock_inject,
        ):
            gcp_helper.publish_audio_chunk_sync(
                mock_publisher,
                topic_path="projects/test/topics/audio",
                feed_id="feed-1",
                feed_name="Central Fire",
                gcs_uri="gs://bucket/audio.flac",
                session_id="sess-1",
                start_timestamp=datetime.datetime(
                    2026, 3, 5, 12, 0, tzinfo=datetime.UTC
                ),
                duration_ms=15000,
            )

        publish_kwargs = mock_publisher.publish.call_args.kwargs
        self.assertEqual(
            publish_kwargs.get("traceparent"),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        )


class TestPublishAudioChunk(unittest.IsolatedAsyncioTestCase):
    """Test suite for the async publish_audio_chunk wrapper."""

    async def test_injects_traceparent_attribute_async(self) -> None:
        """Verifies that async publish logic serializes active traceparent context."""
        mock_pubsub_client, mock_publisher = _make_pubsub_client()
        mock_now = datetime.datetime(2026, 3, 5, 12, 0, tzinfo=datetime.UTC)

        fut = concurrent.futures.Future()
        fut.set_result("message-123")
        mock_publisher.publish.return_value = fut

        def mock_inject(carrier, context=None):
            carrier["traceparent"] = (
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
            )

        with patch(
            "opentelemetry.trace.propagation.tracecontext.TraceContextTextMapPropagator.inject",
            side_effect=mock_inject,
        ):
            await gcp_helper.publish_audio_chunk(
                mock_pubsub_client,
                topic_path="projects/test/topics/audio",
                feed_id="feed-42",
                feed_name="Central Fire",
                gcs_uri="gs://bucket/audio.flac",
                session_id="test-session-1",
                start_timestamp=mock_now,
                duration_ms=15000,
            )

        publish_kwargs = mock_publisher.publish.call_args.kwargs
        self.assertEqual(
            publish_kwargs.get("traceparent"),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        )

    async def test_delegates_to_publish(self) -> None:
        """Async wrapper directly calls publisher.publish and uses wrap_future."""
        mock_pubsub_client, mock_publisher = _make_pubsub_client()
        mock_now = datetime.datetime(2026, 3, 5, 12, 0, tzinfo=datetime.UTC)

        fut = concurrent.futures.Future()
        fut.set_result("message-123")
        mock_publisher.publish.return_value = fut

        result = await gcp_helper.publish_audio_chunk(
            mock_pubsub_client,
            topic_path="projects/test/topics/audio",
            feed_id="feed-42",
            feed_name="Central Fire",
            gcs_uri="gs://bucket/audio.flac",
            session_id="test-session-1",
            start_timestamp=mock_now,
            duration_ms=15000,
        )

        self.assertEqual(result, "message-123")
        mock_publisher.publish.assert_called_once()
        publish_args, publish_kwargs = mock_publisher.publish.call_args
        chunk = ContinuousAudio()
        chunk.ParseFromString(publish_args[1])
        self.assertEqual(chunk.feed_name, "Central Fire")
        self.assertEqual(publish_kwargs["ordering_key"], "feed-42")

    async def test_paused_ordering_key_calls_resume_publish(self) -> None:
        """PublishToPausedOrderingKeyException triggers resume_publish before propagating raw."""
        mock_pubsub_client, mock_publisher = _make_pubsub_client()
        mock_now = datetime.datetime(2026, 4, 25, 12, 0, tzinfo=datetime.UTC)

        fut = concurrent.futures.Future()
        fut.set_exception(PublishToPausedOrderingKeyException("feed-42"))
        mock_publisher.publish.return_value = fut

        with self.assertRaises(PublishToPausedOrderingKeyException):
            await gcp_helper.publish_audio_chunk(
                mock_pubsub_client,
                topic_path="projects/test/topics/audio",
                feed_id="feed-42",
                feed_name="Central Fire",
                gcs_uri="gs://bucket/audio.flac",
                session_id="test-session-1",
                start_timestamp=mock_now,
                duration_ms=15000,
            )

        mock_publisher.resume_publish.assert_called_once_with(
            "projects/test/topics/audio",
            ordering_key="feed-42",
        )

    async def test_resume_publish_failure_swallowed(self) -> None:
        """RuntimeError or ValueError from resume_publish is swallowed; the original PausedOrderingKey exception still propagates."""
        mock_now = datetime.datetime(2026, 4, 25, 12, 0, tzinfo=datetime.UTC)

        for resume_exc in (
            RuntimeError("publisher stopped"),
            ValueError("unseen key"),
        ):
            with self.subTest(resume_exc=type(resume_exc).__name__):
                mock_pubsub_client, mock_publisher = _make_pubsub_client()
                fut = concurrent.futures.Future()
                fut.set_exception(
                    PublishToPausedOrderingKeyException("feed-42")
                )
                mock_publisher.publish.return_value = fut
                mock_publisher.resume_publish.side_effect = resume_exc

                with self.assertRaises(PublishToPausedOrderingKeyException):
                    await gcp_helper.publish_audio_chunk(
                        mock_pubsub_client,
                        topic_path="projects/test/topics/audio",
                        feed_id="feed-42",
                        feed_name="Central Fire",
                        gcs_uri="gs://bucket/audio.flac",
                        session_id="test-session-1",
                        start_timestamp=mock_now,
                        duration_ms=15000,
                    )

                mock_publisher.resume_publish.assert_called_once()


class TestParseGcsUri(unittest.TestCase):
    """Tests for the parse_gcs_uri function."""

    def test_valid_uri(self) -> None:
        bucket, obj = gcp_helper.parse_gcs_uri(
            "gs://my-bucket/path/to/file.flac"
        )
        self.assertEqual(bucket, "my-bucket")
        self.assertEqual(obj, "path/to/file.flac")

    def test_bucket_only(self) -> None:
        bucket, obj = gcp_helper.parse_gcs_uri("gs://my-bucket")
        self.assertEqual(bucket, "my-bucket")
        self.assertEqual(obj, "")

    def test_rejects_non_gs_scheme(self) -> None:
        with self.assertRaises(ValueError):
            gcp_helper.parse_gcs_uri("https://bucket/obj")

    def test_rejects_embedded_gs_prefix(self) -> None:
        with self.assertRaises(ValueError):
            gcp_helper.parse_gcs_uri("https://evil.com/gs://bucket/obj")


class TestDownloadAudio(unittest.IsolatedAsyncioTestCase):
    """Tests for the download_audio function."""

    async def test_download_parses_uri_and_returns_bytes(self) -> None:
        mock_gcs_client, mock_storage = _make_gcs_client()
        mock_storage.download = AsyncMock(return_value=b"audio-data")

        result = await gcp_helper.download_audio(
            mock_gcs_client,
            "gs://my-bucket/path/to/audio.flac",
        )

        mock_storage.download.assert_called_once_with(
            "my-bucket", "path/to/audio.flac"
        )
        self.assertEqual(result, b"audio-data")

    async def test_download_rejects_non_gs_uri(self) -> None:
        mock_gcs_client, _ = _make_gcs_client()

        with self.assertRaises(ValueError) as ctx:
            await gcp_helper.download_audio(
                mock_gcs_client,
                "https://example.com/file.flac",
            )

        self.assertIn("Expected gs:// URI", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()

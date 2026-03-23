from __future__ import annotations

import base64
import datetime
import os
import unittest
from unittest import mock

import numpy as np
from cloudevents.http import CloudEvent

from backend.pipeline.detection.types import (
    CombinedResult,
    SpeechRegion,
)
from backend.pipeline.schema_types.raw_audio_chunk_pb2 import AudioChunk

# Patch module-level initialization before importing the handler.
with (
    mock.patch("google.cloud.logging.Client"),
    mock.patch(
        "backend.pipeline.common.clients.gcs_client.GcsClient",
    ),
    mock.patch(
        "backend.pipeline.common.clients.pubsub_client.PubSubClient",
    ),
    mock.patch.dict(os.environ, {"LOCAL_DEV": "1"}),
):
    from backend.pipeline.detection import normalization_handler
    from backend.pipeline.detection.normalization_handler import normalize as _wrapped

# The aio.cloud_event decorator wraps the async function; unwrap it
# so tests can await the original coroutine directly.
normalize = _wrapped.__wrapped__  # type: ignore[union-attr]

_CE_ATTRS = {
    "type": "google.cloud.pubsub.topic.v1.messagePublished",
    "source": "//pubsub.googleapis.com/projects/test/topics/audio",
}

_DOWNLOAD = "backend.pipeline.detection.normalization_handler.download_audio"
_UPLOAD = "backend.pipeline.detection.normalization_handler.upload_audio"
_PUBLISH = "backend.pipeline.detection.normalization_handler.publish_audio_chunk"


def _make_cloud_event(
    gcs_uri: str = "gs://staging/feeds/abc/audio.flac",
    feed_id: str = "feed-42",
    start_timestamp: datetime.datetime | None = None,
    session_id: str = "test-session",
) -> CloudEvent:
    """Build a CloudEvent with a serialized AudioChunk."""
    if start_timestamp is None:
        start_timestamp = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    chunk = AudioChunk(gcs_uri=gcs_uri, session_id=session_id)
    chunk.start_timestamp.FromDatetime(start_timestamp)
    encoded = base64.b64encode(chunk.SerializeToString()).decode()
    data = {
        "message": {
            "data": encoded,
            "attributes": {"feed_id": feed_id},
        },
    }
    return CloudEvent(_CE_ATTRS, data)


def _empty_combined() -> CombinedResult:
    return CombinedResult(speech_regions=())


def _speech_combined() -> CombinedResult:
    return CombinedResult(
        speech_regions=(
            SpeechRegion(start_sec=0.0, end_sec=1.0, detector_type="test"),
        ),
    )


class TestNormalize(unittest.IsolatedAsyncioTestCase):
    """Tests for the normalize Cloud Function."""

    async def test_empty_message_returns_normally(self) -> None:
        event = CloudEvent(_CE_ATTRS, {"message": {}})
        result = await normalize(event)
        self.assertIsNone(result)

    async def test_empty_gcs_uri_returns_normally(self) -> None:
        chunk = AudioChunk(gcs_uri="")
        chunk.start_timestamp.FromDatetime(
            datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        )
        encoded = base64.b64encode(chunk.SerializeToString()).decode()
        event = CloudEvent(_CE_ATTRS, {"message": {"data": encoded}})
        result = await normalize(event)
        self.assertIsNone(result)

    async def test_malformed_base64_returns_normally(self) -> None:
        event = CloudEvent(
            _CE_ATTRS,
            {"message": {"data": "not-valid-base64!!!"}},
        )
        result = await normalize(event)
        self.assertIsNone(result)

    @mock.patch(_DOWNLOAD, new_callable=mock.AsyncMock)
    async def test_download_failure_raises(self, mock_download) -> None:
        mock_download.side_effect = Exception("Network error")

        with self.assertRaises(Exception, msg="Network error"):
            await normalize(_make_cloud_event())

    @mock.patch(_DOWNLOAD, new_callable=mock.AsyncMock)
    async def test_flac_decode_failure_returns_normally(self, mock_download) -> None:
        mock_download.return_value = b"flac-bytes"

        with mock.patch.object(
            normalization_handler,
            "_decode_flac",
            side_effect=RuntimeError("Bad FLAC"),
        ):
            result = await normalize(_make_cloud_event())

        self.assertIsNone(result)

    @mock.patch(_DOWNLOAD, new_callable=mock.AsyncMock)
    async def test_wrong_sample_rate_returns_normally(self, mock_download) -> None:
        mock_download.return_value = b"flac-bytes"

        with mock.patch.object(
            normalization_handler,
            "_decode_flac",
            return_value=(np.zeros(32000, dtype=np.int16), 32000),
        ):
            result = await normalize(_make_cloud_event())

        self.assertIsNone(result)

    @mock.patch(_UPLOAD, new_callable=mock.AsyncMock)
    @mock.patch.object(
        normalization_handler,
        "_decode_flac",
        return_value=(np.zeros(16000, dtype=np.int16), 16000),
    )
    @mock.patch(_DOWNLOAD, new_callable=mock.AsyncMock)
    async def test_no_detectors_uploads_empty_sidecar(
        self, mock_download, mock_decode, mock_upload
    ) -> None:
        mock_download.return_value = b"flac-bytes"

        with mock.patch.dict(os.environ, {"INGESTION_CANONICAL_BUCKET": "canonical"}):
            result = await normalize(_make_cloud_event())

        self.assertIsNone(result)
        mock_upload.assert_called_once()

    @mock.patch(
        _UPLOAD,
        new_callable=mock.AsyncMock,
        side_effect=Exception("Upload failed"),
    )
    @mock.patch.object(
        normalization_handler,
        "_decode_flac",
        return_value=(np.zeros(16000, dtype=np.int16), 16000),
    )
    @mock.patch(_DOWNLOAD, new_callable=mock.AsyncMock)
    async def test_upload_failure_raises(
        self, mock_download, mock_decode, mock_upload
    ) -> None:
        mock_download.return_value = b"flac-bytes"

        with (
            mock.patch.dict(os.environ, {"INGESTION_CANONICAL_BUCKET": "canonical"}),
            self.assertRaises(Exception, msg="Upload failed"),
        ):
            await normalize(_make_cloud_event())

    @mock.patch(_PUBLISH, new_callable=mock.AsyncMock, return_value="msg-123")
    @mock.patch(_UPLOAD, new_callable=mock.AsyncMock)
    @mock.patch.object(normalization_handler, "_executor")
    @mock.patch.object(
        normalization_handler,
        "_decode_flac",
        return_value=(np.zeros(16000, dtype=np.int16), 16000),
    )
    @mock.patch(_DOWNLOAD, new_callable=mock.AsyncMock)
    async def test_speech_detected_publishes_to_transcription(
        self, mock_download, mock_decode, mock_executor, mock_upload, mock_publish
    ) -> None:
        mock_download.return_value = b"flac-bytes"
        mock_executor.run.return_value = _speech_combined()

        with mock.patch.dict(
            os.environ,
            {
                "INGESTION_CANONICAL_BUCKET": "canonical",
                "TRANSCRIPTION_TOPIC_PATH": "projects/p/topics/t",
            },
        ):
            await normalize(_make_cloud_event())

        mock_publish.assert_called_once()

    @mock.patch(_PUBLISH, new_callable=mock.AsyncMock, return_value="msg-456")
    @mock.patch(_UPLOAD, new_callable=mock.AsyncMock)
    @mock.patch.object(normalization_handler, "_executor")
    @mock.patch.object(
        normalization_handler,
        "_decode_flac",
        return_value=(np.zeros(16000, dtype=np.int16), 16000),
    )
    @mock.patch(_DOWNLOAD, new_callable=mock.AsyncMock)
    async def test_publish_forwards_original_capture_timestamp(
        self, mock_download, mock_decode, mock_executor, mock_upload, mock_publish
    ) -> None:
        mock_download.return_value = b"flac-bytes"
        mock_executor.run.return_value = _speech_combined()

        capture_time = datetime.datetime(2026, 3, 5, 12, 0, tzinfo=datetime.UTC)

        with mock.patch.dict(
            os.environ,
            {
                "INGESTION_CANONICAL_BUCKET": "canonical",
                "TRANSCRIPTION_TOPIC_PATH": "projects/p/topics/t",
            },
        ):
            await normalize(_make_cloud_event(start_timestamp=capture_time))

        mock_publish.assert_called_once()
        call_kwargs = mock_publish.call_args
        self.assertEqual(call_kwargs.kwargs.get("start_timestamp"), capture_time)

    @mock.patch(
        _PUBLISH,
        new_callable=mock.AsyncMock,
        side_effect=Exception("Pub/Sub error"),
    )
    @mock.patch(_UPLOAD, new_callable=mock.AsyncMock)
    @mock.patch.object(normalization_handler, "_executor")
    @mock.patch.object(
        normalization_handler,
        "_decode_flac",
        return_value=(np.zeros(16000, dtype=np.int16), 16000),
    )
    @mock.patch(_DOWNLOAD, new_callable=mock.AsyncMock)
    async def test_publish_failure_still_returns_normally(
        self, mock_download, mock_decode, mock_executor, mock_upload, mock_publish
    ) -> None:
        mock_download.return_value = b"flac-bytes"
        mock_executor.run.return_value = _speech_combined()

        with mock.patch.dict(
            os.environ,
            {
                "INGESTION_CANONICAL_BUCKET": "canonical",
                "TRANSCRIPTION_TOPIC_PATH": "projects/p/topics/t",
            },
        ):
            result = await normalize(_make_cloud_event())

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()

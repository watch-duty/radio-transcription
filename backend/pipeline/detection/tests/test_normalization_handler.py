from __future__ import annotations

import base64
import datetime
import io
import os
import subprocess
import unittest
from unittest import mock

import numpy as np
import soundfile as sf
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


class TestDecodeFlac(unittest.IsolatedAsyncioTestCase):
    """Test _decode_flac handles various FLAC formats including streaming."""

    @classmethod
    def _make_streaming_flac(cls, samples: np.ndarray, sample_rate: int) -> bytes:
        """Create a streaming FLAC (total_samples=0) using ffmpeg.

        Mimics what the icecast collector produces: pipe raw PCM through
        ffmpeg FLAC encoder via pipe, producing a FLAC with total_samples=0
        in STREAMINFO (ffmpeg cannot seek back to update the header).
        """
        raw_pcm = samples.tobytes()
        result = subprocess.run(
            [
                "ffmpeg",
                "-nostdin",
                "-f",
                "s16le",
                "-ar",
                str(sample_rate),
                "-ac",
                "1",
                "-i",
                "pipe:0",
                "-c:a",
                "flac",
                "-compression_level",
                "0",
                "-f",
                "flac",
                "pipe:1",
            ],
            input=raw_pcm,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            msg = f"Failed to create test FLAC: {result.stderr.decode()}"
            raise RuntimeError(msg)
        return result.stdout

    async def test_decode_streaming_flac(self) -> None:
        """Verify _decode_flac handles FLAC with total_samples=0.

        This test would have caught the original libsndfile bug during CI.
        """
        original = np.sin(np.arange(16000, dtype=np.float32) / 16000 * 2 * np.pi * 440)
        original_int16 = (original * 32767).astype(np.int16)
        flac_bytes = self._make_streaming_flac(original_int16, 16000)
        samples, sr = await normalization_handler._decode_flac(flac_bytes)
        self.assertEqual(sr, 16000)
        self.assertEqual(len(samples), len(original_int16))
        np.testing.assert_array_equal(samples, original_int16)

    async def test_decode_valid_flac(self) -> None:
        """Verify _decode_flac works with standard FLAC (soundfile-produced)."""
        original = (
            np.sin(np.arange(16000, dtype=np.float32) / 16000 * 2 * np.pi * 440) * 32767
        ).astype(np.int16)
        buf = io.BytesIO()
        sf.write(buf, original, 16000, format="FLAC", subtype="PCM_16")
        samples, sr = await normalization_handler._decode_flac(buf.getvalue())
        self.assertEqual(sr, 16000)
        self.assertEqual(len(samples), len(original))
        np.testing.assert_array_equal(samples, original)

    async def test_decode_invalid_data_raises(self) -> None:
        """Verify _decode_flac raises RuntimeError with ffmpeg stderr on garbage input."""
        with self.assertRaises(RuntimeError) as ctx:
            await normalization_handler._decode_flac(b"not a flac file")
        self.assertIn("ffmpeg FLAC decode failed", str(ctx.exception))
        self.assertIn("exit", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()

"""End-to-end integration tests for the detection/normalization handler.

Uses fake-gcs-server (Docker) for real GCS operations and the actual
SpectralFlatnessDetector for audio analysis. Pub/Sub is mocked.
"""

from __future__ import annotations

import base64
import io
import json
import os
import unittest
from unittest import mock

import docker
import numpy as np
import requests as sync_requests
import soundfile as sf
from cloudevents.http import CloudEvent
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from backend.pipeline.common.clients.gcs_client import GcsClient
from backend.pipeline.detection.detector_executor import DetectorExecutor
from backend.pipeline.detection.detector_factory import DetectorFactory
from backend.pipeline.ingestion.gcp_helper import upload_normalized_audio
from backend.pipeline.schema_types.raw_audio_chunk_pb2 import AudioChunk
from backend.pipeline.schema_types.sed_metadata_pb2 import SedMetadata  # noqa: TC001

# ---------------------------------------------------------------------------
# Module-level patching: mock Pub/Sub and logging, configure real detector
# ---------------------------------------------------------------------------
_DETECTOR_CONFIG = json.dumps(
    {"detectors": [{"type": "spectral_flatness", "threshold": 0.4}]}
)
_RAW_BUCKET = "test-raw-bucket"
_CANONICAL_BUCKET = "canonical-test-bucket"
_TOPIC_PATH = "projects/test/topics/transcription"

with (
    mock.patch("google.cloud.logging.Client"),
    mock.patch("backend.pipeline.common.clients.pubsub_client.PubSubClient"),
    mock.patch.dict(
        os.environ,
        {
            "LOCAL_DEV": "1",
            "DETECTOR_CONFIG": _DETECTOR_CONFIG,
            "INGESTION_CANONICAL_BUCKET": _CANONICAL_BUCKET,
            "TRANSCRIPTION_TOPIC_PATH": _TOPIC_PATH,
        },
    ),
):
    from backend.pipeline.detection import normalization_handler
    from backend.pipeline.detection.normalization_handler import normalize as _wrapped

normalize = _wrapped.__wrapped__  # type: ignore[union-attr]

_UPLOAD = "backend.pipeline.detection.normalization_handler.upload_normalized_audio"
_PUBLISH = "backend.pipeline.detection.normalization_handler.publish_audio_chunk"

_CE_ATTRS = {
    "type": "google.cloud.pubsub.topic.v1.messagePublished",
    "source": "//pubsub.googleapis.com/projects/test/topics/audio",
}

_FAKE_GCS_PORT = 4443


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _docker_available() -> bool:
    try:
        docker.from_env().ping()
    except Exception:
        return False
    return True


def _make_flac(samples: np.ndarray, sample_rate: int = 16_000) -> bytes:
    """Encode int16 numpy array to FLAC bytes."""
    buf = io.BytesIO()
    sf.write(buf, samples, sample_rate, format="FLAC", subtype="PCM_16")
    return buf.getvalue()


def _make_tone(freq_hz: float, duration_sec: float, sr: int = 16_000) -> np.ndarray:
    """Generate a pure sine tone as int16."""
    t = np.arange(int(sr * duration_sec), dtype=np.float32) / sr
    audio = np.sin(2 * np.pi * freq_hz * t) * 0.5
    return (audio * 32767).astype(np.int16)


def _make_noise(duration_sec: float, sr: int = 16_000, seed: int = 42) -> np.ndarray:
    """Generate white noise as int16."""
    rng = np.random.default_rng(seed)
    audio = rng.standard_normal(int(sr * duration_sec)).astype(np.float32) * 0.1
    return (audio * 32767).astype(np.int16)


def _make_cloud_event(
    gcs_uri: str,
    feed_id: str = "test-feed",
) -> CloudEvent:
    """Build a CloudEvent wrapping a serialized AudioChunk."""
    chunk = AudioChunk(gcs_uri=gcs_uri)
    encoded = base64.b64encode(chunk.SerializeToString()).decode()
    data = {
        "message": {
            "data": encoded,
            "attributes": {"feed_id": feed_id},
        },
    }
    return CloudEvent(_CE_ATTRS, data)


# ---------------------------------------------------------------------------
# Integration test class
# ---------------------------------------------------------------------------


@unittest.skipUnless(_docker_available(), "Docker is not available")
class TestNormalizationHandlerIntegration(unittest.IsolatedAsyncioTestCase):
    """E2E tests: real GCS (fake-gcs-server), real detector, mocked Pub/Sub."""

    gcs_container: DockerContainer

    @classmethod
    def setUpClass(cls) -> None:
        cls.gcs_container = (
            DockerContainer("fsouza/fake-gcs-server")
            .with_exposed_ports(_FAKE_GCS_PORT)
            .with_command(f"-scheme http -port {_FAKE_GCS_PORT}")
        )
        cls.gcs_container.start()
        wait_for_logs(cls.gcs_container, "server started at")

        cls._gcs_host = cls.gcs_container.get_container_host_ip()
        cls._gcs_port = int(cls.gcs_container.get_exposed_port(_FAKE_GCS_PORT))
        cls._gcs_url = f"http://{cls._gcs_host}:{cls._gcs_port}"

        # Create buckets
        for bucket in (_RAW_BUCKET, _CANONICAL_BUCKET):
            resp = sync_requests.post(
                f"{cls._gcs_url}/storage/v1/b",
                json={"name": bucket},
            )
            resp.raise_for_status()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.gcs_container.stop()

    async def asyncSetUp(self) -> None:
        os.environ["STORAGE_EMULATOR_HOST"] = self._gcs_url
        os.environ["INGESTION_CANONICAL_BUCKET"] = _CANONICAL_BUCKET
        os.environ["TRANSCRIPTION_TOPIC_PATH"] = _TOPIC_PATH
        self._gcs_client = GcsClient()
        normalization_handler._gcs_client = self._gcs_client
        # Ensure the real detector ensemble is used regardless of import order
        config = json.loads(_DETECTOR_CONFIG)
        detectors, combiner = DetectorFactory.create_ensemble(config)
        normalization_handler._executor = DetectorExecutor(detectors, combiner)

    async def asyncTearDown(self) -> None:
        await self._gcs_client.close()
        os.environ.pop("STORAGE_EMULATOR_HOST", None)
        os.environ.pop("INGESTION_CANONICAL_BUCKET", None)
        os.environ.pop("TRANSCRIPTION_TOPIC_PATH", None)

    # -- Helpers --

    async def _upload_raw(self, object_name: str, data: bytes) -> str:
        """Upload bytes to the raw bucket and return the gs:// URI."""
        storage = self._gcs_client.get_storage()
        await storage.upload(_RAW_BUCKET, object_name, data)
        return f"gs://{_RAW_BUCKET}/{object_name}"

    async def _download_canonical(self, object_name: str) -> bytes:
        """Download bytes from the canonical bucket."""
        storage = self._gcs_client.get_storage()
        return await storage.download(_CANONICAL_BUCKET, object_name)

    def _make_upload_spy(self) -> tuple[mock.AsyncMock, list[SedMetadata]]:
        """Create a spy that captures sed_metadata while performing the real upload."""
        captured: list[SedMetadata] = []

        async def _spy(*args, **kwargs):
            sed = kwargs.get("sed_metadata") or (args[4] if len(args) > 4 else None)
            if sed is not None:
                captured.append(sed)
            return await upload_normalized_audio(*args, **kwargs)

        return mock.AsyncMock(side_effect=_spy), captured

    # -- Tests --

    @mock.patch(_PUBLISH, new_callable=mock.AsyncMock, return_value="msg-001")
    async def test_speech_detected_uploads_sidecar_and_publishes(
        self,
        mock_publish,
    ) -> None:
        """1kHz tone → detected as speech → sidecar + publish."""
        object_name = "feeds/tone-feed/chunk_001.flac"
        tone = _make_tone(1000.0, 2.0)
        flac_bytes = _make_flac(tone)
        gcs_uri = await self._upload_raw(object_name, flac_bytes)

        upload_spy, captured_sidecars = self._make_upload_spy()
        event = _make_cloud_event(gcs_uri, feed_id="tone-feed")
        with mock.patch(_UPLOAD, upload_spy):
            await normalize(event)

        # Audio uploaded to canonical bucket
        canonical_audio = await self._download_canonical(object_name)
        self.assertEqual(canonical_audio, flac_bytes)

        # SED sidecar has speech events
        self.assertEqual(len(captured_sidecars), 1)
        sed = captured_sidecars[0]
        self.assertEqual(
            sed.source_chunk_id,
            f"gs://{_CANONICAL_BUCKET}/{object_name}",
        )
        self.assertGreater(len(sed.sound_events), 0)

        # Publish called with correct arguments
        mock_publish.assert_called_once()
        call_args = mock_publish.call_args
        self.assertEqual(call_args[0][1], _TOPIC_PATH)
        self.assertEqual(call_args[0][2], "tone-feed")
        self.assertEqual(
            call_args[0][3],
            f"gs://{_CANONICAL_BUCKET}/{object_name}",
        )

    @mock.patch(_PUBLISH, new_callable=mock.AsyncMock)
    async def test_noise_uploads_empty_sidecar_no_publish(
        self,
        mock_publish,
    ) -> None:
        """White noise → no speech → empty sidecar, no publish."""
        object_name = "feeds/noise-feed/chunk_001.flac"
        noise = _make_noise(2.0)
        flac_bytes = _make_flac(noise)
        gcs_uri = await self._upload_raw(object_name, flac_bytes)

        upload_spy, captured_sidecars = self._make_upload_spy()
        event = _make_cloud_event(gcs_uri, feed_id="noise-feed")
        with mock.patch(_UPLOAD, upload_spy):
            await normalize(event)

        # Audio still uploaded to canonical bucket
        canonical_audio = await self._download_canonical(object_name)
        self.assertEqual(canonical_audio, flac_bytes)

        # SED sidecar is empty (no speech)
        self.assertEqual(len(captured_sidecars), 1)
        self.assertEqual(len(captured_sidecars[0].sound_events), 0)

        # Publish NOT called
        mock_publish.assert_not_called()

    @mock.patch(_PUBLISH, new_callable=mock.AsyncMock)
    async def test_invalid_flac_returns_normally(
        self,
        mock_publish,
    ) -> None:
        """Garbage bytes → permanent failure → returns without uploading."""
        object_name = "feeds/bad-feed/chunk_001.flac"
        gcs_uri = await self._upload_raw(object_name, b"not-a-flac-file")

        event = _make_cloud_event(gcs_uri, feed_id="bad-feed")
        result = await normalize(event)

        self.assertIsNone(result)
        mock_publish.assert_not_called()

    @mock.patch(_PUBLISH, new_callable=mock.AsyncMock)
    async def test_wrong_sample_rate_returns_normally(
        self,
        mock_publish,
    ) -> None:
        """8kHz FLAC → rejected due to wrong sample rate."""
        object_name = "feeds/wrong-sr-feed/chunk_001.flac"
        tone_8k = _make_tone(440.0, 1.0, sr=8000)
        flac_bytes = _make_flac(tone_8k, sample_rate=8000)
        gcs_uri = await self._upload_raw(object_name, flac_bytes)

        event = _make_cloud_event(gcs_uri, feed_id="wrong-sr-feed")
        result = await normalize(event)

        self.assertIsNone(result)
        mock_publish.assert_not_called()

    @mock.patch(_PUBLISH, new_callable=mock.AsyncMock, return_value="msg-002")
    async def test_audio_bytes_roundtrip_exact_match(
        self,
        mock_publish,
    ) -> None:
        """Canonical audio bytes are byte-identical to the original FLAC."""
        object_name = "feeds/roundtrip-feed/chunk_001.flac"
        tone = _make_tone(440.0, 2.0)
        flac_bytes = _make_flac(tone)
        gcs_uri = await self._upload_raw(object_name, flac_bytes)

        event = _make_cloud_event(gcs_uri, feed_id="roundtrip-feed")
        await normalize(event)

        canonical_audio = await self._download_canonical(object_name)
        self.assertEqual(canonical_audio, flac_bytes)
        self.assertEqual(canonical_audio[:4], b"fLaC")


if __name__ == "__main__":
    unittest.main()

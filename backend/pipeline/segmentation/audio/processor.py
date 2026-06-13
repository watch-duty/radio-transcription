"""Segmentation-specific audio processing with neural Voice Activity Detection (VAD)."""

import io
import logging
import urllib.parse
from collections.abc import Callable

import av
import numpy as np
from google.cloud import storage

from backend.pipeline.common.constants import MS_PER_SECOND, SAMPLE_RATE_HZ
from backend.pipeline.segmentation.audio import vad
from backend.pipeline.segmentation.constants import (
    GCS_DOWNLOAD_TIMEOUT_SEC,
    MONO_CHANNEL_COUNT,
    PRIMARY_AUDIO_STREAM_INDEX,
)
from backend.pipeline.segmentation.datatypes import AudioChunkData

logger = logging.getLogger(__name__)


def get_vad_engine(config_json: str) -> vad.VoiceActivityDetector:
    """Creates a VoiceActivityDetector engine using optional JSON config."""
    import json  # noqa: PLC0415

    try:
        config = json.loads(config_json) if config_json else {}
    except Exception:
        config = {}
    return vad.VoiceActivityDetector(**config)


class SegmentationAudioProcessor:
    """A Voice Activity Detection (VAD) enabled AudioProcessor for Beam streaming segmentation.

    Encapsulates Silero VAD initialization, ONNX sessions, and chunk speech segment detection.
    """

    def __init__(
        self,
        vad_config: str = "{}",
        vad_instance: vad.VoiceActivityDetector | None = None,
        gcs_client_instance: storage.Client | None = None,
        vad_factory: Callable[[str], vad.VoiceActivityDetector] | None = None,
        gcs_factory: Callable[[], storage.Client] | None = None,
    ) -> None:
        self.vad_config = vad_config
        self.vad_factory = vad_factory
        self.vad = vad_instance
        self.gcs_client = gcs_client_instance
        self.gcs_factory = gcs_factory or storage.Client

    def setup(self) -> None:
        """Initializes the GCS Client and triggers VAD ONNX session warmup."""
        if self.gcs_client is None:
            self.gcs_client = self.gcs_factory()
        active_vad_factory = self.vad_factory or get_vad_engine
        if self.vad is None:
            self.vad = active_vad_factory(self.vad_config)
        self.vad.setup()

    def download_audio_and_detect(
        self,
        gcs_path: str,
        start_ms: int,
        duration_ms: int | None = None,
        prior_audio: bytes | None = None,
    ) -> AudioChunkData:
        """Downloads audio bytes from GCS and runs the speech segment detection natively."""
        if not self.gcs_client:
            msg = "GCS client not initialized. Call setup() first."
            raise RuntimeError(msg)
        if self.vad is None:
            msg = "VAD engine not initialized. Call setup() first."
            raise RuntimeError(msg)

        parsed_uri = urllib.parse.urlparse(gcs_path)
        bucket_name = parsed_uri.netloc
        blob_name = parsed_uri.path.lstrip("/")

        blob = self.gcs_client.bucket(bucket_name).get_blob(
            blob_name, timeout=GCS_DOWNLOAD_TIMEOUT_SEC
        )
        if not blob:
            err_msg = f"GCS object not found: {gcs_path}"
            logger.error(err_msg)
            raise FileNotFoundError(err_msg)

        in_mem_file = io.BytesIO()
        blob.download_to_file(in_mem_file, timeout=GCS_DOWNLOAD_TIMEOUT_SEC)
        in_mem_file.seek(0)

        samples, sr = self._decode_audio_in_memory(in_mem_file)

        speech_segments = []
        if len(samples) > 0:
            # VAD processing requires 16kHz mono
            samples_16k_mono = self._resample_to_16k_mono(samples, sr)
            prior_samples_16k = None
            if prior_audio is not None:
                prior_samples_16k = np.frombuffer(
                    prior_audio, dtype=np.int16
                )  # already 16kHz mono

            speech_segments = self.vad.detect_speech_segments(
                samples_16k_mono,
                sample_rate=SAMPLE_RATE_HZ,
                prior_audio=prior_samples_16k,
            )

        from backend.pipeline.schema_types import (  # noqa: PLC0415
            streaming_state as bp_state,
        )

        speech_segments_proto = [
            bp_state.TimeRangeProto(
                start_ms=int(s * MS_PER_SECOND),
                end_ms=int(e * MS_PER_SECOND),
            )
            for s, e in speech_segments
        ]

        duration_ms = duration_ms or int(len(samples) * MS_PER_SECOND / sr)

        return AudioChunkData(
            start_ms=start_ms,
            audio=samples,
            sample_rate=sr,
            speech_segments=speech_segments_proto,
            gcs_uri=gcs_path,
            duration_ms=duration_ms,
        )

    def _open_container(
        self, in_mem_file: io.BytesIO
    ) -> av.container.InputContainer:
        container = av.open(in_mem_file)
        if not isinstance(container, av.container.InputContainer):

            def _error() -> str:
                return f"Expected InputContainer from av.open, got {type(container).__name__}"

            raise TypeError(_error())
        return container

    def _decode_audio_in_memory(
        self, in_mem_file: io.BytesIO
    ) -> tuple[np.ndarray, int]:
        """Decodes raw audio bytes into 16-bit PCM samples using in-process PyAV."""
        try:
            with self._open_container(in_mem_file) as container:
                stream = container.streams.audio[PRIMARY_AUDIO_STREAM_INDEX]
                resampler = av.AudioResampler(format="s16p")
                decoded_frames = []
                expected_channels = None
                for frame in container.decode(stream):
                    reframed = resampler.resample(frame)
                    if reframed is None:
                        continue
                    frames_to_process = (
                        reframed
                        if isinstance(reframed, (list, tuple))
                        else [reframed]
                    )
                    for f in frames_to_process:
                        arr = f.to_ndarray()
                        if expected_channels is None:
                            expected_channels = arr.shape[0]
                        elif arr.shape[0] != expected_channels:
                            logger.warning(
                                "Channel count changed mid-stream, skipping frame"
                            )
                            continue
                        decoded_frames.append(arr)

                if not decoded_frames:
                    return np.array([], dtype=np.int16), SAMPLE_RATE_HZ

                combined = np.concatenate(decoded_frames, axis=-1)
                # Return 1D array for mono feeds, or (samples, channels) for multi-channel feeds
                # so that _resample_to_16k_mono can perform downmix averaging across channels.
                raw_samples = (
                    combined[PRIMARY_AUDIO_STREAM_INDEX]
                    if combined.shape[0] == MONO_CHANNEL_COUNT
                    else combined.T
                )

                sr = stream.codec_context.sample_rate
                if sr <= 0:
                    logger.warning("Invalid sample rate detected, defaulting")
                    sr = SAMPLE_RATE_HZ

                return raw_samples, sr
        except Exception as e:
            err_msg = str(e)
            logger.exception("PyAV error during audio decode: %s", err_msg)

            def _err() -> str:
                return f"Failed to decode audio via PyAV: {err_msg}"

            raise RuntimeError(_err()) from e

    def _resample_to_16k_mono(self, samples: np.ndarray, sr: int) -> np.ndarray:
        """Downmixes to mono and resamples to 16 kHz for VAD/SED processing."""
        # 1. Downmix if stereo/multi-channel
        if samples.ndim > 1:
            samples = np.mean(samples, axis=1)

        if sr != SAMPLE_RATE_HZ:
            from backend.pipeline.segmentation.audio.dsp import (  # noqa: PLC0415
                TorchaudioHannResampler,
            )

            resampler = TorchaudioHannResampler(sr, SAMPLE_RATE_HZ)
            samples = resampler.resample(samples)

        return samples.astype(np.int16)

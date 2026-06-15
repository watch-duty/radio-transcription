"""Segmentation-specific audio processing with neural Voice Activity Detection (VAD)."""

import io
import json
import logging
from collections.abc import Callable

import av
import numpy as np
from google.cloud import storage

from backend.pipeline.common.constants import MS_PER_SECOND, SAMPLE_RATE_HZ
from backend.pipeline.schema_types import streaming_state as bp_state
from backend.pipeline.segmentation import storage as audio_storage
from backend.pipeline.segmentation.audio import vad
from backend.pipeline.segmentation.constants import (
    MONO_CHANNEL_COUNT,
    PRIMARY_AUDIO_STREAM_INDEX,
)
from backend.pipeline.segmentation.datatypes import AudioChunkData

logger = logging.getLogger(__name__)


def get_vad_engine(config_json: str) -> vad.VoiceActivityDetector:
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
        self.fetcher = audio_storage.GcsAudioFetcher(
            gcs_client_instance=gcs_client_instance,
            gcs_factory=gcs_factory,
        )

    # Compatibility shim to avoid breaking legacy pipeline callers or tests that set processor.gcs_client directly.
    @property
    def gcs_client(self) -> storage.Client | None:
        return self.fetcher.client

    @gcs_client.setter
    def gcs_client(self, val: storage.Client | None) -> None:
        self.fetcher.client = val

    def setup(self) -> None:
        """Initializes the GCS Client and triggers VAD ONNX session warmup."""
        self.fetcher.setup()

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
        in_mem_file = self.fetcher.download_audio_to_memory(gcs_path)

        in_mem_file.seek(0)

        samples, sr = self._decode_audio_in_memory(in_mem_file)

        speech_segments = []
        if len(samples) > 0:
            # 1. Downmix current samples to mono if multi-channel
            mono_samples = self._downmix_to_mono(samples)

            # 2. Downmix prior audio to mono if multi-channel (retaining source sample rate sr)
            prior_samples = None
            if prior_audio is not None:
                prior_arr = np.frombuffer(prior_audio, dtype=np.int16)
                if samples.ndim > 1 and len(prior_arr) > 0:
                    channels = samples.shape[1]
                    try:
                        prior_samples = np.mean(
                            prior_arr.reshape(-1, channels), axis=1
                        ).astype(np.int16)
                    except ValueError:
                        logger.warning(
                            "prior_audio length not divisible by current channel count (%d), treating as mono",
                            channels,
                        )
                        prior_samples = prior_arr
                else:
                    prior_samples = prior_arr

            if self.vad is None:
                msg = "VAD engine not initialized. Call setup() first."
                raise RuntimeError(msg)

            speech_segments = self.vad.detect_speech_segments(
                mono_samples,
                sample_rate=sr,
                prior_audio=prior_samples,
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
                try:
                    for packet in container.demux(stream):
                        try:
                            for frame in packet.decode():
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
                        except Exception as e:
                            packet_err = str(e)
                            logger.warning(
                                "PyAV packet decode error, ignoring damaged frame: %s",
                                packet_err,
                            )
                            continue
                except Exception as e:
                    demux_err = str(e)
                    logger.exception(
                        "PyAV container demux ended with error, retaining recovered frames: %s",
                        demux_err,
                    )

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

    def _downmix_to_mono(self, samples: np.ndarray) -> np.ndarray:
        """Averages multi-channel audio arrays to flat 1D mono arrays."""
        return (
            np.mean(samples, axis=1).astype(np.int16)
            if samples.ndim > 1
            else samples
        )

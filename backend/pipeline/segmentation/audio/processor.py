"""Segmentation-specific audio processing with neural Voice Activity Detection (VAD)."""

import io
import json
import logging
from collections.abc import Callable

import av
import numpy as np
from google.cloud import storage

from backend.pipeline.common import tracing_utils
from backend.pipeline.common.constants import MS_PER_SECOND, SAMPLE_RATE_HZ
from backend.pipeline.schema_types import streaming_state as bp_state
from backend.pipeline.segmentation import cpu_time
from backend.pipeline.segmentation import storage as audio_storage
from backend.pipeline.segmentation.audio import vad
from backend.pipeline.segmentation.constants import (
    MAX_AUDIO_CHUNK_DURATION_SEC,
    MONO_CHANNEL_COUNT,
    PRIMARY_AUDIO_STREAM_INDEX,
)
from backend.pipeline.segmentation.datatypes import (
    AudioChunkData,
    AudioSignal,
)

logger = logging.getLogger(__name__)


def get_vad_engine(config_json: str) -> vad.VoiceActivityDetector:
    try:
        config = json.loads(config_json) if config_json else {}
    except Exception:
        config = {}
    return vad.VoiceActivityDetector(**config)


def _parse_feed_and_segment_from_gcs_path(gcs_path: str) -> tuple[str, str]:
    """Parses the feed name and segment ID from a standard GCS path.

    e.g., "gs://bucket/feed_name/segment_id.flac" -> ("feed_name", "segment_id.flac")
    """
    parts = gcs_path.split("/")
    feed_name = parts[-2] if len(parts) >= 2 else "unknown"
    segment_id = parts[-1] if len(parts) >= 1 else "unknown"
    return feed_name, segment_id


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

    def fetch_and_decode_audio(
        self, gcs_path: str, trace_attrs: dict[str, str] | None = None
    ) -> AudioSignal:
        """Downloads audio bytes from GCS and decodes them to an AudioSignal.

        Args:
            gcs_path: The Google Cloud Storage URI (e.g., 'gs://bucket/object').
            trace_attrs: Optional dictionary of trace attributes (e.g., traceparent).
                Retained for backwards compatibility with legacy callers.

        Returns:
            An AudioSignal object containing the decoded PCM samples and sample rate.
        """
        tracer = tracing_utils.get_tracer(__name__)
        with tracer.start_as_current_span("fetch_and_decode_audio"):
            with self.fetcher.download_audio_to_memory(gcs_path) as in_mem_file:
                in_mem_file.seek(0)
                samples, sr = self.decode_audio_in_memory(in_mem_file)
                return AudioSignal(samples=samples, sample_rate=sr)

    def download_audio_and_detect(
        self,
        gcs_path: str,
        start_ms: int,
        duration_ms: int | None = None,
        prior_audio: bytes | None = None,
        *,
        prefetched_audio: AudioSignal | None = None,
        record_fetch_cpu_us: Callable[[int], None] | None = None,
        record_analysis_cpu_us: Callable[[int], None] | None = None,
    ) -> AudioChunkData:
        """Downloads audio bytes from GCS (or uses pre-fetched decoded samples) and runs speech segment detection natively."""
        if prefetched_audio is not None:
            audio_signal = prefetched_audio
        else:
            fetch_start_ns = cpu_time.read_thread_cpu_ns()
            audio_signal = self.fetch_and_decode_audio(gcs_path)
            if record_fetch_cpu_us is not None:
                record_fetch_cpu_us(
                    cpu_time.elapsed_thread_cpu_us(fetch_start_ns)
                )

        analysis_start_ns = cpu_time.read_thread_cpu_ns()
        samples, sr = audio_signal.samples, audio_signal.sample_rate

        # Safeguard: Reject extremely long audio files (e.g. >300s) to prevent memory exhaustion
        # and Windmill timeouts at the engine level.
        duration_sec = audio_signal.duration_seconds
        if duration_sec > MAX_AUDIO_CHUNK_DURATION_SEC:
            feed_name, segment_id = _parse_feed_and_segment_from_gcs_path(
                gcs_path
            )
            msg = (
                f"[{feed_name} / {segment_id}] Audio chunk duration of {duration_sec:.2f}s for GCS path '{gcs_path}' "
                f"exceeds the maximum safety limit of {MAX_AUDIO_CHUNK_DURATION_SEC}s."
            )
            raise ValueError(msg)

        speech_segments = []
        if len(samples) > 0:
            # 1. Downmix current samples to mono if multi-channel
            mono_samples = self.downmix_to_mono(samples)

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

            detection = self.vad.detect_speech_segments(
                mono_samples,
                sample_rate=sr,
                prior_audio=prior_samples,
                prior_is_preprocessed=True,
            )
            speech_segments = detection.segments
            denoised_arr = detection.preprocessed_audio
        else:
            denoised_arr = None

        speech_segments_proto = [
            bp_state.TimeRangeProto(
                start_ms=int(s * MS_PER_SECOND),
                end_ms=int(e * MS_PER_SECOND),
            )
            for s, e in speech_segments
        ]

        duration_ms = duration_ms or int(len(samples) * MS_PER_SECOND / sr)

        chunk_data = AudioChunkData(
            start_ms=start_ms,
            audio=samples,
            sample_rate=sr,
            speech_segments=speech_segments_proto,
            gcs_uri=gcs_path,
            duration_ms=duration_ms,
            denoised_audio=denoised_arr,
        )
        if record_analysis_cpu_us is not None:
            record_analysis_cpu_us(
                cpu_time.elapsed_thread_cpu_us(analysis_start_ns)
            )
        return chunk_data

    def _open_container(
        self, in_mem_file: io.BytesIO
    ) -> av.container.InputContainer:
        container = av.open(in_mem_file)
        if not isinstance(container, av.container.InputContainer):

            def _error() -> str:
                return f"Expected InputContainer from av.open, got {type(container).__name__}"

            raise TypeError(_error())
        return container

    def _normalize_frame_to_ndarray(self, frame: av.AudioFrame) -> np.ndarray:
        """Converts an av.AudioFrame to a normalized int16 planar numpy array."""
        arr = frame.to_ndarray()

        # Convert format to 16-bit signed PCM using NumPy
        if "flt" in frame.format.name or "dbl" in frame.format.name:
            # float/double to int16 with clipping to prevent overflow wrapping
            arr = np.clip(arr * 32768.0, -32768, 32767).astype(np.int16)
        elif "s32" in frame.format.name:
            # int32 to int16
            arr = (arr.astype(np.int32) >> 16).astype(np.int16)
        elif "s16" in frame.format.name:
            # already int16
            arr = arr.astype(np.int16)
        else:
            logger.warning(
                "Encountered unexpected audio format '%s' during decoding. "
                "Performing fallback cast to int16; audio quality may be degraded.",
                frame.format.name,
            )
            # Fallback cast
            arr = arr.astype(np.int16)

        # De-interleave packed channels to match planar (channels, samples) layout
        if not frame.format.is_planar:
            channels = len(frame.layout.channels)
            arr = arr.reshape(-1, channels).T

        return arr

    def decode_audio_in_memory(
        self, in_mem_file: io.BytesIO
    ) -> tuple[np.ndarray, int]:
        """Decodes raw audio bytes into 16-bit PCM samples using in-process PyAV."""
        try:
            with self._open_container(in_mem_file) as container:
                stream = container.streams.audio[PRIMARY_AUDIO_STREAM_INDEX]
                decoded_frames = []
                expected_channels = None
                try:
                    for packet in container.demux(stream):
                        try:
                            for frame in packet.decode():
                                arr = self._normalize_frame_to_ndarray(frame)

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

    def downmix_to_mono(self, samples: np.ndarray) -> np.ndarray:
        """Averages multi-channel audio arrays to flat 1D mono arrays."""
        return (
            np.mean(samples, axis=1).astype(np.int16)
            if samples.ndim > 1
            else samples
        )

    _decode_audio_in_memory = decode_audio_in_memory
    _downmix_to_mono = downmix_to_mono

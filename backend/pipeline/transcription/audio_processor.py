"""Stateless acoustic manipulation and Voice Activity Detection (VAD) utilities."""

import io
import logging
import urllib.parse
from collections.abc import Callable
from typing import Any

import librosa
import numpy as np
from pydub import AudioSegment, effects

from backend.pipeline.common.constants import (
    AUDIO_FORMAT,
    NUM_AUDIO_CHANNELS,
    SAMPLE_RATE_HZ,
)
from backend.pipeline.transcription.constants import (
    HIGHPASS_FILTER_FREQ,
    LOWPASS_FILTER_FREQ,
)
from backend.pipeline.transcription.datatypes import AudioChunkData
from backend.pipeline.transcription.enums import VadType
from backend.pipeline.transcription.metadata import (
    get_gcs_client,
    read_sed_segments_from_blob,
)
from backend.pipeline.transcription.resources import SharedResources
from backend.pipeline.transcription.vads import VoiceActivityDetector, get_vad_plugin
from backend.pipeline.transcription.vads import (
    VoiceActivityDetector,
    get_vad_plugin,
)

logger = logging.getLogger(__name__)


class AudioProcessor:
    """An acoustic manipulation module.

    Responsible for downloading/parsing audio, applying VAD, and applying bandpass filters.
    All streaming orchestration and API integrations are handled upstream.
    """

    def __init__(
        self,
        vad_type: VadType = VadType.TEN_VAD,
        vad_config: str = "{}",
        shared_resources: SharedResources | None = None,
        vad_factory: Callable[[VadType, str], VoiceActivityDetector] | None = None,
        gcs_factory: Callable[[], Any] | None = None,
    ) -> None:
        self.vad_type = vad_type
        self.vad_config = vad_config
        self.shared_resources = shared_resources
        self.vad_factory = vad_factory
        self.gcs_factory = gcs_factory

        self.vad: VoiceActivityDetector | None = None
        self.gcs_client: Any | None = None

    def setup(self) -> None:
        """Initializes the VAD plugin and GCS client once per worker."""
        active_vad_factory = self.vad_factory or get_vad_plugin
        active_gcs_factory = self.gcs_factory or get_gcs_client

        if self.shared_resources is not None:
            self.vad = self.shared_resources.get_vad(
                active_vad_factory, self.vad_type, self.vad_config
            )
            self.gcs_client = self.shared_resources.get_gcs(active_gcs_factory)
        else:
            self.vad = active_vad_factory(self.vad_type, self.vad_config)
            self.gcs_client = active_gcs_factory()

    def download_audio_and_sed(self, gcs_path: str) -> AudioChunkData:
        """Downloads FLAC bytes and SED metadata from GCS, returning an AudioChunkData."""
        if not self.gcs_client:
            msg = "GCS client not initialized. Call setup() first."
            raise RuntimeError(msg)

        parsed_uri = urllib.parse.urlparse(gcs_path)
        bucket_name = parsed_uri.netloc
        blob_name = parsed_uri.path.lstrip("/")

        blob = self.gcs_client.bucket(bucket_name).get_blob(blob_name)
        if not blob:
            err_msg = f"GCS object not found: {gcs_path}"
            logger.error(err_msg)
            raise FileNotFoundError(err_msg)

        in_mem_file = io.BytesIO()
        blob.download_to_file(in_mem_file)
        in_mem_file.seek(0)

        full_audio_segment = AudioSegment.from_file(
            in_mem_file, format=AUDIO_FORMAT
        )

        file_start_ms, speech_segments = read_sed_segments_from_blob(blob)

        return AudioChunkData(
            start_ms=file_start_ms,
            audio=full_audio_segment,
            speech_segments=speech_segments,
            gcs_uri=gcs_path,
        )

    def check_vad(self, audio_buffer: AudioSegment) -> bool:
        """Evaluates audio buffer with TenVAD and returns True if speech is detected."""
        if self.vad is None:
            msg = "VAD plugin not initialized. Call setup() first."
            raise RuntimeError(msg)

        audio_16k = (
            audio_buffer.set_frame_rate(SAMPLE_RATE_HZ)
            .set_channels(NUM_AUDIO_CHANNELS)
            .set_sample_width(2)
        )
        pcm_bytes = audio_16k.raw_data

        # DSP Pre-Filtering
        # Fast DSP heuristics are applied before the Neural VAD to:
        # 1. Improve performance by short-circuiting computationally heavy neural network evaluation on dead air.
        # 2. Improve robustness by preventing the neural network from hallucinating false-positive speech on uniform white noise (radio squelch).
        # 1. Mathematical Heuristics (Pre-Filters)
        # Convert to float32 normalized array for librosa
        samples = np.frombuffer(pcm_bytes, dtype=np.int16).astype(np.float32) / 32768.0
        n_samples = len(samples)
        if n_samples == 0:
            return False

        # Dynamically scale FFT windows to silence librosa UserWarnings on short fragments
        n_fft = min(2048, n_samples)
        hop_length = min(512, max(1, n_samples // 4))

        # 1a. RMS Silence Gate: Drop chunks that lack physical acoustic energy
        mean_rms = np.mean(
            librosa.feature.rms(y=samples, frame_length=n_fft, hop_length=hop_length)[0]
        )
        if mean_rms < 0.005:  # Approx -46 dBFS (total silence)
            logger.info(f"VAD Heuristic: Dropped quiet segment (RMS: {mean_rms:.5f})")
            return False

        # 1b. Spectral Flatness Noise Gate: Drop chunks that are purely uniform "white noise" static.
        # Human speech contains sharp harmonic peaks (formants) resulting in very low Wiener entropy (< 0.1).
        mean_flatness = np.mean(
            librosa.feature.spectral_flatness(
                y=samples, n_fft=n_fft, hop_length=hop_length
            )[0]
        )
        if mean_flatness > 0.6:  # Featureless static
            logger.info(
                f"VAD Heuristic: Dropped static (Flatness: {mean_flatness:.3f})"
            )
            return False

        # 2. Neural Evaluation (Final Authority)
        return self.vad.evaluate(pcm_bytes, sample_rate=SAMPLE_RATE_HZ)

    def preprocess_audio(self, audio_buffer: AudioSegment) -> AudioSegment:
        """Applies native bandpass filtering to remove rumble and static."""
        audio = effects.high_pass_filter(audio_buffer, HIGHPASS_FILTER_FREQ)
        return effects.low_pass_filter(audio, LOWPASS_FILTER_FREQ)

    def export_flac(self, audio_buffer: AudioSegment) -> bytes:
        """Exports an AudioSegment to FLAC bytes."""
        buf = io.BytesIO()
        audio_buffer.export(buf, format=AUDIO_FORMAT)
        return buf.getvalue()

"""Stateless acoustic manipulation and Voice Activity Detection (VAD) utilities."""

import io
import logging
import urllib.parse
from collections.abc import Callable
from typing import Any

import numpy as np
import soundfile as sf
from google.cloud import storage

from backend.pipeline.common.constants import (
    AUDIO_FORMAT,
    NUM_AUDIO_CHANNELS,
    SAMPLE_RATE_HZ,
)
from backend.pipeline.transcription.constants import (
    BYTES_PER_SAMPLE_16BIT,
    DEFAULT_SED_FFT_SIZE,
    DEFAULT_SED_HOP_SIZE,
    HIGHPASS_FILTER_FREQ,
    INT16_MAX_FLOAT,
    LOWPASS_FILTER_FREQ,
    VAD_FLATNESS_NOISE_THRESHOLD,
    VAD_RMS_SILENCE_THRESHOLD,
)
from backend.pipeline.transcription.datatypes import AudioChunkData
from backend.pipeline.transcription.detectors import AcousticGateDetector
from backend.pipeline.transcription.dsp import (
    compute_rms_energy,
    compute_spectral_flatness,
)
from backend.pipeline.transcription.enums import VadType
from backend.pipeline.transcription.resources import SharedResources
from backend.pipeline.transcription.vads import (
    VoiceActivityDetector,
    get_vad_plugin,
)

logger = logging.getLogger(__name__)


def get_gcs_client() -> storage.Client:
    """Initialize and return a GCS Client. Used natively by the audio processor for isolation."""
    return storage.Client()


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
        vad_factory: Callable[[VadType, str], VoiceActivityDetector]
        | None = None,
        gcs_factory: Callable[[], Any] | None = None,
    ) -> None:
        self.vad_type = vad_type
        self.vad_config = vad_config
        self.shared_resources = shared_resources
        self.vad_factory = vad_factory
        self.gcs_factory = gcs_factory
        self.sed_detector = AcousticGateDetector()

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

    def download_audio_and_detect(
        self, gcs_path: str, start_ms: int
    ) -> AudioChunkData:
        """Downloads audio bytes from GCS and runs the spectral flatness detector natively."""
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

        samples, _ = sf.read(in_mem_file, dtype='int16')
        if samples.ndim > 1:
            samples = np.mean(samples, axis=1).astype(np.int16)

        speech_segments = self.sed_detector.detect(samples)

        return AudioChunkData(
            start_ms=start_ms,
            audio=samples,
            speech_segments=speech_segments,
            gcs_uri=gcs_path,
        )

    def check_vad(self, audio_buffer: np.ndarray) -> bool:
        """Evaluates audio buffer with TenVAD and returns True if speech is detected."""
        if self.vad is None:
            msg = "VAD plugin not initialized. Call setup() first."
            raise RuntimeError(msg)

        pcm_bytes = audio_buffer.tobytes()

        # DSP Pre-Filtering
        # Fast DSP heuristics are applied before the Neural VAD to:
        # 1. Improve performance by short-circuiting computationally heavy neural network evaluation on dead air.
        # 2. Improve robustness by preventing the neural network from hallucinating false-positive speech on uniform white noise (radio squelch).
        # 1. Mathematical Heuristics (Pre-Filters)
        # Convert to float32 normalized array for DSP analysis
        audio_data = (
            audio_buffer.astype(np.float32) / INT16_MAX_FLOAT
        )
        rms_energy = compute_rms_energy(audio_data)
        if rms_energy < VAD_RMS_SILENCE_THRESHOLD:  # Below noise floor
            logger.info(
                f"VAD Heuristic: Dropped near-silence (RMS Energy: {rms_energy:.5f})"
            )
            return False

        mean_flatness = compute_spectral_flatness(
            audio_data,
            n_fft=DEFAULT_SED_FFT_SIZE,
            hop_length=DEFAULT_SED_HOP_SIZE,
        )
        if mean_flatness > VAD_FLATNESS_NOISE_THRESHOLD:  # Featureless static
            logger.info(
                f"VAD Heuristic: Dropped static (Flatness: {mean_flatness:.3f})"
            )
            return False

        # 2. Neural Evaluation (Final Authority)
        return self.vad.evaluate(pcm_bytes, sample_rate=SAMPLE_RATE_HZ)

    def preprocess_audio(self, audio_buffer: np.ndarray) -> np.ndarray:
        """Applies native bandpass filtering to remove rumble and static."""
        import scipy.signal as signal
        nyq = 0.5 * SAMPLE_RATE_HZ
        high = HIGHPASS_FILTER_FREQ / nyq
        low = LOWPASS_FILTER_FREQ / nyq
        b, a = signal.butter(4, [high, low], btype='band')
        filtered = signal.lfilter(b, a, audio_buffer)
        return filtered.astype(np.int16)

    def export_flac(self, audio_buffer: np.ndarray) -> bytes:
        """Exports a NumPy array to FLAC bytes."""
        buf = io.BytesIO()
        sf.write(buf, audio_buffer, SAMPLE_RATE_HZ, format="FLAC")
        return buf.getvalue()

    def export_m4a(self, audio_buffer: np.ndarray) -> bytes:
        """Exports a NumPy array to M4A (AAC) bytes using ffmpeg."""
        import tempfile
        import subprocess
        import os
        
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as wav_file:
            wav_path = wav_file.name
            sf.write(wav_path, audio_buffer, SAMPLE_RATE_HZ)
    
        m4a_path = wav_path.replace(".wav", ".m4a")
        try:
            subprocess.run(
                ["ffmpeg", "-y", "-i", wav_path, "-c:a", "aac", "-b:a", "32k", "-ar", "16000", "-ac", "1", m4a_path],
                check=True,
                capture_output=True,
            )
            with open(m4a_path, "rb") as f:
                return f.read()
        finally:
            if os.path.exists(wav_path):
                os.remove(wav_path)
            if os.path.exists(m4a_path):
                os.remove(m4a_path)

    def process_buffer(
        self, audio_buffer: np.ndarray
    ) -> tuple[bool, bytes | None, np.ndarray | None]:
        """Encapsulates sequence of pre-processing, VAD check, and FLAC export."""
        processed_audio = self.preprocess_audio(audio_buffer)
        if not self.check_vad(processed_audio):
            return False, None, None

        flac_bytes = self.export_flac(processed_audio)
        return True, flac_bytes, processed_audio

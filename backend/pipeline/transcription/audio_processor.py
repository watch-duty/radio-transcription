"""Stateless acoustic manipulation and Voice Activity Detection (VAD) utilities."""

import io
import logging
import subprocess
import tempfile
import urllib.parse
from collections.abc import Callable
from pathlib import Path
from typing import Any

import numpy as np
from google.cloud import storage
from scipy import signal

from backend.pipeline.common.constants import (
    FLAC_COMPRESSION_LEVEL,
    M4A_BITRATE,
    SAMPLE_RATE_HZ,
)
from backend.pipeline.transcription.constants import (
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
logger = logging.LoggerAdapter(
    logger, {"system": "transcription", "component": "audio-processor"}
)


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
        self, gcs_path: str, start_ms: int, duration_ms: int | None = None
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

        # Use ffmpeg to decode audio to raw PCM in memory (handles MP3, FLAC, etc.)
        process = subprocess.run(
            [
                "ffmpeg",
                "-i",
                "pipe:0",  # Read from stdin
                "-f",
                "s16le",  # Output raw 16-bit PCM
                "-ac",
                "1",  # Force mono
                "-ar",
                "16000",  # Force 16kHz
                "pipe:1",  # Write to stdout
            ],
            input=in_mem_file.getvalue(),
            capture_output=True,
            check=False,
        )
        if process.returncode != 0:
            logger.error(
                f"ffmpeg error during audio decode: {process.stderr.decode()}"
            )
            msg = "Failed to decode audio via ffmpeg"
            raise RuntimeError(msg)

        samples = np.frombuffer(process.stdout, dtype=np.int16)
        sr = 16000
        if samples.ndim > 1:
            samples = np.mean(samples, axis=1).astype(np.int16)

        speech_segments = self.sed_detector.detect(samples)

        if duration_ms is None:
            duration_ms = len(samples) // (sr // 1000)

        return AudioChunkData(
            start_ms=start_ms,
            audio=samples,
            sample_rate=sr,
            speech_segments=speech_segments,
            gcs_uri=gcs_path,
            duration_ms=duration_ms,
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
        audio_data = audio_buffer.astype(np.float32) / INT16_MAX_FLOAT
        rms_energy = compute_rms_energy(audio_data)
        mean_rms = np.mean(rms_energy)
        if mean_rms < VAD_RMS_SILENCE_THRESHOLD:  # Below noise floor
            logger.info(
                f"VAD Heuristic: Dropped near-silence (RMS Energy: {mean_rms:.5f})"
            )

            return False

        mean_flatness = compute_spectral_flatness(
            audio_data,
            sample_rate=SAMPLE_RATE_HZ,
            n_fft=DEFAULT_SED_FFT_SIZE,
            hop_length=DEFAULT_SED_HOP_SIZE,
        )
        mean_flatness_val = np.mean(mean_flatness)
        if (
            mean_flatness_val > VAD_FLATNESS_NOISE_THRESHOLD
        ):  # Featureless static
            logger.info(
                f"VAD Heuristic: Dropped static (Flatness: {mean_flatness_val:.3f})"
            )

            return False

        # 2. Neural Evaluation (Final Authority)
        return self.vad.evaluate(pcm_bytes, sample_rate=SAMPLE_RATE_HZ)

    def preprocess_audio(self, audio_buffer: np.ndarray) -> np.ndarray:
        """Applies native bandpass filtering to remove rumble and static."""
        nyq = 0.5 * SAMPLE_RATE_HZ
        high = HIGHPASS_FILTER_FREQ / nyq
        low = LOWPASS_FILTER_FREQ / nyq
        b, a = signal.butter(4, [high, low], btype="band")
        filtered = signal.lfilter(b, a, audio_buffer)
        return filtered.astype(np.int16)

    def export_flac(self, audio_buffer: np.ndarray) -> bytes:
        """Exports a NumPy array to FLAC bytes using ffmpeg."""
        with tempfile.NamedTemporaryFile(
            suffix=".flac", delete=False
        ) as temp_file:
            temp_filename = temp_file.name

        try:
            process = subprocess.run(
                [
                    "ffmpeg",
                    "-y",  # Overwrite output file if it exists
                    "-f",
                    "s16le",
                    "-ar",
                    str(SAMPLE_RATE_HZ),  # Force 16kHz
                    "-ac",
                    "1",  # Mono
                    "-i",
                    "pipe:0",  # Read from stdin
                    "-f",
                    "flac",  # Output format
                    "-compression_level",
                    FLAC_COMPRESSION_LEVEL,
                    temp_filename,  # Write to temp file
                ],
                input=audio_buffer.tobytes(),
                capture_output=True,
                check=False,
            )
            if process.returncode != 0:
                logger.error(
                    f"ffmpeg error during FLAC export: {process.stderr.decode()}"
                )
                msg = "Failed to export FLAC via ffmpeg"
                raise RuntimeError(msg)

            with open(temp_filename, "rb") as f:
                return f.read()
        finally:
            try:
                Path(temp_filename).unlink()
            except OSError:
                pass

    def export_m4a(self, audio_buffer: np.ndarray) -> bytes:
        """Exports a NumPy array to M4A (AAC) bytes using ffmpeg via a temporary file."""
        with tempfile.NamedTemporaryFile(
            suffix=".m4a", delete=False
        ) as temp_file:
            temp_filename = temp_file.name

        try:
            process = subprocess.run(
                [
                    "ffmpeg",
                    "-y",  # Overwrite output file if it exists
                    "-f",
                    "s16le",  # Input format: 16-bit signed little-endian PCM
                    "-ar",
                    str(SAMPLE_RATE_HZ),  # Force 16kHz
                    "-ac",
                    "1",  # Input channels (mono)
                    "-i",
                    "pipe:0",  # Read from stdin
                    "-f",
                    "ipod",  # Output format: M4A/MP4 container
                    "-c:a",
                    "aac",  # Output codec: AAC
                    "-b:a",
                    M4A_BITRATE,  # Output bitrate from constant
                    temp_filename,  # Write to temp file
                ],
                input=audio_buffer.tobytes(),
                capture_output=True,
                check=False,
            )

            if process.returncode != 0:
                logger.error(
                    f"ffmpeg error during M4A export: {process.stderr.decode()}"
                )
                msg = "Failed to export M4A via ffmpeg"
                raise RuntimeError(msg)

            with open(temp_filename, "rb") as f:
                return f.read()
        finally:
            try:
                Path(temp_filename).unlink()
            except OSError:
                pass

    def process_buffer(
        self, audio_buffer: np.ndarray
    ) -> tuple[bool, bytes | None, np.ndarray | None]:
        """Encapsulates sequence of pre-processing, VAD check, and FLAC export."""
        processed_audio = self.preprocess_audio(audio_buffer)
        if not self.check_vad(processed_audio):
            return False, None, None

        flac_bytes = self.export_flac(processed_audio)
        return True, flac_bytes, processed_audio

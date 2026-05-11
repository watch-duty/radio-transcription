"""Stateless acoustic manipulation and Voice Activity Detection (VAD) utilities."""

import io
import json
import subprocess
import tempfile
import urllib.parse
from collections.abc import Callable
from pathlib import Path

import numpy as np
import soundfile as sf
from google.cloud import storage
from pedalboard import HighpassFilter, LowpassFilter, Pedalboard

from backend.pipeline.common.constants import (
    FLAC_COMPRESSION_LEVEL,
    M4A_BITRATE,
)
from backend.pipeline.transcription.audio.dsp import (
    TorchaudioHannResampler,
    compute_rms_energy,
)
from backend.pipeline.transcription.audio.vad import VoiceActivityDetector
from backend.pipeline.transcription.common.constants import (
    HIGHPASS_FILTER_FREQ,
    INT16_MAX_FLOAT,
    LOWPASS_FILTER_FREQ,
    VAD_RMS_SILENCE_THRESHOLD,
)
from backend.pipeline.transcription.common.datatypes import (
    AudioChunkData,
    TimeRange,
)
from backend.pipeline.transcription.common.logging import get_logger

logger = get_logger(
    __name__, {"system": "transcription", "component": "audio-processor"}
)


def get_gcs_client() -> storage.Client:
    """Initialize and return a GCS Client. Used natively by the audio processor for isolation."""
    return storage.Client()


def get_vad_engine(config_json: str) -> VoiceActivityDetector:
    """Creates a VoiceActivityDetector engine using optional JSON config."""
    try:
        config = json.loads(config_json) if config_json else {}
    except Exception:
        config = {}
    return VoiceActivityDetector(**config)


def _resample_to_16k_mono(samples: np.ndarray, sr: int) -> np.ndarray:
    """Downmixes to mono and resamples to 16 kHz for VAD/SED processing."""
    # 1. Downmix if stereo/multi-channel
    if samples.ndim > 1:
        samples = np.mean(samples, axis=1)

    if sr != 16000:
        resampler = TorchaudioHannResampler(sr, 16000)
        samples = resampler.resample(samples)

    return samples.astype(np.int16)


class AudioProcessor:
    """An acoustic manipulation module.

    Responsible for downloading/parsing audio, applying VAD, and applying bandpass filters.
    All streaming orchestration and API integrations are handled upstream.
    """

    def __init__(
        self,
        vad_config: str = "{}",
        vad_instance: VoiceActivityDetector | None = None,
        gcs_client_instance: storage.Client | None = None,
        vad_factory: Callable[[str], VoiceActivityDetector] | None = None,
        gcs_factory: Callable[[], storage.Client] | None = None,
    ) -> None:
        self.vad_config = vad_config
        self.vad_factory = vad_factory
        self.gcs_factory = gcs_factory

        self.vad = vad_instance
        self.gcs_client = gcs_client_instance

    def setup(self) -> None:
        """Initializes the VAD plugin and GCS client once per worker."""
        active_vad_factory = self.vad_factory or get_vad_engine
        active_gcs_factory = self.gcs_factory or get_gcs_client

        if self.vad is None:
            self.vad = active_vad_factory(self.vad_config)
        if self.gcs_client is None:
            self.gcs_client = active_gcs_factory()
        self.vad.setup()

    def download_audio_and_detect(
        self,
        gcs_path: str,
        start_ms: int,
        duration_ms: int | None = None,
        prior_audio: np.ndarray | None = None,
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

        blob = self.gcs_client.bucket(bucket_name).get_blob(blob_name)
        if not blob:
            err_msg = f"GCS object not found: {gcs_path}"
            logger.error(err_msg)
            raise FileNotFoundError(err_msg)

        in_mem_file = io.BytesIO()
        blob.download_to_file(in_mem_file)
        in_mem_file.seek(0)

        with tempfile.NamedTemporaryFile(
            suffix=".flac", delete=False
        ) as temp_file:
            temp_filename = temp_file.name

        try:
            # Use ffmpeg to extract standard FLAC to a seekable temporary file
            process = subprocess.run(
                [
                    "ffmpeg",
                    "-y",
                    "-i",
                    "pipe:0",  # Read from stdin
                    "-f",
                    "flac",  # Output FLAC format
                    temp_filename,
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

            samples, sr = sf.read(temp_filename, dtype="int16")
        finally:
            try:
                Path(temp_filename).unlink()
            except OSError:
                pass

        speech_segments = []
        if len(samples) > 0:
            samples_float = samples.astype(np.float32) / INT16_MAX_FLOAT
            # If prior audio tail is passed from context, normalize it to float32 to match vad signature
            prior_float = (
                prior_audio.astype(np.float32) / INT16_MAX_FLOAT
                if prior_audio is not None
                else None
            )
            raw_segments = self.vad.detect_speech_segments(
                samples_float, sample_rate=sr, prior_audio=prior_float
            )
            for start_sec, end_sec in raw_segments:
                speech_segments.append(
                    TimeRange(
                        start_ms=int(start_sec * 1000.0),
                        end_ms=int(end_sec * 1000.0),
                    )
                )

        if duration_ms is None:
            duration_ms = int(len(samples) / sr * 1000)

        return AudioChunkData(
            start_ms=start_ms,
            audio=samples,
            sample_rate=sr,
            speech_segments=speech_segments,
            gcs_uri=gcs_path,
            duration_ms=duration_ms,
        )

    def check_vad(self, audio_buffer: np.ndarray, sample_rate: int) -> bool:
        """Evaluates audio buffer with the configured VAD and returns True if speech is detected."""
        if self.vad is None:
            msg = "VAD engine not initialized. Call setup() first."
            raise RuntimeError(msg)

        # Convert to float32 normalized array for DSP analysis
        audio_data = audio_buffer.astype(np.float32) / INT16_MAX_FLOAT
        rms_energy = compute_rms_energy(audio_data)
        mean_rms = np.mean(rms_energy)
        if mean_rms < VAD_RMS_SILENCE_THRESHOLD:  # Below noise floor
            logger.info(
                f"VAD Heuristic: Dropped near-silence (RMS Energy: {mean_rms:.5f})"
            )
            return False

        # Detect speech segments using Silero + UL-UNAS
        segments = self.vad.detect_speech_segments(
            audio_data, sample_rate=sample_rate
        )
        return len(segments) > 0

    def preprocess_audio(
        self, audio_buffer: np.ndarray, sample_rate: int
    ) -> np.ndarray:
        """Applies native bandpass filtering to remove rumble and static."""
        # Convert to float32 normalized array for Pedalboard processing
        audio_float = audio_buffer.astype(np.float32) / INT16_MAX_FLOAT

        # Use Pedalboard's highly optimized 12dB/octave Highpass + Lowpass filters
        board = Pedalboard(
            [
                HighpassFilter(cutoff_frequency_hz=HIGHPASS_FILTER_FREQ),
                LowpassFilter(cutoff_frequency_hz=LOWPASS_FILTER_FREQ),
            ]
        )
        filtered_float = board(audio_float, sample_rate)

        # Scale back to 16-bit PCM
        return (filtered_float * INT16_MAX_FLOAT).astype(np.int16)

    def export_flac(self, audio_buffer: np.ndarray, sample_rate: int) -> bytes:
        """Exports a NumPy array to FLAC bytes using ffmpeg."""
        channels = 1 if audio_buffer.ndim == 1 else audio_buffer.shape[1]
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
                    str(sample_rate),
                    "-ac",
                    str(channels),
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

    def export_m4a(self, audio_buffer: np.ndarray, sample_rate: int) -> bytes:
        """Exports a NumPy array to M4A (AAC) bytes using ffmpeg via a temporary file."""
        channels = 1 if audio_buffer.ndim == 1 else audio_buffer.shape[1]
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
                    str(sample_rate),
                    "-ac",
                    str(channels),
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
        self,
        audio_buffer: np.ndarray,
        sample_rate: int,
        speech_segments: list[TimeRange] | None = None,
    ) -> tuple[bool, bytes | None, np.ndarray | None]:
        """Encapsulates sequence of pre-processing, VAD check, and FLAC export."""
        # Bypass the expensive second VAD evaluation if speech segments are already pre-computed
        if (speech_segments is not None and not speech_segments) or (
            speech_segments is None
            and not self.check_vad(audio_buffer, sample_rate)
        ):
            return False, None, None

        processed_audio = self.preprocess_audio(audio_buffer, sample_rate)
        flac_bytes = self.export_flac(processed_audio, sample_rate)
        return True, flac_bytes, processed_audio

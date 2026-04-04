import io
import json
import logging
import os
import subprocess
import tempfile
import urllib.parse
import soundfile as sf
from collections.abc import Callable
from typing import Any

import numpy as np
from google.cloud import storage

from backend.pipeline.common.constants import (
    NUM_AUDIO_CHANNELS,
    SAMPLE_RATE_HZ,
)
from backend.pipeline.transcription.constants import (
    DEFAULT_VAD_POST_ROLL_MS,
    DEFAULT_VAD_PRE_ROLL_MS,
    MS_PER_SEC,
)

# --- Local Constants ---
VAD_DEFAULT_THRESHOLD = 0.15
VAD_DEFAULT_MIN_SPEECH_DURATION_SEC = 0.1
VAD_DEFAULT_MIN_SILENCE_DURATION_SEC = 0.6
VAD_GAP_SOFT_RESET_MS = 500
AUDIO_NORMALIZATION_DENOMINATOR = 32768.0
AUDIO_NORMALIZATION_MULTIPLIER = 32767
PRE_EMPHASIS_COEFF = 0.97
# -----------------------
from backend.pipeline.transcription.datatypes import (
    AudioChunkData,
    PaddedSegment,
    TimeRange,
)
from backend.pipeline.transcription.audio.signal_processing import (
    RadioSignalAnalyzer,
)
from backend.pipeline.transcription.utils import calculate_padded_ranges

from backend.pipeline.transcription.resources import SharedResources

# --- Audio Configuration Constants ---
TARGET_SAMPLE_RATE = SAMPLE_RATE_HZ
# -------------------------------------

logger = logging.getLogger(__name__)


def _get_gcs_client() -> storage.Client:
    """Initialize and return a GCS Client. Used natively by the audio processor for isolation."""
    return storage.Client()


class AudioProcessor:
    """An acoustic manipulation module.

    Responsible for downloading/parsing audio, applying VAD, and applying bandpass filters.
    All streaming orchestration and API integrations are handled upstream.
    """

    def __init__(
        self,
        vad_config: str = "{}",
        shared_resources: SharedResources | None = None,
        gcs_factory: Callable[[], Any] | None = None,
        pre_roll_ms: int = DEFAULT_VAD_PRE_ROLL_MS,
        post_roll_ms: int = DEFAULT_VAD_POST_ROLL_MS,
    ) -> None:
        self.vad_config = vad_config
        self.shared_resources = shared_resources
        self.gcs_factory = gcs_factory
        self.pre_roll_ms = pre_roll_ms
        self.post_roll_ms = post_roll_ms

        self.vad: Any | None = None
        self.denoiser: Any | None = None
        self.gcs_client: Any | None = None
        self.last_audio_end_ms: int | None = None
        self.signal_analyzer = RadioSignalAnalyzer(sample_rate=SAMPLE_RATE_HZ)

    def setup(self) -> None:
        """Initializes the VAD and GCS client once per worker."""
        active_gcs_factory = self.gcs_factory or _get_gcs_client

        if self.shared_resources is not None:
            self.gcs_client = self.shared_resources.get_gcs(active_gcs_factory)
        else:
            self.gcs_client = active_gcs_factory()

    def _create_sherpa_vad(self, config_json: str) -> Any:
        """Factory for Sherpa-ONNX VAD."""
        from backend.pipeline.transcription.audio.silero_vad import (
            create_sherpa_vad,
        )

        try:
            config = json.loads(config_json)
        except json.JSONDecodeError:
            logger.warning(
                "Failed to decode VAD config JSON, using defaults in factory"
            )
            config = {}
        threshold = config.get("threshold", VAD_DEFAULT_THRESHOLD)
        min_silence_duration = config.get(
            "min_silence_duration", VAD_DEFAULT_MIN_SILENCE_DURATION_SEC
        )
        min_speech_duration = config.get(
            "min_speech_duration", VAD_DEFAULT_MIN_SPEECH_DURATION_SEC
        )

        return create_sherpa_vad(
            threshold, min_silence_duration, min_speech_duration
        )

    def _create_sherpa_denoiser(self, *args: Any) -> Any:
        """Factory for Sherpa-ONNX Denoiser. Accepts args to ignore unused parameters from SharedResources."""
        import sherpa_onnx

        gtcrn_config = sherpa_onnx.OfflineSpeechDenoiserGtcrnModelConfig(
            model=os.environ.get(
                "GTCRN_MODEL_PATH",
                "backend/pipeline/transcription/resources/gtcrn_simple.onnx",
            )
        )
        model_config = sherpa_onnx.OfflineSpeechDenoiserModelConfig(
            gtcrn=gtcrn_config, num_threads=1, provider="cpu"
        )
        config = sherpa_onnx.OnlineSpeechDenoiserConfig(model=model_config)
        return sherpa_onnx.OnlineSpeechDenoiser(config)

    def _remove_invalid_segments(
        self, speech_segments: list[TimeRange]
    ) -> list[TimeRange]:
        """Filters out invalid speech segments (where start_ms >= end_ms).

        Sherpa-ONNX VAD sometimes returns invalid segments due to circular buffer state;
        we must skip them to prevent state machine corruption and IndexErrors.
        """
        valid_segments = []
        for reg in speech_segments:
            if reg.start_ms >= reg.end_ms:
                logger.warning(
                    f"Dropping invalid speech segment: start_ms={reg.start_ms}, end_ms={reg.end_ms}"
                )
                continue
            valid_segments.append(reg)
        return valid_segments

    def _calculate_padded_segments(
        self,
        audio: np.ndarray,
        speech_segments: list[TimeRange],
        silence_segments: list[TimeRange],
        noise_segments: list[TimeRange],
        file_start_ms: int,
    ) -> list[PaddedSegment]:
        """Calculates padded boundaries for speech segments and extracts them."""
        padded_segments = []

        valid_speech_segments = self._remove_invalid_segments(speech_segments)
        logger.debug(
            "_calculate_padded_segments: valid_speech_segments count=%s",
            len(valid_speech_segments),
        )

        samples_per_ms = SAMPLE_RATE_HZ // 1000
        duration_ms = len(audio) // samples_per_ms

        for reg in valid_speech_segments:
            append_start_ms = max(0, reg.start_ms - file_start_ms)
            append_end_ms = min(duration_ms, reg.end_ms - file_start_ms)

            if append_end_ms > append_start_ms:
                append_start_sample = append_start_ms * samples_per_ms
                append_end_sample = append_end_ms * samples_per_ms

                padded_audio = audio[append_start_sample:append_end_sample]
                padded_segments.append(
                    PaddedSegment(
                        audio=padded_audio,
                        start_ms=file_start_ms + append_start_ms,
                        speech_start_ms=reg.start_ms,
                        speech_end_ms=reg.end_ms,
                    )
                )
        return padded_segments

    def _detect_speech_and_noise(
        self, samples: np.ndarray, start_ms: int
    ) -> tuple[list[TimeRange], list[TimeRange], list[TimeRange]]:
        """Detects speech and noise segments."""
        return self._detect_speech_and_noise_sherpa(samples, start_ms)

    def _detect_speech_and_noise_sherpa(
        self, samples: np.ndarray, start_ms: int
    ) -> tuple[list[TimeRange], list[TimeRange], list[TimeRange]]:
        """Detects speech segments using Sherpa-ONNX, Sliding AGC, and Fast HNR Gate."""
        if getattr(self, "vad_sensitive", None) is None:
            raise RuntimeError("VAD instance is not initialized")
        if self.denoiser is None:
            raise RuntimeError("Denoiser is not initialized")

        # 1. Gap Management (Keep the VAD state warm, but reset if stream dropped)
        if self.last_audio_end_ms is not None:
            gap_ms = start_ms - self.last_audio_end_ms
            if gap_ms > VAD_GAP_SOFT_RESET_MS:
                logger.info(
                    "Gap > %sms: Resetting VAD and Denoiser.",
                    VAD_GAP_SOFT_RESET_MS,
                )
                self.vad_sensitive.detector.reset()
                self.denoiser.reset()

        duration_ms = (len(samples) * MS_PER_SEC) // SAMPLE_RATE_HZ
        self.last_audio_end_ms = start_ms + duration_ms

        # Call the standalone processing pipeline
        return self._detect_speech_candidates(
            samples,
            start_ms,
            self.vad_sensitive,
            self.denoiser,
            self.signal_analyzer,
        )

    def _detect_speech_candidates(
        self,
        samples: np.ndarray,
        start_ms: int,
        vad_sensitive,
        denoiser,
        signal_analyzer,
    ) -> tuple[list[TimeRange], list[TimeRange], list[TimeRange]]:
        """Pure audio processing pipeline, isolated from AudioProcessor state."""
        from backend.pipeline.transcription.audio.silero_vad import (
            process_vad_streaming,
        )
        from backend.pipeline.transcription.audio.signal_processing import (
            apply_sliding_agc,
        )
        from backend.pipeline.transcription.utils import calculate_padded_ranges

        duration_ms = (len(samples) * MS_PER_SEC) // SAMPLE_RATE_HZ

        # ==========================================
        # PHASE 1: PREPARE (Stateless Signal Cleanup)
        # ==========================================
        samples_float = (
            samples.astype(np.float32) / AUDIO_NORMALIZATION_DENOMINATOR
        )

        # A. Denoise (Frequency Domain)
        denoised_audio_obj = denoiser.run(
            samples_float.tolist(), SAMPLE_RATE_HZ
        )
        denoised_float = np.array(denoised_audio_obj.samples, dtype=np.float32)

        if len(denoised_float) == 0:
            return [], [], []

        # B. Automatic Gain Control (Time Domain - Fixes the 1.0s Masking Bug)
        leveled_float = apply_sliding_agc(
            denoised_float, sample_rate=SAMPLE_RATE_HZ
        )

        # C. Pre-Emphasis
        leveled_int16 = (leveled_float * AUDIO_NORMALIZATION_MULTIPLIER).astype(
            np.int16
        )
        samples_rec = self._apply_pre_emphasis(leveled_int16)

        # ==========================================
        # PHASE 2: DETECT (The Single VAD Pass)
        # ==========================================
        vad_result = process_vad_streaming(samples_rec, start_ms, vad_sensitive)

        raw_speech = []
        known_noise = []

        # ==========================================
        # PHASE 3: GATE (Sort Energy into Human vs. Machine)
        # ==========================================
        for seg in vad_result.speech_segments:
            start_idx = int(
                ((seg.start_ms - start_ms) * SAMPLE_RATE_HZ) / MS_PER_SEC
            )
            end_idx = int(
                ((seg.end_ms - start_ms) * SAMPLE_RATE_HZ) / MS_PER_SEC
            )

            start_idx = max(0, start_idx)
            end_idx = min(len(leveled_float), end_idx)

            gate_audio = leveled_float[start_idx:end_idx]

            seg_duration_ms = seg.end_ms - seg.start_ms
            logger.debug(
                "Gate check: VAD segment %sms - %sms, duration: %sms",
                seg.start_ms,
                seg.end_ms,
                seg_duration_ms,
            )

            # Minimum duration check to run HNR/pYIN (e.g., 50ms)
            if len(gate_audio) > (SAMPLE_RATE_HZ * 0.05):
                result = signal_analyzer.characterize(gate_audio)

                if result.is_transcribable:
                    raw_speech.append(seg)
                else:
                    logger.debug(
                        "Gate flagged segment as %s at %sms. Using as noise bumper.",
                        result.label,
                        seg.start_ms,
                    )
                    known_noise.append(seg)

        # ==========================================
        # PHASE 4: PAD (Dynamic Context Expansion)
        # ==========================================
        # Safely expands boundaries by default pre/post roll unless it hits a known_noise bumper
        padded_ranges = calculate_padded_ranges(
            audio_duration_ms=duration_ms,
            speech_segments=raw_speech,
            noise_segments=known_noise,
            file_start_ms=start_ms,
            pre_roll_ms=DEFAULT_VAD_PRE_ROLL_MS,
            post_roll_ms=DEFAULT_VAD_POST_ROLL_MS,
        )

        # Map back to TimeRange objects
        final_speech_segments = []
        for append_start, append_end in padded_ranges:
            final_speech_segments.append(
                TimeRange(
                    start_ms=start_ms + append_start,
                    end_ms=start_ms + append_end,
                )
            )

        logger.debug(
            "_detect_speech_candidates: returning %s speech segments",
            len(final_speech_segments),
        )
        for seg in final_speech_segments:
            logger.debug("  -> Segment: %sms - %sms", seg.start_ms, seg.end_ms)

        return final_speech_segments, [], known_noise

    def download_audio_and_detect(
        self, gcs_path: str, start_ms: int, session_id: str
    ) -> AudioChunkData:
        """Downloads FLAC bytes from GCS and runs the spectral flatness detector natively."""
        session_sensitive = session_id + "_sensitive"
        session_strict = session_id + "_strict"

        config_sensitive = json.dumps(
            {
                "threshold": 0.15,
                "min_silence_duration": 0.6,
                "min_speech_duration": 0.05,
            }
        )

        config_strict = json.dumps(
            {
                "threshold": 0.25,
                "min_silence_duration": 0.25,
                "min_speech_duration": 0.05,
            }
        )

        if self.shared_resources is not None:
            self.vad_sensitive = self.shared_resources.get_vad(
                session_sensitive, self._create_sherpa_vad, config_sensitive
            )
            self.vad_strict = self.shared_resources.get_vad(
                session_strict, self._create_sherpa_vad, config_strict
            )
            self.denoiser = self.shared_resources.get_denoiser(
                session_id, self._create_sherpa_denoiser
            )
        else:
            if (
                getattr(self, "vad_sensitive", None) is None
                or getattr(self, "current_session_id", None) != session_id
            ):
                self.vad_sensitive = self._create_sherpa_vad(config_sensitive)
                self.vad_strict = self._create_sherpa_vad(config_strict)
                self.denoiser = self._create_sherpa_denoiser()
                self.current_session_id = session_id

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

        data = in_mem_file.getvalue()
        # Skip ICY metadata or HTTP headers by searching for FLAC magic number
        idx = data.find(b"fLaC")
        if idx != -1:
            data = data[idx:]

        # Use ffmpeg to sanitize and decode to WAV in one go to handle dirty files
        ffmpeg_cmd = [
            "ffmpeg",
            "-v",
            "error",
            "-f",
            "flac",  # Force input format to FLAC to skip garbage
            "-i",
            "-",
            "-ac",
            "1",  # Force mono
            "-f",
            "wav",
            "-",
        ]
        try:
            ffmpeg_proc = subprocess.run(
                ffmpeg_cmd,
                input=data,
                capture_output=True,
                check=True,
            )
            import soundfile as sf

            samples_orig, sample_rate = sf.read(
                io.BytesIO(ffmpeg_proc.stdout), dtype="int16"
            )
        except subprocess.CalledProcessError as e:
            logger.error(
                f"FFmpeg failed to sanitize/decode audio. Return Code: {e.returncode}\nFFmpeg STDERR:\n{e.stderr.decode()}"
            )
            raise RuntimeError("Failed to sanitize/decode audio.") from e
        except Exception as e:
            logger.error(f"Failed to read WAV from FFmpeg output: {e}")
            raise RuntimeError("Failed to read decoded audio.") from e

        # Create 16kHz copy for VAD and models
        import librosa

        # librosa expects float32 in range [-1, 1]
        samples_float = samples_orig.astype(np.float32) / 32767.0
        if sample_rate != TARGET_SAMPLE_RATE:
            samples_16k = librosa.resample(
                samples_float,
                orig_sr=sample_rate,
                target_sr=TARGET_SAMPLE_RATE,
            )
        else:
            samples_16k = samples_float

        # Convert back to int16
        samples_16k = (samples_16k * 32767).astype(np.int16)

        return AudioChunkData(
            start_ms=start_ms,
            audio=samples_16k,
            speech_segments=[],
            gcs_uri=gcs_path,
            stored_audio=samples_orig,
            original_sr=sample_rate,
            silence_segments=[],
            noise_segments=[],
            padded_segments=[],
        )

    def detect_vad(
        self,
        chunk_audio: np.ndarray,
        start_ms: int,
    ) -> list[PaddedSegment]:
        """Runs VAD on chunk_audio."""
        # chunk_audio is already assumed to be 16kHz mono int16 numpy array
        samples = chunk_audio

        # 2. Run VAD
        speech_segments, noise_segments, silence_segments = (
            self._detect_speech_and_noise(samples, start_ms)
        )
        logger.debug(
            "detect_vad: _detect_speech_and_noise returned %s segments",
            len(speech_segments),
        )

        # 3. Calculate padded segments
        padded_segments = self._calculate_padded_segments(
            audio=chunk_audio,
            speech_segments=speech_segments,
            silence_segments=silence_segments,
            noise_segments=noise_segments,
            file_start_ms=start_ms,
        )

        return padded_segments

    def _apply_pre_emphasis(
        self, samples: np.ndarray, alpha: float = PRE_EMPHASIS_COEFF
    ) -> np.ndarray:
        """Boosts high-frequency speech features (consonants).
        Input: int16 numpy array
        """
        audio_float = samples.astype(np.float32)
        emphasized = np.append(
            audio_float[0], audio_float[1:] - alpha * audio_float[:-1]
        )
        return emphasized.astype(np.int16)

    def export_flac(
        self, audio_buffer: np.ndarray, sample_rate: int = 16000
    ) -> bytes:
        """Exports a NumPy array to FLAC bytes."""
        buf = io.BytesIO()
        sf.write(buf, audio_buffer, sample_rate, format="FLAC")
        return buf.getvalue()

    def export_m4a(
        self, audio_buffer: np.ndarray, sample_rate: int = 16000
    ) -> bytes:
        """Exports a NumPy array to M4A bytes using ffmpeg."""
        with tempfile.NamedTemporaryFile(
            suffix=".wav", delete=False
        ) as wav_file:
            wav_path = wav_file.name
            sf.write(wav_path, audio_buffer, sample_rate)

        m4a_path = wav_path.replace(".wav", ".m4a")
        try:
            subprocess.run(
                ["ffmpeg", "-y", "-i", wav_path, "-c:a", "aac", m4a_path],
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

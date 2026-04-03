"""Stateless acoustic manipulation and Voice Activity Detection (VAD) utilities."""

import io
import json
import logging
import os
import urllib.parse
from collections.abc import Callable
from typing import Any

import numpy as np
import sherpa_onnx
from google.cloud import storage
from pydub import AudioSegment, effects

from backend.pipeline.common.constants import (
    AUDIO_FORMAT,
    NUM_AUDIO_CHANNELS,
    SAMPLE_RATE_HZ,
)
from backend.pipeline.transcription.constants import (
    BYTES_PER_SAMPLE_16BIT,
    DEFAULT_VAD_POST_ROLL_MS,
    DEFAULT_VAD_PRE_ROLL_MS,
    AUDIO_NORMALIZATION_DENOMINATOR,
    AUDIO_NORMALIZATION_MULTIPLIER,
    DRC_RATIO_VAD,
    DRC_THRESHOLD_VAD,
    VAD_DEFAULT_MIN_SILENCE_DURATION_SEC,
    VAD_DEFAULT_MIN_SPEECH_DURATION_SEC,
    VAD_DEFAULT_THRESHOLD,
    VAD_GAP_HARD_RESET_MS,
    VAD_GAP_SOFT_RESET_MS,
    VAD_HIGHPASS_FREQ,
    VAD_LOWPASS_FREQ,
    MS_PER_SEC,
    INT16_SILENCE_THRESHOLD_RMS,
    PRE_EMPHASIS_COEFF,
)
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
        from backend.pipeline.transcription.audio.silero_vad import create_sherpa_vad
        
        try:
            config = json.loads(config_json)
        except json.JSONDecodeError:
            logger.warning("Failed to decode VAD config JSON, using defaults in factory")
            config = {}
        threshold = config.get("threshold", VAD_DEFAULT_THRESHOLD)
        min_silence_duration = config.get("min_silence_duration", VAD_DEFAULT_MIN_SILENCE_DURATION_SEC)
        min_speech_duration = config.get("min_speech_duration", VAD_DEFAULT_MIN_SPEECH_DURATION_SEC)

        return create_sherpa_vad(threshold, min_silence_duration, min_speech_duration)

    def _create_sherpa_denoiser(self, *args: Any) -> Any:
        """Factory for Sherpa-ONNX Denoiser. Accepts args to ignore unused parameters from SharedResources."""
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
        audio: AudioSegment,
        speech_segments: list[TimeRange],
        silence_segments: list[TimeRange],
        noise_segments: list[TimeRange],
        file_start_ms: int,
    ) -> list[PaddedSegment]:
        """Calculates padded boundaries for speech segments and extracts them."""
        padded_segments = []

        valid_speech_segments = self._remove_invalid_segments(speech_segments)

        padded_ranges = calculate_padded_ranges(
            audio_duration_ms=len(audio),
            speech_segments=valid_speech_segments,
            noise_segments=noise_segments,
            file_start_ms=file_start_ms,
            pre_roll_ms=self.pre_roll_ms,
            post_roll_ms=self.post_roll_ms,
        )

        for i, reg in enumerate(valid_speech_segments):
            append_start, append_end = padded_ranges[i]
            if append_end > append_start:
                padded_audio = audio[append_start:append_end]
                padded_segments.append(
                    PaddedSegment(
                        audio=padded_audio,
                        start_ms=file_start_ms + append_start,
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
        """Detects speech segments using Sherpa-ONNX Silero VAD and GTCRN Denoiser.
        
        Note: This is a large method that handles gaps, denoising, and VAD.
        We will delegate the VAD part to silero_vad.py.
        """
        from backend.pipeline.transcription.audio.silero_vad import process_vad_streaming

        if getattr(self, "vad_sensitive", None) is None or getattr(self, "vad_strict", None) is None:
            msg = "VAD instances are not initialized"
            raise RuntimeError(msg)
        if self.denoiser is None:
            msg = "Denoiser is not initialized"
            raise RuntimeError(msg)

        speech_segments = []

        # 1. Gap Detection & Two-Stage Reset
        if self.last_audio_end_ms is not None:
            gap_ms = start_ms - self.last_audio_end_ms
            logger.info("Audio gap detected: %sms", gap_ms)

            if gap_ms > VAD_GAP_HARD_RESET_MS:
                logger.info("Gap > %sms: Flushing VAD and resetting states.", VAD_GAP_HARD_RESET_MS)
                self.vad_sensitive.detector.flush()
                self.vad_strict.detector.flush()
                
                # Drain any Trapped words
                chunk_data = process_vad_streaming(np.array([], dtype=np.int16), start_ms, self.vad_sensitive)
                speech_segments.extend(chunk_data.speech_segments)

                self.vad_sensitive.detector.reset()
                self.vad_strict.detector.reset()
                self.denoiser.reset()

            elif gap_ms > VAD_GAP_SOFT_RESET_MS:
                logger.info("Gap > %sms: Resetting VAD and Denoiser (No flush).", VAD_GAP_SOFT_RESET_MS)
                self.vad_sensitive.detector.reset()
                self.vad_strict.detector.reset()
                self.denoiser.reset()
            else:
                logger.info("Gap <= %sms: Keeping stream warm.", VAD_GAP_SOFT_RESET_MS)

        # Update last audio end time
        duration_ms = (len(samples) * MS_PER_SEC) // SAMPLE_RATE_HZ
        self.last_audio_end_ms = start_ms + duration_ms

        # 2. Continuous Denoising via Online Denoiser
        samples_float = samples.astype(np.float32) / AUDIO_NORMALIZATION_DENOMINATOR

        logger.info("Running OnlineSpeechDenoiser...")
        denoised_audio_obj = self.denoiser.run(
            samples_float.tolist(), SAMPLE_RATE_HZ
        )
        denoised_audio = np.array(denoised_audio_obj.samples, dtype=np.float32)

        if len(denoised_audio) == 0:
            logger.warning("No denoised audio generated.")
            return [], [], []

        # 3. Signal Recovery (Dynamic Range Compression + Pre-Emphasis)
        denoised_int16 = (denoised_audio * AUDIO_NORMALIZATION_MULTIPLIER).astype(np.int16)
        
        # We simulate the recovery pipeline but pass the samples to process_vad_streaming
        # Wait, process_vad_streaming in silero_vad.py does its own normalization.
        # It expects int16 samples!
        # So we pass denoised_int16 to process_vad_streaming!
        
        logger.info("Delegating VAD to silero_vad.py...")
        # Wait, silero_vad.py process_vad_streaming expects raw int16 pcm and does normalization.
        # It does NOT do DRC or Pre-emphasis yet.
        # If we want to preserve DRC and Pre-emphasis, we must do them here before passing!
        
        denoised_segment = AudioSegment(
            denoised_int16.tobytes(),
            frame_rate=SAMPLE_RATE_HZ,
            sample_width=BYTES_PER_SAMPLE_16BIT,
            channels=NUM_AUDIO_CHANNELS,
        )

        audio_compressed = effects.compress_dynamic_range(
            denoised_segment, threshold=DRC_THRESHOLD_VAD, ratio=DRC_RATIO_VAD
        )

        samples_rec = np.array(
            audio_compressed.get_array_of_samples(), dtype=np.int16
        )
        samples_rec = self._apply_pre_emphasis(samples_rec)

        # Two-Pass VAD Architecture Rationale:
        # We use a two-pass approach to balance precision and recall when isolating transmissions.
        # 
        # Pass 1: Sensitive detection (High Recall)
        # Uses a low threshold and long silence duration to capture all potential speech candidates.
        # This ensures we don't miss quiet words at the beginning/end of transmissions, but it is
        # prone to merging independent transmissions separated by short pauses.
        vad_result1 = process_vad_streaming(samples_rec, start_ms, self.vad_sensitive)
        coarse_segments = vad_result1.speech_segments
        logger.info("Pass 1 (Coarse) segments: %s", coarse_segments)
        
        final_speech_segments = []
        
        # Pass 2: Refinement/Splitting (High Precision for Segmentation)
        # Processes Pass 1 candidates with a higher threshold and shorter silence duration.
        # This accurately splits merged transmissions into independent utterances, fulfilling
        # the requirement to not merge transmissions, at the cost of accepting some noise within candidates.
        for seg in coarse_segments:
            logger.info("Refining segment: [%s, %s]", seg.start_ms, seg.end_ms)
            start_idx = int(((seg.start_ms - start_ms) * SAMPLE_RATE_HZ) / MS_PER_SEC)
            end_idx = int(((seg.end_ms - start_ms) * SAMPLE_RATE_HZ) / MS_PER_SEC)
            
            start_idx = max(0, start_idx)
            end_idx = min(len(samples_rec), end_idx)
            
            segment_samples = samples_rec[start_idx:end_idx]
            
            if len(segment_samples) == 0:
                logger.info("  Empty segment samples, skipping.")
                continue
                
            vad_result2 = process_vad_streaming(segment_samples, seg.start_ms, self.vad_strict)
            logger.info("  Pass 2 split: %s", vad_result2.speech_segments)
            final_speech_segments.extend(vad_result2.speech_segments)

        # Post-VAD Gate: Filter out horns/sirens from detected segments
        filtered_speech = []
        for seg in final_speech_segments:
            start_idx = int(((seg.start_ms - start_ms) * SAMPLE_RATE_HZ) / MS_PER_SEC)
            end_idx = int(((seg.end_ms - start_ms) * SAMPLE_RATE_HZ) / MS_PER_SEC)
            
            start_idx = max(0, start_idx)
            end_idx = min(len(denoised_audio), end_idx)
            
            if end_idx > start_idx:
                seg_audio = denoised_audio[start_idx:end_idx]
                
                # Minimum duration for HNR check (e.g., 0.5s)
                if len(seg_audio) > (SAMPLE_RATE_HZ * VAD_DEFAULT_MIN_SPEECH_DURATION_SEC):
                    result = self.signal_analyzer.characterize(seg_audio)
                    logger.info(
                        "Post-VAD Gate: Segment %s-%s characterized as %s (confidence: %.2f)",
                        seg.start_ms, seg.end_ms, result.label, result.confidence
                    )
                    
                    if (
                        result.label == "deterministic_linear"
                        and result.confidence > SIGNAL_ANALYZER_VOICED_PROB_HIGH_THRESHOLD
                    ):
                        logger.info("Post-VAD Gate: Dropping deterministic segment (Horn/Siren).")
                        continue
                        
            filtered_speech.append(seg)

        speech_segments.extend(filtered_speech)

        return speech_segments, [], []

    def download_audio_and_detect(
        self, gcs_path: str, start_ms: int, session_id: str
    ) -> AudioChunkData:
        """Downloads FLAC bytes from GCS and runs the spectral flatness detector natively."""
        session_sensitive = session_id + "_sensitive"
        session_strict = session_id + "_strict"
        
        config_sensitive = json.dumps({
            "threshold": 0.15,
            "min_silence_duration": 0.6,
            "min_speech_duration": 0.25
        })
        
        config_strict = json.dumps({
            "threshold": 0.25,
            "min_silence_duration": 0.25,
            "min_speech_duration": 0.05
        })

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
            if getattr(self, "vad_sensitive", None) is None or getattr(self, "current_session_id", None) != session_id:
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

        full_audio_segment = AudioSegment.from_file(
            in_mem_file, format=AUDIO_FORMAT
        )

        # Ensure audio is 16kHz mono 16-bit PCM for the detector
        audio_16k = (
            full_audio_segment.set_frame_rate(SAMPLE_RATE_HZ)
            .set_channels(NUM_AUDIO_CHANNELS)
            .set_sample_width(BYTES_PER_SAMPLE_16BIT)
        )



        # VAD detection is moved to the Stitcher for context!
        # Here we just return the raw audio and empty segments.
        return AudioChunkData(
            start_ms=start_ms,
            audio=full_audio_segment,
            speech_segments=[],
            gcs_uri=gcs_path,
            silence_segments=[],
            noise_segments=[],
            padded_segments=[],
        )

    def detect_vad(
        self,
        chunk_audio: AudioSegment,
        start_ms: int,
    ) -> list[PaddedSegment]:
        """Runs VAD on chunk_audio."""
        # 1. Convert to 16kHz mono 16-bit PCM
        audio_16k = (
            chunk_audio.set_frame_rate(SAMPLE_RATE_HZ)
            .set_channels(NUM_AUDIO_CHANNELS)
            .set_sample_width(BYTES_PER_SAMPLE_16BIT)
        )

        pcm_bytes = audio_16k.raw_data
        samples = np.frombuffer(pcm_bytes, dtype=np.int16)

        # 2. Run VAD
        speech_segments, noise_segments, silence_segments = (
            self._detect_speech_and_noise(samples, start_ms)
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





    def export_flac(self, audio_buffer: AudioSegment) -> bytes:
        """Exports an AudioSegment to FLAC bytes."""
        buf = io.BytesIO()
        audio_buffer.export(buf, format=AUDIO_FORMAT)
        return buf.getvalue()

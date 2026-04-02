"""Stateless acoustic manipulation and Voice Activity Detection (VAD) utilities."""

import io
import logging
import urllib.parse
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import librosa
import numpy as np
import ten_vad
from google.cloud import storage
from pydub import AudioSegment, effects
from scipy.signal import butter, lfilter

from backend.pipeline.common.constants import (
    AUDIO_FORMAT,
    NUM_AUDIO_CHANNELS,
    SAMPLE_RATE_HZ,
)
from backend.pipeline.transcription.constants import (
    BYTES_PER_SAMPLE_16BIT,
    DEFAULT_SED_FFT_SIZE,
    DEFAULT_SED_HOP_SIZE,
    DEFAULT_TENVAD_THRESHOLD,
    DEFAULT_VAD_POST_ROLL_MS,
    DEFAULT_VAD_PRE_ROLL_MS,
    INT16_MAX_FLOAT,
    MIN_SPEECH_DURATION_MS,
    VAD_FLATNESS_NOISE_THRESHOLD,
    VAD_RMS_SILENCE_THRESHOLD,
)
from backend.pipeline.transcription.datatypes import (
    AudioChunkData,
    PaddedSegment,
    TimeRange,
)
from backend.pipeline.transcription.detectors import AcousticGateDetector
from backend.pipeline.transcription.dsp import (
    compute_rms_energy,
    compute_spectral_flatness,
)
from backend.pipeline.transcription.enums import VadType
from backend.pipeline.transcription.resources import SharedResources

logger = logging.getLogger(__name__)


def get_gcs_client() -> storage.Client:
    """Initialize and return a GCS Client. Used natively by the audio processor for isolation."""
    return storage.Client()


def calculate_padded_ranges(
    audio_duration_ms: int,
    speech_segments: list[TimeRange],
    noise_segments: list[TimeRange],
    file_start_ms: int,
    pre_roll_ms: int = DEFAULT_VAD_PRE_ROLL_MS,
    post_roll_ms: int = DEFAULT_VAD_POST_ROLL_MS,
) -> list[tuple[int, int]]:
    """Calculates padded boundaries for speech segments, bounded by noise."""
    padded_ranges = []
    for reg in speech_segments:
        if reg.start_ms >= reg.end_ms:
            continue
        global_start_ms = reg.start_ms - file_start_ms
        global_end_ms = reg.end_ms - file_start_ms

        # Pre-roll calculation
        append_start = max(0, global_start_ms - pre_roll_ms)

        # Post-roll calculation
        append_end = min(audio_duration_ms, global_end_ms + post_roll_ms)

        # Noise truncation
        # Find the next noise segment that starts after or at the end of speech
        noise_after = [
            n.start_ms - file_start_ms
            for n in noise_segments
            if n.start_ms >= reg.end_ms
        ]
        if noise_after:
            orig_end = append_end
            append_end = min(append_end, min(noise_after))
            if append_end < orig_end:
                logger.info(
                    f"PaddedSegment post-roll truncated by noise from {orig_end} to {append_end}ms"
                )

        # Find the previous noise segment that ends before or at the start of speech
        noise_before = [
            n.end_ms - file_start_ms
            for n in noise_segments
            if n.end_ms <= reg.start_ms
        ]
        if noise_before:
            append_start = max(append_start, max(noise_before))

        padded_ranges.append((append_start, append_end))
    return padded_ranges


@dataclass
class AudioCharacterization:
    label: str  # 'stochastic', 'deterministic_linear', 'deterministic_static', or 'void'
    is_transcribable: bool
    confidence: float
    hnr: float = 0.0
    trimmed_duration_ms: int = 0


class RadioSignalAnalyzerPyin:
    def __init__(self, sample_rate=16000, cutoff_freq=4000):
        self.fs = sample_rate
        self.cutoff_freq = cutoff_freq
        self.nyquist = self.fs / 2
        self.order = 4

    def _butter_lowpass(self):
        normalized_cutoff = self.cutoff_freq / self.nyquist
        b, a = butter(self.order, normalized_cutoff, btype="low", analog=False)
        return b, a

    def _apply_filter(self, data):
        b, a = self._butter_lowpass()
        return lfilter(b, a, data)

    def characterize(self, audio_data) -> AudioCharacterization:
        # Pre-filter to remove noise above speech range
        clean_audio = self._apply_filter(audio_data)

        # 1. pYIN Pitch Tracking
        f0, voiced_flag, voiced_probs = librosa.pyin(
            clean_audio, fmin=100, fmax=1500, sr=self.fs, hop_length=512
        )

        # 2. HNR Calculation via HPSS
        y_harmonic, y_percussive = librosa.effects.hpss(audio_data)
        harmonic_energy = np.sum(y_harmonic**2)
        noise_energy = np.sum(y_percussive**2)
        hnr = harmonic_energy / (noise_energy + 1e-10)

        # Calculate heuristics
        valid_f0 = f0[voiced_probs > 0.6]
        # Use max probability instead of mean to prevent noise dilution in merged segments
        mean_prob = np.max(voiced_probs) if len(voiced_probs) > 0 else 0.0

        # Calculate trimmed duration based on threshold 0.2
        voiced_indices = np.where(voiced_probs > 0.2)[0]
        if len(voiced_indices) > 0:
            last_voiced_frame = voiced_indices[-1]
            trimmed_duration_ms = int(last_voiced_frame * 512 * 1000 / self.fs)
        else:
            trimmed_duration_ms = 0

        # POC: Keep everything unless it is a confident siren
        pitch_slope = np.diff(valid_f0) if len(valid_f0) > 1 else []
        slope_variance = np.var(pitch_slope) if len(pitch_slope) > 0 else 100.0

        if len(valid_f0) >= 10 and slope_variance < 5.0:
            return AudioCharacterization(
                "deterministic_linear",
                False,
                mean_prob,
                hnr,
                trimmed_duration_ms,
            )

        # Otherwise, assume it's speech!
        return AudioCharacterization(
            "stochastic", True, mean_prob, hnr, trimmed_duration_ms
        )


class RadioSignalAnalyzer:
    """
    Distinguishes between human speech (chaotic frame-to-frame changes)
    and machine horns/alarms (smooth or static frame-to-frame changes).
    Immune to Doppler shifts and intentional siren sweeps.
    """

    def __init__(self, sample_rate=16000):
        self.fs = sample_rate
        # The new tuning knob: We are measuring the chaos of the DELTA.
        # Sirens/Horns glide smoothly (Delta STD < 50 Hz).
        # Speech jumps wildly (Delta STD > 200 Hz).
        self.delta_std_threshold = 100.0

    def characterize(self, audio_data: np.ndarray) -> AudioCharacterization:
        if len(audio_data) == 0:
            return AudioCharacterization("void", False, 1.0, 0.0, 0)

        # 1. Fast calculation of the Spectral Centroid
        try:
            centroids = librosa.feature.spectral_centroid(
                y=audio_data, sr=self.fs, n_fft=512, hop_length=256
            )[0]
        except Exception:
            return AudioCharacterization(
                "stochastic",
                True,
                0.5,
                0.0,
                int((len(audio_data) / self.fs) * 1000),
            )

        # 2. THE DOPPLER FIX: Calculate the frame-to-frame differences
        # This strips out any smooth glide or sweep, isolating the "chaos"
        centroid_deltas = np.diff(centroids)

        # 3. Calculate the variance of the Deltas
        delta_std = float(np.std(centroid_deltas))

        # 4. Classify based on Dynamic Chaos
        if delta_std < self.delta_std_threshold:
            # Smooth glide or static tone -> Horn / Alarm / Siren
            confidence = max(0.0, 1.0 - (delta_std / self.delta_std_threshold))
            return AudioCharacterization(
                label="deterministic_linear",
                is_transcribable=False,
                confidence=confidence,
                hnr=0.0,
                trimmed_duration_ms=0,
            )
        # Erratic jumps -> Human Speech
        duration_ms = int((len(audio_data) / self.fs) * 1000)
        return AudioCharacterization(
            label="stochastic",
            is_transcribable=True,
            confidence=0.95,
            hnr=0.0,
            trimmed_duration_ms=duration_ms,
        )


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
        gcs_factory: Callable[[], Any] | None = None,
    ) -> None:
        self.vad_type = vad_type
        self.vad_config = vad_config
        self.shared_resources = shared_resources
        self.gcs_factory = gcs_factory
        self.sed_detector = AcousticGateDetector()

        self.vad: Any | None = None
        self.denoiser: Any | None = None
        self.gcs_client: Any | None = None
        self.last_audio_end_ms: int | None = None
        self.vad_remainder = np.array([], dtype=np.float32)
        self.stream_start_ms: int | None = None
        self.last_denoised_audio = np.array([], dtype=np.float32)

    def setup(self) -> None:
        """Initializes the VAD and GCS client once per worker."""
        active_gcs_factory = self.gcs_factory or get_gcs_client

        import json

        try:
            config = json.loads(self.vad_config)
        except:
            config = {}

        if self.vad_type == VadType.SILERO_VAD:
            self.vad_threshold = config.get("threshold", 0.15)
            self.vad_min_speech_duration = config.get(
                "min_speech_duration", 0.1
            )
            self.vad_min_silence_duration = config.get(
                "min_silence_duration", 0.6
            )

            def create_sherpa_vad(_type: Any, config_json: str) -> Any:
                import sherpa_onnx

                vad_config = sherpa_onnx.VadModelConfig(
                    silero_vad=sherpa_onnx.SileroVadModelConfig(
                        model="backend/pipeline/transcription/resources/silero_vad.onnx",
                        threshold=self.vad_threshold,
                        min_silence_duration=self.vad_min_silence_duration,
                        min_speech_duration=self.vad_min_speech_duration,
                    ),
                    sample_rate=SAMPLE_RATE_HZ,
                    num_threads=1,
                )
                return sherpa_onnx.VoiceActivityDetector(
                    vad_config, buffer_size_in_seconds=30
                )

            def create_sherpa_denoiser() -> Any:
                import sherpa_onnx

                gtcrn_config = sherpa_onnx.OfflineSpeechDenoiserGtcrnModelConfig(
                    model="backend/pipeline/transcription/resources/gtcrn_simple.onnx"
                )
                model_config = sherpa_onnx.OfflineSpeechDenoiserModelConfig(
                    gtcrn=gtcrn_config, num_threads=1, provider="cpu"
                )
                config = sherpa_onnx.OnlineSpeechDenoiserConfig(
                    model=model_config
                )
                return sherpa_onnx.OnlineSpeechDenoiser(config)

            if self.shared_resources is not None:
                self.vad = self.shared_resources.get_vad(
                    create_sherpa_vad, self.vad_type, self.vad_config
                )
                self.denoiser = self.shared_resources.get_denoiser(
                    create_sherpa_denoiser
                )
                self.gcs_client = self.shared_resources.get_gcs(
                    active_gcs_factory
                )
            else:
                self.vad = create_sherpa_vad(self.vad_type, self.vad_config)
                self.denoiser = create_sherpa_denoiser()
                self.gcs_client = active_gcs_factory()

        else:
            self.vad_threshold = config.get(
                "threshold", DEFAULT_TENVAD_THRESHOLD
            )
            self.vad_hop_size = config.get("hop_size", 256)

            def create_ten_vad(_type: Any, config_json: str) -> ten_vad.TenVad:
                return ten_vad.TenVad(
                    threshold=self.vad_threshold, hop_size=self.vad_hop_size
                )

            if self.shared_resources is not None:
                self.vad = self.shared_resources.get_vad(
                    create_ten_vad, self.vad_type, self.vad_config
                )
                self.gcs_client = self.shared_resources.get_gcs(
                    active_gcs_factory
                )
            else:
                self.vad = create_ten_vad(self.vad_type, self.vad_config)
                self.gcs_client = active_gcs_factory()

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

        if self.vad_type == VadType.SILERO_VAD:
            pre_roll_ms = 200
            post_roll_ms = 0
        else:
            pre_roll_ms = DEFAULT_VAD_PRE_ROLL_MS
            post_roll_ms = DEFAULT_VAD_POST_ROLL_MS

        # Filter out invalid segments first to avoid index mismatch
        valid_speech_segments = self._remove_invalid_segments(speech_segments)

        padded_ranges = calculate_padded_ranges(
            audio_duration_ms=len(audio),
            speech_segments=valid_speech_segments,
            noise_segments=noise_segments,
            file_start_ms=file_start_ms,
            pre_roll_ms=pre_roll_ms,
            post_roll_ms=post_roll_ms,
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

    def _detect_segments(
        self, samples: np.ndarray, sample_rate: int
    ) -> list[TimeRange]:
        """Converts PCM audio into numpy chunks to detect speech segment boundaries."""
        hop_size = self.vad_hop_size
        threshold = self.vad_threshold

        # Apply preprocessing (Band-Pass + Compression + Pre-Emphasis) only for TEN_VAD
        audio_segment = AudioSegment(
            samples.tobytes(),
            frame_rate=sample_rate,
            sample_width=BYTES_PER_SAMPLE_16BIT,
            channels=NUM_AUDIO_CHANNELS,
        )
        audio_filtered = audio_segment.high_pass_filter(300).low_pass_filter(
            3500
        )
        audio_compressed = effects.compress_dynamic_range(
            audio_filtered, threshold=-25.0, ratio=4.0
        )

        samples_processed = np.array(
            audio_compressed.get_array_of_samples(), dtype=np.int16
        )
        samples_processed = self.apply_pre_emphasis(samples_processed)

        probs = []
        for i in range(0, len(samples_processed), hop_size):
            chunk = samples_processed[i : i + hop_size]
            if len(chunk) < hop_size:
                padded_chunk = np.zeros(hop_size, dtype=np.int16)
                padded_chunk[: len(chunk)] = chunk
                chunk = padded_chunk

            prob, _flags = self.vad.process(chunk)
            probs.append(prob)

        probs_array = np.array(probs)
        logger.info(
            f"VAD Probs: Min={np.min(probs_array):.3f}, Max={np.max(probs_array):.3f}, Mean={np.mean(probs_array):.3f}"
        )
        logger.info(
            f"VAD Chunks above threshold ({threshold}): {np.sum(probs_array >= threshold)} / {len(probs)}"
        )

        segments = []
        is_speech = False
        start_ms = 0
        start_idx = 0

        for i, prob in enumerate(probs):
            current_ms = int((i * hop_size * 1000) / sample_rate)
            if prob >= threshold and not is_speech:
                is_speech = True
                start_ms = current_ms
                start_idx = i
            elif prob < threshold and is_speech:
                is_speech = False
                seg_probs = probs_array[start_idx:i]
                avg_prob = np.mean(seg_probs) if len(seg_probs) > 0 else 0.0
                logger.info(
                    f"VAD Candidate Segment: {start_ms}-{current_ms} ms, Avg Prob: {avg_prob:.3f}"
                )
                segments.append(
                    (TimeRange(start_ms=start_ms, end_ms=current_ms), avg_prob)
                )

        if is_speech:
            total_duration_ms = int((len(samples) * 1000) / sample_rate)
            seg_probs = probs_array[start_idx:]
            avg_prob = np.mean(seg_probs) if len(seg_probs) > 0 else 0.0
            logger.info(
                f"VAD Candidate Segment: {start_ms}-{total_duration_ms} ms, Avg Prob: {avg_prob:.3f}"
            )
            segments.append(
                (
                    TimeRange(start_ms=start_ms, end_ms=total_duration_ms),
                    avg_prob,
                )
            )

        # Merge close segments (gap < 200ms) to handle fragmentation
        merged_segments = []
        if segments:
            current_seg = segments[0]
            for next_seg in segments[1:]:
                # next_seg and current_seg are now tuples: (TimeRange, probability)
                if next_seg[0].start_ms - current_seg[0].end_ms < 200:
                    # Merge! Expand the end of the current segment and keep the highest probability!
                    current_seg = (
                        TimeRange(
                            start_ms=current_seg[0].start_ms,
                            end_ms=next_seg[0].end_ms,
                        ),
                        max(current_seg[1], next_seg[1]),
                    )
                else:
                    merged_segments.append(current_seg)
                    current_seg = next_seg
            merged_segments.append(current_seg)
        segments = merged_segments

        # Duration constraint
        min_duration_ms = MIN_SPEECH_DURATION_MS
        segments = [
            s
            for s in segments
            if (s[0].end_ms - s[0].start_ms) >= min_duration_ms
        ]

        return segments

    def _detect_speech_and_noise(
        self, samples: np.ndarray, start_ms: int
    ) -> tuple[list[TimeRange], list[TimeRange], list[TimeRange]]:
        """Detects speech and noise segments."""
        if self.vad_type == VadType.SILERO_VAD:
            return self._detect_speech_and_noise_sherpa(samples, start_ms)
        return self._detect_speech_and_noise_legacy(samples, start_ms)

    def _detect_speech_and_noise_sherpa(
        self, samples: np.ndarray, start_ms: int
    ) -> tuple[list[TimeRange], list[TimeRange], list[TimeRange]]:
        """Detects speech segments using Sherpa-ONNX Silero VAD and GTCRN Denoiser."""
        import logging

        speech_segments = []

        # 1. Gap Detection & Two-Stage Reset
        if self.last_audio_end_ms is not None:
            gap_ms = start_ms - self.last_audio_end_ms
            logger.info(f"Audio gap detected: {gap_ms}ms")

            if gap_ms > 2000:
                logger.info("Gap > 2000ms: Flushing VAD and resetting states.")
                self.vad.flush()
                # Drain any Trapped words
                while not self.vad.empty():
                    segment = self.vad.front
                    seg_start_ms = start_ms + int(segment.start / 16)
                    seg_end_ms = start_ms + int(
                        (segment.start + len(segment.samples)) / 16
                    )
                    speech_segments.append(
                        TimeRange(start_ms=seg_start_ms, end_ms=seg_end_ms)
                    )
                    self.vad.pop()

                # Reset VAD and Denoiser
                self.vad.reset()
                self.denoiser.reset()
                self.vad_remainder = np.array([], dtype=np.float32)
                self.stream_start_ms = None

            elif gap_ms > 500:
                logger.info(
                    "Gap > 500ms: Resetting VAD and Denoiser (No flush)."
                )
                self.vad.reset()
                self.denoiser.reset()
                self.vad_remainder = np.array([], dtype=np.float32)
                self.stream_start_ms = None
            else:
                logger.info("Gap <= 500ms: Keeping stream warm.")

        # Update last audio end time
        duration_ms = (len(samples) * 1000) // SAMPLE_RATE_HZ
        self.last_audio_end_ms = start_ms + duration_ms

        # 2. Continuous Denoising via Online Denoiser
        samples_float = samples.astype(np.float32) / 32768.0

        logger.info("Running OnlineSpeechDenoiser...")
        denoised_audio_obj = self.denoiser.run(
            samples_float.tolist(), SAMPLE_RATE_HZ
        )
        denoised_audio = np.array(denoised_audio_obj.samples, dtype=np.float32)
        self.last_denoised_audio = denoised_audio

        if len(denoised_audio) == 0:
            logger.warning("No denoised audio generated.")
            return [], [], []

        # 3. Signal Recovery (Dynamic Range Compression + Pre-Emphasis)
        from pydub import effects

        denoised_int16 = (denoised_audio * 32767).astype(np.int16)
        denoised_segment = AudioSegment(
            denoised_int16.tobytes(),
            frame_rate=SAMPLE_RATE_HZ,
            sample_width=BYTES_PER_SAMPLE_16BIT,
            channels=NUM_AUDIO_CHANNELS,
        )

        logger.info("Applying dynamic range compression for VAD...")
        audio_compressed = effects.compress_dynamic_range(
            denoised_segment, threshold=-30.0, ratio=5.0
        )

        # Apply pre-emphasis to boost consonants
        samples_rec = np.array(
            audio_compressed.get_array_of_samples(), dtype=np.int16
        )
        samples_rec = self.apply_pre_emphasis(samples_rec)

        # Convert back to float32 for Silero
        vad_audio = samples_rec.astype(np.float32) / 32768.0

        # 4. VAD Loop
        if self.vad is None:
            logger.error("VAD not initialized!")
            return [], [], []

        window_size = 512

        # Initialize stream_start_ms if not set
        if self.stream_start_ms is None:
            self.stream_start_ms = start_ms
            logging.getLogger(__name__).info(
                f"Initialized stream_start_ms to {self.stream_start_ms}"
            )

        # Combine with previous remainder
        all_samples = np.concatenate([self.vad_remainder, vad_audio])

        # Process in chunks of window_size (512)
        processed_len = 0
        for i in range(0, len(all_samples) - window_size + 1, window_size):
            chunk = all_samples[i : i + window_size]
            self.vad.accept_waveform(chunk.tolist())
            processed_len += window_size

            while not self.vad.empty():
                segment = self.vad.front
                # Use stream_start_ms as base!
                seg_start_ms = self.stream_start_ms + int(segment.start / 16)
                seg_end_ms = self.stream_start_ms + int(
                    (segment.start + len(segment.samples)) / 16
                )
                # Post-VAD Gate: Check if segment is a horn!
                start_idx = int(
                    ((seg_start_ms - start_ms) * SAMPLE_RATE_HZ) / 1000
                )
                end_idx = int(((seg_end_ms - start_ms) * SAMPLE_RATE_HZ) / 1000)

                start_idx = max(0, start_idx)
                end_idx = min(len(denoised_audio), end_idx)

                if end_idx > start_idx:
                    seg_audio = denoised_audio[start_idx:end_idx]

                    from backend.pipeline.transcription.audio_processor import (
                        RadioSignalAnalyzerPyin,
                    )

                    analyzer = RadioSignalAnalyzerPyin(
                        sample_rate=SAMPLE_RATE_HZ
                    )

                    if len(seg_audio) > (SAMPLE_RATE_HZ * 0.1):
                        result = analyzer.characterize(seg_audio)
                        logging.getLogger(__name__).info(
                            f"Post-VAD Gate: Segment {seg_start_ms}-{seg_end_ms} characterized as {result.label} (confidence: {result.confidence:.2f})"
                        )

                        if (
                            result.label == "deterministic_linear"
                            and result.confidence > 0.7
                        ):
                            logging.getLogger(__name__).info(
                                "Post-VAD Gate: Dropping deterministic segment (Horn/Siren)."
                            )
                            self.vad.pop()
                            continue

                speech_segments.append(
                    TimeRange(start_ms=seg_start_ms, end_ms=seg_end_ms)
                )
                self.vad.pop()

        # Store remainder
        self.vad_remainder = all_samples[processed_len:]
        logging.getLogger(__name__).info(
            f"Stored {len(self.vad_remainder)} samples as remainder."
        )

        return speech_segments, [], []

    def _detect_speech_and_noise_legacy(
        self, samples: np.ndarray, start_ms: int
    ) -> tuple[list[TimeRange], list[TimeRange], list[TimeRange]]:
        """Detects speech and noise segments using TEN VAD and DSP (Legacy)."""
        import logging

        logger = logging.getLogger(__name__)
        speech_segments = []
        noise_segments = []
        silence_segments = []

        # 1. Get candidate regions from VAD
        vads_segments = self._detect_segments(samples, SAMPLE_RATE_HZ)

        # 2. Convert TimeRange to sample indices for DSP
        candidate_regions = []
        for vseg, avg_prob in vads_segments:
            start_idx = (vseg.start_ms * SAMPLE_RATE_HZ) // 1000
            end_idx = (vseg.end_ms * SAMPLE_RATE_HZ) // 1000
            candidate_regions.append((start_idx, end_idx, avg_prob))

        # 3. DSP Filter and Click Detection
        from backend.pipeline.transcription import constants
        from backend.pipeline.transcription.dsp import compute_rms_energy

        rms_energy = compute_rms_energy(samples.astype(np.float32) / 32768.0)
        DYNAMIC_ENERGY_MULTIPLIER = getattr(
            constants, "DYNAMIC_ENERGY_MULTIPLIER", 5.0
        )

        if len(rms_energy) > 0:
            noise_floor = np.percentile(rms_energy, 10)
            energy_threshold = max(
                0.001, noise_floor * DYNAMIC_ENERGY_MULTIPLIER
            )
            logger.info(
                f"Noise floor: {noise_floor:.6f}, Energy threshold: {energy_threshold:.6f}"
            )
        else:
            energy_threshold = 0.001

        analyzer = RadioSignalAnalyzerPyin(sample_rate=SAMPLE_RATE_HZ)

        for start, end, avg_prob in candidate_regions:
            seg = samples[start:end]
            if len(seg) == 0:
                continue

            # Apply Band-pass to the segment ONLY for characterization to improve accuracy.
            # Convert back to AudioSegment to use pydub filters easily!
            seg_segment = AudioSegment(
                seg.tobytes(),
                frame_rate=SAMPLE_RATE_HZ,
                sample_width=BYTES_PER_SAMPLE_16BIT,
                channels=NUM_AUDIO_CHANNELS,
            )
            seg_clean = seg_segment.high_pass_filter(300).low_pass_filter(3500)

            # Convert to float32 for librosa
            seg_float = (
                np.frombuffer(seg_clean.raw_data, dtype=np.int16).astype(
                    np.float32
                )
                / 32768.0
            )

            # Run pYIN to find the first voiced frame!
            max_pYIN_prob = 0.0
            try:
                # Use pYIN to find boundaries
                f0, voiced_flag, voiced_probs = librosa.pyin(
                    seg_float,
                    fmin=librosa.note_to_hz("C2"),
                    fmax=librosa.note_to_hz("C7"),
                    sr=SAMPLE_RATE_HZ,
                    fill_na=0.0,
                )

                max_pYIN_prob = np.max(voiced_probs)
                logger.info(
                    f"pYIN: Analyzed {len(voiced_probs)} frames. Max voiced prob: {max_pYIN_prob:.3f}"
                )

            except Exception as e:
                logger.warning(f"pYIN failed on segment: {e}")

            # Run characterization on band-passed audio
            result = analyzer.characterize(seg_float)

            logger.info(
                f"Target Label: {result.label}, Prob: {result.confidence:.3f}, HNR: {result.hnr:.2f}, Trimmed Len: {result.trimmed_duration_ms}ms"
            )

            seg_start_ms = start_ms + (start * 1000) // SAMPLE_RATE_HZ
            seg_end_ms = start_ms + (end * 1000) // SAMPLE_RATE_HZ
            duration_ms = ((end - start) * 1000) // SAMPLE_RATE_HZ

            # Enforce independent minimums instead of multiplying
            is_speech = avg_prob >= self.vad_threshold

            is_noise = not result.is_transcribable or not is_speech

            if is_noise:
                logger.info(
                    f"Labeled segment as noise (not speech): {seg_start_ms}-{seg_end_ms} (Conf: {result.confidence:.3f}, Len: {duration_ms}ms)"
                )
                noise_segments.append(
                    TimeRange(
                        start_ms=start_ms + (start * 1000) // SAMPLE_RATE_HZ,
                        end_ms=start_ms + (end * 1000) // SAMPLE_RATE_HZ,
                    )
                )
                continue

            actual_end_ms = start_ms + (end * 1000) // SAMPLE_RATE_HZ
            original_end_ms = start_ms + (end * 1000) // SAMPLE_RATE_HZ

            # Ensure minimum duration
            if (
                actual_end_ms - (start_ms + (start * 1000) // SAMPLE_RATE_HZ)
                < MIN_SPEECH_DURATION_MS
            ):
                logger.info(
                    f"Dropped segment after trimming (too short): {start_ms + (start * 1000) // SAMPLE_RATE_HZ}-{actual_end_ms}"
                )
                noise_segments.append(
                    TimeRange(
                        start_ms=start_ms + (start * 1000) // SAMPLE_RATE_HZ,
                        end_ms=original_end_ms,
                    )
                )
                continue

            chunk_rms = compute_rms_energy(seg_float)
            mean_rms = np.mean(chunk_rms) if len(chunk_rms) > 0 else 0

            duration_ms = actual_end_ms - (
                start_ms + (start * 1000) // SAMPLE_RATE_HZ
            )

            logger.info(
                f"Region: {start_ms + (start * 1000) // SAMPLE_RATE_HZ}ms to {actual_end_ms}ms "
                f"({duration_ms}ms), RMS: {mean_rms:.6f} vs threshold {energy_threshold:.6f}"
            )

            if mean_rms >= energy_threshold:
                speech_segments.append(
                    TimeRange(
                        start_ms=start_ms + (start * 1000) // SAMPLE_RATE_HZ,
                        end_ms=actual_end_ms,
                    )
                )
            else:
                logger.info(
                    f"Dropped segment due to low energy: {start_ms + (start * 1000) // SAMPLE_RATE_HZ}-{actual_end_ms}"
                )
                noise_segments.append(
                    TimeRange(
                        start_ms=start_ms + (start * 1000) // SAMPLE_RATE_HZ,
                        end_ms=original_end_ms,
                    )
                )

        return speech_segments, silence_segments, noise_segments

        # Everything else is implicitly silence or unclassified background
        return speech_segments, silence_segments, noise_segments

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

        full_audio_segment = AudioSegment.from_file(in_mem_file)

        # Ensure audio is 16kHz mono 16-bit PCM for the detector
        audio_16k = (
            full_audio_segment.set_frame_rate(SAMPLE_RATE_HZ)
            .set_channels(NUM_AUDIO_CHANNELS)
            .set_sample_width(BYTES_PER_SAMPLE_16BIT)
        )

        # Apply band-pass filter ONLY for the silence check to avoid out-of-band noise
        # This respects the Isolation Invariant.
        audio_for_check = audio_16k.high_pass_filter(300).low_pass_filter(3500)

        pcm_bytes = audio_for_check.raw_data
        samples = np.frombuffer(pcm_bytes, dtype=np.int16)

        # 1. Stateless Silence Gating: Drop chunks that lack physical acoustic energy
        # Compute RMS with float64 to avoid overflow
        rms = np.sqrt(np.mean(samples.astype(np.float64) ** 2))

        # Silence threshold (safe value based on evaluator where background noise was < 30)
        # Using 100.0 as a safe upper bound for absolute silence/static.
        SILENCE_THRESHOLD = 100.0

        if rms < SILENCE_THRESHOLD:
            logger.info(
                f"Chunk is silent (RMS: {rms:.2f} < {SILENCE_THRESHOLD}). Skipping VAD."
            )
            return AudioChunkData(
                start_ms=start_ms,
                audio=full_audio_segment,
                speech_segments=[],
                gcs_uri=gcs_path,
                silence_segments=[
                    TimeRange(
                        start_ms=start_ms,
                        end_ms=start_ms + len(samples) * 1000 // SAMPLE_RATE_HZ,
                    )
                ],
                noise_segments=[],
                padded_segments=[],
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

    def detect_vad_with_overlap(
        self,
        chunk_audio: AudioSegment,
        prev_chunk_audio: AudioSegment | None,
        start_ms: int,
    ) -> list[PaddedSegment]:
        """Runs VAD on chunk_audio, prepending overlap from prev_chunk_audio if available."""
        # 1. Handle overlap
        overlap_ms = 0
        overlap_duration = 0
        if prev_chunk_audio and overlap_ms > 0:
            overlap = (
                prev_chunk_audio[-overlap_ms:]
                if len(prev_chunk_audio) > overlap_ms
                else prev_chunk_audio
            )
            overlap_duration = len(overlap)
            combined_audio = overlap + chunk_audio
            actual_start_ms = start_ms - overlap_duration
        else:
            combined_audio = chunk_audio
            actual_start_ms = start_ms

        # 2. Convert to 16kHz mono 16-bit PCM
        audio_16k = (
            combined_audio.set_frame_rate(SAMPLE_RATE_HZ)
            .set_channels(NUM_AUDIO_CHANNELS)
            .set_sample_width(BYTES_PER_SAMPLE_16BIT)
        )

        pcm_bytes = audio_16k.raw_data
        samples = np.frombuffer(pcm_bytes, dtype=np.int16)

        # 3. Run VAD
        speech_segments, noise_segments, silence_segments = (
            self._detect_speech_and_noise(samples, actual_start_ms)
        )

        # 4. Calculate padded segments
        padded_segments = self._calculate_padded_segments(
            audio=combined_audio,
            speech_segments=speech_segments,
            silence_segments=silence_segments,
            noise_segments=noise_segments,
            file_start_ms=actual_start_ms,
        )

        # 5. Map back to current chunk timeline
        # Only keep segments that fall within or overlap with the CURRENT chunk's scope
        filtered_segments = []
        for seg in padded_segments:
            if seg.speech_end_ms > start_ms:
                filtered_segments.append(seg)

        return filtered_segments

    def apply_pre_emphasis(
        self, samples: np.ndarray, alpha: float = 0.97
    ) -> np.ndarray:
        """Boosts high-frequency speech features (consonants).
        Input: int16 numpy array
        """
        audio_float = samples.astype(np.float32)
        emphasized = np.append(
            audio_float[0], audio_float[1:] - alpha * audio_float[:-1]
        )
        return emphasized.astype(np.int16)

    def check_vad(self, audio_buffer: AudioSegment) -> bool:
        """Evaluates audio buffer with TenVAD and returns True if speech is detected."""
        if self.vad is None:
            msg = "VAD plugin not initialized. Call setup() first."
            raise RuntimeError(msg)

        audio_16k = (
            audio_buffer.set_frame_rate(SAMPLE_RATE_HZ)
            .set_channels(NUM_AUDIO_CHANNELS)
            .set_sample_width(BYTES_PER_SAMPLE_16BIT)
        )

        # Apply Band-pass Filter (300Hz to 3500Hz) first to de-noise for gates
        audio_for_vad = audio_16k.high_pass_filter(300).low_pass_filter(3500)

        pcm_bytes_vad = audio_for_vad.raw_data
        samples_vad = (
            np.frombuffer(pcm_bytes_vad, dtype=np.int16).astype(np.float32)
            / INT16_MAX_FLOAT
        )
        n_samples = len(samples_vad)

        if n_samples == 0:
            return False

        # Dynamically scale FFT windows to silence UserWarnings on short fragments
        n_fft = min(DEFAULT_SED_FFT_SIZE, n_samples)
        hop_length = min(DEFAULT_SED_HOP_SIZE, max(1, n_samples // 4))

        # 1a. RMS Silence Gate: Drop chunks that lack physical acoustic energy
        # Now running on band-passed audio!
        mean_rms = np.mean(
            compute_rms_energy(
                samples=samples_vad, frame_length=n_fft, hop_length=hop_length
            )
        )
        if (
            mean_rms < VAD_RMS_SILENCE_THRESHOLD
        ):  # Approx -46 dBFS (total silence)
            logger.info(
                f"VAD Heuristic: Dropped quiet segment (RMS: {mean_rms:.5f})"
            )
            return False

        # 1b. Spectral Flatness Noise Gate: Drop chunks that are purely uniform "white noise" static.
        # Now running on band-passed audio!
        mean_flatness = np.mean(
            compute_spectral_flatness(
                samples=samples_vad,
                sample_rate=SAMPLE_RATE_HZ,
                n_fft=n_fft,
                hop_length=hop_length,
            )
        )
        if mean_flatness > VAD_FLATNESS_NOISE_THRESHOLD:  # Featureless static
            logger.info(
                f"VAD Heuristic: Dropped static (Flatness: {mean_flatness:.3f})"
            )
            return False

        # 2. Signal Recovery (Dynamic Range Compression + Pre-Emphasis)
        logger.info("Applying dynamic range compression for VAD...")
        audio_compressed = effects.compress_dynamic_range(
            audio_for_vad, threshold=-25.0, ratio=4.0
        )

        # Apply pre-emphasis
        samples_rec = np.array(
            audio_compressed.get_array_of_samples(), dtype=np.int16
        )
        samples_rec = self.apply_pre_emphasis(samples_rec)

        # 3. Neural Evaluation (Final Authority)
        if self.vad_type == VadType.SILERO_VAD:
            # Convert to float32 for Sherpa VAD
            samples_float = samples_rec.astype(np.float32) / 32768.0

            window_size = 512
            has_speech = False

            for i in range(0, len(samples_float), window_size):
                chunk = samples_float[i : i + window_size]
                if len(chunk) < window_size:
                    chunk = np.pad(
                        chunk, (0, window_size - len(chunk)), "constant"
                    )

                self.vad.accept_waveform(chunk)

                while not self.vad.empty():
                    has_speech = True
                    self.vad.pop()

            self.vad.flush()
            while not self.vad.empty():
                has_speech = True
                self.vad.pop()

            return has_speech
        # Pass the fully recovered audio bytes to TenVAD!
        return self.vad.evaluate(
            samples_rec.tobytes(), sample_rate=SAMPLE_RATE_HZ
        )

    def preprocess_audio(self, audio_buffer: AudioSegment) -> AudioSegment:
        """Applies native bandpass filtering to remove rumble and static."""
        # Commented out to preserve signal integrity (including clicks for DSP detection)
        # audio = effects.high_pass_filter(audio_buffer, HIGHPASS_FILTER_FREQ)
        # return effects.low_pass_filter(audio, LOWPASS_FILTER_FREQ)
        return audio_buffer

    def get_spectral_flatness(self, audio_buffer: AudioSegment) -> np.ndarray:
        """Computes continuous spectral flatness across the entire audio buffer."""
        audio_16k = (
            audio_buffer.set_frame_rate(SAMPLE_RATE_HZ)
            .set_channels(NUM_AUDIO_CHANNELS)
            .set_sample_width(BYTES_PER_SAMPLE_16BIT)
        )
        pcm_bytes = audio_16k.raw_data
        samples = (
            np.frombuffer(pcm_bytes, dtype=np.int16).astype(np.float32)
            / INT16_MAX_FLOAT
        )

        from backend.pipeline.transcription.constants import (
            DEFAULT_SED_FFT_SIZE,
            DEFAULT_SED_HOP_SIZE,
        )
        from backend.pipeline.transcription.dsp import compute_spectral_flatness

        n_samples = len(samples)
        if n_samples == 0:
            return np.array([], dtype=np.float32)

        n_fft = min(DEFAULT_SED_FFT_SIZE, n_samples)
        hop_length = min(DEFAULT_SED_HOP_SIZE, max(1, n_samples // 4))

        return compute_spectral_flatness(
            samples=samples,
            sample_rate=SAMPLE_RATE_HZ,
            n_fft=n_fft,
            hop_length=hop_length,
        )

    def export_flac(self, audio_buffer: AudioSegment) -> bytes:
        """Exports an AudioSegment to FLAC bytes."""
        buf = io.BytesIO()
        audio_buffer.export(buf, format=AUDIO_FORMAT)
        return buf.getvalue()

    def export_m4a(self, audio_buffer: AudioSegment) -> bytes:
        """Exports an AudioSegment to M4A (AAC) bytes optimized for voice."""
        buf = io.BytesIO()
        # "ipod" is the ffmpeg format identifier for M4A.
        # We optimize for voice by forcing 16kHz, mono, and 32kbps bitrate.
        audio_buffer.export(
            buf,
            format="ipod",
            codec="aac",
            parameters=["-ar", "16000", "-ac", "1", "-b:a", "32k"],
        )
        return buf.getvalue()

    def process_buffer(
        self, audio_buffer: AudioSegment
    ) -> tuple[bool, bytes | None, AudioSegment | None]:
        """Encapsulates sequence of pre-processing, VAD check, and FLAC export."""
        processed_audio = self.preprocess_audio(audio_buffer)
        if not self.check_vad(processed_audio):
            return False, None, None

        flac_bytes = self.export_flac(processed_audio)
        return True, flac_bytes, processed_audio

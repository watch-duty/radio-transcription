"""Voice Activity Detection (VAD) Engine utilizing the Silero + UL-UNAS neural network pipeline.

Designed to align directly with watch-duty's optimized audio segmentation and denoiser heuristics,
avoiding the overhead of multi-VAD abstractions.
"""

import math
from pathlib import Path
from typing import Any

import numpy as np
import onnxruntime as ort
from pedalboard import (
    Compressor,
    HighpassFilter,
    LowpassFilter,
    PeakFilter,
    Pedalboard,
)

from backend.pipeline.normalization.audio.dsp import (
    TorchaudioHannResampler,
    custom_numpy_istft,
    custom_numpy_stft,
)
from backend.pipeline.normalization.common.constants import (
    VAD_DEFAULT_BLEND_RATIO,
    VAD_DEFAULT_BOOST_FREQ_HZ,
    VAD_DEFAULT_BOOST_GAIN_DB,
    VAD_DEFAULT_COMP_ATTACK_MS,
    VAD_DEFAULT_COMP_PEAK_THRESHOLD,
    VAD_DEFAULT_COMP_RATIO,
    VAD_DEFAULT_COMP_RELEASE_MS,
    VAD_DEFAULT_COMP_THRESHOLD_DB,
    VAD_DEFAULT_FALLBACK_PRIMING_SEC,
    VAD_DEFAULT_HIGHPASS_HZ,
    VAD_DEFAULT_LOWPASS_HZ,
    VAD_DEFAULT_MIN_RMS_THRESHOLD,
    VAD_DEFAULT_MIN_SILENCE_DURATION_MS,
    VAD_DEFAULT_MIN_SPEECH_DURATION_MS,
    VAD_DEFAULT_PAD_SEC,
    VAD_DEFAULT_PEAK_FILTER_Q,
    VAD_DEFAULT_PRIMING_SEC,
    VAD_DEFAULT_SEED,
    VAD_DEFAULT_SPIKINESS_RATIO_THRESHOLD,
    VAD_DEFAULT_THRESHOLD_OFFSET,
    VAD_DEFAULT_THRESHOLD_ONSET,
    VAD_NORMALIZATION_MIN_PEAK,
    VAD_NORMALIZATION_TARGET_PEAK,
)
from backend.pipeline.normalization.common.logging import get_task_logger

logger = get_task_logger(
    __name__, {"system": "transcription", "component": "vad"}
)

MODELS_DIR = Path(__file__).parent / "models"
TARGET_SAMPLE_RATE = 16000
DEFAULT_SILERO_WINDOW_SIZE = 512


class VoiceActivityDetector:
    """The voice activity detector engine incorporating a Pedalboard filter,
    UL-UNAS denoiser, presence boost, and the Silero VAD model to perform full speech segment detection.

    ### Core Priming & Slicing Strategy
    The normalization pipeline operates on both contiguous live streaming chunks (where prior audio tail history is present)
    and Segment 0 starts/offline evaluations (where no history exists). To ensure absolute safety and sensitivity:

    1. **Fallback Priming (Call Starts / Segment 0):**
       When no prior audio history is present, we fallback to generating exactly 1.0s of synthetic colored comfort noise
       directly at the native sample rate before single-pass resampling. This 1.0s pre-roll safely warms up the
       recurrent denoiser states (UL-UNAS), preventing vocal onset clipping and transient click muting.
       We ALWAYS slice off this 1.0s preamble before feeding the signal to VAD to maintain maximum onset sensitivity.

    2. **Contiguous Streaming Boundaries:**
       On contiguous live streaming chunk boundaries, a real-world prior audio tail is passed from the previous chunk.
       We concatenate the native prior tail with the current chunk and resample them in a single pass to guarantee absolute
       phase continuity.
       Because this is a streaming transition, we ALWAYS slice off the prior tail preamble before feeding the signal to VAD.
       Slicing off the preamble prevents VAD RNN state bleed-through, preventing false speech triggers across silent boundaries.
       This also cleanly eliminates complex time-coordinate shifting mathematics on all streaming coordinates.
    """

    def __init__(
        self,
        *,
        comp_threshold_db: float = VAD_DEFAULT_COMP_THRESHOLD_DB,
        comp_ratio: float = VAD_DEFAULT_COMP_RATIO,
        comp_attack_ms: float = VAD_DEFAULT_COMP_ATTACK_MS,
        comp_release_ms: float = VAD_DEFAULT_COMP_RELEASE_MS,
        highpass_hz: float = VAD_DEFAULT_HIGHPASS_HZ,
        lowpass_hz: float = VAD_DEFAULT_LOWPASS_HZ,
        blend_ratio: float = VAD_DEFAULT_BLEND_RATIO,
        boost_freq_hz: float = VAD_DEFAULT_BOOST_FREQ_HZ,
        boost_gain_db: float = VAD_DEFAULT_BOOST_GAIN_DB,
        peak_filter_q: float = VAD_DEFAULT_PEAK_FILTER_Q,
        threshold_onset: float = VAD_DEFAULT_THRESHOLD_ONSET,
        threshold_offset: float = VAD_DEFAULT_THRESHOLD_OFFSET,
        min_speech_duration_ms: int = VAD_DEFAULT_MIN_SPEECH_DURATION_MS,
        min_silence_duration_ms: int = VAD_DEFAULT_MIN_SILENCE_DURATION_MS,
        pad_sec: float = VAD_DEFAULT_PAD_SEC,
        priming_sec: float = VAD_DEFAULT_PRIMING_SEC,
        fallback_priming_sec: float = VAD_DEFAULT_FALLBACK_PRIMING_SEC,
        comp_peak_threshold: float = VAD_DEFAULT_COMP_PEAK_THRESHOLD,
        normalization_target_peak: float = VAD_NORMALIZATION_TARGET_PEAK,
        normalization_min_peak: float = VAD_NORMALIZATION_MIN_PEAK,
        seed: int = VAD_DEFAULT_SEED,
        spikiness_ratio_threshold: float = VAD_DEFAULT_SPIKINESS_RATIO_THRESHOLD,
        min_rms_threshold: float = VAD_DEFAULT_MIN_RMS_THRESHOLD,
        models_dir: str | Path = MODELS_DIR,
    ) -> None:

        self.comp_threshold_db = comp_threshold_db
        self.comp_ratio = comp_ratio
        self.comp_attack_ms = comp_attack_ms
        self.comp_release_ms = comp_release_ms
        self.highpass_hz = highpass_hz
        self.lowpass_hz = lowpass_hz
        self.blend_ratio = blend_ratio
        self.boost_freq_hz = boost_freq_hz
        self.boost_gain_db = boost_gain_db
        self.peak_filter_q = peak_filter_q
        self.threshold_onset = threshold_onset
        self.threshold_offset = threshold_offset
        self.min_speech_duration_ms = min_speech_duration_ms
        self.min_silence_duration_ms = min_silence_duration_ms
        self.pad_sec = pad_sec
        self.priming_sec = priming_sec
        self.fallback_priming_sec = fallback_priming_sec
        self.comp_peak_threshold = comp_peak_threshold
        self.normalization_target_peak = normalization_target_peak
        self.normalization_min_peak = normalization_min_peak
        self.seed = seed
        self.spikiness_ratio_threshold = spikiness_ratio_threshold
        self.min_rms_threshold = min_rms_threshold

        self.silero_path = Path(models_dir) / "silero_vad.onnx"
        self.ulunas_path = Path(models_dir) / "ulunas_stream_simple.onnx"

        # Eagerly initialize ONNX sessions
        opts = ort.SessionOptions()
        opts.intra_op_num_threads = 1
        opts.inter_op_num_threads = 1
        opts.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL

        logger.info("Initializing VAD sessions. Models path: %s", models_dir)
        if not self.silero_path.exists():
            msg = f"Silero ONNX model not found at: {self.silero_path}"
            raise FileNotFoundError(msg)
        if not self.ulunas_path.exists():
            msg = f"UL-UNAS ONNX model not found at: {self.ulunas_path}"
            raise FileNotFoundError(msg)

        self.silero_session = ort.InferenceSession(
            str(self.silero_path),
            sess_options=opts,
            providers=["CPUExecutionProvider"],
        )
        self.ulunas_session = ort.InferenceSession(
            str(self.ulunas_path),
            sess_options=opts,
            providers=["CPUExecutionProvider"],
        )
        logger.info("Silero & UL-UNAS ONNX sessions successfully initialized.")

        # Warm up Numba compiler to eliminate first-run latency spikes on Dataflow workers
        try:
            logger.info("Warming up Numba compiler...")
            dummy_wave = np.zeros(
                (1, DEFAULT_SILERO_WINDOW_SIZE), dtype=np.float32
            )
            dummy_stft = custom_numpy_stft(
                dummy_wave, n_fft=DEFAULT_SILERO_WINDOW_SIZE, hop_length=256
            )
            _ = custom_numpy_istft(
                dummy_stft,
                length=DEFAULT_SILERO_WINDOW_SIZE,
                n_fft=DEFAULT_SILERO_WINDOW_SIZE,
                hop_length=256,
            )
            logger.info("Numba compiler successfully warmed up.")
        except Exception as e:
            logger.warning("Failed to warm up Numba compiler: %s", e)

    def setup(self) -> None:
        """No-op. sessions are eagerly initialized in constructor."""

    def denoise(
        self,
        audio_array: np.ndarray,
        n_fft: int = DEFAULT_SILERO_WINDOW_SIZE,
        hop_length: int = 256,
    ) -> np.ndarray:
        """Applies the UL-UNAS neural denoiser ONNX model on the normalized float32 audio array."""
        self.setup()
        if self.ulunas_session is None:
            msg = "UL-UNAS session not initialized."
            raise RuntimeError(msg)

        inputs = self.ulunas_session.get_inputs()
        outputs = self.ulunas_session.get_outputs()
        audio_input_name = inputs[0].name

        states = {}
        for inp in inputs[1:]:
            shape = [s if isinstance(s, int) else 1 for s in inp.shape]
            states[inp.name] = np.zeros(shape, dtype=np.float32)

        bp_audio_batched = np.expand_dims(audio_array, axis=0)
        stft_features = custom_numpy_stft(
            bp_audio_batched, n_fft=n_fft, hop_length=hop_length
        )

        num_frames = stft_features.shape[2]
        out_stft = np.zeros_like(stft_features)

        # Pre-allocate and reuse the input dictionary to avoid heavy object allocation overhead in Python loop
        ort_inputs: dict[str, Any] = dict(states)

        # Thread safety: `states` and `ort_inputs` are call-local; each caller
        # maintains its own recurrent denoiser state (UL-UNAS hidden tensors).
        # ulunas_session.run() is thread-safe — the ONNX graph is immutable after
        # construction and no mutable state lives on the session object itself.
        # No lock is needed even though the session is shared across threads via
        # Beam's Shared() handle.
        for i in range(num_frames):
            ort_inputs[audio_input_name] = stft_features[:, :, i : i + 1, :]
            ort_outs = self.ulunas_session.run(None, ort_inputs)
            out_stft[:, :, i : i + 1, :] = ort_outs[0]
            for j in range(1, len(outputs)):
                name = inputs[j].name
                val = ort_outs[j]
                states[name] = val
                ort_inputs[name] = val

        return custom_numpy_istft(
            out_stft,
            length=audio_array.shape[0],
            n_fft=n_fft,
            hop_length=hop_length,
        )[0]

    def preprocess(
        self, audio_array: np.ndarray, prior_len_sec: float = 0.0
    ) -> np.ndarray:
        """Applies the VAD bandpass, denoiser, and eq presence boost pipeline."""
        bp_board = Pedalboard(
            [
                HighpassFilter(cutoff_frequency_hz=self.highpass_hz),
                # Wider bandwidth cutoff (4000 Hz) for maximal speech onset VAD sensitivity.
                # This is wider than the export path's 3000 Hz cutoff, which is optimized for clean transcription.
                LowpassFilter(cutoff_frequency_hz=self.lowpass_hz),
            ]
        )
        bp_audio = bp_board(audio_array, TARGET_SAMPLE_RATE)

        # Dynamically apply Compressor only if the raw signal is quiet (peak < self.comp_peak_threshold)
        # Compute the peak amplitude strictly from the actual current chunk (slicing off the pre-roll preamble)
        # to avoid signal volume contamination from preceding dispatches across streaming boundaries.
        preamble_samples = int(prior_len_sec * TARGET_SAMPLE_RATE)
        current_chunk = (
            audio_array[preamble_samples:]
            if preamble_samples > 0
            else audio_array
        )
        peak = np.max(np.abs(current_chunk)) if len(current_chunk) > 0 else 0.0

        if peak < self.comp_peak_threshold:
            comp_board = Pedalboard(
                [
                    Compressor(
                        threshold_db=self.comp_threshold_db,
                        ratio=self.comp_ratio,
                        attack_ms=self.comp_attack_ms,
                        release_ms=self.comp_release_ms,
                    )
                ]
            )
            comp_audio = comp_board(bp_audio, TARGET_SAMPLE_RATE)
        else:
            comp_audio = bp_audio

        ulunas_denoised = self.denoise(comp_audio)

        mixed_audio = (
            np.float32(1.0 - self.blend_ratio) * comp_audio
            + np.float32(self.blend_ratio) * ulunas_denoised
        )

        eq_board = Pedalboard(
            [
                PeakFilter(
                    cutoff_frequency_hz=self.boost_freq_hz,
                    gain_db=self.boost_gain_db,
                    q=self.peak_filter_q,
                )
            ]
        )
        return eq_board(mixed_audio, TARGET_SAMPLE_RATE)

    def _trim_and_shift_segments(
        self,
        raw_segments: list[tuple[float, float]],
        prior_len_sec: float,
    ) -> list[tuple[float, float]]:
        """Isolates detected speech segments relative strictly to the current audio chunk."""
        shifted_segments = []
        for start, end in raw_segments:
            if end <= prior_len_sec:
                continue
            start_sec = max(0.0, start - prior_len_sec)
            end_sec = end - prior_len_sec
            shifted_segments.append((start_sec, end_sec))
        return shifted_segments

    def _pad_and_merge_segments(
        self,
        segments: list[tuple[float, float]],
        audio_len_sec: float,
    ) -> list[tuple[float, float]]:
        """Pads and merges overlapping speech segments using the configured padding limits."""
        padded_segments = []
        for start, end in segments:
            p_start = max(0.0, start - self.pad_sec)
            p_end = min(audio_len_sec, end + self.pad_sec)
            if padded_segments and padded_segments[-1][1] >= p_start:
                padded_segments[-1] = (
                    padded_segments[-1][0],
                    max(padded_segments[-1][1], p_end),
                )
            else:
                padded_segments.append((p_start, p_end))
        return padded_segments

    def _peak_normalize(self, audio_array: np.ndarray) -> np.ndarray:
        """Applies software peak volume normalization if the signal is not completely silent."""
        peak = np.max(np.abs(audio_array))
        if peak >= self.normalization_min_peak:
            return audio_array / peak * self.normalization_target_peak
        return audio_array

    def _generate_comfort_noise(self, sample_rate: int) -> np.ndarray:
        """Generates shaped synthetic comfort noise static for Segment 0 priming."""
        priming_samples = int(self.fallback_priming_sec * sample_rate)
        if priming_samples <= 0:
            return np.empty(0, dtype=np.float32)

        # Use default RNG with self.seed to guarantee deterministic output for tests
        noise = (
            np.random.default_rng(seed=self.seed)
            .normal(0.0, 0.002, priming_samples)
            .astype(np.float32)
        )
        # Apply 5-point moving average lowpass filter to shape it into soft colored comfort noise.
        # We use mode='full' and truncate to priming_samples to completely prevent edge wrap artifacts.
        return np.convolve(noise, np.ones(5) / 5, mode="full")[:priming_samples]

    def _is_speech_segment(self, sig: np.ndarray, chunk_size: int) -> bool:
        """Applies dynamic range/spikiness heuristics to reject transient static clicks and quiet noise."""
        if len(sig) == 0:
            # Handle empty slices due to potential rounding edge cases at boundaries
            return False

        # Compute RMS in chunk-sized windows
        seg_rms = []
        for w_start in range(0, len(sig), chunk_size):
            window = sig[w_start : w_start + chunk_size]
            if len(window) < chunk_size:
                window = np.pad(window, (0, chunk_size - len(window)))
            seg_rms.append(np.sqrt(np.mean(window**2)))

        seg_rms = np.array(seg_rms)
        mean_rms = np.mean(seg_rms)
        median_rms = np.median(seg_rms)
        rms_ratio = mean_rms / median_rms if median_rms > 1e-5 else 999.0

        # 1. Ratio check: reject if there are high spikes with very quiet median (clicks/transients)
        if rms_ratio > self.spikiness_ratio_threshold:
            # High spikiness detected: perform tandem verification using spectral flatness
            spec = np.abs(np.fft.rfft(sig)) ** 2
            spec = np.maximum(spec[1:], 1e-10)
            a_mean = np.mean(spec)
            g_mean = np.exp(np.mean(np.log(spec)))
            flatness = float(g_mean / a_mean) if a_mean > 1e-10 else 1.0
            if flatness < 0.0005:  # Static noise shelf is ~0.0003
                return False

        # 2. Floor check: reject if the segment is extremely quiet
        if mean_rms < self.min_rms_threshold:
            return False

        return True

    def _filter_noise_segments(
        self,
        shifted_segments: list[tuple[float, float]],
        vad_input: np.ndarray,
        vad_offset_sec: float,
        chunk_size: int,
    ) -> list[tuple[float, float]]:
        """Filters out transient clicks/noise segments from the list of VAD-detected segments."""
        filtered_segments = []
        for start, end in shifted_segments:
            start_idx = int((start + vad_offset_sec) * TARGET_SAMPLE_RATE)
            end_idx = int((end + vad_offset_sec) * TARGET_SAMPLE_RATE)
            seg_signal = vad_input[start_idx:end_idx]
            if self._is_speech_segment(seg_signal, chunk_size):
                filtered_segments.append((start, end))
        return filtered_segments

    def _extract_vad_frames(
        self,
        vad_input: np.ndarray,
        chunk_size: int,
        context_size: int,
    ) -> list[tuple[float, float]]:
        """Iteratively processes audio chunks through the Silero VAD ONNX inference engine."""
        if self.silero_session is None:
            msg = "Silero VAD session not initialized."
            raise RuntimeError(msg)

        state_input = self.silero_session.get_inputs()[1]
        state_shape = [
            s if isinstance(s, int) else 1 for s in state_input.shape
        ]
        state = np.zeros(state_shape, dtype=np.float32)
        context = np.zeros(context_size, dtype=np.float32)
        sr_tensor = np.array([TARGET_SAMPLE_RATE], dtype=np.int64)
        min_speech_frames = int(
            (self.min_speech_duration_ms * TARGET_SAMPLE_RATE / 1000)
            / chunk_size
        )
        min_silence_frames = math.ceil(
            (self.min_silence_duration_ms * TARGET_SAMPLE_RATE / 1000)
            / chunk_size
        )

        triggered = False
        temp_end = 0
        current_speech: dict[str, int | float] = {}
        raw_segments = []

        def frames_to_sec(frame_idx: float) -> float:
            return (frame_idx * chunk_size) / TARGET_SAMPLE_RATE

        # Thread safety: `state` and `context` are call-local numpy arrays
        # initialized above. silero_session.run() is thread-safe for the same
        # reason as ulunas_session — the ONNX graph is immutable; all VAD RNN
        # state is passed in and returned as tensors rather than stored on the
        # session.
        for frame_idx, i in enumerate(range(0, len(vad_input), chunk_size)):
            chunk = vad_input[i : i + chunk_size]
            if len(chunk) < chunk_size:
                chunk = np.pad(chunk, (0, chunk_size - len(chunk)))

            x_with_context = np.concatenate([context, chunk])
            ort_inputs = {
                "input": x_with_context.reshape(
                    1, chunk_size + context_size
                ).astype(np.float32),
                "state": state,
                "sr": sr_tensor,
            }

            outputs = self.silero_session.run(None, ort_inputs)
            prob = float(np.asarray(outputs[0]).flatten()[0])
            state = outputs[1]
            context = x_with_context[-context_size:]

            if not triggered:
                if prob >= self.threshold_onset:
                    triggered = True
                    current_speech["start"] = frame_idx
                    temp_end = 0
            else:  # noqa: PLR5501
                if prob < self.threshold_offset:
                    temp_end += 1
                    if temp_end >= min_silence_frames:
                        current_speech["end"] = frame_idx - temp_end
                        if (
                            current_speech["end"] - current_speech["start"]
                        ) >= min_speech_frames:
                            raw_segments.append((
                                frames_to_sec(current_speech["start"]),
                                frames_to_sec(current_speech["end"]),
                            ))
                        triggered = False
                        temp_end = 0
                        current_speech = {}
                else:
                    temp_end = 0

        if triggered:
            current_speech["end"] = len(vad_input) / chunk_size
            if (
                current_speech["end"] - current_speech["start"]
            ) >= min_speech_frames:
                raw_segments.append((
                    frames_to_sec(current_speech["start"]),
                    frames_to_sec(current_speech["end"]),
                ))

        return raw_segments

    def detect_speech_segments(
        self,
        audio_array: np.ndarray,
        sample_rate: int = TARGET_SAMPLE_RATE,
        chunk_size: int = DEFAULT_SILERO_WINDOW_SIZE,
        context_size: int = 64,
        prior_audio: np.ndarray | None = None,
    ) -> list[tuple[float, float]]:
        """Analyzes a normalized float32 audio array, returning speech segments as (start_sec, end_sec)."""
        if len(audio_array) == 0:
            return []

        if np.issubdtype(audio_array.dtype, np.integer):
            audio_array = audio_array.astype(np.float32) / 32768.0

        # Check for silence early on the raw input audio to prevent redundant execution
        if (
            len(audio_array) == 0
            or np.max(np.abs(audio_array)) < self.min_rms_threshold
        ):
            return []
        if prior_audio is not None and np.issubdtype(
            prior_audio.dtype, np.integer
        ):
            prior_audio = prior_audio.astype(np.float32) / 32768.0

        # Peak Normalization Heuristic
        audio_array = self._peak_normalize(audio_array)

        # Fallback Priming (Call Starts / Segment 0):
        is_fallback_priming = False
        if prior_audio is None:
            prior_audio = self._generate_comfort_noise(sample_rate)
            is_fallback_priming = True

        # 1. Perform physical audio concatenation at native sample_rate
        if prior_audio is not None and len(prior_audio) > 0:
            if not is_fallback_priming:
                prior_audio = self._peak_normalize(prior_audio)
            prior_len_sec = len(prior_audio) / float(sample_rate)
            extended_native = np.concatenate([prior_audio, audio_array])
        else:
            prior_len_sec = 0.0
            extended_native = audio_array

        # 2. Resample the entire unified array in a single pass to TARGET_SAMPLE_RATE
        if sample_rate != TARGET_SAMPLE_RATE:
            resampler = TorchaudioHannResampler(sample_rate, TARGET_SAMPLE_RATE)
            extended_audio = resampler.resample(extended_native)
        else:
            extended_audio = extended_native

        preprocessed = self.preprocess(
            extended_audio, prior_len_sec=prior_len_sec
        )

        # 3. Slicing strategy for VAD state warming (Lookback Priming):
        # We run VAD starting from up to 1.5 seconds of the preamble to warm up the VAD RNN states,
        # preventing cold-start vocal onset clipping.
        # However, we only do this VAD state warming if we have a genuine prior audio tail.
        # Warming up the VAD on synthetic comfort noise (is_fallback_priming=True) biases the VAD LSTM
        # toward silence, which drastically reduces sensitivity at vocal onset.
        preamble_samples = int(prior_len_sec * TARGET_SAMPLE_RATE)
        if is_fallback_priming:
            vad_input = (
                preprocessed[preamble_samples:]
                if preamble_samples > 0
                else preprocessed
            )
            vad_offset_sec = 0.0
        else:
            max_warmup_sec = 1.5
            warmup_sec = min(prior_len_sec, max_warmup_sec)
            warmup_samples = int(warmup_sec * TARGET_SAMPLE_RATE)
            vad_input = (
                preprocessed[preamble_samples - warmup_samples :]
                if preamble_samples > 0
                else preprocessed
            )
            vad_offset_sec = warmup_sec

        raw_segments = self._extract_vad_frames(
            vad_input, chunk_size, context_size
        )

        # Time Coordinate Trimming & Shifting Mathematics:
        shifted_segments = self._trim_and_shift_segments(
            raw_segments, vad_offset_sec
        )

        filtered_segments = self._filter_noise_segments(
            shifted_segments, vad_input, vad_offset_sec, chunk_size
        )

        # Pad and merge overlapping segments using the clean native-rate duration calculation
        audio_len_sec = len(audio_array) / float(sample_rate)
        return self._pad_and_merge_segments(filtered_segments, audio_len_sec)

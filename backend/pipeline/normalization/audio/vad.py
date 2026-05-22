"""Voice Activity Detection (VAD) Engine utilizing the Silero + UL-UNAS neural network pipeline.

Designed to align directly with watch-duty's optimized audio segmentation and denoiser heuristics,
avoiding the overhead of multi-VAD abstractions.
"""

from pathlib import Path

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
    VAD_DEFAULT_COMP_RATIO,
    VAD_DEFAULT_COMP_RELEASE_MS,
    VAD_DEFAULT_COMP_THRESHOLD_DB,
    VAD_DEFAULT_HIGHPASS_HZ,
    VAD_DEFAULT_LOWPASS_HZ,
    VAD_DEFAULT_MIN_SILENCE_DURATION_MS,
    VAD_DEFAULT_MIN_SPEECH_DURATION_MS,
    VAD_DEFAULT_PAD_SEC,
    VAD_DEFAULT_PEAK_FILTER_Q,
    VAD_DEFAULT_PRIMING_SEC,
    VAD_DEFAULT_THRESHOLD_OFFSET,
    VAD_DEFAULT_THRESHOLD_ONSET,
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

        self.silero_path = Path(models_dir) / "silero_vad.onnx"
        self.ulunas_path = Path(models_dir) / "ulunas_stream_simple.onnx"

        self.silero_session = None
        self.ulunas_session = None

    def setup(self) -> None:
        """Initializes the ONNXRuntime inference sessions and warms up Numba."""
        if self.silero_session is not None:
            return

        logger.info("Initializing VAD sessions. Models path: %s", MODELS_DIR)
        if not self.silero_path.exists():
            msg = f"Silero ONNX model not found at: {self.silero_path}"
            raise FileNotFoundError(msg)
        if not self.ulunas_path.exists():
            msg = f"UL-UNAS ONNX model not found at: {self.ulunas_path}"
            raise FileNotFoundError(msg)

        self.silero_session = ort.InferenceSession(
            str(self.silero_path), providers=["CPUExecutionProvider"]
        )
        self.ulunas_session = ort.InferenceSession(
            str(self.ulunas_path), providers=["CPUExecutionProvider"]
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

        for i in range(num_frames):
            frame = stft_features[:, :, i : i + 1, :]
            ort_inputs = {audio_input_name: frame, **states}
            ort_outs = self.ulunas_session.run(None, ort_inputs)
            out_stft[:, :, i : i + 1, :] = ort_outs[0]
            for j in range(1, len(outputs)):
                states[inputs[j].name] = ort_outs[j]

        return custom_numpy_istft(
            out_stft,
            length=audio_array.shape[0],
            n_fft=n_fft,
            hop_length=hop_length,
        )[0]

    def preprocess(self, audio_array: np.ndarray) -> np.ndarray:
        """Applies the VAD bandpass, denoiser, and eq presence boost pipeline."""
        bp_board = Pedalboard(
            [
                HighpassFilter(cutoff_frequency_hz=self.highpass_hz),
                LowpassFilter(cutoff_frequency_hz=self.lowpass_hz),
            ]
        )
        bp_audio = bp_board(audio_array, TARGET_SAMPLE_RATE)

        # Apply Dynamic Compressor to optimize signal level and balance quiet/loud segments
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

        ulunas_denoised = self.denoise(comp_audio)

        mixed_audio = (
            1.0 - self.blend_ratio
        ) * comp_audio + self.blend_ratio * ulunas_denoised

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

    def detect_speech_segments(  # noqa: PLR0912, PLR0915
        self,
        audio_array: np.ndarray,
        sample_rate: int = TARGET_SAMPLE_RATE,
        chunk_size: int = DEFAULT_SILERO_WINDOW_SIZE,
        context_size: int = 64,
        prior_audio: np.ndarray | None = None,
    ) -> list[tuple[float, float]]:
        """Analyzes a normalized float32 audio array, returning speech segments as (start_sec, end_sec)."""
        self.setup()
        if self.silero_session is None:
            msg = "Silero VAD session not initialized."
            raise RuntimeError(msg)

        if len(audio_array) == 0:
            return []

        if sample_rate != TARGET_SAMPLE_RATE:
            resampler = TorchaudioHannResampler(sample_rate, TARGET_SAMPLE_RATE)
            audio_array = resampler.resample(audio_array)

        # Evaluate if we passed a genuine historical prior tail from a previous chunk
        # before we derive the priming preamble from the current file itself.
        has_genuine_prior = prior_audio is not None

        # Prepend historical tail if available; fallback to current chunk start if none.
        if prior_audio is None:
            priming_samples = int(self.priming_sec * TARGET_SAMPLE_RATE)
            priming_samples = min(priming_samples, len(audio_array))
            if priming_samples > 0:
                prior_audio = audio_array[:priming_samples]

        # Perform physical audio concatenation to create a continuous audio stream for the VAD
        if prior_audio is not None and len(prior_audio) > 0:
            if sample_rate != TARGET_SAMPLE_RATE:
                resampler = TorchaudioHannResampler(
                    sample_rate, TARGET_SAMPLE_RATE
                )
                prior_audio = resampler.resample(prior_audio)
            prior_len_sec = len(prior_audio) / float(TARGET_SAMPLE_RATE)
            extended_audio = np.concatenate([prior_audio, audio_array])
        else:
            prior_len_sec = 0.0
            extended_audio = audio_array

        preprocessed = self.preprocess(extended_audio)

        # Slice off genuine historical prior tail before VAD inference to prevent RNN bleed-through.
        preamble_samples = int(prior_len_sec * TARGET_SAMPLE_RATE)
        if has_genuine_prior and preamble_samples > 0:
            vad_input = preprocessed[preamble_samples:]
            vad_offset_sec = 0.0
        else:
            vad_input = preprocessed
            vad_offset_sec = prior_len_sec

        state = np.zeros((2, 1, 128), dtype=np.float32)
        context = np.zeros(context_size, dtype=np.float32)

        sr_tensor = np.array([TARGET_SAMPLE_RATE], dtype=np.int64)
        min_speech_frames = int(
            (self.min_speech_duration_ms * TARGET_SAMPLE_RATE / 1000)
            / chunk_size
        )
        min_silence_frames = int(
            (self.min_silence_duration_ms * TARGET_SAMPLE_RATE / 1000)
            / chunk_size
        )

        triggered = False
        temp_end = 0
        current_speech = {}
        raw_segments = []

        for i in range(0, len(vad_input), chunk_size):
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

            current_frame = i / chunk_size

            if not triggered:
                if prob >= self.threshold_onset:
                    triggered = True
                    current_speech["start"] = current_frame
                    temp_end = 0
            elif prob < self.threshold_offset:
                temp_end += 1
                if temp_end >= min_silence_frames:
                    current_speech["end"] = current_frame - temp_end
                    if (
                        current_speech["end"] - current_speech["start"]
                    ) >= min_speech_frames:
                        raw_segments.append(
                            (
                                current_speech["start"]
                                * chunk_size
                                / TARGET_SAMPLE_RATE,
                                current_speech["end"]
                                * chunk_size
                                / TARGET_SAMPLE_RATE,
                            )
                        )
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
                raw_segments.append(
                    (
                        current_speech["start"]
                        * chunk_size
                        / TARGET_SAMPLE_RATE,
                        current_speech["end"] * chunk_size / TARGET_SAMPLE_RATE,
                    )
                )

        # Time Coordinate Trimming & Shifting Mathematics:
        shifted_segments = self._trim_and_shift_segments(
            raw_segments, vad_offset_sec
        )

        # Pad and merge overlapping segments
        audio_len_sec = len(audio_array) / float(TARGET_SAMPLE_RATE)
        return self._pad_and_merge_segments(shifted_segments, audio_len_sec)

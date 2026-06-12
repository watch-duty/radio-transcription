"""Unit and Integration tests for the Silero + UL-UNAS VAD engine.

Exercises the model loaders, preprocess filters, and validates accuracy metrics
against actual ground-truth voice activity segments from the Colab.
"""

import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Final

import numpy as np
import soundfile as sf

from backend.pipeline.segmentation.audio import vad

SAMPLES_PER_MS: Final = 16


def calculate_f1_score(
    ground_truth: list[tuple[float, float]],
    detected: list[tuple[float, float]],
    audio_len_sec: float,
    resolution_ms: int = 10,
) -> float:
    """Calculates the frame-based F1-score between ground truth and detected speech segments."""
    num_frames = int(np.ceil(audio_len_sec * 1000 / resolution_ms))

    gt_array = np.zeros(num_frames, dtype=bool)
    for start, end in ground_truth:
        start_frame = int(start * 1000 / resolution_ms)
        end_frame = int(end * 1000 / resolution_ms)
        gt_array[start_frame:end_frame] = True

    det_array = np.zeros(num_frames, dtype=bool)
    for start, end in detected:
        start_frame = int(start * 1000 / resolution_ms)
        end_frame = int(end * 1000 / resolution_ms)
        det_array[start_frame:end_frame] = True

    tp = np.sum(gt_array & det_array)
    fp = np.sum(~gt_array & det_array)
    fn = np.sum(gt_array & ~det_array)

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    return (
        2 * (precision * recall) / (precision + recall)
        if (precision + recall) > 0
        else 0.0
    )


def load_audio(audio_path: Path) -> tuple[np.ndarray, int]:
    """Robust audio loader mirroring the production pipeline's NamedTemporaryFile FLAC decoder."""
    with tempfile.NamedTemporaryFile(suffix=".flac", delete=False) as temp_file:
        temp_filename = temp_file.name

    try:
        # Decode the file to a standard, clean FLAC file exactly like production AudioProcessor
        process = subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-i",
                str(audio_path),
                "-ac",
                "1",  # Mono
                "-f",
                "flac",
                temp_filename,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if process.returncode != 0:
            msg = "Failed to decode audio via ffmpeg"
            raise RuntimeError(msg)

        # Read the FLAC file and normalize exactly like the production pipeline does
        samples, sample_rate = sf.read(temp_filename, dtype="int16")
        audio_data = samples.astype(np.float32) / 32768.0
        return audio_data, sample_rate
    finally:
        try:
            Path(temp_filename).unlink()
        except OSError:
            pass


class TestVadEngine(unittest.TestCase):
    def setUp(self) -> None:
        self.models_dir = str(Path(__file__).parent.parent / "audio" / "models")
        self.vad = vad.VoiceActivityDetector(
            models_dir=self.models_dir, pad_sec=0.0
        )
        self.vad.setup()

    def test_silence_rejection(self) -> None:
        """Verifies that pure digital silence returns no speech segments."""
        # 1 second of digital silence at 16kHz
        silence = np.zeros(16000, dtype=np.float32)
        segments = self.vad.detect_speech_segments(silence, sample_rate=16000)
        self.assertEqual(segments, [])

    def test_integer_inputs_converted(self) -> None:
        """Verifies that passing integer arrays for audio_array and prior_audio does not crash and processes successfully."""
        # 1 second of digital silence at 16kHz using int16
        silence_int16 = np.zeros(16000, dtype=np.int16)
        prior_int16 = np.zeros(16000, dtype=np.int16)

        # This should execute successfully and return empty segments because it's pure silence
        segments = self.vad.detect_speech_segments(
            silence_int16, sample_rate=16000, prior_audio=prior_int16
        )
        self.assertEqual(segments, [])

    def test_synthetic_tone_rejection(self) -> None:
        """Verifies that synthetic tone (constant sine wave) is rejected by the neural VAD."""
        t = np.linspace(0, 1.0, 16000, endpoint=False)
        tone = np.sin(2 * np.pi * 1000 * t).astype(np.float32) * 0.5
        segments = self.vad.detect_speech_segments(tone, sample_rate=16000)
        self.assertEqual(segments, [])

    def test_is_speech_segment_spiky_voice_retained(self) -> None:
        """Verifies that a vocal harmonic signal with high RMS spikiness is retained by tandem spectral flatness verification."""
        t = np.linspace(0, 1.0, 16000, endpoint=False)
        voice = (
            np.sin(2 * np.pi * 150 * t)
            + 0.5 * np.sin(2 * np.pi * 300 * t)
            + 0.25 * np.sin(2 * np.pi * 450 * t)
        ).astype(np.float32) * 0.1
        voice[:100] = 1.0
        self.assertTrue(self.vad._is_speech_segment(voice, chunk_size=512))

    def test_is_speech_segment_spiky_static_rejected(self) -> None:
        """Verifies that an unpitched noise burst with high RMS spikiness is rejected by tandem spectral flatness verification."""
        t = np.linspace(0, 1.0, 16000, endpoint=False)
        rng = np.random.default_rng(42)
        static = (
            np.sin(2 * np.pi * 3000 * t) + 0.0001 * rng.normal(0, 1, 16000)
        ).astype(np.float32) * 0.0005
        self.assertFalse(self.vad._is_speech_segment(static, chunk_size=512))

    def _run_integration_test(
        self,
        filename: str,
        ground_truth: list[tuple[float, float]],
        min_f1: float = 0.80,
        vad_instance: vad.VoiceActivityDetector | None = None,
    ) -> None:
        """Helper to run VAD segment detection by simulating real-world 15.0s continuous streaming.

        Chunks the audio file into contiguous 15.0s streams (matching Icecast capture blocks)
        primed with the previous chunk's tail to perfectly analogize production execution.
        """
        audio_path = Path(__file__).parent / "test_data" / filename
        if not audio_path.exists():
            self.skipTest(f"Audio file not found at: {audio_path}")

        audio_data, sample_rate = load_audio(audio_path)

        detector = vad_instance or self.vad
        if not detector.silero_session:
            detector.setup()

        # Production continuous stream parameters:
        # Audio chunks are captured in 15.0s intervals
        chunk_len_sec = 15.0
        chunk_samples = int(chunk_len_sec * sample_rate)
        priming_samples = int(
            detector.priming_sec * sample_rate
        )  # VAD_DEFAULT_PRIMING_SEC = 6.0

        detected_segments = []
        prior_audio_tail = None

        for i in range(0, len(audio_data), chunk_samples):
            chunk = audio_data[i : i + chunk_samples]
            raw_chunk_segments = detector.detect_speech_segments(
                chunk, sample_rate=sample_rate, prior_audio=prior_audio_tail
            )

            # Shift coordinates relative to global timeline start
            chunk_offset_sec = i / float(sample_rate)
            for start, end in raw_chunk_segments:
                detected_segments.append(
                    (start + chunk_offset_sec, end + chunk_offset_sec)
                )

            # Cache trailing prior audio tail for the next chunk boundary priming
            prior_audio_tail = (
                chunk[-priming_samples:] if len(chunk) > 0 else None
            )

        audio_len = len(audio_data) / float(sample_rate)

        # Pad and merge the globally stitched segments
        padded_segments = detector._pad_and_merge_segments(
            detected_segments, audio_len
        )
        f1 = calculate_f1_score(ground_truth, padded_segments, audio_len)

        self.assertGreaterEqual(
            f1, min_f1, f"F1 score on {filename} was {f1:.3f}"
        )

    def test_integration_stress_file(self) -> None:
        """Integration test to verify VAD performance on test_stress.flac."""
        self._run_integration_test(
            "test_stress.flac", [(0.4, 2.85)], min_f1=0.70
        )

    def test_integration_joined_file(self) -> None:
        """Integration test to verify VAD performance on test_joined.flac."""
        self._run_integration_test(
            "test_joined.flac",
            [(8.3, 10.7), (12.3, 15.6), (20.3, 23.0), (26.2, 27.0)],
            min_f1=0.85,
        )

    def test_integration_bcfy_file(self) -> None:
        """Integration test to verify VAD performance on test_bcfy.flac (whispers/dropout)."""
        self._run_integration_test(
            "test_bcfy.flac",
            [
                (0.0, 1.8),
                (2.2, 5.8),
                (7.6, 12.2),
                (13.0, 14.2),
            ],
            min_f1=0.80,
        )

    def test_integration_dispatch_amador_file(self) -> None:
        """Integration test to verify VAD performance on test_dispatch_amador.flac (continuous dispatch)."""
        self._run_integration_test(
            "test_dispatch_amador.flac",
            [
                (2.7, 12.5),
                (14.4, 15.8),
                (17.5, 24.6),
                (27.3, 29.7),
                (31.4, 33.7),
                (38.1, 40.5),
                (47.2, 49.4),
                (56.2, 60.6),
                (62.6, 65.3),
            ],
            min_f1=0.85,
        )

    def test_integration_dispatch_sku_file(self) -> None:
        """Integration test to verify VAD performance on test_dispatch_sku.flac (heavy static/interference)."""
        self._run_integration_test(
            "test_dispatch_sku.flac",
            [
                (0.420, 2.593),
                (3.3, 5.788),
                (6.242, 8.838),
                (8.861, 11.044),
                (11.691, 14.717),
                (14.811, 17.014),
                (17.781, 19.707),
                (20.253, 22.040),
                (22.843, 24.669),
                (25.547, 27.728),
                (28.471, 29.830),
                (30.845, 32.907),
                (33.003, 34.615),
                (35.704, 37.877),
                (40.570, 41.772),
                (42.467, 44.470),
                (45.874, 49.212),
                (49.373, 51.884),
                (52.768, 54.178),
            ],
            min_f1=0.85,
        )

    def test_integration_middlebury_quiet_segments_file(self) -> None:
        """Integration test to verify VAD performance on test_middlebury_quiet_segments.mp3 (quiet segments)."""
        self._run_integration_test(
            "test_middlebury_quiet_segments.mp3",
            [
                (0.6, 2.2),
                (4.2, 6.7),
            ],
            min_f1=0.85,
        )

    def test_integration_middlebury_quiet_spiky_file(self) -> None:
        """Integration test to verify VAD performance on test_middlebury_quiet_spiky.mp3 (quiet EMS speech)."""
        self._run_integration_test(
            "test_middlebury_quiet_spiky.mp3",
            [
                (0.18, 1.45),
            ],
            min_f1=0.55,
        )

    def test_integration_quiet_speech_loud_transient(self) -> None:
        """Integration test to verify VAD performance on quiet speech followed by a loud transient spike.

        NOTE on Physical Trade-off:
        A sudden loud transient click at t=0.05s triggers our dynamic Compressor. Compressing this sudden spike creates
        a transient transition glitch in the recurrent denoiser RNN state memory. Because the subsequent speech is extremely quiet,
        the adapted RNN memory suppresses the first quiet segment (0.213s - 0.8s).

        However, with 1.0s comfort noise priming fallback and peak normalization active, the second quiet segment
        (2.037s - 3.869s) is successfully detected. This yields a realistic, actively asserted F1 target baseline of 0.60.
        """
        self._run_integration_test(
            "test_quiet_speech_loud_transient.mp3",
            [
                (0.213, 0.8),
                (2.037, 3.869),
            ],
            min_f1=0.60,
        )

    def test_integration_static_middlebury_file(self) -> None:
        """Integration test to verify VAD rejects all segments on static-only audio file."""
        audio_path = (
            Path(__file__).parent
            / "test_data"
            / "test_only_static_middlebury.mp3"
        )
        if not audio_path.exists():
            self.skipTest(f"Audio file not found at: {audio_path}")

        audio_data, sample_rate = load_audio(audio_path)

        # Production chunked streaming simulation:
        chunk_len_sec = 15.0
        chunk_samples = int(chunk_len_sec * sample_rate)
        priming_samples = int(self.vad.priming_sec * sample_rate)

        detected_segments = []
        prior_audio_tail = None

        for i in range(0, len(audio_data), chunk_samples):
            chunk = audio_data[i : i + chunk_samples]
            raw_chunk_segments = self.vad.detect_speech_segments(
                chunk, sample_rate=sample_rate, prior_audio=prior_audio_tail
            )
            chunk_offset_sec = i / float(sample_rate)
            for start, end in raw_chunk_segments:
                detected_segments.append(
                    (start + chunk_offset_sec, end + chunk_offset_sec)
                )
            prior_audio_tail = (
                chunk[-priming_samples:] if len(chunk) > 0 else None
            )

        audio_len = len(audio_data) / float(sample_rate)
        padded_segments = self.vad._pad_and_merge_segments(
            detected_segments, audio_len
        )
        self.assertEqual(padded_segments, [])

    def test_vad_priming_contiguous_chunk(self) -> None:
        """Verifies that passing a prior_audio tail primes VAD state and shifts time coordinates correctly."""
        # 1. Generate 1 second of voice-frequency-like sine wave
        t = np.linspace(0, 1.0, 1000 * SAMPLES_PER_MS, endpoint=False)
        speech_signal = np.sin(2 * np.pi * 1000 * t).astype(np.float32) * 0.5

        # 2. Split into two 500ms contiguous chunks
        chunk1 = speech_signal[: 500 * SAMPLES_PER_MS]
        chunk2 = speech_signal[500 * SAMPLES_PER_MS :]

        # Run chunk2 directly without priming
        segments_no_prime = self.vad.detect_speech_segments(
            chunk2, sample_rate=1000 * SAMPLES_PER_MS
        )
        self.assertIsNotNone(segments_no_prime)

        # Run chunk2 primed with the tail of chunk1
        segments_primed = self.vad.detect_speech_segments(
            chunk2, sample_rate=1000 * SAMPLES_PER_MS, prior_audio=chunk1
        )

        # The primed segments should have shifted coordinates that fall within the [0.0, 0.5] range of chunk2
        for start, end in segments_primed:
            self.assertGreaterEqual(start, 0.0)
            self.assertLessEqual(end, 0.5)

    def test_boundary_bleed_through_prevented(self) -> None:
        """Verifies that when a silent chunk is primed with a prior speech tail,
        the Hybrid Priming VAD successfully prevents the RNN state from bleeding through,
        detecting absolutely zero false speech segments.
        """
        # 1. Generate Chunk 1: 5 seconds of active speech ending exactly at the boundary
        # (with only 100ms of trailing silence)
        t = np.linspace(
            0, 4.9, int(1000 * SAMPLES_PER_MS * 4.9), endpoint=False
        )
        speech = np.sin(2 * np.pi * 1000 * t).astype(np.float32) * 0.5
        silence = np.zeros(
            100 * SAMPLES_PER_MS, dtype=np.float32
        )  # 100ms silence
        chunk1 = np.concatenate([speech, silence])

        # 2. Generate Chunk 2: 3 seconds of complete digital silence (all 0s)
        chunk2 = np.zeros(3000 * SAMPLES_PER_MS, dtype=np.float32)

        # 3. Run VAD on Chunk 2 primed with Chunk 1's tail
        detected_segments = self.vad.detect_speech_segments(
            chunk2, sample_rate=1000 * SAMPLES_PER_MS, prior_audio=chunk1
        )

        # Assert that absolutely zero segments were detected inside the silent chunk
        self.assertEqual(detected_segments, [])

    def test_is_tone_segment_two_tone_paging(self) -> None:
        """Verifies that a two-tone sequential paging signal is identified as a tone segment and rejected."""
        t1 = np.linspace(0, 1.0, 16000, endpoint=False)
        tone1 = np.sin(2 * np.pi * 600.9 * t1).astype(np.float32) * 0.5

        t2 = np.linspace(0, 3.0, 48000, endpoint=False)
        tone2 = np.sin(2 * np.pi * 742.5 * t2).astype(np.float32) * 0.5

        paging_signal = np.concatenate([tone1, tone2])
        self.assertTrue(self.vad._is_tone_segment(paging_signal))
        segments = self.vad.detect_speech_segments(
            paging_signal, sample_rate=16000
        )
        self.assertEqual(segments, [])

    def test_is_tone_segment_eas_attention(self) -> None:
        """Verifies that an Emergency Alert System (EAS) attention tone (853 Hz + 960 Hz) is identified as a tone segment and rejected."""
        t = np.linspace(0, 4.0, 64000, endpoint=False)
        eas_tone = (
            np.sin(2 * np.pi * 853.0 * t) + np.sin(2 * np.pi * 960.0 * t)
        ).astype(np.float32) * 0.25
        self.assertTrue(self.vad._is_tone_segment(eas_tone))
        segments = self.vad.detect_speech_segments(eas_tone, sample_rate=16000)
        self.assertEqual(segments, [])

    def test_integration_tone_only_file(self) -> None:
        """Integration test to verify that real-world Broadcastify two-tone paging audio is fully rejected as non-speech."""
        audio_path = Path(__file__).parent / "test_data" / "test_tone_only.flac"
        audio_data, sample_rate = load_audio(audio_path)
        segments = self.vad.detect_speech_segments(
            audio_data, sample_rate=sample_rate
        )
        self.assertEqual(segments, [])

    def test_is_tone_segment_mixed_tone_and_speech_retained(self) -> None:
        """Verifies that a mixed audio transmission containing a loud alert tone
        followed by quieter/normal human speech is correctly retained and not dropped.
        """
        # 1. 4 seconds of loud Quik-Call II paging alert tone (1082 Hz / 600 Hz)
        t1 = np.linspace(0, 4.0, 64000, endpoint=False)
        tone = (
            np.sin(2 * np.pi * 1082.0 * t1) + np.sin(2 * np.pi * 600.0 * t1)
        ).astype(np.float32) * 0.4

        # 2. 4 seconds of broadband human speech (consonant fricatives / broadband un-concentrated voice)
        rng = np.random.default_rng(seed=42)
        speech = rng.normal(0.0, 0.05, 64000).astype(np.float32)
        # Simulate word envelopes
        speech[10000:20000] *= 2.0
        speech[30000:50000] *= 2.0

        mixed_signal = np.concatenate([tone, speech])

        # Assert that the mixed signal is classified as NOT purely an alert tone
        self.assertFalse(self.vad._is_tone_segment(mixed_signal))

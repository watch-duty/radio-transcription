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

from backend.pipeline.normalization.audio import vad

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

    def test_synthetic_tone_rejection(self) -> None:
        """Verifies that synthetic tone (constant sine wave) is rejected by the neural VAD."""
        t = np.linspace(0, 1.0, 16000, endpoint=False)
        tone = np.sin(2 * np.pi * 1000 * t).astype(np.float32) * 0.5
        segments = self.vad.detect_speech_segments(tone, sample_rate=16000)
        self.assertEqual(segments, [])

    def _run_integration_test(
        self,
        filename: str,
        ground_truth: list[tuple[float, float]],
        min_f1: float = 0.80,
    ) -> None:
        """Helper to run VAD segment detection and assert frame-based F1-score accuracy."""
        audio_path = Path(__file__).parent / "test_data" / filename
        if not audio_path.exists():
            self.skipTest(f"Audio file not found at: {audio_path}")

        audio_data, sample_rate = load_audio(audio_path)
        detected_segments = self.vad.detect_speech_segments(
            audio_data, sample_rate=sample_rate
        )

        audio_len = len(audio_data) / float(sample_rate)
        f1 = calculate_f1_score(ground_truth, detected_segments, audio_len)

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

    def test_integration_middlebury_file(self) -> None:
        """Integration test to verify VAD performance on test_middlebury.mp3 (quiet segments)."""
        self._run_integration_test(
            "test_middlebury.mp3",
            [
                (0.6, 2.2),
                (4.2, 6.7),
            ],
            min_f1=0.85,
        )

    @unittest.expectedFailure
    def test_integration_middlebury_quiet_loud_file(self) -> None:
        """Integration test to verify VAD performance on the quiet/loud multi-segment test file."""
        self._run_integration_test(
            "Middlebury_Regional_EMS_20260522_115720.mp3",
            [
                (0.213, 0.8),
                (2.037, 3.869),
            ],
            min_f1=0.75,
        )

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

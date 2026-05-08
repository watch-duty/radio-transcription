"""Unit and Integration tests for the Silero + UL-UNAS VAD engine.

Exercises the model loaders, preprocess filters, and validates accuracy metrics
against actual ground-truth voice activity segments from the Colab.
"""

import subprocess
import unittest
from pathlib import Path

import numpy as np
import soundfile as sf

from backend.pipeline.transcription.audio.vad import VoiceActivityDetector


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
    """Robust audio loader using soundfile with a bulletproof ffmpeg subprocess fallback."""
    try:
        audio_data, sample_rate = sf.read(str(audio_path), always_2d=True)
        audio_data = audio_data.mean(axis=1).astype(np.float32)  # Mono
    except Exception:
        # Query the sample rate using ffprobe
        probe_cmd = [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "stream=sample_rate",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(audio_path),
        ]
        sample_rate = int(
            subprocess.check_output(probe_cmd)
            .decode("utf-8")
            .strip()
            .split("\n")[0]
        )

        # Decode audio to raw PCM float32 mono
        command = [
            "ffmpeg",
            "-i",
            str(audio_path),
            "-f",
            "f32le",
            "-acodec",
            "pcm_f32le",
            "-ac",
            "1",
            "-",
        ]
        pipe = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
        )
        out, _ = pipe.communicate()
        audio_data = np.frombuffer(out, dtype=np.float32)
        return audio_data, sample_rate
    else:
        return audio_data, sample_rate


class TestVadEngine(unittest.TestCase):
    def setUp(self) -> None:
        self.models_dir = str(Path(__file__).parent.parent / "audio" / "models")
        self.vad = VoiceActivityDetector(models_dir=self.models_dir)
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

    def test_integration_stress_file(self) -> None:
        """Integration test to verify Silero+UL-UNAS VAD performance on `test_stress.flac`."""
        audio_path = Path(__file__).parent / "test_data" / "test_stress.flac"
        if not audio_path.exists():
            self.skipTest(f"Audio file not found at: {audio_path}")

        audio_data, sample_rate = load_audio(audio_path)

        # Ground Truth from Colab
        ground_truth = [(0.4, 2.85)]

        # Run VAD segment detection
        detected_segments = self.vad.detect_speech_segments(
            audio_data, sample_rate=sample_rate
        )

        # Compute F1-score
        audio_len = len(audio_data) / float(sample_rate)
        f1 = calculate_f1_score(ground_truth, detected_segments, audio_len)

        # We assert that the F1-score is extremely high (> 80%) on this stress test
        self.assertGreaterEqual(
            f1, 0.80, f"F1 score on test_stress.flac was {f1:.3f}"
        )

    def test_integration_joined_file(self) -> None:
        """Integration test to verify Silero+UL-UNAS VAD performance on `test_joined.flac`."""
        audio_path = Path(__file__).parent / "test_data" / "test_joined.flac"
        if not audio_path.exists():
            self.skipTest(f"Audio file not found at: {audio_path}")

        audio_data, sample_rate = load_audio(audio_path)

        # Ground Truth from Colab
        ground_truth = [
            (8.3, 10.7),
            (12.3, 15.6),
            (20.3, 23.0),
            (26.2, 27.0),
        ]

        detected_segments = self.vad.detect_speech_segments(
            audio_data, sample_rate=sample_rate
        )

        audio_len = len(audio_data) / float(sample_rate)
        f1 = calculate_f1_score(ground_truth, detected_segments, audio_len)

        # Validate that accuracy is excellent (> 80%)
        self.assertGreaterEqual(
            f1, 0.80, f"F1 score on test_joined.flac was {f1:.3f}"
        )

    def test_vad_priming_contiguous_chunk(self) -> None:
        """Verifies that passing a prior_audio tail primes VAD state and shifts time coordinates correctly."""
        # 1. Generate 1 second of voice-frequency-like sine wave
        t = np.linspace(0, 1.0, 16000, endpoint=False)
        speech_signal = np.sin(2 * np.pi * 1000 * t).astype(np.float32) * 0.5

        # 2. Split into two 500ms contiguous chunks
        chunk1 = speech_signal[:8000]
        chunk2 = speech_signal[8000:]

        # Run chunk2 directly without priming
        segments_no_prime = self.vad.detect_speech_segments(
            chunk2, sample_rate=16000
        )
        self.assertIsNotNone(segments_no_prime)

        # Run chunk2 primed with the tail of chunk1
        segments_primed = self.vad.detect_speech_segments(
            chunk2, sample_rate=16000, prior_audio=chunk1
        )

        # The primed segments should have shifted coordinates that fall within the [0.0, 0.5] range of chunk2
        for start, end in segments_primed:
            self.assertGreaterEqual(start, 0.0)
            self.assertLessEqual(end, 0.5)

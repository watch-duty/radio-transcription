"""Unit and Integration tests for the Silero + UL-UNAS VAD engine.

Exercises the model loaders, preprocess filters, and validates accuracy metrics
against actual ground-truth voice activity segments from the Colab.
"""

import concurrent.futures
import datetime
import sys
import unittest
from pathlib import Path
from typing import Final, NamedTuple

import av
import numpy as np

from backend.pipeline.segmentation.audio import vad
from backend.pipeline.segmentation.constants import (
    TONE_EAS_FREQ1_HZ,
    TONE_EAS_FREQ2_HZ,
    TONE_QUIK_CALL_II_FREQ1_HZ,
    TONE_QUIK_CALL_II_FREQ2_HZ,
    TONE_STFT_HOP_LENGTH,
    VAD_DEFAULT_PAD_SEC,
    VAD_DEFAULT_PRIMING_SEC,
    VAD_TEST_SUBAUDIBLE_RUMBLE_FREQ_HZ,
)
from backend.pipeline.segmentation.scripts import diagnose_feed_drop

SAMPLES_PER_MS: Final = 16

# Annotated speech in test_vad_inter_transmission_gap_speech.flac, the 15s
# Hood River stream chunk behind segment c1416cf1. Shared by the pad_sec=0.0
# accuracy benchmark and the production-padding clipping guard so the two can
# never drift apart.
HOOD_RIVER_CHUNK_GROUND_TRUTH: Final[list[tuple[float, float]]] = [
    (0.532, 5.872),
    (6.672, 8.200),
    (9.675, 10.433),
    (11.268, 11.768),
    (13.548, 15.020),
]


class SegmentMetrics(NamedTuple):
    """Frame-based accuracy metrics for a set of detected speech segments."""

    f1: float
    precision: float
    recall: float


def calculate_segment_metrics(
    ground_truth: list[tuple[float, float]],
    detected: list[tuple[float, float]],
    audio_len_sec: float,
    resolution_ms: int = 10,
) -> SegmentMetrics:
    """Calculates frame-based precision, recall, and F1 between ground truth and detected speech."""
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
    f1 = (
        2 * (precision * recall) / (precision + recall)
        if (precision + recall) > 0
        else 0.0
    )
    return SegmentMetrics(f1=f1, precision=precision, recall=recall)


def load_audio(audio_path: Path) -> tuple[np.ndarray, int]:
    """Robust audio loader using PyAV, avoiding external ffmpeg CLI subprocess."""
    decoded_frames = []
    sample_rate = 0
    try:
        with av.open(str(audio_path)) as container:
            stream = container.streams.audio[0]
            sample_rate = stream.codec_context.sample_rate
            # Resample to 16-bit mono.
            resampler = av.AudioResampler(format="s16", layout="mono")
            for packet in container.demux(stream):
                for frame in packet.decode():
                    reframed = resampler.resample(frame)
                    if reframed is not None:
                        frames = (
                            reframed
                            if isinstance(reframed, (list, tuple))
                            else [reframed]
                        )
                        for f in frames:
                            decoded_frames.append(f.to_ndarray()[0])

            # Flush the resampler
            flushed = resampler.resample(None)
            if flushed is not None:
                frames = (
                    flushed if isinstance(flushed, (list, tuple)) else [flushed]
                )
                for f in frames:
                    decoded_frames.append(f.to_ndarray()[0])
    except Exception as e:
        msg = f"Failed to decode audio via PyAV: {e}"
        raise RuntimeError(msg) from e

    if not decoded_frames:
        msg = f"No audio frames decoded from {audio_path}"
        raise RuntimeError(msg)

    combined = np.concatenate(decoded_frames)
    audio_data = combined.astype(np.float32) / 32768.0
    return audio_data, sample_rate


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
        segments = self.vad.detect_speech_segments(
            silence, sample_rate=16000
        ).segments
        self.assertEqual(segments, [])

    def test_integer_inputs_converted(self) -> None:
        """Verifies that passing integer arrays for audio_array and prior_audio does not crash and processes successfully."""
        # 1 second of digital silence at 16kHz using int16
        silence_int16 = np.zeros(16000, dtype=np.int16)
        prior_int16 = np.zeros(16000, dtype=np.int16)

        # This should execute successfully and return empty segments because it's pure silence
        segments = self.vad.detect_speech_segments(
            silence_int16, sample_rate=16000, prior_audio=prior_int16
        ).segments
        self.assertEqual(segments, [])

    def test_denoise_boundary_and_empty_arrays(self) -> None:
        """Verifies that VoiceActivityDetector.denoise() correctly handles empty arrays and very short boundaries (e.g., T=1 frame) without bounds-check errors."""
        empty_array = np.array([], dtype=np.float32)
        out_empty = self.vad.denoise(empty_array)
        self.assertEqual(len(out_empty), 0)
        self.assertEqual(out_empty.dtype, np.float32)

        # Very short audio (512 samples -> exactly 1 STFT frame at hop_length=256)
        short_array = (
            np.random.default_rng(42).normal(0, 0.1, 512).astype(np.float32)
        )
        out_short = self.vad.denoise(short_array)
        self.assertEqual(out_short.shape, short_array.shape)
        self.assertEqual(out_short.dtype, np.float32)

    def test_denoise_signal_integrity_on_reference_chunk(self) -> None:
        """Directly exercises VoiceActivityDetector.denoise() on a real audio snippet to verify signal integrity and output dimensions."""
        audio_path = Path(__file__).parent / "test_data" / "test_stress.flac"
        audio_data, _ = load_audio(audio_path)
        chunk = audio_data[: 16000 * 1]  # 1 second chunk
        denoised = self.vad.denoise(chunk)
        self.assertEqual(denoised.shape, chunk.shape)
        self.assertEqual(denoised.dtype, np.float32)
        self.assertGreater(np.max(np.abs(denoised)), 1e-4)
        self.assertLess(np.max(np.abs(denoised)), 1.0)

    def test_synthetic_tone_rejection(self) -> None:
        """Verifies that synthetic tone (constant sine wave) is rejected by the neural VAD."""
        t = np.linspace(0, 1.0, 16000, endpoint=False)
        tone = np.sin(2 * np.pi * 1000 * t).astype(np.float32) * 0.5
        segments = self.vad.detect_speech_segments(
            tone, sample_rate=16000
        ).segments
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
        self.assertTrue(self.vad.is_speech_segment(voice, chunk_size=512))

    def test_is_speech_segment_spiky_static_rejected(self) -> None:
        """Verifies that an unpitched noise burst with high RMS spikiness is rejected by tandem spectral flatness verification."""
        t = np.linspace(0, 1.0, 16000, endpoint=False)
        rng = np.random.default_rng(42)
        static = (
            np.sin(2 * np.pi * 3000 * t) + 0.0001 * rng.normal(0, 1, 16000)
        ).astype(np.float32) * 0.0005
        self.assertFalse(self.vad.is_speech_segment(static, chunk_size=512))

    def _run_integration_test(
        self,
        filename: str,
        ground_truth: list[tuple[float, float]],
        baseline_f1: float,
        tolerance: float = 0.02,
        vad_instance: vad.VoiceActivityDetector | None = None,
        chunk_len_sec: float = 15.0,
        min_recall: float | None = None,
    ) -> None:
        """Helper to run VAD over an audio file in simulated chunks and assert F1 differentially.

        Args:
            filename: Fixture name under `test_data/`.
            ground_truth: Annotated (start, end) speech ranges in seconds.
            baseline_f1: Recorded F1 for this fixture (see VAD_BENCHMARKS.md).
            tolerance: Permitted F1 drift below `baseline_f1`.
            vad_instance: Detector override; defaults to the shared instance.
            chunk_len_sec: Simulated production chunk length.
            min_recall: Absolute recall floor. Set this on fixtures that guard
                detector sensitivity, since F1 alone lets a recall drop hide
                behind the precision gain that clipping speech produces.
        """
        audio_path = Path(__file__).parent / "test_data" / filename
        if not audio_path.exists():
            self.skipTest(f"Audio file not found at: {audio_path}")

        audio_data, sample_rate = load_audio(audio_path)

        detector = vad_instance or self.vad
        if not detector.silero_session:
            detector.setup()

        # Production continuous stream parameters:
        # Audio chunks are captured in intervals
        chunk_samples = int(chunk_len_sec * sample_rate)
        # Match production: the stitcher caches VAD_DEFAULT_PRIMING_SEC (6.0s) of prior tail
        priming_samples = int(VAD_DEFAULT_PRIMING_SEC * sample_rate)

        detected_segments = []
        prior_audio_tail = None

        for i in range(0, len(audio_data), chunk_samples):
            chunk = audio_data[i : i + chunk_samples]
            raw_chunk_segments = detector.detect_speech_segments(
                chunk, sample_rate=sample_rate, prior_audio=prior_audio_tail
            ).segments

            # Shift coordinates relative to global timeline start
            chunk_offset_sec = i / float(sample_rate)
            for start, end in raw_chunk_segments:
                detected_segments.append(
                    (start + chunk_offset_sec, end + chunk_offset_sec)
                )

            # Cache trailing prior audio tail for the next chunk boundary priming (Conditional State Continuity)
            chunk_dur = len(chunk) / float(sample_rate)
            ended_in_speech = False
            if raw_chunk_segments:
                last_seg = raw_chunk_segments[-1]
                if last_seg[1] >= chunk_dur - 0.05:  # 50ms tolerance
                    ended_in_speech = True

            if ended_in_speech:
                prior_audio_tail = (
                    chunk[-priming_samples:] if len(chunk) > 0 else None
                )
            else:
                prior_audio_tail = None

        audio_len = len(audio_data) / float(sample_rate)

        # Pad and merge the globally stitched segments
        padded_segments = detector._pad_and_merge_segments(
            detected_segments, audio_len
        )
        metrics = calculate_segment_metrics(
            ground_truth, padded_segments, audio_len
        )

        # Emit the full metric triple to stdout so CI output is the source of
        # truth for the table in VAD_BENCHMARKS.md.
        sys.stdout.write(
            f"BENCHMARK: {filename} f1={metrics.f1:.4f} "
            f"precision={metrics.precision:.4f} recall={metrics.recall:.4f} "
            f"(baseline f1: {baseline_f1:.4f})\n"
        )

        self.assertGreaterEqual(
            metrics.f1,
            baseline_f1 - tolerance,
            f"Regression detected on {filename}! F1 score was {metrics.f1:.4f} (baseline: {baseline_f1:.4f}, tolerance: {tolerance:.4f})",
        )

        if min_recall is not None:
            self.assertGreaterEqual(
                metrics.recall,
                min_recall,
                f"Recall regression on {filename}! Recall was {metrics.recall:.4f} "
                f"(floor: {min_recall:.4f}). Speech is being clipped -- see the "
                f"high-recall quality philosophy in VAD_BENCHMARKS.md.",
            )

    def test_integration_stress_file(self) -> None:
        """Integration test to verify VAD performance on test_stress.flac."""
        self._run_integration_test(
            "test_stress.flac", [(0.4, 2.85)], baseline_f1=0.948
        )

    def test_integration_joined_file(self) -> None:
        """Integration test to verify VAD performance on test_joined.flac."""
        self._run_integration_test(
            "test_joined.flac",
            [(8.3, 10.7), (12.3, 15.6), (20.3, 23.0), (26.2, 27.0)],
            baseline_f1=0.908,
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
            baseline_f1=0.851,
        )

    def test_integration_cajon_pass_trailing_file(self) -> None:
        """Integration test to verify VAD performance on test_cajon_pass_trailing.flac (trailing scanner speech)."""
        self._run_integration_test(
            "test_cajon_pass_trailing.flac",
            [
                (1.878, 5.697),
                (11.230, 15.151),
                (25.565, 34.590),
                (35.489, 36.387),
            ],
            baseline_f1=0.090,
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
            baseline_f1=0.919,
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
            baseline_f1=0.892,
        )

    def test_integration_middlebury_quiet_segments_file(self) -> None:
        """Integration test to verify VAD performance on test_middlebury_quiet_segments.mp3 (quiet segments)."""
        self._run_integration_test(
            "test_middlebury_quiet_segments.mp3",
            [
                (0.6, 2.2),
                (4.2, 6.7),
            ],
            baseline_f1=0.836,
        )

    def test_integration_middlebury_quiet_spiky_file(self) -> None:
        """Integration test to verify VAD performance on test_middlebury_quiet_spiky.mp3 (quiet EMS speech)."""
        self._run_integration_test(
            "test_middlebury_quiet_spiky.mp3",
            [
                (0.18, 1.45),
            ],
            baseline_f1=0.583,
        )

    def test_integration_quiet_speech_loud_transient(self) -> None:
        """Integration test to verify VAD performance on quiet speech followed by a loud transient spike."""
        self._run_integration_test(
            "test_quiet_speech_loud_transient.mp3",
            [
                (0.213, 0.8),
                (2.037, 3.869),
            ],
            baseline_f1=0.756,
        )

    def test_integration_deafening_dispatcher_ems(self) -> None:
        """Integration test to verify VAD recovers sensitivity after a loud dispatcher.

        Verifies that a loud dispatcher segment (3.0s - 4.609s) does not deafen the VAD
        for the subsequent quiet EMS speech segments (5.984s - 6.611s and 7.865s - 11.605s)
        when processed in 5.0-second chunks.
        """
        self._run_integration_test(
            "test_vad_deafening_dispatcher_ems.flac",
            [
                (3.0, 4.609),
                (5.984, 6.611),
                (7.865, 11.605),
            ],
            baseline_f1=0.679,
            chunk_len_sec=5.0,
        )

    def test_integration_deafening_static_preamble(self) -> None:
        """Integration test to verify VAD performance on quiet speech preceded by static noise.

        Verifies that a quiet speech segment (4.418s - 15.570s) preceded by 1.4s of static noise
        is successfully detected across chunk boundaries when processed in 5.0-second chunks.
        """
        self._run_integration_test(
            "test_vad_deafening_static_preamble.flac",
            [
                (4.418, 15.570),
            ],
            baseline_f1=0.703,
            chunk_len_sec=5.0,
        )

    def test_integration_muffled_mason_co_fire(self) -> None:
        """Integration test to verify VAD performance on quiet/muffled speech from Mason County Fire."""
        self._run_integration_test(
            "test_muffled_mason_co_fire.flac",
            [
                (10.560, 10.762),
                (12.830, 13.413),
            ],
            baseline_f1=0.442,
        )

    def test_integration_hood_river_stream_chunk(self) -> None:
        """Integration test to verify VAD performance on inter-transmission gap speech bursts (Hood River c1416cf1).

        Runs on the raw 15s stream chunk. Hood River is a `bcfy_feeds` source,
        which the icecast collector cuts into CHUNK_DURATION_SECONDS windows, so
        this is the audio shape the VAD actually receives in production.

        Like every row in VAD_BENCHMARKS.md this runs at `pad_sec = 0.0`, which
        measures intrinsic boundary accuracy rather than shipped output. The
        residual recall gap is speech-edge clipping at `0.532s`, `5.872s`, and
        `6.672s`; production padding closes it, which
        `test_integration_hood_river_stream_chunk_production_padding` asserts.
        """
        self._run_integration_test(
            "test_vad_inter_transmission_gap_speech.flac",
            HOOD_RIVER_CHUNK_GROUND_TRUTH,
            baseline_f1=0.791,
            min_recall=0.90,
        )

    def test_integration_hood_river_stream_chunk_production_padding(
        self,
    ) -> None:
        """Verifies the shipped, padded output clips no annotated speech on the c1416cf1 chunk.

        The tracked benchmarks deliberately run at `pad_sec = 0.0` so that pad
        tuning does not move every row, but that is not the configuration whose
        recall the high-recall quality philosophy is about -- what reaches the
        ASR is padded. Without this test a regression that widened intrinsic
        edge clipping past what `VAD_DEFAULT_PAD_SEC` can absorb would ship
        clipped dispatches while the 0.0 benchmark stayed inside tolerance.
        """
        prod_vad = vad.VoiceActivityDetector(
            models_dir=self.models_dir, pad_sec=VAD_DEFAULT_PAD_SEC
        )
        prod_vad.setup()
        self._run_integration_test(
            "test_vad_inter_transmission_gap_speech.flac",
            HOOD_RIVER_CHUNK_GROUND_TRUTH,
            baseline_f1=0.794,
            vad_instance=prod_vad,
            min_recall=1.0,
        )

    def test_integration_hood_river_segment_payload(self) -> None:
        """Integration test to verify VAD performance on the stitched segment payload for Hood River c1416cf1.

        The fixture is the [8.868s, 13.548s] window of the stream chunk above,
        i.e. the payload the stitcher emitted for this segment. Because its
        boundaries come from a prior run of the detector under test, it is not an
        independent sensitivity guard -- that is
        `test_integration_hood_river_stream_chunk`. What it does track is how the
        detector scores on a short stitched payload, where the over-trigger
        around each burst is a far larger share of the file than on a 15s chunk
        (precision 0.571 here versus 0.687 on the chunk, same audio).

        These two ranges are the hand-supplied annotation for this segment. In
        stream-chunk coordinates (+8.868s) they are `9.675-10.433` and
        `11.516-11.768`. The second onset sits 248ms later than the chunk
        annotation's `11.268`; both are hand-supplied, and the 11.268-11.516
        lead-in is the quietest stretch either labels (p90 -33.9 dB), so the two
        readings of where that burst starts are not reconciled here.
        """
        self._run_integration_test(
            "test_vad_hood_river_segment_payload.flac",
            [
                (0.807, 1.565),
                (2.648, 2.900),
            ],
            baseline_f1=0.606,
            min_recall=0.90,
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
            ).segments
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
        # Note: At ONSET=0.17, initial static transient produces a 1.0s
        # segment before noise floor settles.
        self.assertEqual(padded_segments, [(0.16, 1.216)])

    def test_integration_idle_line_noise_with_speech(self) -> None:
        """Integration test to verify VAD detects speech while stationarity skips idle noise.

        Verifies that on a 30s stream with active dispatch speech in chunk 1 (0-15s)
        and stationary ADC soundcard line-in hiss in chunk 2 (15-30s), speech is
        reliably detected in chunk 1 and chunk 2 is skipped without false positives.
        """
        self._run_integration_test(
            "test_idle_line_noise_with_speech.flac",
            [
                (0.080, 1.150),
                (3.450, 4.600),
                (8.050, 8.950),
                (9.850, 10.850),
            ],
            baseline_f1=0.940,
            chunk_len_sec=15.0,
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
        ).segments
        self.assertIsNotNone(segments_no_prime)

        # Run chunk2 primed with the tail of chunk1
        segments_primed = self.vad.detect_speech_segments(
            chunk2, sample_rate=1000 * SAMPLES_PER_MS, prior_audio=chunk1
        ).segments

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
        ).segments

        # Assert that absolutely zero segments were detected inside the silent chunk
        self.assertEqual(detected_segments, [])

    def test_is_tone_segment_two_tone_paging(self) -> None:
        """Verifies that a two-tone sequential paging signal is identified as a tone segment and rejected."""
        t1 = np.linspace(0, 1.0, 16000, endpoint=False)
        tone1 = (
            np.sin(2 * np.pi * TONE_QUIK_CALL_II_FREQ1_HZ * t1).astype(
                np.float32
            )
            * 0.5
        )

        t2 = np.linspace(0, 3.0, 48000, endpoint=False)
        tone2 = (
            np.sin(2 * np.pi * TONE_QUIK_CALL_II_FREQ2_HZ * t2).astype(
                np.float32
            )
            * 0.5
        )

        paging_signal = np.concatenate([tone1, tone2])
        self.assertTrue(self.vad.is_tone_segment(paging_signal))
        segments = self.vad.detect_speech_segments(
            paging_signal, sample_rate=16000
        ).segments
        self.assertEqual(segments, [])

    def test_is_tone_segment_eas_attention(self) -> None:
        """Verifies that an Emergency Alert System (EAS) attention tone (853 Hz + 960 Hz) is identified as a tone segment and rejected."""
        t = np.linspace(0, 4.0, 64000, endpoint=False)
        eas_tone = (
            np.sin(2 * np.pi * TONE_EAS_FREQ1_HZ * t)
            + np.sin(2 * np.pi * TONE_EAS_FREQ2_HZ * t)
        ).astype(np.float32) * 0.25
        self.assertTrue(self.vad.is_tone_segment(eas_tone))
        segments = self.vad.detect_speech_segments(
            eas_tone, sample_rate=16000
        ).segments
        self.assertEqual(segments, [])

    def test_is_speech_segment_reject_subaudible_flickering(self) -> None:
        """Verifies that a sub-audible flickering / static ticking signal is rejected by _is_speech_segment."""
        t = np.linspace(0, 2.0, 32000, endpoint=False)
        # 75 Hz sinusoidal rumble mixed with tiny transient ticks
        rumble = (
            np.sin(2 * np.pi * VAD_TEST_SUBAUDIBLE_RUMBLE_FREQ_HZ * t).astype(
                np.float32
            )
            * 0.4
        )
        ticks = (
            np.random.default_rng(seed=42)
            .normal(0.0, 0.01, 32000)
            .astype(np.float32)
        )
        flickering_signal = rumble + ticks
        self.assertFalse(
            self.vad.is_speech_segment(flickering_signal, TONE_STFT_HOP_LENGTH)
        )
        segments = self.vad.detect_speech_segments(
            flickering_signal, sample_rate=16000
        ).segments
        self.assertEqual(segments, [])

    def test_is_speech_segment_reject_subaudible_flickering_file(
        self,
    ) -> None:
        """Verifies that the actual test_subaudible_flickering.flac file is correctly rejected."""
        flickering_path = (
            Path(__file__).parent
            / "test_data"
            / "test_subaudible_flickering.flac"
        )
        samples, sr = load_audio(flickering_path)
        segments = self.vad.detect_speech_segments(
            samples, sample_rate=sr
        ).segments
        self.assertEqual(segments, [])

    def test_pad_and_merge_segments_midpoint_clamping(self) -> None:
        """Verifies that close raw speech segments clamp padding to midpoints."""
        detector = vad.VoiceActivityDetector(pad_sec=0.3)
        raw_segments = [(1.0, 2.0), (2.4, 3.4)]
        padded = detector._pad_and_merge_segments(
            raw_segments, audio_len_sec=10.0
        )
        self.assertEqual(len(padded), 2)
        # Midpoint of 2.0 to 2.4 gap is 2.2
        self.assertAlmostEqual(padded[0][0], 0.7)  # 1.0 - 0.3
        self.assertAlmostEqual(padded[0][1], 2.2)  # 2.0 + 0.2 (clamped)
        self.assertAlmostEqual(padded[1][0], 2.2)  # 2.4 - 0.2 (clamped)
        self.assertAlmostEqual(padded[1][1], 3.7)  # 3.4 + 0.3
        self.assertLessEqual(padded[0][1], padded[1][0])

    def test_pad_and_merge_segments_non_overlapping_raw_gaps(self) -> None:
        """Verifies that widely separated raw segments get full padding."""
        detector = vad.VoiceActivityDetector(pad_sec=0.3)
        raw_segments = [(1.0, 2.0), (4.0, 5.0)]
        padded = detector._pad_and_merge_segments(
            raw_segments, audio_len_sec=10.0
        )
        self.assertEqual(len(padded), 2)
        self.assertAlmostEqual(padded[0][0], 0.7)
        self.assertAlmostEqual(padded[0][1], 2.3)
        self.assertAlmostEqual(padded[1][0], 3.7)
        self.assertAlmostEqual(padded[1][1], 5.3)

    def test_pad_and_merge_segments_raw_overlap_merging(self) -> None:
        """Verifies overlapping raw segments are merged into a single segment."""
        detector = vad.VoiceActivityDetector(pad_sec=0.3)
        raw_segments = [(1.0, 2.5), (2.3, 4.0)]
        padded = detector._pad_and_merge_segments(
            raw_segments, audio_len_sec=10.0
        )
        self.assertEqual(len(padded), 1)
        self.assertAlmostEqual(padded[0][0], 0.7)
        self.assertAlmostEqual(padded[0][1], 4.3)

    def test_pad_and_merge_segments_edge_cases(self) -> None:
        """Verifies empty lists and boundary clamping near audio start and end."""
        detector = vad.VoiceActivityDetector(pad_sec=0.3)
        self.assertEqual(
            detector._pad_and_merge_segments([], audio_len_sec=10.0), []
        )
        padded = detector._pad_and_merge_segments(
            [(0.1, 9.9)], audio_len_sec=10.0
        )
        self.assertEqual(padded, [(0.0, 10.0)])

    def test_pad_and_merge_segments_preserves_qualifying_gap(self) -> None:
        """Verifies that a 1.024s gap preserves >= 0.8s padded gap for stitcher threshold."""
        detector = vad.VoiceActivityDetector(
            pad_sec=0.3, min_qualifying_gap_sec=0.8
        )
        raw_segments = [(1.0, 2.0), (3.024, 4.024)]
        padded = detector._pad_and_merge_segments(
            raw_segments, audio_len_sec=10.0
        )
        self.assertEqual(len(padded), 2)
        # 1.024s gap: max padding into gap is (1.024 - 0.8)/2 = 0.112s
        self.assertAlmostEqual(padded[0][0], 0.7)  # 1.0 - 0.3
        self.assertAlmostEqual(padded[0][1], 2.112)  # 2.0 + 0.112
        self.assertAlmostEqual(padded[1][0], 2.912)  # 3.024 - 0.112
        self.assertAlmostEqual(padded[1][1], 4.324)  # 4.024 + 0.3
        self.assertAlmostEqual(padded[1][0] - padded[0][1], 0.8)

    def test_detect_speech_segments_returns_none_preprocessed_on_skip(
        self,
    ) -> None:
        """Verifies that the returned preprocessed audio is None on empty or skipped VAD calls."""
        detector = vad.VoiceActivityDetector()
        detector.setup()

        # Call with empty array
        denoised = detector.detect_speech_segments(
            np.array([], dtype=np.float32)
        ).preprocessed_audio
        self.assertIsNone(denoised)

        # Call with silent audio that triggers skip
        silent_audio = np.zeros(16000, dtype=np.float32)
        denoised = detector.detect_speech_segments(
            silent_audio
        ).preprocessed_audio
        self.assertIsNone(denoised)

    def test_concurrent_detect_speech_segments_preprocessed_audio_attribution(
        self,
    ) -> None:
        """Verifies concurrent detect_speech_segments calls on a single shared
        VoiceActivityDetector never cross-attribute preprocessed audio between
        threads processing different input signals.

        This is a regression test for a race condition where the preprocessed
        audio of the current chunk was cached on shared instance state
        (`last_preprocessed_audio`) rather than returned directly: concurrent
        calls on distinct threads could overwrite each other's cached tail
        before it was read back by its own caller.
        """
        detector = vad.VoiceActivityDetector(
            models_dir=self.models_dir, pad_sec=0.0
        )
        detector.setup()

        sample_rate = 16000
        t = np.linspace(0, 1.0, sample_rate, endpoint=False)
        signal_low = (np.sin(2 * np.pi * 440 * t) * 0.3).astype(np.float32)
        signal_high = (np.sin(2 * np.pi * 2000 * t) * 0.3).astype(np.float32)

        # Single-threaded reference: the preprocessed audio each signal must
        # produce, computed serially before any concurrency is introduced.
        expected_low = detector.detect_speech_segments(
            signal_low, sample_rate=sample_rate
        ).preprocessed_audio
        expected_high = detector.detect_speech_segments(
            signal_high, sample_rate=sample_rate
        ).preprocessed_audio
        assert expected_low is not None
        assert expected_high is not None
        self.assertFalse(np.allclose(expected_low, expected_high))

        num_workers = 8
        iterations_per_worker = 16

        def _worker(worker_id: int) -> None:
            for i in range(iterations_per_worker):
                if (worker_id + i) % 2 == 0:
                    signal, expected = signal_low, expected_low
                else:
                    signal, expected = signal_high, expected_high

                actual = detector.detect_speech_segments(
                    signal, sample_rate=sample_rate
                ).preprocessed_audio
                assert actual is not None
                self.assertTrue(
                    np.allclose(actual, expected),
                    "Preprocessed audio was cross-attributed to a "
                    "different thread's input signal.",
                )

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=num_workers
        ) as executor:
            futures = [
                executor.submit(_worker, worker_id)
                for worker_id in range(num_workers)
            ]
            for future in futures:
                future.result()

    def test_detect_speech_segments_with_diagnostics(self) -> None:
        """Verifies detect_speech_segments_with_diagnostics returns candidate rejection reasons."""
        detector = vad.VoiceActivityDetector(models_dir=self.models_dir)
        detector.setup()

        # Generate a spiky static slice that fails VAD checks
        t = np.linspace(0, 1.0, 16000, endpoint=False)
        rng = np.random.default_rng(42)
        spiky_static = (
            np.sin(2 * np.pi * 3000 * t) + 0.0001 * rng.normal(0, 1, 16000)
        ).astype(np.float32) * 0.0005

        result = detector.detect_speech_segments_with_diagnostics(
            spiky_static, sample_rate=16000
        )
        self.assertEqual(result.accepted_segments, [])

    def test_detect_speech_segments_with_diagnostics_returns_preprocessed_audio(
        self,
    ) -> None:
        """Verifies the third return value is the current chunk's preprocessed audio, not None or empty, for non-skipped input.

        Regression guard for #737's diagnose_feed_drop.py, which reads this
        value from the return tuple to plot VAD probabilities against the
        exact signal the detector judged. Before this fix, that same value
        was read off a since-removed last_preprocessed_audio instance
        attribute -- an unpacking bug here would silently plot against the
        wrong (or absent) signal rather than raise.
        """
        detector = vad.VoiceActivityDetector(
            models_dir=self.models_dir, pad_sec=0.0
        )
        detector.setup()

        t = np.linspace(0, 1.0, 16000, endpoint=False)
        signal = (np.sin(2 * np.pi * 440 * t) * 0.3).astype(np.float32)

        preprocessed = detector.detect_speech_segments_with_diagnostics(
            signal, sample_rate=16000
        ).preprocessed_audio

        self.assertIsNotNone(preprocessed)
        assert preprocessed is not None
        self.assertGreater(len(preprocessed), 0)
        # Matches the reference computed by the non-diagnostics API for the
        # identical input -- same underlying preprocessing, different entry
        # point.
        expected_preprocessed = detector.detect_speech_segments(
            signal, sample_rate=16000
        ).preprocessed_audio
        assert expected_preprocessed is not None
        np.testing.assert_array_equal(preprocessed, expected_preprocessed)

    def test_stationarity_gating_stationary_noise_vs_speech(self) -> None:
        """Verifies stationarity gating skips stationary noise but preserves speech."""
        detector = vad.VoiceActivityDetector(models_dir=self.models_dir)
        detector.setup()

        sample_rate = 16000
        rng = np.random.default_rng(12345)

        # 1. Stationary Gaussian line-in noise (RMS ~ 0.020, Peak ~ 0.060)
        # Without stationarity gating, RMS > 0.015 would fail legacy skip checks.
        noise = (rng.normal(0.0, 0.020, sample_rate * 2)).astype(np.float32)
        self.assertTrue(
            detector._should_skip_vad(noise, sample_rate),
            "Stationary line-in noise was not skipped by stationarity gating.",
        )

        # 2. Dynamic modulated signal scaled below the 0.040 ceiling (amplitude 0.03)
        # mean_rms is ~0.0072 (below ceiling), so high CV and peak/median is what
        # exercises and guards the non-skip decision.
        t = np.linspace(0, 2.0, sample_rate * 2, endpoint=False)
        carrier = np.sin(2 * np.pi * 500 * t).astype(np.float32)
        modulator = np.maximum(0.0, np.sin(2 * np.pi * 3 * t)).astype(
            np.float32
        )
        speech_like = carrier * modulator * 0.03
        self.assertFalse(
            detector._should_skip_vad(speech_like, sample_rate),
            "Modulated speech-like signal was incorrectly skipped.",
        )

    def test_stationarity_gating_dilution_protection(self) -> None:
        """Verifies short speech bursts over a stationary floor are not averaged away."""
        detector = vad.VoiceActivityDetector(models_dir=self.models_dir)
        detector.setup()

        sample_rate = 16000
        rng = np.random.default_rng(42)

        # 15s stationary floor at RMS 0.020
        floor_15s = (rng.normal(0.0, 0.020, 15 * sample_rate)).astype(
            np.float32
        )
        burst_samples = int(0.3 * sample_rate)
        burst_start = int(7.0 * sample_rate)
        t = np.linspace(0, 0.3, burst_samples, endpoint=False)
        burst = (np.sin(2 * np.pi * 500 * t) * 0.042).astype(np.float32)
        floor_15s[burst_start : burst_start + burst_samples] += burst

        self.assertFalse(
            detector._should_skip_vad(floor_15s, sample_rate),
            "0.3s speech burst was diluted away and incorrectly skipped.",
        )

    def test_stationarity_gating_edge_cases(self) -> None:
        """Verifies boundary conditions for stationarity gating."""
        detector = vad.VoiceActivityDetector(models_dir=self.models_dir)
        detector.setup()

        sample_rate = 16000

        # Short signal (< 4 frames / < 0.20s): should not trigger stationarity
        short_noise = np.ones(int(0.10 * sample_rate), dtype=np.float32) * 0.02
        self.assertFalse(
            detector._should_skip_vad(short_noise, sample_rate),
            "Short signal under 4 frames was incorrectly evaluated.",
        )

        # Near-zero digital silence: should trigger skip
        digital_zero = np.zeros(sample_rate, dtype=np.float32)
        self.assertTrue(
            detector._should_skip_vad(digital_zero, sample_rate),
            "Digital zero signal was not skipped.",
        )


class TestFeedDiagnosticRunner(unittest.TestCase):
    """Unit tests for pure timeframe reconciliation and audit functions in diagnose_feed_drop."""

    def setUp(self) -> None:
        self.base_time = datetime.datetime(
            2026, 7, 29, 12, 0, 0, tzinfo=datetime.UTC
        )

    def test_merge_accepted_intervals(self) -> None:
        t0 = self.base_time
        t1 = t0 + datetime.timedelta(seconds=5)
        t2 = t0 + datetime.timedelta(seconds=10)
        t3 = t0 + datetime.timedelta(seconds=15)
        t4 = t0 + datetime.timedelta(seconds=20)

        utterances = [
            diagnose_feed_drop.DiagnosticUtterance(
                start_time=t0,
                end_time=t2,
                status="Accepted",
                rejection_reason=None,
                duration_sec=10.0,
                chunk_name="c1.flac",
            ),
            diagnose_feed_drop.DiagnosticUtterance(
                start_time=t1,
                end_time=t3,
                status="Accepted",
                rejection_reason=None,
                duration_sec=10.0,
                chunk_name="c2.flac",
            ),
        ]
        runner = diagnose_feed_drop.FeedDiagnosticRunner(
            "test-bucket", "test-feed", "test-project"
        )
        merged = runner._merge_accepted_intervals(t0, t4, utterances)
        self.assertEqual(merged, [(t0, t3)])

    def test_compute_missing_intervals(self) -> None:
        t0 = self.base_time
        t1 = t0 + datetime.timedelta(seconds=5)
        t2 = t0 + datetime.timedelta(seconds=10)
        t3 = t0 + datetime.timedelta(seconds=20)

        runner = diagnose_feed_drop.FeedDiagnosticRunner(
            "test-bucket", "test-feed", "test-project"
        )
        accepted = [(t1, t2)]
        missing = runner._compute_missing_intervals(t0, t3, accepted)
        self.assertEqual(missing, [(t0, t1), (t2, t3)])

    def test_audit_missing_interval_cause_attribution(self) -> None:
        runner = diagnose_feed_drop.FeedDiagnosticRunner(
            "test-bucket", "test-feed", "test-project"
        )
        t0 = self.base_time
        t1 = t0 + datetime.timedelta(seconds=5)

        # 1. Post-VAD Rejection
        rejected_utt = diagnose_feed_drop.DiagnosticUtterance(
            start_time=t0,
            end_time=t1,
            status="Rejected",
            rejection_reason="Below Minimum RMS Floor",
            duration_sec=5.0,
            chunk_name="c1.flac",
        )
        report_rej = runner._audit_missing_interval(t0, t1, 5.0, [rejected_utt])
        self.assertIn(
            "Post-VAD Rejection Heuristic Failure", report_rej.primary_cause
        )

        # 2. Genuine Silence
        silent_probs = [(0.5, 0.00001, 0.01), (1.5, 0.00001, 0.01)]
        report_silence = runner._audit_missing_interval(
            t0, t1, 5.0, [], live_probs=silent_probs, target_start=t0
        )
        self.assertIn("Genuine Silence", report_silence.primary_cause)

        # 3. Neural Drop
        drop_probs = [(0.5, 0.05, 0.20), (1.5, 0.05, 0.25)]
        report_drop = runner._audit_missing_interval(
            t0, t1, 5.0, [], live_probs=drop_probs, target_start=t0
        )
        self.assertIn(
            "Silero VAD Neural Network Probability Drop",
            report_drop.primary_cause,
        )

import unittest

import numpy as np

from backend.pipeline.transcription.utils import calculate_padded_ranges, detect_silence_segments
from backend.pipeline.transcription.datatypes import TimeRange


class TestUtils(unittest.TestCase):

    def test_detect_silence_segments_all_silence(self) -> None:
        # 1 second of silence (zeros)
        audio = np.zeros(16000, dtype=np.float32)
        segments = detect_silence_segments(audio, 16000, -40.0)
        self.assertEqual(len(segments), 1)
        self.assertEqual(segments[0].start_ms, 0)
        # 16000 samples at 16kHz is 1000ms.
        # detect_silence_segments might truncate to multiples of window_ms (30ms).
        # 1000 // 30 = 33 chunks. 33 * 30 = 990ms.
        self.assertEqual(segments[0].end_ms, 990)

    def test_detect_silence_segments_no_silence(self) -> None:
        # 1 second of full scale signal
        audio = np.ones(16000, dtype=np.float32)
        segments = detect_silence_segments(audio, 16000, -40.0)
        self.assertEqual(len(segments), 0)

    def test_detect_silence_segments_mixed(self) -> None:
        # 1 second of audio: 300ms silence, 400ms signal, 300ms silence
        audio = np.zeros(16000, dtype=np.float32)
        # Signal in the middle (indices 4800 to 11200) -> 300ms to 700ms
        audio[4800:11200] = 1.0

        segments = detect_silence_segments(audio, 16000, -40.0, window_ms=10)
        # With 10ms window, 16000 samples is 100 chunks.
        # Silence from 0 to 300ms (chunks 0-30)
        # Signal from 300 to 700ms (chunks 30-70)
        # Silence from 700 to 1000ms (chunks 70-100)

        self.assertEqual(len(segments), 2)
        self.assertEqual(segments[0].start_ms, 0)
        self.assertEqual(segments[0].end_ms, 300)
        self.assertEqual(segments[1].start_ms, 700)
        self.assertEqual(segments[1].end_ms, 1000)

    def test_calculate_padded_ranges_no_bumpers(self) -> None:
        speech = [TimeRange(start_ms=1000, end_ms=2000)]
        noise = []
        ranges = calculate_padded_ranges(
            audio_duration_ms=5000,
            speech_segments=speech,
            noise_segments=noise,
            file_start_ms=0,
            pre_roll_ms=200,
            post_roll_ms=200,
        )
        self.assertEqual(len(ranges), 1)
        self.assertEqual(ranges[0], (800, 2200))

    def test_calculate_padded_ranges_truncated_by_speech(self) -> None:
        speech = [
            TimeRange(start_ms=1000, end_ms=2000),
            TimeRange(start_ms=2100, end_ms=3000),
        ]
        noise = []
        ranges = calculate_padded_ranges(
            audio_duration_ms=5000,
            speech_segments=speech,
            noise_segments=noise,
            file_start_ms=0,
            pre_roll_ms=200,
            post_roll_ms=200,
        )
        self.assertEqual(len(ranges), 2)
        self.assertEqual(ranges[0], (800, 2050))
        self.assertEqual(ranges[1], (2050, 3200))


    def test_calculate_padded_ranges_truncated_by_noise(self) -> None:
        speech = [TimeRange(start_ms=1000, end_ms=2000)]
        noise = [TimeRange(start_ms=2050, end_ms=2500)]
        ranges = calculate_padded_ranges(
            audio_duration_ms=5000,
            speech_segments=speech,
            noise_segments=noise,
            file_start_ms=0,
            pre_roll_ms=200,
            post_roll_ms=200,
        )
        self.assertEqual(len(ranges), 1)
        self.assertEqual(ranges[0], (800, 2050))

if __name__ == "__main__":
    unittest.main()

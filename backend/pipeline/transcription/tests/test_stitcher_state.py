import unittest

from pydub import AudioSegment

from backend.pipeline.transcription.datatypes import (
    AppendBufferAction,
    AudioChunkData,
    DropAction,
    FlushAction,
    ScheduleStaleTimerAction,
    StateMachineAction,
    StitchAudioConfig,
    StitcherContext,
    TimeRange,
    UpdateStateAction,
)
from backend.pipeline.transcription.enums import VadType
from backend.pipeline.transcription.stitcher_state import (
    AudioStitchingStateMachine,
)


def get_test_stitch_config(
    significant_gap_ms: int = 3000,
    stale_timeout_ms: int = 45000,
    max_transmission_duration_ms: int = 60000,
    vad_pre_roll_ms: int = 500,
    vad_post_roll_ms: int = 500,
) -> StitchAudioConfig:
    """Helper to generate a rapid-test config."""
    return StitchAudioConfig(
        project_id="test",
        vad_type=VadType.TEN_VAD,
        vad_config="",
        metrics_exporter_type="none",
        metrics_config="",
        significant_gap_ms=significant_gap_ms,
        stale_timeout_ms=stale_timeout_ms,
        max_transmission_duration_ms=max_transmission_duration_ms,
        vad_pre_roll_ms=vad_pre_roll_ms,
        vad_post_roll_ms=vad_post_roll_ms,
    )


def mock_audio_chunk(
    start_ms: int,
    duration_ms: int,
    speech_segments: list[tuple[float, float]],
    gcs_uri: str = "gs://fake/1.flac",
) -> AudioChunkData:
    return AudioChunkData(
        start_ms=start_ms,
        audio=AudioSegment.silent(duration=duration_ms),
        speech_segments=[
            TimeRange(int(s * 1000), int(e * 1000)) for s, e in speech_segments
        ],
        gcs_uri=gcs_uri,
    )


class AudioStitchingStateMachineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.config = get_test_stitch_config(significant_gap_ms=3000)
        self.state_machine = AudioStitchingStateMachine(self.config)
        self.ctx = StitcherContext(
            feed_id="test-feed-xyz",
            current_gcs_uri="gs://fake/init.flac",
            contributing_audio_uris=[],
            file_start_ms=0,
        )

    def _process(self, chunk: AudioChunkData) -> list[StateMachineAction]:
        self.ctx.current_gcs_uri = chunk.gcs_uri
        self.ctx.file_start_ms = chunk.start_ms
        return self.state_machine.process_chunk(chunk, self.ctx)

    def test_discard_initial_silence(self) -> None:
        """Verifies completely silent chunks lacking prior context are entirely flushed via DropAction."""
        chunk = mock_audio_chunk(0, 15000, [])
        actions = self._process(chunk)

        self.assertTrue(any(isinstance(a, DropAction) for a in actions))
        self.assertTrue(
            any(isinstance(a, ScheduleStaleTimerAction) for a in actions)
        )

    def test_continuous_speech_accumulation(self) -> None:
        """Verifies adjacent speech segments beneath gap boundaries strictly trigger AppendBuffer bounds across sequential requests."""
        # Chunk 1: Speech from 1.0s to 12.0s
        chunk1 = mock_audio_chunk(0, 15000, [(1.0, 12.0)], "gs://fake/1.flac")
        actions1 = self._process(chunk1)

        # Should output an Append buffer from 0.5s (due to 500ms pre-roll) to 12.5s (due to 500ms post-roll)
        self.assertTrue(
            any(isinstance(a, AppendBufferAction) for a in actions1)
        )
        self.assertFalse(any(isinstance(a, FlushAction) for a in actions1))
        self.assertEqual(self.ctx.start_audio_offset_ms, 500)
        self.assertIn("gs://fake/1.flac", self.ctx.contributing_audio_uris)

        # Chunk 2: Arrives at 15.0s, speech from 0.0s to 4.0s.
        # Last speech ended at 12.0s. Next chunk speech starts at 15.0s. Gap is 3.0s.
        # Wait, our significant gap is 3000ms. Gap = 3000ms. So it precisely crosses the threshold and FLUSHES!
        chunk2 = mock_audio_chunk(
            15000, 15000, [(0.0, 4.0)], "gs://fake/2.flac"
        )
        actions2 = self._process(chunk2)

        # Verify a flush occurred due to reaching exactly 3s gap
        flush_action = next(
            (a for a in actions2 if isinstance(a, FlushAction)), None
        )
        self.assertIsNotNone(flush_action)
        assert flush_action is not None
        self.assertEqual(flush_action.reason, "Significant gap detected")

    def test_internal_silence_keeps_transmission_alive(self) -> None:
        """Verifies chunks containing absolutely no speech don't drop context if tracking an active transmission stream."""
        # Chunk 1: Speech from 10.0s to 15.0s
        chunk1 = mock_audio_chunk(0, 15000, [(10.0, 15.0)])
        self._process(chunk1)

        # Chunk 2: Dead air. Speech ended at 15.0s.
        # Missing chunk 2 speech means the gap timer is accumulating implicitly.
        chunk2 = mock_audio_chunk(15000, 15000, [])
        actions2 = self._process(chunk2)

        # It's not explicitly flushed yet, just tracking state
        self.assertTrue(any(isinstance(a, UpdateStateAction) for a in actions2))
        self.assertFalse(any(isinstance(a, FlushAction) for a in actions2))

        # Chunk 3: Dead air. Arrives at 30.0s.
        # Now 30.0s total time elapsed - 15.0s last end = 15.0s gap. Gap > 3.0s!
        # Since it's a silent file that explicitly triggers the gap overrun mid-silence, it FLUSHES.
        chunk3 = mock_audio_chunk(30000, 15000, [])
        actions3 = self._process(chunk3)

        flush_action = next(
            (a for a in actions3 if isinstance(a, FlushAction)), None
        )
        self.assertIsNotNone(flush_action)
        assert flush_action is not None
        self.assertEqual(
            flush_action.reason, "Significant gap detected from silent file"
        )

    def test_max_transmission_duration_mid_stream_severing(self) -> None:
        """Verifies infinite-length callers are violently disconnected gracefully the instant they exceed bounded operational processing timeouts."""
        config = get_test_stitch_config(
            max_transmission_duration_ms=10000
        )  # 10s max
        self.state_machine = AudioStitchingStateMachine(config)

        # Send chunk 1
        chunk1 = mock_audio_chunk(0, 15000, [(0.0, 15.0)])
        self._process(chunk1)

        # Send chunk 2, spanning 15.0 to 30.0
        # When evaluating Chunk 2 at timestamp 15.0, the max duration (10.0) from Chunk 1 start was already hit. So we expect a split!
        chunk2 = mock_audio_chunk(15000, 15000, [(0.0, 15.0)])
        actions2 = self._process(chunk2)

        flush_action = next(
            (a for a in actions2 if isinstance(a, FlushAction)), None
        )
        self.assertIsNotNone(flush_action)
        assert flush_action is not None

        self.assertEqual(
            flush_action.reason, "Maximum transmission duration exceeded"
        )
        self.assertTrue(flush_action.missing_post_context)

        # And because it was severed arbitrarily, the NEXT queued segment inherits a severed head (missing prior context)
        self.assertTrue(self.ctx.missing_prior_context)

    def test_late_chunk_isolated_discard(self) -> None:
        """Verifies severely misordered messages falling outside chronological bounds are skipped over and isolated securely without corrupting native timeline context."""
        # Formal timeline moved sequentially forward to 30.0s
        self.ctx.expected_next_chunk_start_ms = 30000

        # Received a ghost echo from 15.0s!
        chunk_late = mock_audio_chunk(15000, 15000, [(0.0, 5.0)])
        actions = self._process(chunk_late)

        # It must eject via FlushAction purely to isolate Traversing the backend independently
        flush_action = next(
            (a for a in actions if isinstance(a, FlushAction)), None
        )
        self.assertIsNotNone(flush_action)
        assert flush_action is not None

        self.assertEqual(
            flush_action.reason, "Flushing isolated late-arriving audio chunk"
        )
        self.assertTrue(flush_action.missing_prior_context)
        self.assertFalse(flush_action.missing_post_context)

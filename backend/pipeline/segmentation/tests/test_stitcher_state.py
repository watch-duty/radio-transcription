import unittest
from typing import Final

import numpy as np

from backend.pipeline.segmentation.datatypes import (
    AppendBufferAction,
    AudioChunkData,
    DropAction,
    FlushAction,
    StateMachineAction,
    StitchAudioConfig,
    StitcherContext,
    TimeRange,
    UpdateStateAction,
)
from backend.pipeline.segmentation.state.stitcher_state import (
    AudioStitchingStateMachine,
)

SAMPLES_PER_MS: Final = 16


def get_test_stitch_config(
    significant_gap_ms: int = 3000,
    stale_timeout_ms: int = 45000,
    max_transmission_duration_ms: int = 60000,
) -> StitchAudioConfig:
    """Helper to generate a rapid-test config."""
    return StitchAudioConfig(
        project_id="test",
        vad_config="",
        significant_gap_ms=significant_gap_ms,
        stale_timeout_ms=stale_timeout_ms,
        max_transmission_duration_ms=max_transmission_duration_ms,
    )


def mock_audio_chunk(
    start_ms: int,
    duration_ms: int,
    speech_segments: list[tuple[float, float]],
    gcs_uri: str = "gs://fake/1.flac",
) -> AudioChunkData:
    return AudioChunkData(
        start_ms=start_ms,
        audio=np.zeros(int((duration_ms) * 16), dtype=np.int16),
        speech_segments=[
            TimeRange(int(s * 1000), int(e * 1000)) for s, e in speech_segments
        ],
        gcs_uri=gcs_uri,
        duration_ms=duration_ms,
        sample_rate=16000,
    )


class AudioStitchingStateMachineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.config = get_test_stitch_config(significant_gap_ms=3000)
        self.state_machine = AudioStitchingStateMachine(self.config)
        self.ctx = StitcherContext(
            feed_id="test-feed-xyz",
            current_gcs_uri="gs://fake/init.flac",
            session_id="fake-session",
            contributing_audio_uris=[],
            file_start_ms=0,
            last_segment_end_time_ms=None,
            transmission_start_time_ms=None,
            buffer_start_time_ms=None,
            missing_prior_context=False,
            expected_next_chunk_start_ms=None,
            start_audio_offset_ms=None,
            buffer_duration_ms=0,
        )

    def _process(self, chunk: AudioChunkData) -> list[StateMachineAction]:
        self.ctx.current_gcs_uri = chunk.gcs_uri
        self.ctx.file_start_ms = chunk.start_ms
        return self.state_machine.process_chunk(chunk, self.ctx)

    def test_discard_initial_silence_segmented(self) -> None:
        """Verifies completely silent chunks on segmented streams are dropped."""
        config = get_test_stitch_config()
        config = StitchAudioConfig(
            project_id=config.project_id,
            vad_config=config.vad_config,
            significant_gap_ms=config.significant_gap_ms,
            stale_timeout_ms=config.stale_timeout_ms,
            max_transmission_duration_ms=config.max_transmission_duration_ms,
            isolate_segmented_chunks=True,
        )
        self.state_machine = AudioStitchingStateMachine(config)
        chunk = mock_audio_chunk(0, 15000, [])
        actions = self._process(chunk)

        self.assertTrue(any(isinstance(a, DropAction) for a in actions))

    def test_stitch_initial_non_speech_continuous(self) -> None:
        """Verifies completely silent chunks on continuous streams are stitched into a non-speech transmission."""
        chunk = mock_audio_chunk(0, 15000, [])
        actions = self._process(chunk)

        # Should append the silent chunk's audio and NOT drop it
        self.assertTrue(any(isinstance(a, AppendBufferAction) for a in actions))
        self.assertFalse(any(isinstance(a, DropAction) for a in actions))
        self.assertEqual(self.ctx.transmission_start_time_ms, 0)
        self.assertEqual(len(self.ctx.speech_segments), 0)

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
        self.assertEqual(self.ctx.start_audio_offset_ms, 0)
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

        # And because it was severed arbitrarily, the NEXT queued segment inherits a severed head (missing prior context)
        self.assertTrue(self.ctx.missing_prior_context)

    def test_max_non_speech_duration_split(self) -> None:
        """Verifies that long silent non-speech segments are force-split when exceeding max_transmission_duration_ms."""
        config = get_test_stitch_config(
            max_transmission_duration_ms=10000
        )  # 10s max
        self.state_machine = AudioStitchingStateMachine(config)

        # Chunk 1: Silent 15s (0 to 15s)
        chunk1 = mock_audio_chunk(0, 15000, [])
        self._process(chunk1)

        # Chunk 2: Silent 15s (15s to 30s)
        chunk2 = mock_audio_chunk(15000, 15000, [])
        actions2 = self._process(chunk2)

        # We expect a flush action for maximum duration exceeded
        flush_action = next(
            (a for a in actions2 if isinstance(a, FlushAction)), None
        )
        self.assertIsNotNone(flush_action)
        assert flush_action is not None
        self.assertEqual(
            flush_action.reason,
            "Maximum non-speech transmission duration exceeded",
        )
        self.assertTrue(flush_action.missing_post_context)
        self.assertEqual(
            flush_action.audio_classification, 2
        )  # SEGMENTED_AUDIO_NO_SPEECH

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

    def test_process_speech_segments_avoids_overlap(self) -> None:
        """Verifies that _process_speech_segments avoids overlap by updating actual_start_ms."""
        # Chunk speech from 1.0s to 12.0s.
        chunk = mock_audio_chunk(0, 15000, [(1.0, 12.0)])

        # Mock context having processed audio ending at 5000ms (5.0s) in absolute time!
        self.ctx.last_segment_end_time_ms = 5000

        # Expected actions should only append audio starting from 5.0s offset in chunk!
        actions = self.state_machine._process_speech_segments(chunk, self.ctx)

        # Verify that buffer append action only has audio size equivalent to 7000ms (12.0s - 5.0s).
        append_action = next(
            (a for a in actions if isinstance(a, AppendBufferAction)), None
        )
        self.assertIsNotNone(append_action)
        assert append_action is not None

        # Buffer duration ms updated by global_end_ms - max(0, global_start).
        # global_end=12000. global_start=updated to 5000.
        # Expected append_end - append_start = 12000 - 5000 = 7000ms.
        # Size is 7000 * 16 = 112000 samples.
        self.assertEqual(
            append_action.audio_buffer.size, (7000 * SAMPLES_PER_MS)
        )

    def test_contiguous_chunks_are_stitched(self) -> None:
        """Verifies that perfectly contiguous speech segments across chunks are stitched without flushing."""
        # Chunk 1: Speech from 1.0s to 15.0s (full length)
        chunk1 = mock_audio_chunk(0, 15000, [(1.0, 15.0)], "gs://fake/1.flac")
        actions1 = self._process(chunk1)

        self.assertTrue(
            any(isinstance(a, AppendBufferAction) for a in actions1)
        )
        self.assertFalse(any(isinstance(a, FlushAction) for a in actions1))

        # Chunk 2: Starts at 15.0s. Speech from 0.0s to 5.0s.
        chunk2 = mock_audio_chunk(
            15000, 15000, [(0.0, 5.0)], "gs://fake/2.flac"
        )
        actions2 = self._process(chunk2)

        # Should NOT flush Chunk 1!
        self.assertFalse(any(isinstance(a, FlushAction) for a in actions2))
        # Should append Chunk 2 audio!
        self.assertTrue(
            any(isinstance(a, AppendBufferAction) for a in actions2)
        )

        # Contributing URIs should have both!
        self.assertIn("gs://fake/1.flac", self.ctx.contributing_audio_uris)
        self.assertIn("gs://fake/2.flac", self.ctx.contributing_audio_uris)

    def test_late_chunk_excessive_speech_duration_split(self) -> None:
        """Verifies that an isolated late-arriving chunk containing speech exceeding
        max_transmission_duration_ms is force-split internally without corrupting main timeline.
        """
        config = get_test_stitch_config(
            max_transmission_duration_ms=10000
        )  # 10s limit
        self.state_machine = AudioStitchingStateMachine(config)

        # Main timeline moved forward to 50.0s
        self.ctx.expected_next_chunk_start_ms = 50000

        # Received a late chunk starting at 10.0s with 15.0s of continuous speech (> 10s limit)
        chunk_late = mock_audio_chunk(10000, 15000, [(0.0, 15.0)])
        actions = self._process(chunk_late)

        # Since it's processed independently via _process_late_chunk_independently, it filters
        # actions to preserve isolation. We expect two FlushActions: one for severed max limit,
        # and one for the trailing tail.
        flush_actions = [a for a in actions if isinstance(a, FlushAction)]
        self.assertEqual(len(flush_actions), 2)

        # First flush: hits max duration limit mid-stream
        self.assertEqual(
            flush_actions[0].reason, "Maximum transmission duration exceeded"
        )
        self.assertTrue(flush_actions[0].missing_post_context)
        self.assertTrue(flush_actions[0].missing_prior_context)

        # Second flush: flushes remaining tail
        self.assertEqual(
            flush_actions[1].reason,
            "Flushing isolated late-arriving audio chunk",
        )
        self.assertTrue(flush_actions[1].missing_post_context)

    def test_stale_last_segment_end_time_from_previous_transmission(
        self,
    ) -> None:
        """Verifies that last_segment_end_time_ms from a previous transmission
        is ignored when flushing a new non-speech transmission, preventing negative offsets.
        """
        # Chunk 1: Speech ending at 5.0s (5000ms).
        chunk1 = mock_audio_chunk(0, 15000, [(1.0, 5.0)])
        self._process(chunk1)

        # Process a silent chunk starting at 15000.
        # This will trigger a significant gap flush of the first speech transmission,
        # and then initialize a new non-speech transmission starting at 5500ms (5000ms + 500ms post-roll).
        actions = self.state_machine.process_chunk(
            mock_audio_chunk(15000, 15000, []), self.ctx
        )
        flush_action1 = next(
            (a for a in actions if isinstance(a, FlushAction)), None
        )
        self.assertIsNotNone(flush_action1)

        # Verify that ctx.last_segment_end_time_ms is retained (5000ms)
        self.assertEqual(self.ctx.last_segment_end_time_ms, 5000)
        # Verify that ctx.buffer_start_time_ms was set to 5500 (start of the non-speech transmission)
        self.assertEqual(self.ctx.buffer_start_time_ms, 5500)

        # Now trigger a flush of this new non-speech transmission
        flush_action = self.state_machine._flush_current_transmission(
            "test_flush",
            self.ctx,
            missing_post_context=False,
        )

        # Verify that the flushed time range is valid and offsets are >= 0
        self.assertGreaterEqual(
            flush_action.speech_time_range.end_ms,
            flush_action.speech_time_range.start_ms,
        )
        self.assertGreaterEqual(flush_action.end_audio_offset_ms, 0)
        # Specifically, since there is no speech in the current transmission, end_ms should fall back to transmission_start_time_ms (5500)
        self.assertEqual(
            flush_action.speech_time_range.end_ms,
            self.ctx.transmission_start_time_ms,
        )

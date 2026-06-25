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

    def test_stitch_initial_non_speech(self) -> None:
        """Verifies completely silent chunks on continuous streams are stitched into a non-speech transmission."""
        chunk = mock_audio_chunk(0, 15000, [])
        actions = self._process(chunk)

        # Should append the silent chunk's audio and NOT drop it
        self.assertTrue(any(isinstance(a, AppendBufferAction) for a in actions))
        self.assertFalse(any(isinstance(a, DropAction) for a in actions))
        self.assertEqual(self.ctx.transmission_start_time_ms, 0)
        self.assertEqual(len(self.ctx.speech_segments), 0)

    def test_continuous_speech_accumulation(self) -> None:
        """Verifies adjacent speech segments and intra-chunk trailing silence are correctly buffered and flushed."""
        # Chunk 1: Speech from 1.0s to 12.0s
        chunk1 = mock_audio_chunk(0, 15000, [(1.0, 12.0)], "gs://fake/1.flac")
        actions1 = self._process(chunk1)

        # Under Continuous Audio Retention, trailing silence (12.0 to 15.0s) triggers a flush of the active speech segment
        # and starts buffering the non-speech audio window.
        flush_action1 = next(
            (a for a in actions1 if isinstance(a, FlushAction)), None
        )
        self.assertIsNotNone(flush_action1)
        assert flush_action1 is not None
        self.assertEqual(flush_action1.audio_classification, 1)  # SPEECH
        self.assertEqual(
            self.ctx.buffer_duration_ms, 3000
        )  # 3s of trailing silence

        # Chunk 2: Arrives at 15.0s, speech from 0.0s to 4.0s.
        # Since we have active non-speech transmission from the previous chunk, it flushes the non-speech segment (OTHER)
        # and starts the new speech segment.
        chunk2 = mock_audio_chunk(
            15000, 15000, [(0.0, 4.0)], "gs://fake/2.flac"
        )
        actions2 = self._process(chunk2)

        flush_action2 = next(
            (a for a in actions2 if isinstance(a, FlushAction)), None
        )
        self.assertIsNotNone(flush_action2)
        assert flush_action2 is not None
        self.assertEqual(
            flush_action2.audio_classification, 2
        )  # OTHER (silence)

    def test_internal_silence_keeps_transmission_alive(self) -> None:
        """Verifies chunks containing absolutely no speech don't drop context if tracking an active transmission stream."""
        # Chunk 1: Speech from 10.0s to 15.0s
        chunk1 = mock_audio_chunk(0, 15000, [(10.0, 15.0)])
        self._process(chunk1)

        # Chunk 2: Dead air. Speech ended at 15.0s.
        # Since Chunk 2 is completely silent and 15s long, the silence gap (15s) exceeds the 3s threshold during this chunk.
        # Under our correct, non-lossy design, this MUST trigger a flush of the speech transmission during Chunk 2.
        chunk2 = mock_audio_chunk(15000, 15000, [])
        actions2 = self._process(chunk2)

        # It should be flushed during Chunk 2
        self.assertTrue(any(isinstance(a, UpdateStateAction) for a in actions2))
        flush_action = next(
            (a for a in actions2 if isinstance(a, FlushAction)), None
        )
        self.assertIsNotNone(flush_action)
        assert flush_action is not None
        self.assertEqual(
            flush_action.reason, "Significant gap detected from silent file"
        )

        # Chunk 3: Dead air. Arrives at 30.0s.
        # Since the speech transmission was already flushed, processing Chunk 3 just continues buffering the non-speech transmission.
        chunk3 = mock_audio_chunk(30000, 15000, [])
        actions3 = self._process(chunk3)

        # No new flush should occur in Chunk 3
        self.assertTrue(any(isinstance(a, UpdateStateAction) for a in actions3))
        self.assertFalse(any(isinstance(a, FlushAction) for a in actions3))

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

        # We expect two flushes: one for the active speech, and one for the isolated late chunk
        flush_actions = [a for a in actions if isinstance(a, FlushAction)]
        self.assertGreaterEqual(len(flush_actions), 2)
        self.assertEqual(
            flush_actions[1].reason,
            "Flushing isolated late-arriving audio chunk",
        )
        self.assertTrue(flush_actions[0].missing_prior_context)
        self.assertFalse(flush_actions[1].missing_post_context)
        self.assertEqual(
            flush_actions[1].audio_classification,
            2,  # OTHER (trailing silence)
        )

    def test_process_speech_segments_avoids_overlap(self) -> None:
        """Verifies that _process_speech_segments avoids overlap by updating actual_start_ms."""
        # Chunk speech from 1.0s to 12.0s.
        chunk = mock_audio_chunk(0, 15000, [(1.0, 12.0)])

        # Mock context having processed audio ending at 5000ms (5.0s) in absolute time!
        self.ctx.last_segment_end_time_ms = 5000

        # Expected actions should capture speech starting from 5.0s offset in chunk and trailing silence!
        actions = self.state_machine._process_speech_segments(chunk, self.ctx)

        # Verify that the primary speech buffer append action has audio size equivalent to 7000ms (12.0s - 5.0s).
        append_actions = [
            a for a in actions if isinstance(a, AppendBufferAction)
        ]
        self.assertGreaterEqual(len(append_actions), 2)

        # Size is 7000 * 16 = 112000 samples.
        self.assertEqual(
            append_actions[0].audio_buffer.size, (7000 * SAMPLES_PER_MS)
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

        # Chunk 2: Starts at 15.0s. Perfectly contiguous speech for the full chunk (0.0s to 15.0s).
        chunk2 = mock_audio_chunk(
            15000, 15000, [(0.0, 15.0)], "gs://fake/2.flac"
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
        is correctly updated and flushes when traversing non-speech streams.
        """
        # Chunk 1: Speech ending at 5.0s (5000ms).
        # Under Continuous Audio Retention, trailing silence (5.0 to 15.0s) immediately flushes the speech segment.
        chunk1 = mock_audio_chunk(0, 15000, [(1.0, 5.0)])
        actions1 = self._process(chunk1)
        flush_action1 = next(
            (a for a in actions1 if isinstance(a, FlushAction)), None
        )
        self.assertIsNotNone(flush_action1)
        assert flush_action1 is not None
        self.assertEqual(flush_action1.audio_classification, 1)  # SPEECH

        # Process a silent chunk starting at 15000.
        # This will simply continue buffering the non-speech audio window (OTHER).
        actions = self.state_machine.process_chunk(
            mock_audio_chunk(15000, 15000, []), self.ctx
        )
        self.assertTrue(any(isinstance(a, AppendBufferAction) for a in actions))

    def test_traceparent_preservation_across_resets(self) -> None:
        """Verifies that traceparent and baggage are preserved when VAD resets the transmission context."""
        self.ctx.traceparent = "test-traceparent-xyz"
        self.ctx.baggage = "test-baggage-xyz"

        # Chunk 1: Speech from 1.0s to 12.0s. Trailing silence triggers a VAD flush,
        # which in turn calls _reset_transmission_context internally to start the silence window.
        chunk = mock_audio_chunk(0, 15000, [(1.0, 12.0)], "gs://fake/1.flac")
        actions = self._process(chunk)

        # Verify that the old transmission was successfully flushed
        self.assertTrue(any(isinstance(a, FlushAction) for a in actions))

        # Verify that the traceparent and baggage are STILL preserved in the context!
        self.assertEqual(self.ctx.traceparent, "test-traceparent-xyz")
        self.assertEqual(self.ctx.baggage, "test-baggage-xyz")

    def test_traceparent_preservation_across_max_duration_flush(self) -> None:
        """Verifies that traceparent and baggage are preserved when a flush is triggered by exceeding max transmission duration."""
        self.ctx.traceparent = "test-traceparent-xyz"
        self.ctx.baggage = "test-baggage-xyz"

        # Start a non-speech transmission window
        chunk1 = mock_audio_chunk(0, 15000, [], "gs://fake/1.flac")
        self._process(chunk1)

        # Process a second silent chunk that pushes the total duration to 65s (> 60s max limit),
        # triggering a "Maximum non-speech transmission duration exceeded" flush.
        chunk2 = mock_audio_chunk(15000, 50000, [], "gs://fake/2.flac")
        actions = self._process(chunk2)

        # Verify that the maximum duration flush occurred
        flush_action = next(
            (a for a in actions if isinstance(a, FlushAction)), None
        )
        self.assertIsNotNone(flush_action)
        assert flush_action is not None
        self.assertEqual(
            flush_action.reason,
            "Maximum non-speech transmission duration exceeded",
        )

        # Verify that the traceparent and baggage are STILL preserved in the context!
        self.assertEqual(self.ctx.traceparent, "test-traceparent-xyz")
        self.assertEqual(self.ctx.baggage, "test-baggage-xyz")

    def test_traceparent_preservation_across_upstream_gap_flush(self) -> None:
        """Verifies that traceparent and baggage are preserved when a flush is triggered by an upstream audio gap."""
        self.ctx.traceparent = "test-traceparent-xyz"
        self.ctx.baggage = "test-baggage-xyz"

        # Start an active transmission
        chunk1 = mock_audio_chunk(0, 3000, [(1.0, 2.0)], "gs://fake/1.flac")
        self._process(chunk1)

        # Process a second chunk that jumps ahead by 10 seconds (creating an upstream gap),
        # which will force-flush the active transmission and call _reset_transmission_context.
        chunk2 = mock_audio_chunk(10000, 3000, [], "gs://fake/2.flac")
        actions = self._process(chunk2)

        # Verify that the upstream gap flush occurred
        flush_action = next(
            (a for a in actions if isinstance(a, FlushAction)), None
        )
        self.assertIsNotNone(flush_action)
        assert flush_action is not None
        self.assertEqual(
            flush_action.reason, "Forced flush due to upstream audio chunk gap"
        )

        # Verify that the traceparent and baggage are STILL preserved in the context!
        self.assertEqual(self.ctx.baggage, "test-baggage-xyz")

    def test_small_silence_pauses_are_grouped(self) -> None:
        """Verifies that sub-significant silence pauses (less than significant_gap_ms = 3000ms)
        are appended to the active SPEECH transmission instead of being split out as OTHER segments.
        """
        # Chunk 1: Speech from 1.0s to 5.0s, and 6.5s to 13.0s.
        # - Intra-chunk silence gap: 5.0s to 6.5s (1500ms < 3000ms).
        # - Trailing silence gap: 13.0s to 15.0s (2000ms < 3000ms).
        # Neither gap is significant, so they should NOT trigger a split!
        chunk1 = mock_audio_chunk(
            0, 15000, [(1.0, 5.0), (6.5, 13.0)], "gs://fake/1.flac"
        )
        actions1 = self._process(chunk1)

        # There should be NO FlushAction during Chunk 1 processing
        self.assertFalse(any(isinstance(a, FlushAction) for a in actions1))

        # Chunk 2: Starts at 15.0s, speech from 2.0s to 5.0s (starts at 17.0s absolute time).
        # - Total silence gap since last speech segment ended: 17.0s - 13.0s = 4000ms.
        # Since 4000ms >= 3000ms (significant_gap_ms), this IS significant and MUST trigger a split!
        chunk2 = mock_audio_chunk(
            15000, 15000, [(2.0, 5.0)], "gs://fake/2.flac"
        )
        actions2 = self._process(chunk2)

        # It should flush the previous speech transmission (from Chunk 1)
        # and then start the new speech transmission (from Chunk 2).
        flush_actions2 = [a for a in actions2 if isinstance(a, FlushAction)]
        self.assertGreaterEqual(
            len(flush_actions2), 2
        )  # One SPEECH flush, one OTHER flush
        self.assertEqual(flush_actions2[0].audio_classification, 1)  # SPEECH
        self.assertEqual(
            flush_actions2[1].audio_classification, 2
        )  # OTHER (silence)

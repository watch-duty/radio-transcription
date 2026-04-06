"""A framework-agnostic state machine isolating sequential audio transmission boundary logic."""

import logging

from backend.pipeline.common.constants import (
    CHUNK_DURATION_SECONDS,
    MS_PER_SECOND,
)

logger = logging.getLogger(__name__)
from backend.pipeline.transcription.datatypes import (
    AppendBufferAction,
    AppendIsolatedBufferAction,
    AudioChunkData,
    DropAction,
    FlushAction,
    PaddedSegment,
    ScheduleStaleTimerAction,
    StateMachineAction,
    StitchAudioConfig,
    StitcherContext,
    TimeRange,
    UpdateStateAction,
    VadResult,
)


class AudioStitchingStateMachine:
    """A framework-agnostic state machine responsible for the core logic of stitching audio chunks together.

    It evaluates incoming audio segments, tracks continuous speech, and decides when to flush
    transmissions based on significant gaps or maximum duration limits.

    It returns a list of `StateMachineAction`s that describe the subsequent state updates or
    flushes that should occur, completely decoupled from the Apache Beam pipeline runtime.
    """

    def __init__(self, config: StitchAudioConfig) -> None:
        """Binds the pipeline configuration limits for gap detection and max durations."""
        self.config = config

    def process_chunk(
        self, chunk_data: AudioChunkData, ctx: StitcherContext, padded_segments: list[PaddedSegment], vad_result: VadResult
    ) -> list[StateMachineAction]:
        """Evaluates an incoming chunk against the state machine to produce imperative actions."""
        # 0. Detect if this is an out-of-order LATE chunk
        is_late_chunk = (
            ctx.expected_next_chunk_start_ms is not None
            and chunk_data.start_ms < ctx.expected_next_chunk_start_ms
        )

        if is_late_chunk:
            return self._process_late_chunk_independently(chunk_data, ctx, padded_segments, vad_result)

        chunk_duration_ms = int(CHUNK_DURATION_SECONDS * MS_PER_SECOND)
        actions: list[StateMachineAction] = []

        # 2. Proceed with normal evaluation
        if not vad_result.speech_segments:
            new_actions = self._process_silent_chunk(chunk_data, ctx, vad_result)
        else:
            new_actions = self._process_speech_segments(chunk_data, ctx, padded_segments, vad_result)

        actions.extend(new_actions)

        # 3. Always update the expected contiguous start time for the NEXT chunk
        ctx.expected_next_chunk_start_ms = (
            chunk_data.start_ms + chunk_duration_ms
        )
        actions.append(UpdateStateAction())
        return actions

    def _flush_current_transmission(
        self,
        reason: str,
        ctx: StitcherContext,
        *,
        missing_post_context: bool = False,
    ) -> FlushAction:
        """Concludes the active transmission by calculating duration and yields a FlushRequest."""
        if ctx.transmission_start_time_ms is None:
            msg = "Cannot flush empty current transmission"
            raise ValueError(msg)

        # When flushing due to dropped chunks, we might not have a last_segment_end_time_ms yet
        # if the transmission was very short, so fallback to transmission_start_time_ms.
        end_ms = ctx.last_segment_end_time_ms or ctx.transmission_start_time_ms
        if (
            end_ms is None
            or ctx.transmission_start_time_ms is None
            or ctx.buffer_start_time_ms is None
        ):
            msg = "Missing boundary times for buffer flush."
            raise ValueError(msg)

        padded_end_time_ms = end_ms
        if ctx.buffer_duration_ms > 0 and ctx.buffer_start_time_ms is not None:
            padded_end_time_ms = (
                ctx.buffer_start_time_ms + ctx.buffer_duration_ms
            )

        return FlushAction(
            reason=reason,
            feed_id=ctx.feed_id,
            contributing_audio_uris=ctx.contributing_audio_uris.copy(),
            time_range=TimeRange(
                start_ms=ctx.buffer_start_time_ms,
                end_ms=padded_end_time_ms,
            ),
            speech_time_range=TimeRange(
                start_ms=ctx.transmission_start_time_ms,
                end_ms=end_ms,
            ),
            missing_prior_context=ctx.missing_prior_context,
            missing_post_context=missing_post_context,
            start_audio_offset_ms=ctx.start_audio_offset_ms,
            end_audio_offset_ms=None,
        )

    def _process_late_chunk_independently(
        self, chunk_data: AudioChunkData, ctx: StitcherContext, padded_segments: list[PaddedSegment], vad_result: VadResult
    ) -> list[StateMachineAction]:
        """Flushes a late-arriving chunk immediately as an independent short transmission."""
        # Create a detached context to prevent state corruption of the leading edge

        temp_ctx = StitcherContext(
            feed_id=ctx.feed_id,
            current_gcs_uri=ctx.current_gcs_uri,
            contributing_audio_uris=[],
            file_start_ms=chunk_data.start_ms,
            missing_prior_context=True,
        )

        raw_actions: list[StateMachineAction] = []
        if not vad_result.speech_segments:
            raw_actions.extend(self._process_silent_chunk(chunk_data, temp_ctx, vad_result))
        else:
            raw_actions.extend(
                self._process_speech_segments(chunk_data, temp_ctx, padded_segments, vad_result)
            )

        # Force flush whatever remaining audio was appended via actions
        if temp_ctx.transmission_start_time_ms is not None:
            # We must determine if the trailing audio was chopped by the late chunk's boundary.
            last_segment = (
                vad_result.speech_segments[-1]
                if vad_result.speech_segments
                else None
            )
            is_chopped_at_end = (
                last_segment is not None
                and last_segment.end_ms >= chunk_data.duration_ms
            )
            raw_actions.append(
                self._flush_current_transmission(
                    "Flushing isolated late-arriving audio chunk",
                    temp_ctx,
                    missing_post_context=is_chopped_at_end,
                )
            )

        filtered_actions: list[StateMachineAction] = []
        for action in raw_actions:
            if isinstance(action, AppendBufferAction):
                filtered_actions.append(
                    AppendIsolatedBufferAction(
                        start_offset_ms=action.start_offset_ms,
                        end_offset_ms=action.end_offset_ms,
                    )
                )
            else:
                filtered_actions.append(action)

        return filtered_actions

    def _reset_transmission_context(self, ctx: StitcherContext) -> None:
        """Resets the ongoing state metrics (timers, timestamps) to begin a fresh transmission window."""
        ctx.transmission_start_time_ms = None
        ctx.buffer_start_time_ms = None
        ctx.contributing_audio_uris.clear()
        ctx.start_audio_offset_ms = None
        ctx.buffer_duration_ms = 0

    def _process_silent_chunk(
        self, chunk_data: AudioChunkData, ctx: StitcherContext, vad_result: VadResult
    ) -> list[StateMachineAction]:
        """Handles VAD silence events, updating trailing statistics without extending speech logic."""
        actions: list[StateMachineAction] = []
        file_start_ms = chunk_data.start_ms

        # A "significant gap" occurs if the start time of the incoming chunk
        # is significantly later than the end time of the last chunk we processed.
        # This usually means the radio transmission naturally ended and a new one is beginning.
        is_significant_gap = (
            ctx.last_segment_end_time_ms is not None
            and ((file_start_ms) - ctx.last_segment_end_time_ms)
            >= self.config.significant_gap_ms
        )
        # We enforce a maximum transmission duration to prevent unbounded buffering,
        # which could lead to OOMs or simply holding a transcription indefinitely
        # if a scanner is stuck open.
        is_max_duration_exceeded = (
            ctx.transmission_start_time_ms is not None
            and (
                (file_start_ms + chunk_data.duration_ms)
                - ctx.transmission_start_time_ms
            )
            >= self.config.max_transmission_duration_ms
        )

        if is_significant_gap or is_max_duration_exceeded:
            if is_significant_gap:
                reason = "Significant gap detected from silent file"
            else:
                reason = "Maximum transmission duration exceeded by silent file"

            if ctx.transmission_start_time_ms is not None:
                if ctx.last_segment_end_time_ms is None:
                    msg = "Unreachable: active transmission without segment anchor"
                    raise RuntimeError(msg)

                target_post_roll_end = (
                    ctx.last_segment_end_time_ms + self.config.vad_post_roll_ms
                ) - file_start_ms

                # Option 2: Find the silence region that starts at this segment's end!
                matching_silence = [
                    s
                    for s in vad_result.silence_segments
                    if s.start_ms == ctx.last_segment_end_time_ms
                ]
                if matching_silence:
                    silence_limit = matching_silence[0].end_ms - file_start_ms
                    append_end = min(silence_limit, target_post_roll_end)
                else:
                    # If no silence region borders the speech, we fallback to no post-roll!
                    append_end = 0

                if append_end > 0:
                    start_idx = 0
                    end_idx = append_end * 16
                    raw_slice = chunk_data.audio[start_idx:end_idx]
                    actions.append(
                        AppendBufferAction(
                            raw_audio=raw_slice, denoised_audio=raw_slice,
                            start_offset_ms=0, end_offset_ms=append_end
                        )
                    )
                    ctx.buffer_duration_ms += append_end

                actions.append(
                    self._flush_current_transmission(
                        reason,
                        ctx,
                        missing_post_context=is_max_duration_exceeded,
                    )
                )

                # Since we successfully flushed a clean transmission, reset the context flag for the next one.
                ctx.missing_prior_context = False

            # Reset connection state for the next transmission
            self._reset_transmission_context(ctx)

            actions.append(UpdateStateAction())
            if is_significant_gap or is_max_duration_exceeded:
                actions.append(ScheduleStaleTimerAction(deadline_ms=0))
            return actions

        logger.info(
            "  _process_silent_chunk: transmission_start_time_ms=%s",
            ctx.transmission_start_time_ms,
        )
        if ctx.transmission_start_time_ms is None:
            # We aren't currently tracking an active transmission, and this chunk
            # has no speech in it. It's totally useless, so we discard it.
            logger.info(
                "  _process_silent_chunk: No active transmission, dropping."
            )
            actions.append(DropAction(reason="Discarding silent file"))
            actions.append(ScheduleStaleTimerAction(deadline_ms=0))
            return actions

        # The chunk didn't end the transmission, but it was just internal silence.
        # We record that we saw it, update state, and bump the stale timer so it
        # doesn't time out while waiting for the user to speak again.
        if ctx.current_gcs_uri not in ctx.contributing_audio_uris:
            ctx.contributing_audio_uris.append(ctx.current_gcs_uri)
        actions.append(UpdateStateAction())
        expected_stale_deadline_ms = (
            ctx.last_segment_end_time_ms
            or (chunk_data.start_ms + chunk_data.duration_ms)
        ) + self.config.stale_timeout_ms
        actions.append(
            ScheduleStaleTimerAction(deadline_ms=expected_stale_deadline_ms)
        )

        # IMPORTANT: We must append the silent audio to preserve post-roll tails!
        if (
            ctx.transmission_start_time_ms is not None
            and ctx.last_segment_end_time_ms is not None
        ):
            target_post_roll_end = (
                ctx.last_segment_end_time_ms + self.config.vad_post_roll_ms
            )

            # Option 2: Find the silence region that starts at this segment's end!
            matching_silence = [
                s
                for s in vad_result.silence_segments
                if s.start_ms == ctx.last_segment_end_time_ms
            ]
            if matching_silence:
                silence_limit = matching_silence[0].end_ms - file_start_ms
                append_end = min(
                    silence_limit, max(0, target_post_roll_end - file_start_ms)
                )
            else:
                # If no silence region borders the speech, we fallback to no post-roll!
                append_end = 0
            if append_end > 0:
                start_idx = 0
                end_idx = append_end * 16
                raw_slice = chunk_data.audio[start_idx:end_idx]
                actions.append(
                    AppendBufferAction(
                        raw_audio=raw_slice, denoised_audio=raw_slice,
                        start_offset_ms=0, end_offset_ms=append_end
                    )
                )
                ctx.buffer_duration_ms += append_end

        return actions

    def _process_speech_segments(
        self, chunk_data: AudioChunkData, ctx: StitcherContext, padded_segments: list[PaddedSegment], vad_result: VadResult
    ) -> list[StateMachineAction]:
        """Evaluates consecutive speech data, updating length counters and triggering mid-stream flushes if gap or duration limits are reached."""
        actions: list[StateMachineAction] = []
        for segment in padded_segments:
            # 1. Check if the gap between the last speech segment and this new one
            # is significant enough to warrant splitting into a new transmission.
            is_significant_gap = (
                ctx.last_segment_end_time_ms is not None
                and (segment.speech_start_ms - ctx.last_segment_end_time_ms)
                >= self.config.significant_gap_ms
            )

            # 2. Check if this segment would exceed the maximum allowed duration of a transmission.
            is_max_duration_exceeded = (
                ctx.transmission_start_time_ms is not None
                and (segment.speech_start_ms - ctx.transmission_start_time_ms)
                >= self.config.max_transmission_duration_ms
            )

            # 3. If there is a gap OR max duration is exceeded, flush whatever is in the buffer currently.
            if is_significant_gap or is_max_duration_exceeded:
                if is_max_duration_exceeded:
                    reason = "Maximum transmission duration exceeded"
                else:
                    reason = "Significant gap detected"

                if ctx.transmission_start_time_ms is not None:
                    # 3.1. Add post-roll to the buffer if available in this chunk
                    if ctx.last_segment_end_time_ms is not None:
                        start_offset = max(
                            0,
                            ctx.last_segment_end_time_ms - chunk_data.start_ms,
                        )
                        # Target post-roll end
                        target_end_ms = (
                            ctx.last_segment_end_time_ms
                            + self.config.vad_post_roll_ms
                        )

                        # Find matching silence region to limit padding
                        matching_silence = [
                            s
                            for s in vad_result.silence_segments
                            if s.start_ms == ctx.last_segment_end_time_ms
                        ]
                        if matching_silence:
                            target_end_ms = min(
                                target_end_ms, matching_silence[0].end_ms
                            )

                        end_offset = min(
                            chunk_data.duration_ms,
                            target_end_ms - chunk_data.start_ms,
                        )

                        if end_offset > start_offset:
                            start_idx = start_offset * 16
                            end_idx = end_offset * 16
                            raw_slice = chunk_data.audio[start_idx:end_idx]
                            actions.append(
                                AppendBufferAction(
                                    raw_audio=raw_slice, denoised_audio=raw_slice,
                                    start_offset_ms=start_offset, end_offset_ms=end_offset
                                )
                            )
                            ctx.buffer_duration_ms += end_offset - start_offset

                        actions.append(
                            self._flush_current_transmission(
                                reason,
                                ctx,
                                missing_post_context=is_max_duration_exceeded,
                            )
                        )
                    self._reset_transmission_context(ctx)
                    ctx.missing_prior_context = is_max_duration_exceeded

            # 4. Append continuous audio from chunk to the state
            if ctx.transmission_start_time_ms is None:
                ctx.transmission_start_time_ms = segment.speech_start_ms
                ctx.buffer_start_time_ms = segment.start_ms
                ctx.start_audio_offset_ms = (
                    segment.start_ms - chunk_data.start_ms
                )

                # For the very first segment, we append from its padded start
                # to include pre-roll naturally!
                start_offset = max(0, segment.start_ms - chunk_data.start_ms)
            else:
                # For subsequent segments, we normally append from the end of the previous segment
                # to preserve small gaps naturally.
                if ctx.last_segment_end_time_ms is None:
                    msg = (
                        "last_segment_end_time_ms is None in subsequent segment"
                    )
                    raise RuntimeError(msg)
            # Calculate where to start in the segment to avoid overlap
            actual_start_ms = segment.start_ms
            if ctx.last_segment_end_time_ms is not None:
                actual_start_ms = max(actual_start_ms, ctx.last_segment_end_time_ms)
                
            # Only fill the gap if it is fully covered by verified silence!
            gap_covered_by_silence = False
            for silence in vad_result.silence_segments:
                if (
                    silence.start_ms <= ctx.last_segment_end_time_ms
                    and silence.end_ms >= segment.speech_start_ms
                ):
                    gap_covered_by_silence = True
                    break
                    
            if not gap_covered_by_silence:
                # Skip appending the gap, only append the current speech segment!
                actual_start_ms = max(actual_start_ms, segment.speech_start_ms)
                
            seg_start_idx = max(0, (actual_start_ms - segment.start_ms) * 16)
            seg_end_idx = len(segment.raw_audio) # Take everything that was padded
            
            if seg_end_idx > seg_start_idx:
                start_offset_ms = actual_start_ms - chunk_data.start_ms
                end_offset_ms = start_offset_ms + (seg_end_idx - seg_start_idx) // 16
                actions.append(
                    AppendBufferAction(
                        raw_audio=segment.raw_audio[seg_start_idx:seg_end_idx],
                        denoised_audio=segment.denoised_audio[seg_start_idx:seg_end_idx],
                        start_offset_ms=start_offset_ms,
                        end_offset_ms=end_offset_ms
                    )
                )
                ctx.buffer_duration_ms += (seg_end_idx - seg_start_idx) // 16

            ctx.last_segment_end_time_ms = segment.speech_end_ms

            if ctx.current_gcs_uri not in ctx.contributing_audio_uris:
                ctx.contributing_audio_uris.append(ctx.current_gcs_uri)

        # Preserve trailing silence up to vad_post_roll_ms at the end of the chunk!
        if ctx.last_segment_end_time_ms is not None:
            needed_post_roll_end = (
                ctx.last_segment_end_time_ms + self.config.vad_post_roll_ms
            )

            silence_end = None
            for silence in vad_result.silence_segments:
                if (
                    silence.start_ms <= ctx.last_segment_end_time_ms
                    and silence.end_ms > ctx.last_segment_end_time_ms
                ):
                    silence_end = silence.end_ms
                    break

            if silence_end is not None:
                needed_post_roll_end = min(needed_post_roll_end, silence_end)
            else:
                # If the end of speech is not in silence, we cannot pad into silence!
                # We cap it at the end of speech itself.
                needed_post_roll_end = ctx.last_segment_end_time_ms

            if chunk_data.start_ms < needed_post_roll_end:
                start_offset = max(
                    0, ctx.last_segment_end_time_ms - chunk_data.start_ms
                )
                end_offset = min(
                    chunk_data.duration_ms,
                    needed_post_roll_end - chunk_data.start_ms,
                )

                if end_offset > start_offset:
                    start_idx = start_offset * 16
                    end_idx = end_offset * 16
                    raw_slice = chunk_data.audio[start_idx:end_idx]
                    actions.append(
                        AppendBufferAction(
                            raw_audio=raw_slice, denoised_audio=raw_slice,
                            start_offset_ms=start_offset, end_offset_ms=end_offset
                        )
                    )
                    ctx.buffer_duration_ms += end_offset - start_offset

        # Always record that we successfully processed this entire chunk
        actions.append(UpdateStateAction())
        if ctx.last_segment_end_time_ms is not None:
            expected_stale_deadline_ms = (
                ctx.last_segment_end_time_ms + self.config.stale_timeout_ms
            )
        else:
            expected_stale_deadline_ms = 0

        # Register the stale timer to ensure Dataflow doesn't hold this buffer forever
        actions.append(
            ScheduleStaleTimerAction(deadline_ms=expected_stale_deadline_ms)
        )
        return actions

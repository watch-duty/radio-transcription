from backend.pipeline.shared_constants import CHUNK_DURATION_SECONDS
from backend.pipeline.transcription.datatypes import (
    AudioChunkData,
    DropAction,
    FlushAction,
    FlushRequest,
    ScheduleStaleTimerAction,
    StateMachineAction,
    StitchAudioConfig,
    StitcherContext,
    TimeRange,
    UpdateStateAction,
)


class AudioStitchingStateMachine:
    """
    A pure Python state machine responsible for the core logic of stitching audio chunks together.
    It evaluates incoming audio segments, tracks continuous speech, and decides when to flush
    transmissions based on significant gaps or maximum duration limits.

    It returns a list of `StateMachineAction`s that describe the subsequent state updates or
    flushes that should occur, completely decoupled from the Apache Beam pipeline runtime.
    """

    def __init__(self, config: StitchAudioConfig) -> None:
        self.config = config

    def process_chunk(
        self, chunk_data: AudioChunkData, ctx: StitcherContext
    ) -> list[StateMachineAction]:
        # 0. Detect if this is an out-of-order LATE chunk
        is_late_chunk = (
            ctx.expected_next_chunk_start_ms is not None
            and chunk_data.start_ms < ctx.expected_next_chunk_start_ms
        )

        if is_late_chunk:
            return self._process_late_chunk_independently(chunk_data, ctx)

        chunk_duration_ms = int(CHUNK_DURATION_SECONDS * 1000)
        actions: list[StateMachineAction] = []

        # 1. Detect if we skipped over a chunk (dropped audio)
        is_dropped_chunk = (
            ctx.expected_next_chunk_start_ms is not None
            and chunk_data.start_ms > ctx.expected_next_chunk_start_ms
        )

        if is_dropped_chunk:
            # If we had an active transmission, we must flush it because the auditory context was violently severed.
            if ctx.current_buffer and ctx.transmission_start_time_ms is not None:
                actions.append(
                    self._flush_current_transmission(
                        reason="Forced flush due to dropped audio chunk gap",
                        ctx=ctx,
                        missing_post_context=True,
                    )
                )
                self._reset_transmission_context(ctx)

            # The next audio we process is jumping in after missing audio, so it lacks prior context.
            ctx.missing_prior_context = True

        # 2. Proceed with normal evaluation
        if not chunk_data.speech_segments:
            new_actions = self._process_silent_chunk(chunk_data, ctx)
        else:
            new_actions = self._process_speech_segments(chunk_data, ctx)

        actions.extend(new_actions)

        # 3. Always update the expected contiguous start time for the NEXT chunk
        ctx.expected_next_chunk_start_ms = chunk_data.start_ms + chunk_duration_ms
        actions.append(UpdateStateAction())
        return actions

    def _flush_current_transmission(
        self, reason: str, ctx: StitcherContext, *, missing_post_context: bool = False
    ) -> FlushAction:
        if not ctx.current_buffer:
            msg = "Cannot flush empty current buffer"
            raise ValueError(msg)

        # When flushing due to dropped chunks, we might not have a last_segment_end_time_ms yet
        # if the transmission was very short, so fallback to transmission_start_time_ms.
        end_ms = ctx.last_segment_end_time_ms or ctx.transmission_start_time_ms
        if end_ms is None or ctx.transmission_start_time_ms is None:
            msg = "Missing boundary times for buffer flush."
            raise ValueError(msg)

        return FlushAction(
            reason=reason,
            flush_request=FlushRequest(
                buffer=ctx.current_buffer,
                feed_id=ctx.feed_id,
                contributing_audio_uris=ctx.contributing_audio_uris.copy(),
                time_range=TimeRange(
                    start_ms=ctx.transmission_start_time_ms,
                    end_ms=end_ms,
                ),
                missing_prior_context=ctx.missing_prior_context,
                missing_post_context=missing_post_context,
                start_audio_offset_ms=ctx.start_audio_offset_ms,
                end_audio_offset_ms=ctx.end_audio_offset_ms,
            ),
        )

    def _process_late_chunk_independently(
        self, chunk_data: AudioChunkData, ctx: StitcherContext
    ) -> list[StateMachineAction]:
        # Create a detached context to prevent state corruption of the leading edge
        temp_ctx = StitcherContext(
            feed_id=ctx.feed_id,
            current_gcs_uri=ctx.current_gcs_uri,
            current_buffer=None,
            contributing_audio_uris=[],
            file_start_ms=chunk_data.start_ms,
            missing_prior_context=True,
        )

        raw_actions: list[StateMachineAction] = []
        if not chunk_data.speech_segments:
            raw_actions.extend(self._process_silent_chunk(chunk_data, temp_ctx))
        else:
            raw_actions.extend(self._process_speech_segments(chunk_data, temp_ctx))

        # Force flush whatever remaining audio is stranded in the temporary buffer
        if temp_ctx.current_buffer and temp_ctx.transmission_start_time_ms is not None:
            # We must determine if the trailing audio was chopped by the late chunk's boundary.
            last_segment = (
                chunk_data.speech_segments[-1] if chunk_data.speech_segments else None
            )
            is_chopped_at_end = last_segment is not None and last_segment.end_ms >= int(
                CHUNK_DURATION_SECONDS * 1000
            )
            raw_actions.append(
                self._flush_current_transmission(
                    reason="Isolated forced flush of late chunk tail",
                    ctx=temp_ctx,
                    missing_post_context=is_chopped_at_end,
                )
            )

        filtered_actions: list[StateMachineAction] = []
        for action in raw_actions:
            match action:
                case FlushAction():
                    filtered_actions.append(
                        FlushAction(
                            reason=action.reason,
                            flush_request=action.flush_request,
                            clear_state=False,
                        )
                    )
                case DropAction():
                    filtered_actions.append(action)

        return filtered_actions

    def _reset_transmission_context(self, ctx: StitcherContext) -> None:
        ctx.current_buffer = None
        ctx.transmission_start_time_ms = None
        ctx.contributing_audio_uris.clear()
        ctx.start_audio_offset_ms = None
        ctx.end_audio_offset_ms = None

    def _process_silent_chunk(
        self, chunk_data: AudioChunkData, ctx: StitcherContext
    ) -> list[StateMachineAction]:
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
                (file_start_ms + len(chunk_data.audio)) - ctx.transmission_start_time_ms
            )
            >= self.config.max_transmission_duration_ms
        )

        if is_significant_gap or is_max_duration_exceeded:
            if is_significant_gap:
                reason = "Significant gap detected from silent file"
            else:
                reason = "Maximum transmission duration exceeded by silent file"

            if ctx.current_buffer and ctx.transmission_start_time_ms is not None:
                actions.append(
                    self._flush_current_transmission(
                        reason, ctx, missing_post_context=is_max_duration_exceeded
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

        if not ctx.current_buffer:
            # We aren't currently tracking an active transmission, and this chunk
            # has no speech in it. It's totally useless, so we discard it.
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
            or (chunk_data.start_ms + len(chunk_data.audio))
        ) + self.config.stale_timeout_ms
        actions.append(ScheduleStaleTimerAction(deadline_ms=expected_stale_deadline_ms))
        return actions

    def _process_speech_segments(
        self, chunk_data: AudioChunkData, ctx: StitcherContext
    ) -> list[StateMachineAction]:
        actions: list[StateMachineAction] = []
        file_start_ms = chunk_data.start_ms

        for segment in chunk_data.speech_segments:
            global_start_ms = segment.start_ms
            global_end_ms = segment.end_ms

            # 1. Check if the gap between the last speech segment and this new one
            # is significant enough to warrant splitting into a new transmission.
            is_significant_gap = (
                ctx.last_segment_end_time_ms is not None
                and ((file_start_ms + global_start_ms) - ctx.last_segment_end_time_ms)
                >= self.config.significant_gap_ms
            )

            # 2. Check if this segment would exceed the maximum allowed duration of a transmission.
            is_max_duration_exceeded = (
                ctx.transmission_start_time_ms is not None
                and ((file_start_ms + global_start_ms) - ctx.transmission_start_time_ms)
                >= self.config.max_transmission_duration_ms
            )

            # 3. If there is a gap OR max duration is exceeded, flush whatever is in the buffer currently.
            if is_significant_gap or is_max_duration_exceeded:
                if is_significant_gap:
                    reason = "Significant gap detected"
                else:
                    reason = "Maximum transmission duration exceeded"

                # Flush the currently buffered transmission before starting this next segment
                if ctx.current_buffer and ctx.transmission_start_time_ms is not None:
                    actions.append(
                        self._flush_current_transmission(
                            reason, ctx, missing_post_context=is_max_duration_exceeded
                        )
                    )

                    # If this was cut arbitrarily by max_duration mid-stream, the next segment inherits a severed head flag!
                    ctx.missing_prior_context = is_max_duration_exceeded

                # Reset state to cleanly begin accumulating the next transmission
                self._reset_transmission_context(ctx)

            # 4. Append this specific speech segment to the state
            # Slice the valid speech audio out of the chunk using the bounds found by VAD
            speech_audio = chunk_data.audio[global_start_ms:global_end_ms]

            if not ctx.current_buffer:
                # If the VAD segment starts beyond 0.0, the speaker starts after the file begins.
                # The start of their sentence is perfectly robust and untruncated.
                if ctx.missing_prior_context and global_start_ms > 0:
                    ctx.missing_prior_context = False

                # Initialize a new transmission buffer
                ctx.current_buffer = speech_audio
                ctx.transmission_start_time_ms = file_start_ms + global_start_ms
                ctx.start_audio_offset_ms = global_start_ms
            else:
                # Append to the ongoing transmission
                ctx.current_buffer += speech_audio

            ctx.end_audio_offset_ms = global_end_ms
            if ctx.current_gcs_uri not in ctx.contributing_audio_uris:
                ctx.contributing_audio_uris.append(ctx.current_gcs_uri)
            ctx.last_segment_end_time_ms = file_start_ms + global_end_ms

        # Always record that we successfully processed this entire chunk
        actions.append(UpdateStateAction())
        if ctx.last_segment_end_time_ms is not None:
            expected_stale_deadline_ms = (
                ctx.last_segment_end_time_ms + self.config.stale_timeout_ms
            )
        else:
            expected_stale_deadline_ms = 0

        # Register the stale timer to ensure Dataflow doesn't hold this buffer forever
        actions.append(ScheduleStaleTimerAction(deadline_ms=expected_stale_deadline_ms))
        return actions

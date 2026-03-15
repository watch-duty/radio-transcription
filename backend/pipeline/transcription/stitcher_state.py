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
        self, reason: str, ctx: StitcherContext
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
                processed_uuids=set(ctx.processed_uuids),
                time_range=TimeRange(
                    start_ms=ctx.transmission_start_time_ms,
                    end_ms=end_ms,
                ),
                missing_prior_context=ctx.missing_prior_context,
                start_chunk_id=ctx.start_chunk_id,
                end_chunk_id=ctx.source_file_uuid,
            ),
        )

    def _reset_transmission_context(self, ctx: StitcherContext) -> None:
        ctx.current_buffer = None
        ctx.processed_uuids = set()
        ctx.transmission_start_time_ms = None
        ctx.last_segment_end_time_ms = None
        ctx.start_chunk_id = None

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
                actions.append(self._flush_current_transmission(reason, ctx))

                # Since we successfully flushed a clean transmission, reset the context flag for the next one.
                ctx.missing_prior_context = False

            # Reset connection state for the next transmission
            self._reset_transmission_context(ctx)

            ctx.processed_uuids.add(ctx.source_file_uuid)
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
        ctx.processed_uuids.add(ctx.source_file_uuid)
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
                    actions.append(self._flush_current_transmission(reason, ctx))

                    # We successfully ended the prior transmission, so the next one starts fresh.
                    ctx.missing_prior_context = False

                # Reset state to cleanly begin accumulating the next transmission
                self._reset_transmission_context(ctx)

            # 4. Append this specific speech segment to the state
            # Slice the valid speech audio out of the chunk using the bounds found by VAD
            speech_audio = chunk_data.audio[global_start_ms:global_end_ms]

            if not ctx.current_buffer:
                # Initialize a new transmission buffer
                ctx.current_buffer = speech_audio
                ctx.transmission_start_time_ms = file_start_ms + global_start_ms
                ctx.start_chunk_id = ctx.source_file_uuid
            else:
                # Append to the ongoing transmission
                ctx.current_buffer += speech_audio

            ctx.processed_uuids.add(ctx.source_file_uuid)
            ctx.last_segment_end_time_ms = file_start_ms + global_end_ms

        # Always record that we successfully processed this entire chunk
        ctx.processed_uuids.add(ctx.source_file_uuid)

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

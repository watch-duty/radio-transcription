"""A framework-agnostic state machine isolating sequential audio transmission boundary logic."""

import logging

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

logger = logging.getLogger(__name__)


class AudioStitchingStateMachine:
    """A framework-agnostic state machine responsible for the core logic of stitching audio chunks together."""

    def __init__(self, config: StitchAudioConfig) -> None:
        """Binds the pipeline configuration limits for gap detection and max durations."""
        self.config = config

    def process_chunk(
        self, chunk_data: AudioChunkData, ctx: StitcherContext
    ) -> list[StateMachineAction]:
        """Evaluates an incoming chunk against the state machine to produce imperative actions."""
        # 0. Detect if this is an out-of-order LATE chunk
        is_late_chunk = (
            ctx.expected_next_chunk_start_ms is not None
            and chunk_data.start_ms < ctx.expected_next_chunk_start_ms
        )

        if is_late_chunk:
            return self._process_late_chunk_independently(chunk_data, ctx)

        actions: list[StateMachineAction] = []

        # 1. Detect if we skipped over a chunk (dropped audio)
        is_dropped_chunk = (
            ctx.expected_next_chunk_start_ms is not None
            and chunk_data.start_ms > ctx.expected_next_chunk_start_ms
        )

        if is_dropped_chunk:
            # If we had an active transmission, we must flush it because the auditory context was abruptly disconnected.
            if ctx.transmission_start_time_ms is not None:
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
        ctx.expected_next_chunk_start_ms = (
            chunk_data.start_ms + chunk_data.duration_ms
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
        """Concludes the active transmission by calculating duration and yields a FlushAction."""
        if ctx.transmission_start_time_ms is None:
            msg = "Cannot flush empty current transmission"
            raise ValueError(msg)

        # When flushing due to dropped chunks, we might not have a last_segment_end_time_ms yet
        # if the transmission was very short, so fallback to transmission_start_time_ms.
        end_ms = (
            ctx.last_segment_end_time_ms
            if ctx.last_segment_end_time_ms is not None
            else ctx.transmission_start_time_ms
        )
        if (
            end_ms is None
            or ctx.transmission_start_time_ms is None
            or ctx.buffer_start_time_ms is None
            or ctx.start_audio_offset_ms is None
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
            end_audio_offset_ms=end_ms - ctx.buffer_start_time_ms,
            clear_state=True,
            isolated_audio_buffer=[],
        )

    def _process_late_chunk_independently(
        self, chunk_data: AudioChunkData, ctx: StitcherContext
    ) -> list[StateMachineAction]:
        """Flushes a late-arriving chunk immediately as an independent short transmission."""
        logger.info(
            "[%s] Processing late/overlapping chunk independently: %s",
            ctx.feed_id,
            ctx.current_gcs_uri,
        )
        # Create a detached context to prevent state corruption of the leading edge

        temp_ctx = StitcherContext(
            feed_id=ctx.feed_id,
            current_gcs_uri=ctx.current_gcs_uri,
            session_id=ctx.session_id,
            contributing_audio_uris=[],
            file_start_ms=chunk_data.start_ms,
            missing_prior_context=True,
            last_segment_end_time_ms=None,
            transmission_start_time_ms=None,
            buffer_start_time_ms=None,
            expected_next_chunk_start_ms=None,
            start_audio_offset_ms=None,
            end_audio_offset_ms=None,
            buffer_duration_ms=0,
        )

        raw_actions: list[StateMachineAction] = []
        if not chunk_data.speech_segments:
            raw_actions.extend(self._process_silent_chunk(chunk_data, temp_ctx))
        else:
            raw_actions.extend(
                self._process_speech_segments(chunk_data, temp_ctx)
            )

        # Force flush whatever remaining audio was appended via actions
        if temp_ctx.transmission_start_time_ms is not None:
            # We must determine if the trailing audio was chopped by the late chunk's boundary.
            last_segment = (
                chunk_data.speech_segments[-1]
                if chunk_data.speech_segments
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
        isolated_audio_buffer = []

        for action in raw_actions:
            match action:
                case AppendBufferAction(audio_buffer=audio):
                    isolated_audio_buffer.append(audio)
                case FlushAction():
                    filtered_actions.append(
                        FlushAction(
                            reason=action.reason,
                            feed_id=action.feed_id,
                            time_range=action.time_range,
                            speech_time_range=action.speech_time_range,
                            contributing_audio_uris=action.contributing_audio_uris,
                            missing_prior_context=action.missing_prior_context,
                            missing_post_context=action.missing_post_context,
                            start_audio_offset_ms=action.start_audio_offset_ms,
                            end_audio_offset_ms=action.end_audio_offset_ms,
                            clear_state=False,
                            isolated_audio_buffer=isolated_audio_buffer.copy(),
                        )
                    )
                case DropAction():
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
        self, chunk_data: AudioChunkData, ctx: StitcherContext
    ) -> list[StateMachineAction]:
        """Handles VAD silence events, updating trailing statistics without extending speech logic."""
        actions: list[StateMachineAction] = []
        file_start_ms = chunk_data.start_ms

        is_significant_gap = (
            ctx.last_segment_end_time_ms is not None
            and ((file_start_ms) - ctx.last_segment_end_time_ms)
            >= self.config.significant_gap_ms
        )
        is_max_duration_exceeded = (
            ctx.transmission_start_time_ms is not None
            and (
                (
                    file_start_ms
                    + (
                        len(chunk_data.audio)
                        // (chunk_data.sample_rate // 1000)
                    )
                )
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
                append_end = min(
                    (len(chunk_data.audio) // (chunk_data.sample_rate // 1000)),
                    target_post_roll_end,
                )
                if append_end > 0:
                    actions.append(
                        AppendBufferAction(
                            audio_buffer=chunk_data.audio[
                                0 : append_end
                                * (chunk_data.sample_rate // 1000)
                            ]
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

                ctx.missing_prior_context = False

            self._reset_transmission_context(ctx)

            actions.append(UpdateStateAction())
            if is_significant_gap or is_max_duration_exceeded:
                actions.append(ScheduleStaleTimerAction(deadline_ms=0))
            return actions

        if ctx.transmission_start_time_ms is None:
            actions.append(DropAction(reason="Discarding silent file"))
            actions.append(ScheduleStaleTimerAction(deadline_ms=0))
            return actions

        if ctx.current_gcs_uri not in ctx.contributing_audio_uris:
            ctx.contributing_audio_uris.append(ctx.current_gcs_uri)
        actions.append(UpdateStateAction())
        expected_stale_deadline_ms = (
            ctx.last_segment_end_time_ms
            if ctx.last_segment_end_time_ms is not None
            else (
                chunk_data.start_ms
                + (len(chunk_data.audio) // (chunk_data.sample_rate // 1000))
            )
        ) + self.config.stale_timeout_ms
        actions.append(
            ScheduleStaleTimerAction(deadline_ms=expected_stale_deadline_ms)
        )

        if (
            ctx.transmission_start_time_ms is not None
            and ctx.last_segment_end_time_ms is not None
        ):
            target_post_roll_end = (
                ctx.last_segment_end_time_ms + self.config.vad_post_roll_ms
            )
            append_end = min(
                (len(chunk_data.audio) // (chunk_data.sample_rate // 1000)),
                max(0, target_post_roll_end - file_start_ms),
            )
            if append_end > 0:
                actions.append(
                    AppendBufferAction(
                        audio_buffer=chunk_data.audio[
                            0 : append_end * (chunk_data.sample_rate // 1000)
                        ]
                    )
                )
                ctx.buffer_duration_ms += append_end

        return actions

    def _evaluate_mid_stream_flush(
        self,
        ctx: StitcherContext,
        chunk_data: AudioChunkData,
        actions: list[StateMachineAction],
        file_start_ms: int,
        global_start_ms: int,
        audio_append_cursor_ms: int | None,
        *,
        is_significant_gap: bool,
        is_max_duration_exceeded: bool,
    ) -> int | None:
        if not (is_significant_gap or is_max_duration_exceeded):
            return audio_append_cursor_ms

        if is_significant_gap:
            reason = "Significant gap detected"
        else:
            reason = "Maximum transmission duration exceeded"

        if ctx.transmission_start_time_ms is not None:
            if ctx.last_segment_end_time_ms is None:
                msg = "Unreachable: active transmission without segment anchor"
                raise RuntimeError(msg)
            target_post_roll_end = (
                ctx.last_segment_end_time_ms + self.config.vad_post_roll_ms
            ) - file_start_ms
            append_end = min(target_post_roll_end, global_start_ms)
            append_start = audio_append_cursor_ms or 0

            if 0 <= append_start < append_end:
                actions.append(
                    AppendBufferAction(
                        audio_buffer=chunk_data.audio[
                            append_start
                            * (chunk_data.sample_rate // 1000) : append_end
                            * (chunk_data.sample_rate // 1000)
                        ]
                    )
                )
                ctx.buffer_duration_ms += append_end - append_start

            actions.append(
                self._flush_current_transmission(
                    reason, ctx, missing_post_context=is_max_duration_exceeded
                )
            )

            ctx.missing_prior_context = is_max_duration_exceeded

        self._reset_transmission_context(ctx)
        return None

    def _process_speech_segments(
        self, chunk_data: AudioChunkData, ctx: StitcherContext
    ) -> list[StateMachineAction]:
        """Evaluates consecutive speech data, updating length counters and triggering mid-stream flushes."""
        actions: list[StateMachineAction] = []
        file_start_ms = chunk_data.start_ms
        audio_append_cursor_ms: int | None = None

        for segment in chunk_data.speech_segments:
            # Calculate where to start to avoid overlap
            actual_start_ms = segment.start_ms
            if ctx.last_segment_end_time_ms is not None:
                actual_start_ms = max(
                    actual_start_ms,
                    ctx.last_segment_end_time_ms - file_start_ms,
                )

            global_start_ms = actual_start_ms
            global_end_ms = segment.end_ms

            if global_start_ms >= global_end_ms:
                continue

            # 1. Check if the gap between the last speech segment and this new one
            # exceeds the threshold for flushing.
            is_significant_gap = (
                ctx.last_segment_end_time_ms is not None
                and (
                    (file_start_ms + global_start_ms)
                    - ctx.last_segment_end_time_ms
                )
                >= self.config.significant_gap_ms
            )

            # 2. Check if appending this chunk would exceed the maximum allowed
            # duration for a single transmission.
            is_max_duration_exceeded = (
                ctx.transmission_start_time_ms is not None
                and (
                    (file_start_ms + global_start_ms)
                    - ctx.transmission_start_time_ms
                )
                >= self.config.max_transmission_duration_ms
            )

            audio_append_cursor_ms = self._evaluate_mid_stream_flush(
                ctx=ctx,
                chunk_data=chunk_data,
                actions=actions,
                file_start_ms=file_start_ms,
                global_start_ms=global_start_ms,
                audio_append_cursor_ms=audio_append_cursor_ms,
                is_significant_gap=is_significant_gap,
                is_max_duration_exceeded=is_max_duration_exceeded,
            )

            if ctx.transmission_start_time_ms is None:
                if ctx.missing_prior_context and global_start_ms > 0:
                    ctx.missing_prior_context = False

                ctx.transmission_start_time_ms = file_start_ms + global_start_ms
                append_start = max(
                    0, global_start_ms - self.config.vad_pre_roll_ms
                )
                ctx.start_audio_offset_ms = append_start
                ctx.buffer_start_time_ms = file_start_ms + append_start
            else:
                append_start = (
                    audio_append_cursor_ms
                    if audio_append_cursor_ms is not None
                    else 0
                )

            append_end = min(
                (len(chunk_data.audio) // (chunk_data.sample_rate // 1000)),
                global_end_ms + self.config.vad_post_roll_ms,
            )

            if append_end > append_start:
                actions.append(
                    AppendBufferAction(
                        audio_buffer=chunk_data.audio[
                            append_start
                            * (chunk_data.sample_rate // 1000) : append_end
                            * (chunk_data.sample_rate // 1000)
                        ]
                    )
                )
                audio_append_cursor_ms = append_end
                ctx.buffer_duration_ms += append_end - append_start

            if ctx.current_gcs_uri not in ctx.contributing_audio_uris:
                ctx.contributing_audio_uris.append(ctx.current_gcs_uri)
            ctx.last_segment_end_time_ms = file_start_ms + global_end_ms

        actions.append(UpdateStateAction())
        if ctx.last_segment_end_time_ms is not None:
            expected_stale_deadline_ms = (
                ctx.last_segment_end_time_ms + self.config.stale_timeout_ms
            )
        else:
            expected_stale_deadline_ms = 0

        actions.append(
            ScheduleStaleTimerAction(deadline_ms=expected_stale_deadline_ms)
        )
        return actions

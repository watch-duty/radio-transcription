from backend.pipeline.transcription.datatypes import (
    AudioFileData,
    DropAction,
    FlushAction,
    FlushRequest,
    ScheduleStaleTimerAction,
    StateMachineAction,
    StitchAndTranscribeConfig,
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

    def __init__(self, config: StitchAndTranscribeConfig) -> None:
        self.config = config

    def process_chunk(
        self, chunk_data: AudioFileData, ctx: StitcherContext
    ) -> list[StateMachineAction]:
        if not chunk_data.speech_segments:
            return self._process_silent_chunk(chunk_data, ctx)
        return self._process_speech_segments(chunk_data, ctx)

    def _process_silent_chunk(
        self, chunk_data: AudioFileData, ctx: StitcherContext
    ) -> list[StateMachineAction]:
        actions: list[StateMachineAction] = []
        file_start_ms = chunk_data.start_ms

        is_significant_gap = (
            ctx.last_segment_end_time_ms
            and ((file_start_ms + len(chunk_data.audio)) - ctx.last_segment_end_time_ms)
            >= self.config.significant_gap_ms
        )
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
                    FlushAction(
                        reason=reason,
                        flush_request=FlushRequest(
                            buffer=ctx.current_buffer,
                            feed_id=ctx.feed_id,
                            processed_uuids=set(ctx.processed_uuids),
                            time_range=TimeRange(
                                start_ms=ctx.transmission_start_time_ms,
                                end_ms=ctx.last_segment_end_time_ms,
                            ),
                        ),
                    )
                )

            ctx.current_buffer = None
            ctx.processed_uuids = set()
            ctx.transmission_start_time_ms = None

            ctx.processed_uuids.add(ctx.source_file_uuid)
            actions.append(UpdateStateAction())
            if is_significant_gap or is_max_duration_exceeded:
                actions.append(ScheduleStaleTimerAction(deadline_ms=0))
            return actions

        if not ctx.current_buffer:
            actions.append(DropAction(reason="Discarding silent file"))
            actions.append(ScheduleStaleTimerAction(deadline_ms=0))
            return actions

        ctx.processed_uuids.add(ctx.source_file_uuid)
        actions.append(UpdateStateAction())
        expected_stale_deadline_ms = (
            ctx.last_segment_end_time_ms + self.config.stale_timeout_ms
        )
        actions.append(ScheduleStaleTimerAction(deadline_ms=expected_stale_deadline_ms))
        return actions

    def _process_speech_segments(
        self, chunk_data: AudioFileData, ctx: StitcherContext
    ) -> list[StateMachineAction]:
        actions: list[StateMachineAction] = []
        file_start_ms = chunk_data.start_ms

        for segment in chunk_data.speech_segments:
            global_start_ms = segment.start_ms
            global_end_ms = segment.end_ms

            is_significant_gap = (
                ctx.last_segment_end_time_ms
                and ((file_start_ms + global_start_ms) - ctx.last_segment_end_time_ms)
                >= self.config.significant_gap_ms
            )
            is_max_duration_exceeded = (
                ctx.transmission_start_time_ms is not None
                and ((file_start_ms + global_start_ms) - ctx.transmission_start_time_ms)
                >= self.config.max_transmission_duration_ms
            )

            if is_significant_gap or is_max_duration_exceeded:
                if is_significant_gap:
                    reason = "Significant gap detected"
                else:
                    reason = "Maximum transmission duration exceeded"

                if ctx.current_buffer and ctx.transmission_start_time_ms is not None:
                    actions.append(
                        FlushAction(
                            reason=reason,
                            flush_request=FlushRequest(
                                buffer=ctx.current_buffer,
                                feed_id=ctx.feed_id,
                                processed_uuids=set(ctx.processed_uuids),
                                time_range=TimeRange(
                                    start_ms=ctx.transmission_start_time_ms,
                                    end_ms=ctx.last_segment_end_time_ms,
                                ),
                            ),
                        )
                    )

                ctx.current_buffer = None
                ctx.processed_uuids = set()
                ctx.transmission_start_time_ms = None

            speech_audio = chunk_data.audio[global_start_ms:global_end_ms]

            if not ctx.current_buffer:
                ctx.current_buffer = speech_audio
                ctx.transmission_start_time_ms = file_start_ms + global_start_ms
            else:
                ctx.current_buffer += speech_audio

            ctx.processed_uuids.add(ctx.source_file_uuid)
            ctx.last_segment_end_time_ms = file_start_ms + global_end_ms

        actions.append(UpdateStateAction())
        expected_stale_deadline_ms = (
            ctx.last_segment_end_time_ms + self.config.stale_timeout_ms
        )
        actions.append(ScheduleStaleTimerAction(deadline_ms=expected_stale_deadline_ms))
        return actions

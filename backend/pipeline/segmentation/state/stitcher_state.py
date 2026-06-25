"""A framework-agnostic state machine isolating sequential audio transmission boundary logic."""

import logging

from backend.pipeline.common.constants import MS_PER_SECOND
from backend.pipeline.schema_types.segmented_audio_pb2 import SegmentedAudio
from backend.pipeline.segmentation.constants import (
    DEFAULT_VAD_POST_ROLL_MS,
)
from backend.pipeline.segmentation.datatypes import (
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
from backend.pipeline.segmentation.utils import Metrics

logger = logging.getLogger(__name__)


class AudioStitchingStateMachine:
    """A framework-agnostic state machine responsible for the core logic of stitching audio chunks together."""

    def __init__(self, config: StitchAudioConfig) -> None:
        """Binds the pipeline configuration limits for gap detection and max durations."""
        self.config = config
        self.late_arriving_chunks = Metrics.counter(
            self.__class__, "late_arriving_chunks"
        )
        self.upstream_audio_gaps = Metrics.counter(
            self.__class__, "upstream_audio_gaps"
        )

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
            self.late_arriving_chunks.inc()
            return self._process_late_chunk_independently(chunk_data, ctx)

        actions: list[StateMachineAction] = []

        # 1. Detect if we skipped over a chunk (upstream gap)
        is_upstream_gap = (
            ctx.expected_next_chunk_start_ms is not None
            and chunk_data.start_ms > ctx.expected_next_chunk_start_ms
        )

        if is_upstream_gap:
            self.upstream_audio_gaps.inc()
            # If we had an active transmission, we must flush it because the auditory context was abruptly disconnected.
            if ctx.transmission_start_time_ms is not None:
                actions.append(
                    self._flush_current_transmission(
                        reason="Forced flush due to upstream audio chunk gap",
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
        audio_classification: int | None = None,
    ) -> FlushAction:
        """Concludes the active transmission by calculating duration and yields a FlushAction."""
        if ctx.transmission_start_time_ms is None:
            msg = "Cannot flush empty current transmission"
            raise ValueError(msg)

        if audio_classification is None:
            audio_classification = (
                SegmentedAudio.AUDIO_CLASSIFICATION_SPEECH
                if ctx.speech_segments
                else SegmentedAudio.AUDIO_CLASSIFICATION_OTHER
            )

        # When flushing due to dropped chunks, we might not have a last_segment_end_time_ms yet
        # if the transmission was very short, so fallback to transmission_start_time_ms.
        #
        # Rationale:
        # ctx.last_segment_end_time_ms is retained across transmissions in StitcherContext to
        # measure silence gaps between speech windows. However, if the current transmission window is
        # completely silent (non-speech), no new speech segments are added to update this timestamp.
        # To determine the proper end_ms of the current transmission:
        # 1. If we have speech segments in the current window, the end time is the end of the last speech segment.
        # 2. If it is a non-speech segment transitioning to speech, last_segment_end_time_ms is explicitly updated
        #    to the speech onset (which is >= buffer_start_time_ms).
        # 3. Otherwise, we fall back to transmission_start_time_ms.
        has_valid_last_segment = (
            ctx.last_segment_end_time_ms is not None
            and ctx.buffer_start_time_ms is not None
            and ctx.last_segment_end_time_ms >= ctx.buffer_start_time_ms
        )
        if ctx.speech_segments:
            end_ms = ctx.speech_segments[-1].end_ms
        elif has_valid_last_segment:
            end_ms = ctx.last_segment_end_time_ms
        else:
            end_ms = ctx.transmission_start_time_ms
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

        # Ensure invariants: end time is never before start time
        def clamp_to_start(time_value: int, variable_name: str) -> int:
            if (
                ctx.buffer_start_time_ms is not None
                and time_value < ctx.buffer_start_time_ms
            ):
                logger.warning(
                    "Stitcher invariant violated for feed %s: %s (%d) is before buffer_start_time_ms (%d). Clamping to start.",
                    ctx.feed_id,
                    variable_name,
                    time_value,
                    ctx.buffer_start_time_ms,
                )
                return ctx.buffer_start_time_ms
            return time_value

        padded_end_time_ms = clamp_to_start(
            padded_end_time_ms, "padded_end_time_ms"
        )
        end_ms = clamp_to_start(end_ms, "end_ms")

        return FlushAction(
            reason=reason,
            feed_id=ctx.feed_id,
            contributing_audio_uris=ctx.contributing_audio_uris.copy(),
            contributing_chunks=list(ctx.contributing_chunks),
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
            start_audio_offset_ms=max(0, ctx.start_audio_offset_ms or 0),
            end_audio_offset_ms=max(
                0,
                min(ctx.buffer_duration_ms, end_ms - ctx.buffer_start_time_ms),
            ),
            clear_state=True,
            isolated_audio_buffer=[],
            speech_segments=ctx.speech_segments.copy(),
            traceparent=ctx.traceparent,
            baggage=ctx.baggage,
            audio_classification=audio_classification,
        )

    def _process_late_chunk_independently(
        self, chunk_data: AudioChunkData, ctx: StitcherContext
    ) -> list[StateMachineAction]:
        """Flushes a late-arriving chunk immediately as an independent short transmission."""
        logger.debug(
            "[%s] Processing late/overlapping chunk independently: %s",
            ctx.feed_id,
            ctx.current_gcs_uri,
        )
        # Create a detached context to prevent state corruption of the leading edge.
        # traceparent is propagated from ctx so that Cloud Trace spans created during
        # independent processing of this late chunk are correctly parented to the
        # active session trace, rather than appearing as orphaned root spans.
        temp_ctx = StitcherContext(
            feed_id=ctx.feed_id,
            current_gcs_uri=ctx.current_gcs_uri,
            session_id=ctx.session_id,
            file_start_ms=chunk_data.start_ms,
            missing_prior_context=True,
            last_segment_end_time_ms=None,
            transmission_start_time_ms=None,
            buffer_start_time_ms=None,
            expected_next_chunk_start_ms=None,
            start_audio_offset_ms=None,
            buffer_duration_ms=0,
            traceparent=ctx.traceparent,
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
                            contributing_chunks=action.contributing_chunks,
                            missing_prior_context=action.missing_prior_context,
                            missing_post_context=action.missing_post_context,
                            start_audio_offset_ms=action.start_audio_offset_ms,
                            end_audio_offset_ms=action.end_audio_offset_ms,
                            clear_state=False,
                            isolated_audio_buffer=isolated_audio_buffer.copy(),
                            traceparent=action.traceparent,
                            baggage=action.baggage,
                            audio_classification=action.audio_classification,
                        )
                    )
                case DropAction():
                    filtered_actions.append(action)

        return filtered_actions

    def _reset_transmission_context(self, ctx: StitcherContext) -> None:
        """Resets the ongoing state metrics (timers, timestamps) to begin a fresh transmission window.

        Note: We deliberately do NOT clear ctx.traceparent or ctx.baggage here. Any immediate
        subsequent sub-windows or timers within the same processing bundle must retain the active
        transaction trace context. Once the session ends, a clearing flush will transition the
        persistent state to IdleFeedState, which safely purges the tracing context from the database
        (preventing Trace Context Hijacking).
        """
        ctx.transmission_start_time_ms = None
        ctx.buffer_start_time_ms = None
        ctx.contributing_chunks.clear()
        ctx.start_audio_offset_ms = None
        ctx.buffer_duration_ms = 0
        ctx.speech_segments.clear()

    def _process_silent_chunk(
        self, chunk_data: AudioChunkData, ctx: StitcherContext
    ) -> list[StateMachineAction]:
        """Handles VAD silence events, updating trailing statistics without extending speech logic."""
        file_start_ms = chunk_data.start_ms
        chunk_duration = len(chunk_data.audio) // (
            chunk_data.sample_rate // 1000
        )

        is_speech_active = (
            ctx.transmission_start_time_ms is not None
            and len(ctx.speech_segments) > 0
        )

        if is_speech_active:
            return self._process_silent_chunk_speech_active(
                chunk_data, ctx, file_start_ms, chunk_duration
            )

        return self._process_silent_chunk_non_speech(
            chunk_data, ctx, file_start_ms, chunk_duration
        )

    def _process_silent_chunk_speech_active(
        self,
        chunk_data: AudioChunkData,
        ctx: StitcherContext,
        file_start_ms: int,
        chunk_duration: int,
    ) -> list[StateMachineAction]:
        """Handles silent chunk processing when a speech transmission is active."""
        actions: list[StateMachineAction] = []
        is_significant_gap = (
            ctx.last_segment_end_time_ms is not None
            and (
                (file_start_ms + chunk_duration) - ctx.last_segment_end_time_ms
            )
            >= self.config.significant_gap_ms
        )
        is_max_duration_exceeded = (
            ctx.transmission_start_time_ms is not None
            and (
                (file_start_ms + chunk_duration)
                - ctx.transmission_start_time_ms
            )
            >= self.config.max_transmission_duration_ms
        )

        if is_significant_gap or is_max_duration_exceeded:
            if is_significant_gap:
                reason = "Significant gap detected from silent file"
            else:
                reason = "Maximum transmission duration exceeded by silent file"

            if ctx.last_segment_end_time_ms is None:
                msg = "Unreachable: active transmission without segment anchor"
                raise RuntimeError(msg)

            target_post_roll_end = (
                ctx.last_segment_end_time_ms + DEFAULT_VAD_POST_ROLL_MS
            ) - file_start_ms
            append_end = min(chunk_duration, target_post_roll_end)
            if append_end > 0:
                actions.append(
                    AppendBufferAction(
                        audio_buffer=chunk_data.audio[
                            0 : append_end * (chunk_data.sample_rate // 1000)
                        ]
                    )
                )
                ctx.buffer_duration_ms += append_end

            actions.append(
                self._flush_current_transmission(
                    reason,
                    ctx,
                    missing_post_context=is_max_duration_exceeded,
                    audio_classification=SegmentedAudio.AUDIO_CLASSIFICATION_SPEECH,
                )
            )

            ctx.missing_prior_context = False
            self._reset_transmission_context(ctx)

            # Start a new NON_SPEECH transmission for the remainder of the chunk
            remaining_samples = len(chunk_data.audio) - (
                append_end * (chunk_data.sample_rate // 1000)
            )
            if remaining_samples > 0:
                ctx.transmission_start_time_ms = file_start_ms + append_end
                ctx.buffer_start_time_ms = file_start_ms + append_end
                ctx.start_audio_offset_ms = 0
                ctx.add_contributing_chunk(ctx.current_gcs_uri, file_start_ms)
                actions.append(
                    AppendBufferAction(
                        audio_buffer=chunk_data.audio[
                            append_end * (chunk_data.sample_rate // 1000) :
                        ]
                    )
                )
                ctx.buffer_duration_ms = remaining_samples // (
                    chunk_data.sample_rate // 1000
                )

            actions.append(UpdateStateAction())
            actions.append(ScheduleStaleTimerAction(deadline_ms=0))
            return actions

        ctx.add_contributing_chunk(ctx.current_gcs_uri, file_start_ms)

        if ctx.last_segment_end_time_ms is not None:
            target_post_roll_end = (
                ctx.last_segment_end_time_ms + DEFAULT_VAD_POST_ROLL_MS
            )
            append_end = min(
                chunk_duration,
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

        expected_stale_deadline_ms = (
            ctx.last_segment_end_time_ms
            if ctx.last_segment_end_time_ms is not None
            else (file_start_ms + chunk_duration)
        ) + self.config.stale_timeout_ms

        actions.append(UpdateStateAction())
        actions.append(
            ScheduleStaleTimerAction(deadline_ms=expected_stale_deadline_ms)
        )
        return actions

    def _process_silent_chunk_non_speech(
        self,
        chunk_data: AudioChunkData,
        ctx: StitcherContext,
        file_start_ms: int,
        chunk_duration: int,
    ) -> list[StateMachineAction]:
        """Handles silent chunk processing when no speech transmission is active."""
        actions: list[StateMachineAction] = []
        is_max_duration_exceeded = (
            ctx.transmission_start_time_ms is not None
            and (
                (file_start_ms + chunk_duration)
                - ctx.transmission_start_time_ms
            )
            >= self.config.max_transmission_duration_ms
        )

        if is_max_duration_exceeded:
            actions.append(
                self._flush_current_transmission(
                    "Maximum non-speech transmission duration exceeded",
                    ctx,
                    missing_post_context=True,
                    audio_classification=SegmentedAudio.AUDIO_CLASSIFICATION_OTHER,
                )
            )
            ctx.missing_prior_context = True
            self._reset_transmission_context(ctx)

        if ctx.transmission_start_time_ms is None:
            ctx.transmission_start_time_ms = file_start_ms
            ctx.buffer_start_time_ms = file_start_ms
            ctx.start_audio_offset_ms = 0
            ctx.buffer_duration_ms = 0

        ctx.add_contributing_chunk(ctx.current_gcs_uri, file_start_ms)

        # Append the whole chunk!
        actions.append(AppendBufferAction(audio_buffer=chunk_data.audio))
        ctx.buffer_duration_ms += chunk_duration

        expected_stale_deadline_ms = (
            file_start_ms + chunk_duration
        ) + self.config.stale_timeout_ms

        actions.append(UpdateStateAction())
        actions.append(
            ScheduleStaleTimerAction(deadline_ms=expected_stale_deadline_ms)
        )
        return actions

    def _evaluate_mid_stream_flush(
        self,
        ctx: StitcherContext,
        chunk_data: AudioChunkData,
        actions: list[StateMachineAction],
        file_start_ms: int,
        global_start_ms: int,
        active_file_cursor_ms: int,
        *,
        is_significant_gap: bool,
        is_max_duration_exceeded: bool,
    ) -> int:
        """Evaluates whether an active transmission must be forcefully flushed mid-stream.

        Under the Continuous Audio Retention policy, significant silence gaps are already captured
        and flushed as OTHER segments prior to calling this method. However, this method remains
        critical for enforcing maximum transmission duration splits (is_max_duration_exceeded)
        during excessively long, continuous speech utterances.
        """
        if not (is_significant_gap or is_max_duration_exceeded):
            return active_file_cursor_ms

        if is_significant_gap:
            reason = "Significant gap detected"
        else:
            reason = "Maximum transmission duration exceeded"

        if ctx.transmission_start_time_ms is not None:
            if ctx.last_segment_end_time_ms is None:
                msg = "Unreachable: active transmission without segment anchor"
                raise RuntimeError(msg)
            target_post_roll_end = (
                ctx.last_segment_end_time_ms + DEFAULT_VAD_POST_ROLL_MS
            ) - file_start_ms
            append_end = min(target_post_roll_end, global_start_ms)

            if active_file_cursor_ms < append_end:
                actions.append(
                    AppendBufferAction(
                        audio_buffer=chunk_data.audio[
                            active_file_cursor_ms
                            * (chunk_data.sample_rate // 1000) : append_end
                            * (chunk_data.sample_rate // 1000)
                        ]
                    )
                )
                ctx.buffer_duration_ms += append_end - active_file_cursor_ms
                active_file_cursor_ms = append_end

            actions.append(
                self._flush_current_transmission(
                    reason, ctx, missing_post_context=is_max_duration_exceeded
                )
            )

            ctx.missing_prior_context = is_max_duration_exceeded

        self._reset_transmission_context(ctx)
        return active_file_cursor_ms

    def _pre_split_excessive_speech_segments(
        self, speech_segments: list[TimeRange]
    ) -> list[TimeRange]:
        """Divides speech segments exceeding max_transmission_duration_ms into smaller ranges."""
        processed_segments = []
        max_dur = self.config.max_transmission_duration_ms
        for segment in speech_segments:
            seg_start = segment.start_ms
            seg_end = segment.end_ms

            while seg_end - seg_start > max_dur:
                processed_segments.append(
                    TimeRange(start_ms=seg_start, end_ms=seg_start + max_dur)
                )
                seg_start += max_dur
            if seg_end > seg_start:
                processed_segments.append(
                    TimeRange(start_ms=seg_start, end_ms=seg_end)
                )
        return processed_segments

    def _flush_active_non_speech_transmission(
        self,
        chunk_data: AudioChunkData,
        ctx: StitcherContext,
        first_speech_start_rel_ms: int,
        samples_per_ms: int,
    ) -> list[StateMachineAction]:
        """Flushes an open non-speech transmission from a previous chunk up to the new speech onset."""
        actions: list[StateMachineAction] = []
        if first_speech_start_rel_ms > 0:
            actions.append(
                AppendBufferAction(
                    audio_buffer=chunk_data.audio[
                        0 : first_speech_start_rel_ms * samples_per_ms
                    ]
                )
            )
            ctx.buffer_duration_ms += first_speech_start_rel_ms

        ctx.last_segment_end_time_ms = (
            chunk_data.start_ms + first_speech_start_rel_ms
        )
        ctx.add_contributing_chunk(ctx.current_gcs_uri, chunk_data.start_ms)

        actions.append(
            self._flush_current_transmission(
                reason="Speech onset: ending active non-speech segment",
                ctx=ctx,
                audio_classification=SegmentedAudio.AUDIO_CLASSIFICATION_OTHER,
            )
        )
        self._reset_transmission_context(ctx)
        ctx.missing_prior_context = False
        return actions

    def _capture_intra_chunk_silence(
        self,
        chunk_data: AudioChunkData,
        ctx: StitcherContext,
        current_file_cursor_ms: int,
        speech_start_rel_ms: int,
        samples_per_ms: int,
    ) -> list[StateMachineAction]:
        """Captures and flushes pure silence intervals between consecutive speech utterances within a chunk."""
        actions: list[StateMachineAction] = []
        file_start_ms = chunk_data.start_ms

        silence_start_rel_ms = current_file_cursor_ms
        if ctx.last_segment_end_time_ms is not None:
            silence_start_rel_ms = max(
                silence_start_rel_ms,
                ctx.last_segment_end_time_ms - file_start_ms,
            )

        if silence_start_rel_ms >= speech_start_rel_ms:
            return actions

        silence_duration_ms = speech_start_rel_ms - silence_start_rel_ms
        silence_samples = chunk_data.audio[
            silence_start_rel_ms * samples_per_ms : speech_start_rel_ms
            * samples_per_ms
        ]

        # If we had an active SPEECH transmission, flush it first
        if ctx.transmission_start_time_ms is not None and ctx.speech_segments:
            actions.append(
                self._flush_current_transmission(
                    reason="Silence onset: ending active speech segment",
                    ctx=ctx,
                    audio_classification=SegmentedAudio.AUDIO_CLASSIFICATION_SPEECH,
                )
            )
            self._reset_transmission_context(ctx)
            ctx.missing_prior_context = False

        # Buffer and flush this pure silence gap
        ctx.transmission_start_time_ms = file_start_ms + silence_start_rel_ms
        ctx.buffer_start_time_ms = file_start_ms + silence_start_rel_ms
        ctx.start_audio_offset_ms = silence_start_rel_ms
        ctx.buffer_duration_ms = silence_duration_ms
        ctx.add_contributing_chunk(ctx.current_gcs_uri, file_start_ms)

        actions.append(AppendBufferAction(audio_buffer=silence_samples))
        ctx.last_segment_end_time_ms = file_start_ms + speech_start_rel_ms

        actions.append(
            self._flush_current_transmission(
                reason="Speech onset: ending active non-speech segment",
                ctx=ctx,
                audio_classification=SegmentedAudio.AUDIO_CLASSIFICATION_OTHER,
            )
        )
        self._reset_transmission_context(ctx)
        ctx.missing_prior_context = False
        return actions

    def _process_single_speech_utterance(
        self,
        chunk_data: AudioChunkData,
        ctx: StitcherContext,
        segment: TimeRange,
        current_file_cursor_ms: int,
        samples_per_ms: int,
    ) -> list[StateMachineAction]:
        """Executes overlap validation, mid-stream severing, and appends a single speech utterance to buffer."""
        actions: list[StateMachineAction] = []
        file_start_ms = chunk_data.start_ms
        actual_start_ms = segment.start_ms
        if ctx.last_segment_end_time_ms is not None:
            actual_start_ms = max(
                actual_start_ms,
                ctx.last_segment_end_time_ms - file_start_ms,
            )

        global_start_ms = actual_start_ms
        global_end_ms = segment.end_ms

        if global_start_ms >= global_end_ms:
            return actions

        is_significant_gap = (
            ctx.last_segment_end_time_ms is not None
            and (
                (file_start_ms + global_start_ms) - ctx.last_segment_end_time_ms
            )
            >= self.config.significant_gap_ms
        )

        is_max_duration_exceeded = (
            ctx.transmission_start_time_ms is not None
            and (
                (file_start_ms + global_start_ms)
                - ctx.transmission_start_time_ms
            )
            >= self.config.max_transmission_duration_ms
        )

        _ = self._evaluate_mid_stream_flush(
            ctx=ctx,
            chunk_data=chunk_data,
            actions=actions,
            file_start_ms=file_start_ms,
            global_start_ms=global_start_ms,
            active_file_cursor_ms=current_file_cursor_ms,
            is_significant_gap=is_significant_gap,
            is_max_duration_exceeded=is_max_duration_exceeded,
        )

        if ctx.transmission_start_time_ms is None:
            ctx.transmission_start_time_ms = file_start_ms + global_start_ms
            ctx.start_audio_offset_ms = max(0, global_start_ms)
            ctx.buffer_start_time_ms = file_start_ms + max(0, global_start_ms)

        speech_samples = chunk_data.audio[
            global_start_ms * samples_per_ms : global_end_ms * samples_per_ms
        ]
        if len(speech_samples) > 0:
            actions.append(AppendBufferAction(audio_buffer=speech_samples))
            ctx.buffer_duration_ms += global_end_ms - global_start_ms

        ctx.add_contributing_chunk(ctx.current_gcs_uri, file_start_ms)
        ctx.last_segment_end_time_ms = file_start_ms + global_end_ms
        ctx.speech_segments.append(
            TimeRange(
                start_ms=file_start_ms + global_start_ms,
                end_ms=file_start_ms + global_end_ms,
            )
        )
        return actions

    def _capture_trailing_chunk_silence(
        self,
        chunk_data: AudioChunkData,
        ctx: StitcherContext,
        current_file_cursor_ms: int,
        samples_per_ms: int,
    ) -> list[StateMachineAction]:
        """Initiates a non-speech transmission window for pure silence at the trailing end of a chunk."""
        actions: list[StateMachineAction] = []
        silence_duration_ms = chunk_data.duration_ms - current_file_cursor_ms
        silence_samples = chunk_data.audio[
            current_file_cursor_ms * samples_per_ms : chunk_data.duration_ms
            * samples_per_ms
        ]

        # If we had an active SPEECH transmission, flush it first
        if ctx.transmission_start_time_ms is not None and ctx.speech_segments:
            actions.append(
                self._flush_current_transmission(
                    reason="Chunk boundary: ending active speech segment before trailing silence",
                    ctx=ctx,
                    audio_classification=SegmentedAudio.AUDIO_CLASSIFICATION_SPEECH,
                )
            )
            self._reset_transmission_context(ctx)
            ctx.missing_prior_context = False

        # Start active NON_SPEECH transmission for trailing silence
        if ctx.transmission_start_time_ms is None:
            ctx.transmission_start_time_ms = (
                chunk_data.start_ms + current_file_cursor_ms
            )
            ctx.buffer_start_time_ms = (
                chunk_data.start_ms + current_file_cursor_ms
            )
            ctx.start_audio_offset_ms = current_file_cursor_ms
            ctx.buffer_duration_ms = 0
            ctx.add_contributing_chunk(ctx.current_gcs_uri, chunk_data.start_ms)

        actions.append(AppendBufferAction(audio_buffer=silence_samples))
        ctx.buffer_duration_ms += silence_duration_ms
        ctx.last_segment_end_time_ms = (
            chunk_data.start_ms + chunk_data.duration_ms
        )
        return actions

    def _process_speech_segments(
        self, chunk_data: AudioChunkData, ctx: StitcherContext
    ) -> list[StateMachineAction]:
        """Evaluates consecutive speech data and exhaustive non-speech padding to guarantee 100% audio retention."""
        actions: list[StateMachineAction] = []
        current_file_cursor_ms = 0
        samples_per_ms = chunk_data.sample_rate // 1000

        processed_segments = self._pre_split_excessive_speech_segments(
            chunk_data.speech_segments
        )

        # 0. Handle any existing non-speech transmission from a previous chunk
        is_non_speech_active = (
            not self.config.isolate_segmented_chunks
            and ctx.transmission_start_time_ms is not None
            and not ctx.speech_segments
        )

        if is_non_speech_active:
            first_speech_start_rel_ms = processed_segments[0].start_ms
            new_actions = self._flush_active_non_speech_transmission(
                chunk_data, ctx, first_speech_start_rel_ms, samples_per_ms
            )
            actions.extend(new_actions)
            current_file_cursor_ms = first_speech_start_rel_ms

        # 1. Process each speech segment
        for segment in processed_segments:
            speech_start_rel_ms = segment.start_ms

            if (
                not self.config.isolate_segmented_chunks
                and speech_start_rel_ms > current_file_cursor_ms
                and ctx.last_segment_end_time_ms is not None
            ):
                silence_start_rel_ms = max(
                    current_file_cursor_ms,
                    ctx.last_segment_end_time_ms - chunk_data.start_ms,
                )

                if silence_start_rel_ms < speech_start_rel_ms:
                    silence_gap_ms = (
                        chunk_data.start_ms + speech_start_rel_ms
                    ) - ctx.last_segment_end_time_ms
                    if silence_gap_ms >= self.config.significant_gap_ms:
                        new_actions = self._capture_intra_chunk_silence(
                            chunk_data,
                            ctx,
                            current_file_cursor_ms,
                            speech_start_rel_ms,
                            samples_per_ms,
                        )
                        actions.extend(new_actions)
                        current_file_cursor_ms = speech_start_rel_ms
                    else:
                        # Append the non-significant silence to the active transmission
                        silence_samples = chunk_data.audio[
                            silence_start_rel_ms
                            * samples_per_ms : speech_start_rel_ms
                            * samples_per_ms
                        ]
                        if len(silence_samples) > 0:
                            actions.append(
                                AppendBufferAction(audio_buffer=silence_samples)
                            )
                            ctx.buffer_duration_ms += (
                                speech_start_rel_ms - silence_start_rel_ms
                            )
                        current_file_cursor_ms = speech_start_rel_ms

            new_actions = self._process_single_speech_utterance(
                chunk_data, ctx, segment, current_file_cursor_ms, samples_per_ms
            )
            actions.extend(new_actions)
            current_file_cursor_ms = max(current_file_cursor_ms, segment.end_ms)

        # 2. Capture trailing non-speech audio at the end of the file ONLY if we have an active established stream
        if (
            not self.config.isolate_segmented_chunks
            and current_file_cursor_ms < chunk_data.duration_ms
            and ctx.last_segment_end_time_ms is not None
        ):
            silence_start_rel_ms = max(
                current_file_cursor_ms,
                ctx.last_segment_end_time_ms - chunk_data.start_ms,
            )
            if silence_start_rel_ms < chunk_data.duration_ms:
                silence_gap_ms = (
                    chunk_data.start_ms + chunk_data.duration_ms
                ) - ctx.last_segment_end_time_ms
                if silence_gap_ms >= self.config.significant_gap_ms:
                    new_actions = self._capture_trailing_chunk_silence(
                        chunk_data, ctx, silence_start_rel_ms, samples_per_ms
                    )
                    actions.extend(new_actions)
                else:
                    # Append trailing silence to the active speech transmission
                    trailing_silence_ms = (
                        chunk_data.duration_ms - silence_start_rel_ms
                    )
                    silence_samples = chunk_data.audio[
                        silence_start_rel_ms
                        * samples_per_ms : chunk_data.duration_ms
                        * samples_per_ms
                    ]
                    if len(silence_samples) > 0:
                        actions.append(
                            AppendBufferAction(audio_buffer=silence_samples)
                        )
                        ctx.buffer_duration_ms += trailing_silence_ms

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

    def _append_speech_audio_to_buffer(
        self,
        chunk_data: AudioChunkData,
        ctx: StitcherContext,
        file_start_ms: int,
        global_start_ms: int,
        global_end_ms: int,
        audio_append_cursor_ms: int | None,
        actions: list[StateMachineAction],
    ) -> int | None:
        """Calculates speech boundaries and appends clean, boundary-clamped padded audio to buffer."""
        if ctx.transmission_start_time_ms is None:
            is_natural_gap = (
                ctx.last_segment_end_time_ms is not None
                and (file_start_ms + global_start_ms)
                > ctx.last_segment_end_time_ms
            )
            if ctx.missing_prior_context and is_natural_gap:
                ctx.missing_prior_context = False

            ctx.transmission_start_time_ms = file_start_ms + global_start_ms

            # Pristine canonical slice starting exactly at the VAD onset boundary
            append_start = max(0, global_start_ms)
            ctx.start_audio_offset_ms = 0
            ctx.buffer_start_time_ms = file_start_ms + append_start
        else:
            append_start = (
                audio_append_cursor_ms
                if audio_append_cursor_ms is not None
                else 0
            )

        append_end = min(
            (
                len(chunk_data.audio)
                // (chunk_data.sample_rate // MS_PER_SECOND)
            ),
            global_end_ms,
        )

        if append_end > append_start:
            current_audio = chunk_data.audio[
                append_start
                * (chunk_data.sample_rate // MS_PER_SECOND) : append_end
                * (chunk_data.sample_rate // MS_PER_SECOND)
            ]
            appended_audio = current_audio
            buffer_added_duration = append_end - append_start

            actions.append(AppendBufferAction(audio_buffer=appended_audio))
            audio_append_cursor_ms = append_end
            ctx.buffer_duration_ms += buffer_added_duration

        return audio_append_cursor_ms

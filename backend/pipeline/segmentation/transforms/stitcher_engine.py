"""Core Stitcher Engine executing non-Beam stitching, VAD analysis, and flushes.

This module defines the pure-Python domain logic for audio segment stitching
completely decoupled from Apache Beam runtime boundaries. It exposes:
- StitcherEngine: Coordinates GCS audio downloads, lazy resource initialization,
  Voice Activity Detection (VAD) evaluation, and FSM transition tracking.
- StaleTimerManager: A context helper encapsulating event-time and wall-clock
  timer setups for high-level stateful flushes.

By abstracting all non-Beam logic into this stateless execution engine, we
enforce 100% pickling/serialization safety for Dataflow workers and achieve
straightforward, light-speed unit testing capabilities.
"""

import logging as std_logging
from collections.abc import Callable, Iterator
from dataclasses import replace
from typing import Any

import numpy as np
from apache_beam.metrics import Metrics
from google.cloud import storage

from backend.pipeline.common import constants as common_constants
from backend.pipeline.common.log_helper import get_task_logger
from backend.pipeline.segmentation import constants as trans_constants
from backend.pipeline.segmentation import datatypes
from backend.pipeline.segmentation import logging as segmentation_logging
from backend.pipeline.segmentation import utils as trans_utils
from backend.pipeline.segmentation.audio import processor as audio_processor
from backend.pipeline.segmentation.audio import vad
from backend.pipeline.segmentation.state import stitcher_state

# WARNING: Do NOT remove or bypass setup_logging().
# It explicitly configures structured log propagation for the
# Dataflow worker harness. Removing this will cause all worker logs
# to be rendered as DEBUG severity in Cloud Logging.
segmentation_logging.setup_logging()

logger = get_task_logger(
    __name__, {"system": "transcription", "component": "stitcher-engine"}
)


def _get_task_logger(
    feed_id: str, session_id: str | None, component: str
) -> std_logging.LoggerAdapter[std_logging.Logger]:
    """Contextual logger creation helper."""
    return get_task_logger(
        __name__,
        {
            "system": "transcription",
            "component": component,
            "feed_id": feed_id,
            "session_id": session_id or "none",
        },
    )


def _get_audio_buffer(
    action: datatypes.FlushAction,
    local_buffer: list[np.ndarray] | None,
    transmission_buffer: Any,
) -> list[np.ndarray]:
    """Resolves the raw audio buffer to use for flushing, prioritizing local in-memory state."""
    if not action.clear_state:
        # Isolated/Late chunk: process buffer individually
        return list(action.isolated_audio_buffer)
    if action.isolated_audio_buffer:
        return list(action.isolated_audio_buffer)
    if local_buffer is not None:
        return local_buffer
    # Fallback to reading directly from state and converting to np.ndarray
    return [
        np.frombuffer(b, dtype=np.int16)
        for b in (transmission_buffer.read() or [])
    ]


class StitcherEngine:
    """Pure Python stitching engine completely decoupled from Apache Beam watermark timers.

    Encapsulates GCS downloads, VAD execution, FSM state tracking, and chunk flushes.
    """

    def __init__(
        self,
        stitch_config: datatypes.StitchAudioConfig,
        order_config: datatypes.OrderRestorerConfig,
        vad_config: str = "{}",
        vad_instance: vad.VoiceActivityDetector | None = None,
        gcs_client_instance: storage.Client | None = None,
        vad_factory: Callable[[str], vad.VoiceActivityDetector] | None = None,
        gcs_factory: Callable[[], storage.Client] | None = None,
    ) -> None:
        self.stitch_config = stitch_config
        self.order_config = order_config
        self.vad_config = vad_config

        # Incoming chunk counters
        self.chunks_received = Metrics.counter(
            self.__class__, "chunks_received"
        )

        # VAD evaluated chunk counters
        self.vad_speech_chunks = Metrics.counter(
            self.__class__, "vad_speech_chunks"
        )
        self.vad_silence_chunks = Metrics.counter(
            self.__class__, "vad_silence_chunks"
        )

        # Total speech utterances/segments count
        self.speech_segments_count = Metrics.counter(
            self.__class__, "speech_segments_count"
        )

        # Pipeline health & flushes
        self.stale_flushes = Metrics.counter(self.__class__, "stale_flushes")
        self.oversized_audio_chunks = Metrics.counter(
            self.__class__, "oversized_audio_chunks"
        )

        # Instantiate the stateless AudioProcessor
        self.processor = audio_processor.SegmentationAudioProcessor(
            vad_config=vad_config,
            vad_instance=vad_instance,
            gcs_client_instance=gcs_client_instance,
            vad_factory=vad_factory,
            gcs_factory=gcs_factory,
        )

    def setup(self) -> None:
        """Initializes the processor ONNXRuntime inference sessions and Numba compilation."""
        self.processor.setup()

    def process_ordering_chunk(
        self,
        chunk: datatypes.BufferedChunk,
        feed_id: str,
        curr_context: datatypes.ActiveStitchingState,
        transmission_context_state: Any,
        transmission_buffer_state: Any,
        last_start_ms_state: Any,
        timer_manager: Any,
        previous_expected_ts: int | None,
        *,
        is_backfill: bool,
        clear_buffer: bool = False,
    ) -> tuple[
        list[tuple[str, datatypes.FlushRequest] | tuple[str, dict[str, Any]]],
        int,
    ]:
        """Stitches a single element after session validation and OOO buffer restoral.

        Args:
            chunk: The buffered chunk payload.
            feed_id: Unique identifier of the active feed.
            curr_context: The current transmission sequence context.
            transmission_context_state: Runtime Beam state mapping for contexts.
            transmission_buffer_state: Runtime Beam state mapping for audio buffer.
            last_start_ms_state: Runtime Beam state mapping for last start time.
            timer_manager: Contextual timer scheduler interface.
            previous_expected_ts: The expected next sequence timestamp baseline.
            is_backfill: True if the chunk falls behind the real-time watermark.
            clear_buffer: If True, ignore existing state and treat the buffer as empty.

        Returns:
            A tuple of:
                - A list of elements to emit (FlushRequest or TaggedOutput DLQ).
                - The next expected sequence timestamp.
        """
        task_logger = _get_task_logger(
            feed_id, curr_context.session_id, "transcription-stitcher"
        )

        # Instantiate our state machine on top of our stitch settings
        state_machine = stitcher_state.AudioStitchingStateMachine(
            self.stitch_config
        )

        new_context: datatypes.TransmissionContext
        chunk_outputs, new_context, next_expected_ts = (
            self._process_single_stitch_chunk(
                chunk=chunk,
                feed_id=feed_id,
                curr_context=curr_context,
                transmission_context_state=transmission_context_state,
                transmission_buffer_state=transmission_buffer_state,
                last_start_ms_state=last_start_ms_state,
                timer_manager=timer_manager,
                state_machine=state_machine,
                previous_expected_ts=previous_expected_ts,
                task_logger=task_logger,
                is_backfill=is_backfill,
                clear_buffer=clear_buffer,
            )
        )

        # Write back the context state update
        if (
            isinstance(new_context, datatypes.IdleFeedState)
            and not new_context.out_of_order_buffer
            and not new_context.order_timer_active
        ):
            transmission_context_state.clear()
        else:
            transmission_context_state.write(new_context)
        return chunk_outputs, next_expected_ts

    def handle_stale_transmission(
        self,
        key: str,
        transmission_buffer: Any,
        transmission_context: Any,
        last_start_ms_state: Any,
        timer_manager: Any,
    ) -> Iterator[
        tuple[str, datatypes.FlushRequest] | tuple[str, dict[str, Any]]
    ]:
        """Orchestrates stale flushes when watermarks cross the timeout threshold.

        Args:
            key: Unique key of the active feed partition.
            transmission_buffer: Runtime Beam state mapping for audio buffer.
            transmission_context: Runtime Beam state mapping for contexts.
            last_start_ms_state: Runtime Beam state mapping for last start time.
            timer_manager: Contextual timer scheduler interface.

        Yields:
            Emitted elements (FlushRequest or TaggedOutput DLQ).
        """
        feed_id = key
        curr_ctx = transmission_context.read() or datatypes.IdleFeedState()
        if isinstance(curr_ctx, datatypes.IdleFeedState):
            return

        session_id = curr_ctx.session_id

        task_logger = _get_task_logger(
            feed_id, session_id, "transcription-stitcher"
        )

        start_time_ms = curr_ctx.stale_start_time_ms
        end_time_ms = curr_ctx.last_end_time_ms
        processed_uris = curr_ctx.contributing_audio_uris
        raw_buffer = list(transmission_buffer.read())
        audio_buffer = [
            b if isinstance(b, np.ndarray) else np.frombuffer(b, dtype=np.int16)
            for b in raw_buffer
        ]

        if (
            audio_buffer
            and start_time_ms is not None
            and end_time_ms is not None
            and curr_ctx.buffer_start_time_ms is not None
        ):
            try:
                time_range = datatypes.TimeRange(
                    start_ms=curr_ctx.buffer_start_time_ms,
                    end_ms=curr_ctx.buffer_start_time_ms
                    + curr_ctx.buffer_duration_ms,
                )
                segment_id = trans_utils.generate_segment_id(
                    curr_ctx.session_id,
                    time_range,
                    curr_ctx.buffer_duration_ms,
                )

                task_logger.info(
                    f"[Stale Timer] Fired for session {session_id}. Emitting buffered contents {segment_id}."
                )
                self.stale_flushes.inc()

                audio_classification = (
                    datatypes.AudioClassification.AUDIO_CLASSIFICATION_SPEECH
                    if curr_ctx.speech_segments
                    else datatypes.AudioClassification.AUDIO_CLASSIFICATION_OTHER
                )

                yield (
                    feed_id,
                    datatypes.FlushRequest(
                        buffer=np.concatenate(audio_buffer).tobytes(),
                        feed_id=feed_id,
                        session_id=curr_ctx.session_id,
                        contributing_audio_uris=processed_uris,
                        time_range=time_range,
                        missing_prior_context=curr_ctx.missing_prior_context,
                        missing_post_context=True,
                        start_audio_offset_ms=max(
                            0, curr_ctx.start_audio_offset_ms or 0
                        ),
                        end_audio_offset_ms=curr_ctx.buffer_duration_ms,
                        speech_segments=curr_ctx.speech_segments,
                        segment_id=segment_id,
                        feed_metadata=curr_ctx.feed_metadata,
                        sample_rate=curr_ctx.sample_rate
                        or common_constants.SAMPLE_RATE_HZ,
                        traceparent=curr_ctx.traceparent,
                        baggage=curr_ctx.baggage,
                        audio_classification=audio_classification,
                    ),
                )
            except Exception as e:
                if not self.stitch_config.route_to_dlq:
                    raise
                task_logger.exception(
                    "Error yielding stale buffer for feed %s", feed_id
                )
                yield (
                    trans_constants.DEAD_LETTER_QUEUE_TAG,
                    {"error": str(e), "feed_id": feed_id, "stale_flush": True},
                )

        # Clear state context cleanly
        transmission_context.clear()
        transmission_buffer.clear()
        timer_manager.clear()

    def _apply_flush_action(
        self,
        action: datatypes.FlushAction,
        transmission_context: Any,
        transmission_buffer: Any,
        last_start_ms_state: Any,
        timer_manager: Any,
        session_id: str,
        curr_context: datatypes.ActiveStitchingState,
        chunk_data: datatypes.AudioChunkData | None = None,
        local_buffer: list[np.ndarray] | None = None,
        *,
        is_backfill: bool = False,
    ) -> Iterator[tuple[str, datatypes.FlushRequest]]:
        """Emits a structured FlushRequest payload downstream and resets internal state fields."""
        task_logger = _get_task_logger(
            action.feed_id, session_id, "transcription-stitcher"
        )

        processed_uris = action.contributing_audio_uris or list(
            curr_context.contributing_audio_uris
        )

        raw_buffer = _get_audio_buffer(
            action, local_buffer, transmission_buffer
        )

        if raw_buffer:
            audio_buffer = raw_buffer
            segment_id = trans_utils.generate_segment_id(
                session_id,
                action.time_range,
                trans_utils.get_duration_ms(action.time_range),
            )
            task_logger.info(
                f"[Flush] Emitting segment {segment_id} with {len(processed_uris)} chunks"
            )

            # In backfill/catch-up mode (e.g., pipeline recovering from maintenance or outage),
            # we skip overlap validations and avoid updating `last_start_ms_state`.
            # This prevents older backlogged audio timestamps from corrupting the sequence tracking
            # of the active mainline stream, which would otherwise trigger false overlap warnings
            # and redundant segment outputs once the pipeline catches up to real-time.
            if action.clear_state and not is_backfill:
                current_start_ms = action.time_range.start_ms
                last_start_ms = last_start_ms_state.read()

                if (
                    last_start_ms is not None
                    and abs(current_start_ms - last_start_ms)
                    < trans_constants.OVERLAPPING_TRANSMISSION_TOLERANCE_MS
                ):
                    task_logger.warning(
                        f"Potential growing/overlapping transmission detected! "
                        f"Starts at nearly the same time ({current_start_ms}ms) as previous ({last_start_ms}ms)."
                    )

                last_start_ms_state.write(current_start_ms)

            yield (
                action.feed_id,
                datatypes.FlushRequest(
                    buffer=np.concatenate(audio_buffer).tobytes(),
                    feed_id=action.feed_id,
                    session_id=session_id,
                    contributing_audio_uris=processed_uris,
                    time_range=action.time_range,
                    missing_prior_context=action.missing_prior_context,
                    missing_post_context=action.missing_post_context,
                    start_audio_offset_ms=action.start_audio_offset_ms,
                    end_audio_offset_ms=action.end_audio_offset_ms,
                    speech_segments=action.speech_segments,
                    segment_id=segment_id,
                    feed_metadata=curr_context.feed_metadata,
                    sample_rate=curr_context.sample_rate
                    or (chunk_data.sample_rate if chunk_data else None)
                    or common_constants.SAMPLE_RATE_HZ,
                    traceparent=action.traceparent,
                    baggage=action.baggage,
                    audio_classification=datatypes.AudioClassification(
                        action.audio_classification
                    ),
                ),
            )

    def _record_chunk_evaluation_metrics(
        self, chunk_data: datatypes.AudioChunkData
    ) -> None:
        """Records VAD evaluation outcomes and chunk volume."""
        self.chunks_received.inc()
        if chunk_data.speech_segments:
            self.vad_speech_chunks.inc()
            self.speech_segments_count.inc(len(chunk_data.speech_segments))
        else:
            self.vad_silence_chunks.inc()

    def _process_single_stitch_chunk(
        self,
        chunk: datatypes.BufferedChunk,
        feed_id: str,
        curr_context: datatypes.ActiveStitchingState,
        transmission_context_state: Any,
        transmission_buffer_state: Any,
        last_start_ms_state: Any,
        timer_manager: Any,
        state_machine: stitcher_state.AudioStitchingStateMachine,
        previous_expected_ts: int | None,
        task_logger: Any,
        *,
        is_backfill: bool,
        clear_buffer: bool = False,
    ) -> tuple[
        list[tuple[str, datatypes.FlushRequest] | tuple[str, dict[str, Any]]],
        datatypes.TransmissionContext,
        int,
    ]:
        """Downloads and stitches a single chunk through the state machine.

        Args:
            chunk: The buffered chunk payload.
            feed_id: Unique identifier of the active feed.
            curr_context: The current transmission sequence context.
            transmission_context_state: Runtime Beam state mapping for contexts.
            transmission_buffer_state: Runtime Beam state mapping for audio buffer.
            last_start_ms_state: Runtime Beam state mapping for last start time.
            timer_manager: Contextual timer scheduler interface.
            state_machine: Audio stitching FSM logic.
            previous_expected_ts: The expected next sequence timestamp baseline.
            task_logger: Contextual logger instance.
            is_backfill: True if the chunk falls behind the real-time watermark.
            clear_buffer: If True, ignore existing state and treat the buffer as empty.

        Returns:
            A tuple of:
                - A list of elements to emit (FlushRequest or TaggedOutput DLQ).
                - The updated transmission sequence context.
                - The next expected sequence timestamp.
        """
        from backend.pipeline.common.tracing_utils import (  # noqa: PLC0415
            get_current_traceparent,
            with_tracer_context,
        )

        trace_attrs: dict[str, str] = {}
        if chunk.traceparent:
            trace_attrs["traceparent"] = chunk.traceparent
        baggage_val = chunk.baggage
        if baggage_val:
            trace_attrs["baggage"] = str(baggage_val)

        if curr_context.session_id is None:
            msg = "Session ID cannot be None in _process_single_stitch_chunk"
            raise ValueError(msg)

        feed_metadata = curr_context.feed_metadata
        if feed_metadata is None:
            msg = "feed_metadata cannot be None in _process_single_stitch_chunk"
            raise ValueError(msg)

        try:
            with with_tracer_context(
                trace_attrs,
                "stitching_single_chunk",
                "backend.pipeline.segmentation.transforms.stateful",
            ):
                if (
                    previous_expected_ts is not None
                    and chunk.timestamp_ms > previous_expected_ts
                ):
                    # watermark contiguous gap, clear prior tail primordial state
                    curr_context = replace(curr_context, prior_audio_tail=None)

                # 1. Download audio and run speech detection
                task_logger.debug(
                    f"[Download] Downloading audio for {chunk.gcs_uri}"
                )
                chunk_data = self.processor.download_audio_and_detect(
                    chunk.gcs_uri,
                    chunk.timestamp_ms,
                    prior_audio=curr_context.prior_audio_tail,
                )
                self._record_chunk_evaluation_metrics(chunk_data)
                task_logger.debug(
                    f"[Download] Downloaded audio for {chunk.gcs_uri}"
                )

                payload = datatypes.DownloadedChunkPayload(
                    chunk.gcs_uri,
                    chunk_data,
                    curr_context.session_id or "unknown",
                )

                # 3. Initialize State Machine context
                ctx = datatypes.StitcherContext(
                    feed_id=feed_id,
                    current_gcs_uri=chunk.gcs_uri,
                    session_id=curr_context.session_id,
                    contributing_audio_uris=curr_context.contributing_audio_uris.copy(),
                    file_start_ms=chunk.timestamp_ms,
                    last_segment_end_time_ms=curr_context.last_end_time_ms,
                    transmission_start_time_ms=curr_context.stale_start_time_ms,
                    buffer_start_time_ms=curr_context.buffer_start_time_ms,
                    missing_prior_context=curr_context.missing_prior_context,
                    expected_next_chunk_start_ms=previous_expected_ts,
                    start_audio_offset_ms=curr_context.start_audio_offset_ms,
                    end_audio_offset_ms=None,
                    buffer_duration_ms=curr_context.buffer_duration_ms,
                    speech_segments=curr_context.speech_segments.copy(),
                    traceparent=chunk.traceparent
                    or curr_context.traceparent
                    or get_current_traceparent(),
                    baggage=chunk.baggage or curr_context.baggage,
                    prior_audio_tail=curr_context.prior_audio_tail,
                )

                actions = state_machine.process_chunk(payload.chunk_data, ctx)

                # 4. Apply outputs
                new_context: datatypes.TransmissionContext
                outputs, new_context = self._apply_stitcher_actions(
                    actions=actions,
                    curr_context=curr_context,
                    transmission_context_state=transmission_context_state,
                    transmission_buffer_state=transmission_buffer_state,
                    last_start_ms_state=last_start_ms_state,
                    timer_manager=timer_manager,
                    chunk_data=chunk_data,
                    ctx=ctx,
                    is_backfill=is_backfill,
                    clear_buffer=clear_buffer,
                )

                return (
                    outputs,
                    new_context,
                    chunk.timestamp_ms + chunk_data.duration_ms,
                )

        except Exception as e:
            if "exceeds maximum limit" in str(e):
                self.oversized_audio_chunks.inc()

            if not self.stitch_config.route_to_dlq:
                raise
            task_logger.exception(
                "Error processing chunk %s for feed %s",
                chunk.gcs_uri,
                feed_id,
            )
            dlq_payload = {
                "feed_id": feed_id,
                "gcs_uri": chunk.gcs_uri,
                "session_id": curr_context.session_id or "unknown",
                "error_message": str(e),
                "traceparent": curr_context.traceparent,
            }
            fallback_expected = previous_expected_ts or (
                chunk.timestamp_ms + common_constants.MS_PER_SECOND
            )
            return (
                [(trans_constants.DEAD_LETTER_QUEUE_TAG, dlq_payload)],
                curr_context,
                fallback_expected,
            )

    def _apply_stitcher_actions(
        self,
        actions: list[datatypes.StateMachineAction],
        curr_context: datatypes.ActiveStitchingState,
        transmission_context_state: Any,
        transmission_buffer_state: Any,
        last_start_ms_state: Any,
        timer_manager: Any,
        chunk_data: datatypes.AudioChunkData,
        ctx: datatypes.StitcherContext,
        *,
        is_backfill: bool,
        clear_buffer: bool = False,
    ) -> tuple[list[tuple[str, Any]], datatypes.TransmissionContext]:
        """Processes and executes state transitions generated by the stitching state machine."""
        chunk_outputs = []
        # Stash original active context properties before any actions reassign or clear them.
        # This ensures that sequential flushes within a single chunk processing run (such as split segmented chunks)
        # always resolve native session metadata without throwing TypeErrors/AttributeErrors.
        active_context = curr_context
        active_session_id = curr_context.session_id
        active_feed_metadata = curr_context.feed_metadata
        active_sample_rate = curr_context.sample_rate

        new_context: datatypes.TransmissionContext = curr_context

        # Read the existing transmission buffer state once at the start.
        # This avoids multiple redundant reads and guarantees that we don't hit runner-specific state cache issues
        # when reading after clearing/adding within the same element processing.
        local_buffer_bytes = (
            [] if clear_buffer else (transmission_buffer_state.read() or [])
        )
        local_buffer = [
            np.frombuffer(b, dtype=np.int16) for b in local_buffer_bytes
        ]

        buffer_cleared = False
        newly_appended = []

        for action in actions:
            match action:
                case datatypes.FlushAction():
                    chunk_outputs.extend(
                        list(
                            self._apply_flush_action(
                                action,
                                transmission_context_state,
                                transmission_buffer_state,
                                last_start_ms_state,
                                timer_manager,
                                active_session_id or "unknown",
                                active_context,
                                chunk_data,
                                local_buffer=local_buffer,
                                is_backfill=is_backfill,
                            )
                        )
                    )
                    if action.clear_state:
                        # Clear our local in-memory buffer
                        local_buffer = []
                        buffer_cleared = True
                        newly_appended = []

                        # Force-transition the context back to IdleFeedState to prevent
                        # Trace Context Hijacking and session ID leaks into subsequent independent chunks.
                        new_context = datatypes.IdleFeedState(
                            out_of_order_buffer=list(
                                new_context.out_of_order_buffer
                            ),
                            order_timer_active=new_context.order_timer_active,
                        )
                case datatypes.AppendBufferAction():
                    local_buffer.append(action.audio_buffer)
                    newly_appended.append(action.audio_buffer.tobytes())
                case datatypes.UpdateStateAction():
                    # If we flushed/cleared active state but the same chunk immediately starts a new active
                    # transmission window (e.g. during split-segment forced flushes), we must transition the
                    # IdleFeedState back to ActiveStitchingState using the original stashed details before writing fields.
                    if (
                        isinstance(new_context, datatypes.IdleFeedState)
                        and ctx.transmission_start_time_ms is not None
                    ):
                        new_context = datatypes.ActiveStitchingState(
                            session_id=ctx.session_id or "unknown",
                            feed_metadata=active_feed_metadata,
                            out_of_order_buffer=list(
                                new_context.out_of_order_buffer
                            ),
                            order_timer_active=new_context.order_timer_active,
                            traceparent=ctx.traceparent,
                            baggage=ctx.baggage,
                            sample_rate=active_sample_rate
                            or chunk_data.sample_rate,
                        )

                    if isinstance(new_context, datatypes.ActiveStitchingState):
                        # Priming Strategy: cache tail of contiguous samples
                        priming_samples = int(
                            trans_constants.VAD_DEFAULT_PRIMING_SEC
                            * chunk_data.sample_rate
                        )
                        prior_tail = (
                            chunk_data.audio[-priming_samples:].tobytes()
                            if len(chunk_data.audio) > 0
                            else None
                        )
                        new_context = replace(
                            new_context,
                            contributing_audio_uris=ctx.contributing_audio_uris,
                            last_end_time_ms=ctx.last_segment_end_time_ms,
                            stale_start_time_ms=ctx.transmission_start_time_ms,
                            buffer_start_time_ms=ctx.buffer_start_time_ms,
                            missing_prior_context=ctx.missing_prior_context,
                            start_audio_offset_ms=ctx.start_audio_offset_ms,
                            buffer_duration_ms=ctx.buffer_duration_ms,
                            speech_segments=ctx.speech_segments,
                            prior_audio_tail=prior_tail,
                            sample_rate=chunk_data.sample_rate,
                            traceparent=ctx.traceparent,
                            baggage=ctx.baggage,
                        )
                case datatypes.ScheduleStaleTimerAction():
                    timer_manager.schedule(
                        deadline_ms=action.deadline_ms,
                        is_backfill=is_backfill,
                    )

        # Commit final buffer state to Beam runner
        if buffer_cleared:
            final_buffer_bytes = newly_appended
        else:
            final_buffer_bytes = local_buffer_bytes + newly_appended

        if not final_buffer_bytes:
            transmission_buffer_state.clear()
        else:
            transmission_buffer_state.write(final_buffer_bytes)

        return chunk_outputs, new_context

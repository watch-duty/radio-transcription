import logging
import time
from collections.abc import Iterator
from dataclasses import replace

import apache_beam as beam
import numpy as np
from apache_beam.transforms.userstate import (
    BagRuntimeState,
    BagStateSpec,
    ReadModifyWriteRuntimeState,
    ReadModifyWriteStateSpec,
    RuntimeTimer,
    TimerSpec,
    on_timer,
)
from apache_beam.utils.timestamp import Timestamp

from backend.pipeline.common.constants import MS_PER_SECOND
from backend.pipeline.transcription.audio_processor import AudioProcessor
from backend.pipeline.transcription.constants import (
    DEAD_LETTER_QUEUE_TAG,
    DEFAULT_FLOAT_TOLERANCE_MS,
)
from backend.pipeline.transcription.datatypes import (
    AppendBufferAction,
    BufferedChunk,
    ChunkMetadata,
    DownloadedChunkPayload,
    DropAction,
    FlushAction,
    FlushRequest,
    OrderRestorerConfig,
    ScheduleStaleTimerAction,
    StitchAudioConfig,
    StitcherContext,
    TimeRange,
    TransmissionContext,
    UpdateStateAction,
)
from backend.pipeline.transcription.resources import (
    SHARED_RESOURCE_HANDLE,
    SharedResources,
)
from backend.pipeline.transcription.sequence_buffer import SequenceBuffer
from backend.pipeline.transcription.stitcher_state import (
    AudioStitchingStateMachine,
)
from backend.pipeline.transcription.utils import generate_transmission_id

logger = logging.getLogger(__name__)


class StaleTimerManager:
    """Helper class to manage both event-time and processing-time stale timers."""

    def __init__(
        self,
        event_timer: RuntimeTimer,
        proc_timer: RuntimeTimer,
        config: StitchAudioConfig,
    ) -> None:
        self.event_timer = event_timer
        self.proc_timer = proc_timer
        self.config = config

    def schedule(self, deadline_ms: int, *, is_backfill: bool) -> None:
        """Schedules either or both timers based on the backfill mode."""
        if is_backfill:
            # In backfill mode, use ONLY Event Time (Watermark) timer.
            # Processing time is irrelevant when catching up on historical data.
            self.proc_timer.clear()
            if deadline_ms > 0:
                deadline_s = deadline_ms / MS_PER_SECOND
                self.event_timer.set(Timestamp(seconds=deadline_s))
            else:
                self.event_timer.clear()
        # In streaming mode, use BOTH timers for double coverage.
        # Event time handles gaps in data, while processing time handles total stalls.
        elif deadline_ms > 0:
            # 1. Set event time timer based on data timeline
            deadline_s = deadline_ms / MS_PER_SECOND
            self.event_timer.set(Timestamp(seconds=deadline_s))

            # 2. Set processing time timer based on wall-clock time
            deadline_proc_s = (
                time.time() + self.config.stale_timeout_ms / 1000.0
            )
            self.proc_timer.set(Timestamp(seconds=deadline_proc_s))
        else:
            self.event_timer.clear()
            self.proc_timer.clear()

    def clear(self) -> None:
        """Clears both timers."""
        self.event_timer.clear()
        self.proc_timer.clear()


@beam.typehints.with_input_types(tuple[str, ChunkMetadata])
@beam.typehints.with_output_types(tuple[str, FlushRequest])
class OrderedStitchAudioFn(beam.DoFn):
    """Merged DoFn that handles both Jitter Buffering (ordering) and Audio Stitching.

    It eliminates the shuffle between the two stages and reduces state I/O.
    """

    # --- State Specs ---

    # From RestoreOrderFn
    OUT_OF_ORDER_BUFFER_SPEC = BagStateSpec(
        "out_of_order_buffer", beam.coders.PickleCoder()
    )
    OUT_OF_ORDER_BUFFER_STATE = beam.DoFn.StateParam(OUT_OF_ORDER_BUFFER_SPEC)

    # From StitchAudioFn
    TRANSMISSION_BUFFER_SPEC = BagStateSpec(
        "transmission_buffer", beam.coders.PickleCoder()
    )
    TRANSMISSION_BUFFER_STATE = beam.DoFn.StateParam(TRANSMISSION_BUFFER_SPEC)

    TRANSMISSION_CONTEXT_SPEC = ReadModifyWriteStateSpec(
        "transmission_context", beam.coders.PickleCoder()
    )
    TRANSMISSION_CONTEXT_STATE = beam.DoFn.StateParam(TRANSMISSION_CONTEXT_SPEC)

    # --- Timers ---

    # From RestoreOrderFn
    OUT_OF_ORDER_TIMER_SPEC = TimerSpec(
        "out_of_order_timer", beam.TimeDomain.WATERMARK
    )
    OUT_OF_ORDER_TIMER = beam.DoFn.TimerParam(OUT_OF_ORDER_TIMER_SPEC)

    # From StitchAudioFn
    STALE_TIMER_EVENT_SPEC = TimerSpec(
        "stale_timer_event", beam.TimeDomain.WATERMARK
    )
    STALE_TIMER_EVENT_PARAM = beam.DoFn.TimerParam(STALE_TIMER_EVENT_SPEC)

    STALE_TIMER_PROC_SPEC = TimerSpec(
        "stale_timer_proc", beam.TimeDomain.REAL_TIME
    )
    STALE_TIMER_PROC_PARAM = beam.DoFn.TimerParam(STALE_TIMER_PROC_SPEC)

    def __init__(
        self,
        order_config: OrderRestorerConfig,
        stitch_config: StitchAudioConfig,
    ) -> None:
        self.order_config = order_config
        self.stitch_config = stitch_config

    def setup(self) -> None:
        self.audio_processor = AudioProcessor(
            self.stitch_config.vad_type,
            self.stitch_config.vad_config,
            shared_resources=SHARED_RESOURCE_HANDLE.acquire(SharedResources),
        )
        self.audio_processor.setup()

    def process(
        self,
        element: tuple[str, ChunkMetadata],
        timestamp: Timestamp = beam.DoFn.TimestampParam,  # type: ignore
        out_of_order_buffer_state: BagRuntimeState = OUT_OF_ORDER_BUFFER_STATE,  # type: ignore
        transmission_buffer_state: BagRuntimeState = TRANSMISSION_BUFFER_STATE,  # type: ignore
        transmission_context_state: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        out_of_order_timer: RuntimeTimer = OUT_OF_ORDER_TIMER,  # type: ignore
        stale_timer_event: RuntimeTimer = STALE_TIMER_EVENT_PARAM,  # type: ignore
        stale_timer_proc: RuntimeTimer = STALE_TIMER_PROC_PARAM,  # type: ignore
    ) -> Iterator[tuple[str, FlushRequest] | beam.pvalue.TaggedOutput]:
        """Processes incoming chunks, orders them, downloads audio, and stitches them."""
        feed_id, metadata = element
        curr_context = (
            transmission_context_state.read() or TransmissionContext()
        )

        # Session change detection
        if curr_context.session_id != metadata.session_id:
            logger.info(
                f"[{feed_id}] Session changed from {curr_context.session_id} to {metadata.session_id}. Resetting state."
            )
            # Clear all state
            out_of_order_buffer_state.clear()
            transmission_buffer_state.clear()
            transmission_context_state.clear()
            out_of_order_timer.clear()
            stale_timer_event.clear()
            stale_timer_proc.clear()

            # Initialize new context
            curr_context = TransmissionContext(session_id=metadata.session_id)
            transmission_context_state.write(curr_context)

        sequence_buffer = SequenceBuffer(self.order_config)
        buffer_elements = list(out_of_order_buffer_state.read())
        current_ts_ms = int(float(timestamp) * MS_PER_SECOND)

        # Process chunk through jitter buffer
        (
            new_expected_next_ts,
            new_buffer_elements,
            elements_to_emit,
            _was_late,
            _was_buffered,
        ) = sequence_buffer.process_chunk(
            current_ts_ms=current_ts_ms,
            gcs_uri=metadata.gcs_uri,
            expected_next_ts=curr_context.expected_next_chunk_start_ms,
            buffer_elements=buffer_elements,
            chunk_duration_ms=metadata.duration_ms,
        )

        # Update jitter buffer state
        curr_context = replace(
            curr_context, expected_next_chunk_start_ms=new_expected_next_ts
        )
        transmission_context_state.write(curr_context)

        out_of_order_buffer_state.clear()
        for chunk in new_buffer_elements:
            out_of_order_buffer_state.add(chunk)

        # Handle Timer for Gap Timeout (from RestoreOrderFn)
        if new_buffer_elements and not curr_context.order_timer_active:
            deadline = timestamp + (
                self.order_config.out_of_order_timeout_ms / float(MS_PER_SECOND)
            )
            out_of_order_timer.set(deadline)
            curr_context = replace(curr_context, order_timer_active=True)
            transmission_context_state.write(curr_context)
        elif not new_buffer_elements and curr_context.order_timer_active:
            out_of_order_timer.clear()
            curr_context = replace(curr_context, order_timer_active=False)
            transmission_context_state.write(curr_context)

        # Handle ready elements (Download and Stitch!)
        if elements_to_emit:
            if not self.audio_processor:
                msg = "AudioProcessor not initialized. setup() must be called."
                raise RuntimeError(msg)

            timer_manager = StaleTimerManager(
                stale_timer_event, stale_timer_proc, self.stitch_config
            )

            # Determine if we are in backfill mode (based on the CURRENT element's lateness!)
            lateness = time.time() * MS_PER_SECOND - current_ts_ms
            is_backfill = (
                lateness >= self.stitch_config.backfill_lateness_threshold_ms
            )

            yield from self._download_and_stitch(
                elements_to_emit,
                curr_context,
                transmission_context_state,
                transmission_buffer_state,
                timer_manager,
                feed_id,
                is_backfill=is_backfill,
            )

    def _apply_flush_action(
        self,
        action: FlushAction,
        transmission_context: ReadModifyWriteRuntimeState,
        transmission_buffer: BagRuntimeState,
        timer_manager: StaleTimerManager,
        session_id: str,
    ) -> Iterator[tuple[str, FlushRequest]]:
        """Clears current internal state arrays and yields a compiled FlushRequest downstream."""
        processed_uris = action.isolated_audio_buffer_uris or list(
            transmission_context.read().contributing_audio_uris
        )

        audio_buffer = action.isolated_audio_buffer or list(
            transmission_buffer.read()
        )
        if audio_buffer:
            # Create a deterministic UUID using our shared helper so that Beam retries produce the exact same ID
            transmission_id = generate_transmission_id(
                session_id,
                action.speech_time_range.start_ms,
                action.speech_time_range.end_ms,
            )
            logger.info(
                "Generated transmission_id: %s for feed: %s",
                transmission_id,
                action.feed_id,
            )

            yield (
                action.feed_id,
                FlushRequest(
                    buffer=np.concatenate(audio_buffer),
                    feed_id=action.feed_id,
                    session_id=session_id,
                    contributing_audio_uris=processed_uris,
                    time_range=action.speech_time_range,
                    missing_prior_context=action.missing_prior_context,
                    missing_post_context=action.missing_post_context,
                    start_audio_offset_ms=action.start_audio_offset_ms,
                    end_audio_offset_ms=action.end_audio_offset_ms,
                    transmission_id=transmission_id,
                ),
            )

        if action.clear_state:
            transmission_context.clear()
            transmission_buffer.clear()
            timer_manager.clear()

    def _download_and_stitch(
        self,
        elements_to_emit: list[BufferedChunk],
        curr_context: TransmissionContext,
        transmission_context_state: ReadModifyWriteRuntimeState,
        transmission_buffer_state: BagRuntimeState,
        timer_manager: StaleTimerManager,
        feed_id: str,
        *,
        is_backfill: bool,
    ) -> Iterator[tuple[str, FlushRequest] | beam.pvalue.TaggedOutput]:
        """Helper to download and stitch a list of ready chunks."""
        if not self.audio_processor:
            msg = "AudioProcessor not initialized. setup() must be called."
            raise RuntimeError(msg)

        state_machine = AudioStitchingStateMachine(self.stitch_config)

        for chunk in elements_to_emit:
            try:
                # 1. Download audio!
                chunk_data = self.audio_processor.download_audio_and_detect(
                    chunk.gcs_uri, chunk.timestamp_ms
                )

                payload = DownloadedChunkPayload(
                    chunk.gcs_uri,
                    chunk_data,
                    curr_context.session_id or "unknown",
                )

                # 2. Reconstruct StitcherContext!
                ctx = StitcherContext(
                    feed_id=feed_id,
                    current_gcs_uri=chunk.gcs_uri,
                    session_id=curr_context.session_id,
                    contributing_audio_uris=curr_context.contributing_audio_uris.copy(),
                    file_start_ms=chunk.timestamp_ms,
                    last_segment_end_time_ms=curr_context.last_end_time_ms,
                    transmission_start_time_ms=curr_context.stale_start_time_ms,
                    buffer_start_time_ms=curr_context.buffer_start_time_ms,
                    missing_prior_context=curr_context.missing_prior_context,
                    expected_next_chunk_start_ms=curr_context.expected_next_chunk_start_ms,
                    start_audio_offset_ms=curr_context.start_audio_offset_ms,
                    end_audio_offset_ms=None,
                    buffer_duration_ms=curr_context.buffer_duration_ms,
                )

                # 3. Process through state machine!
                actions = state_machine.process_chunk(payload.chunk_data, ctx)

                # 4. Apply actions!
                for action in actions:
                    match action:
                        case FlushAction():
                            yield from self._apply_flush_action(
                                action,
                                transmission_context_state,
                                transmission_buffer_state,
                                timer_manager,
                                session_id=curr_context.session_id or "unknown",
                            )
                        case AppendBufferAction():
                            transmission_buffer_state.add(action.audio_buffer)
                        case UpdateStateAction():
                            curr_context = replace(
                                curr_context,
                                contributing_audio_uris=ctx.contributing_audio_uris,
                                last_end_time_ms=ctx.last_segment_end_time_ms,
                                stale_start_time_ms=ctx.transmission_start_time_ms,
                                buffer_start_time_ms=ctx.buffer_start_time_ms,
                                missing_prior_context=ctx.missing_prior_context,
                                start_audio_offset_ms=ctx.start_audio_offset_ms,
                                buffer_duration_ms=ctx.buffer_duration_ms,
                            )
                            transmission_context_state.write(curr_context)
                        case ScheduleStaleTimerAction():
                            timer_manager.schedule(
                                action.deadline_ms, is_backfill=is_backfill
                            )
                        case DropAction(reason=reason):
                            logger.info(f"{reason}: {chunk.gcs_uri}")

            except Exception as e:
                if not self.stitch_config.route_to_dlq:
                    raise
                logger.exception(
                    "Error processing chunk %s for feed %s",
                    chunk.gcs_uri,
                    feed_id,
                )
                msg = str(e)
                yield beam.pvalue.TaggedOutput(
                    DEAD_LETTER_QUEUE_TAG, {"error": msg, "feed_id": feed_id}
                )

    @on_timer(OUT_OF_ORDER_TIMER_SPEC)
    def handle_gap_timeout(
        self,
        feed_id: str = beam.DoFn.KeyParam,  # type: ignore
        out_of_order_buffer_state: BagRuntimeState = OUT_OF_ORDER_BUFFER_STATE,  # type: ignore
        transmission_buffer_state: BagRuntimeState = TRANSMISSION_BUFFER_STATE,  # type: ignore
        transmission_context_state: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        stale_timer_event: RuntimeTimer = STALE_TIMER_EVENT_PARAM,  # type: ignore
        stale_timer_proc: RuntimeTimer = STALE_TIMER_PROC_PARAM,  # type: ignore
    ) -> Iterator[tuple[str, FlushRequest] | beam.pvalue.TaggedOutput]:
        """Handles the gap timeout by advancing the expected sequence."""
        curr_context = (
            transmission_context_state.read() or TransmissionContext()
        )
        curr_context = replace(curr_context, order_timer_active=False)
        transmission_context_state.write(curr_context)

        buffer_elements = list(out_of_order_buffer_state.read())
        if buffer_elements:
            sorted_elements = sorted(buffer_elements)
            new_expected = sorted_elements[0].timestamp_ms

            logger.warning(
                f"[{feed_id}] Gap timeout! Advancing expected from {curr_context.expected_next_chunk_start_ms} to {new_expected}."
            )

            curr_context = replace(
                curr_context,
                expected_next_chunk_start_ms=new_expected,
                missing_prior_context=True,
            )
            transmission_context_state.write(curr_context)

            sequence_buffer = SequenceBuffer(self.order_config)

            new_expected_next_ts, new_buffer_elements, elements_to_emit = (
                sequence_buffer.drain_ready_elements(
                    expected_next_ts=new_expected,
                    buffer_elements=buffer_elements,
                    epsilon_ms=DEFAULT_FLOAT_TOLERANCE_MS,
                )
            )

            curr_context = replace(
                curr_context, expected_next_chunk_start_ms=new_expected_next_ts
            )
            transmission_context_state.write(curr_context)

            out_of_order_buffer_state.clear()
            for chunk in new_buffer_elements:
                out_of_order_buffer_state.add(chunk)

            # Handle ready elements
            if elements_to_emit:
                timer_manager = StaleTimerManager(
                    stale_timer_event, stale_timer_proc, self.stitch_config
                )

                # Assume backfill in timeout!
                is_backfill = True

                yield from self._download_and_stitch(
                    elements_to_emit,
                    curr_context,
                    transmission_context_state,
                    transmission_buffer_state,
                    timer_manager,
                    feed_id,
                    is_backfill=is_backfill,
                )

    @on_timer(STALE_TIMER_EVENT_SPEC)
    def handle_stale_transmission_event(
        self,
        key: str = beam.DoFn.KeyParam,  # type: ignore
        transmission_buffer: BagRuntimeState = TRANSMISSION_BUFFER_STATE,  # type: ignore
        transmission_context: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        stale_timer_event: RuntimeTimer = STALE_TIMER_EVENT_PARAM,  # type: ignore
        stale_timer_proc: RuntimeTimer = STALE_TIMER_PROC_PARAM,  # type: ignore
    ) -> Iterator[tuple[str, FlushRequest] | beam.pvalue.TaggedOutput]:
        """Handles stale flushes triggered by event time."""
        timer_manager = StaleTimerManager(
            stale_timer_event, stale_timer_proc, self.stitch_config
        )
        yield from self._handle_stale_transmission(
            key, transmission_buffer, transmission_context, timer_manager
        )

    @on_timer(STALE_TIMER_PROC_SPEC)
    def handle_stale_transmission_proc(
        self,
        key: str = beam.DoFn.KeyParam,  # type: ignore
        transmission_buffer: BagRuntimeState = TRANSMISSION_BUFFER_STATE,  # type: ignore
        transmission_context: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        stale_timer_event: RuntimeTimer = STALE_TIMER_EVENT_PARAM,  # type: ignore
        stale_timer_proc: RuntimeTimer = STALE_TIMER_PROC_PARAM,  # type: ignore
    ) -> Iterator[tuple[str, FlushRequest] | beam.pvalue.TaggedOutput]:
        """Handles stale flushes triggered by processing time."""
        timer_manager = StaleTimerManager(
            stale_timer_event, stale_timer_proc, self.stitch_config
        )
        yield from self._handle_stale_transmission(
            key, transmission_buffer, transmission_context, timer_manager
        )

    def _handle_stale_transmission(
        self,
        key: str,
        transmission_buffer: BagRuntimeState,
        transmission_context: ReadModifyWriteRuntimeState,
        timer_manager: StaleTimerManager,
    ) -> Iterator[tuple[str, FlushRequest] | beam.pvalue.TaggedOutput]:
        """Common logic for handling stale transmissions."""
        curr_context = transmission_context.read() or TransmissionContext()
        start_time_ms = curr_context.stale_start_time_ms
        end_time_ms = curr_context.last_end_time_ms
        processed_uris = curr_context.contributing_audio_uris
        audio_buffer = list(transmission_buffer.read())

        if (
            audio_buffer
            and start_time_ms is not None
            and end_time_ms is not None
            and curr_context.buffer_start_time_ms is not None
        ):
            try:
                # Create a deterministic UUID
                transmission_id = generate_transmission_id(
                    curr_context.session_id or "unknown",
                    start_time_ms,
                    end_time_ms,
                )

                yield (
                    key,
                    FlushRequest(
                        buffer=np.concatenate(audio_buffer),
                        feed_id=key,
                        session_id=curr_context.session_id or "unknown",
                        contributing_audio_uris=processed_uris,
                        time_range=TimeRange(
                            start_ms=start_time_ms, end_ms=end_time_ms
                        ),
                        missing_prior_context=curr_context.missing_prior_context,
                        missing_post_context=True,
                        start_audio_offset_ms=curr_context.start_audio_offset_ms,
                        end_audio_offset_ms=end_time_ms
                        - curr_context.buffer_start_time_ms,
                        transmission_id=transmission_id,
                    ),
                )
            except Exception as e:
                if not self.stitch_config.route_to_dlq:
                    raise
                logger.exception("Error yielding stale buffer for feed %s", key)
                msg = str(e)
                yield beam.pvalue.TaggedOutput(
                    DEAD_LETTER_QUEUE_TAG,
                    {"error": msg, "feed_id": key, "stale_flush": True},
                )

        transmission_context.clear()
        transmission_buffer.clear()
        timer_manager.clear()

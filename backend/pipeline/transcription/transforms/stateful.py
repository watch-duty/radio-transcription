import logging
import time
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, cast, override

import apache_beam as beam
import numpy as np
from apache_beam.metrics import Metrics
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

from backend.pipeline.common.constants import MS_PER_SECOND, SAMPLE_RATE_HZ
from backend.pipeline.common.storage.gcs_uploader import GCSAudioUploader
from backend.pipeline.transcription.audio.audio_processor import AudioProcessor
from backend.pipeline.transcription.common.constants import (
    DEAD_LETTER_QUEUE_TAG,
    DEFAULT_FLOAT_TOLERANCE_MS,
)
from backend.pipeline.transcription.common.datatypes import (
    AppendBufferAction,
    BufferedChunk,
    ChunkMetadata,
    DownloadedChunkPayload,
    DropAction,
    FeedMetadata,
    FlushAction,
    FlushRequest,
    OrderRestorerConfig,
    ScheduleStaleTimerAction,
    StitchAudioConfig,
    StitcherContext,
    TimeRange,
    TranscribeAudioConfig,
    TranscriptionResult,
    TransmissionContext,
    UpdateStateAction,
)
from backend.pipeline.transcription.resources import (
    SHARED_RESOURCE_HANDLE,
    SharedResources,
)
from backend.pipeline.transcription.services.transcribers import (
    Transcriber,
    get_transcriber,
)
from backend.pipeline.transcription.state.sequence_buffer import SequenceBuffer
from backend.pipeline.transcription.state.stitcher_state import (
    AudioStitchingStateMachine,
)
from backend.pipeline.transcription.utils import generate_transmission_id

logger = logging.getLogger(__name__)
logger = logging.LoggerAdapter(
    logger, {"system": "transcription", "component": "ordered-stitcher"}
)


def _get_task_logger(
    feed_id: str, session_id: str | None, component: str
) -> logging.LoggerAdapter:
    """Creates a contextual LoggerAdapter for tracing items through the pipeline."""
    return logging.LoggerAdapter(
        logging.getLogger(__name__),
        {
            "system": "transcription",
            "component": component,
            "feed_id": feed_id,
            "session_id": session_id or "unknown",
        },
    )


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


def process_ordering(
    element: tuple[str, ChunkMetadata],
    timestamp: Timestamp,
    curr_context: TransmissionContext,
    out_of_order_timer: RuntimeTimer,
    order_config: OrderRestorerConfig,
) -> tuple[list[BufferedChunk], TransmissionContext, bool]:
    """Handles session change detection and chronological ordering via SequenceBuffer."""
    feed_id, metadata = element
    session_changed = False

    task_logger = _get_task_logger(
        feed_id, metadata.session_id, "sequence-buffer"
    )

    # Session change detection
    if curr_context.session_id != metadata.session_id:
        task_logger.info(
            f"Session ID changed from {curr_context.session_id} to {metadata.session_id}. Resetting state."
        )
        session_changed = True
        out_of_order_timer.clear()
        curr_context = TransmissionContext(
            session_id=metadata.session_id,
            feed_metadata=metadata.feed_metadata,
        )

    sequence_buffer = SequenceBuffer(order_config)
    buffer_elements = curr_context.out_of_order_buffer
    current_ts_ms = int(float(timestamp) * MS_PER_SECOND)

    # Process chunk through jitter buffer
    (
        new_expected_next_ts,
        new_buffer_elements,
        elements_to_emit,
        was_late,
        was_buffered,
    ) = sequence_buffer.process_chunk(
        current_ts_ms=current_ts_ms,
        gcs_uri=metadata.gcs_uri,
        expected_next_ts=curr_context.expected_next_chunk_start_ms,
        buffer_elements=buffer_elements,
        chunk_duration_ms=metadata.duration_ms,
    )

    if was_late:
        task_logger.info(f"[Order] Late chunk: {metadata.gcs_uri}")
    if was_buffered:
        task_logger.info(
            f"[Order] Buffered chunk from future: {metadata.gcs_uri}"
        )
    if elements_to_emit:
        task_logger.info(f"[Order] Releasing {len(elements_to_emit)} chunks")

    # Update jitter buffer state
    curr_context = replace(
        curr_context,
        expected_next_chunk_start_ms=new_expected_next_ts,
        out_of_order_buffer=new_buffer_elements,
    )

    # Handle Timer for Gap Timeout
    if new_buffer_elements and not curr_context.order_timer_active:
        deadline = timestamp + (
            order_config.out_of_order_timeout_ms / float(MS_PER_SECOND)
        )
        out_of_order_timer.set(deadline)
        curr_context = replace(curr_context, order_timer_active=True)
    elif not new_buffer_elements and curr_context.order_timer_active:
        out_of_order_timer.clear()
        curr_context = replace(curr_context, order_timer_active=False)

    return elements_to_emit, curr_context, session_changed


@beam.typehints.with_input_types(tuple[str, ChunkMetadata])
@beam.typehints.with_output_types(tuple[str, FlushRequest])
class OrderedStitchAudioFn(beam.DoFn):
    """Merged DoFn that handles both Jitter Buffering (ordering) and Audio Stitching.

    It eliminates the shuffle between the two stages and reduces state I/O.
    """

    # --- State Specs ---

    # From StitchAudioFn
    TRANSMISSION_BUFFER_SPEC = BagStateSpec(
        "transmission_buffer", beam.coders.PickleCoder()
    )
    TRANSMISSION_BUFFER_STATE = beam.DoFn.StateParam(TRANSMISSION_BUFFER_SPEC)

    TRANSMISSION_CONTEXT_SPEC = ReadModifyWriteStateSpec(
        "transmission_context", beam.coders.PickleCoder()
    )
    TRANSMISSION_CONTEXT_STATE = beam.DoFn.StateParam(TRANSMISSION_CONTEXT_SPEC)

    LAST_START_MS_SPEC = ReadModifyWriteStateSpec(
        "last_start_ms", beam.coders.VarIntCoder()
    )
    LAST_START_MS_STATE = beam.DoFn.StateParam(LAST_START_MS_SPEC)

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
        transmission_buffer_state: BagRuntimeState = TRANSMISSION_BUFFER_STATE,  # type: ignore
        transmission_context_state: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        last_start_ms_state: ReadModifyWriteRuntimeState = LAST_START_MS_STATE,  # type: ignore
        out_of_order_timer: RuntimeTimer = OUT_OF_ORDER_TIMER,  # type: ignore
        stale_timer_event: RuntimeTimer = STALE_TIMER_EVENT_PARAM,  # type: ignore
        stale_timer_proc: RuntimeTimer = STALE_TIMER_PROC_PARAM,  # type: ignore
    ) -> Iterator[tuple[str, FlushRequest] | beam.pvalue.TaggedOutput]:
        """Processes incoming chunks, orders them, downloads audio, and stitches them."""
        feed_id, metadata = element
        current_ts_ms = int(float(timestamp) * MS_PER_SECOND)
        curr_context = (
            transmission_context_state.read() or TransmissionContext()
        )
        previous_expected_ts = curr_context.expected_next_chunk_start_ms

        # Handle session change and ordering via helper function
        elements_to_emit, curr_context, session_changed = process_ordering(
            element,
            timestamp,
            curr_context,
            out_of_order_timer,
            self.order_config,
        )

        task_logger = _get_task_logger(
            feed_id, curr_context.session_id, "transcription-stitcher"
        )
        task_logger.info(f"[Process] Processing chunk {metadata.gcs_uri}")

        if curr_context.feed_metadata is None:
            curr_context = replace(
                curr_context, feed_metadata=metadata.feed_metadata
            )
        if session_changed:
            # Also clear stitching state!
            transmission_buffer_state.clear()
            stale_timer_event.clear()
            stale_timer_proc.clear()

        # Always write updated context!
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
                last_start_ms_state,
                timer_manager,
                feed_id,
                is_backfill=is_backfill,
                previous_expected_ts=previous_expected_ts,
            )

    def _apply_flush_action(
        self,
        action: FlushAction,
        transmission_context: ReadModifyWriteRuntimeState,
        transmission_buffer: BagRuntimeState,
        last_start_ms_state: ReadModifyWriteRuntimeState,
        timer_manager: StaleTimerManager,
        session_id: str,
    ) -> Iterator[tuple[str, FlushRequest]]:
        """Clears current internal state arrays and yields a compiled FlushRequest downstream."""
        task_logger = _get_task_logger(
            action.feed_id, session_id, "transcription-stitcher"
        )

        curr_ctx = transmission_context.read() or TransmissionContext()
        processed_uris = action.isolated_audio_buffer_uris or list(
            curr_ctx.contributing_audio_uris
        )

        audio_buffer = action.isolated_audio_buffer or list(
            transmission_buffer.read()
        )
        if audio_buffer:
            # Create a deterministic UUID using our shared helper so that Beam retries produce the exact same ID
            transmission_id = generate_transmission_id(
                session_id,
                action.speech_time_range,
            )
            task_logger.info(
                f"[Flush] Emitting transmission {transmission_id} with {len(processed_uris)} chunks"
            )

            if curr_ctx.feed_metadata is None:
                msg = "feed_metadata cannot be None in _apply_flush_action"
                raise ValueError(msg)

            current_start_ms = action.speech_time_range.start_ms
            last_start_ms = last_start_ms_state.read()

            if (
                last_start_ms is not None
                and abs(current_start_ms - last_start_ms) < 100
            ):
                task_logger.warning(
                    f"Potential growing/overlapping transmission detected! "
                    f"Starts at nearly the same time ({current_start_ms}ms) as previous ({last_start_ms}ms)."
                )

            last_start_ms_state.write(current_start_ms)

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
                    feed_metadata=curr_ctx.feed_metadata,
                ),
            )

        if action.clear_state:
            transmission_context.write(
                TransmissionContext(feed_metadata=curr_ctx.feed_metadata)
            )
            transmission_buffer.clear()
            timer_manager.clear()

    def _download_and_stitch(
        self,
        elements_to_emit: list[BufferedChunk],
        curr_context: TransmissionContext,
        transmission_context_state: ReadModifyWriteRuntimeState,
        transmission_buffer_state: BagRuntimeState,
        last_start_ms_state: ReadModifyWriteRuntimeState,
        timer_manager: StaleTimerManager,
        feed_id: str,
        *,
        is_backfill: bool,
        previous_expected_ts: int | None = None,
    ) -> Iterator[tuple[str, FlushRequest] | beam.pvalue.TaggedOutput]:
        """Helper to download and stitch a list of ready chunks."""
        if curr_context.session_id is None:
            msg = "Session ID cannot be None in _download_and_stitch"
            raise ValueError(msg)
        task_logger = _get_task_logger(
            feed_id,
            curr_context.session_id,
            "transcription-stitcher",
        )

        if not self.audio_processor:
            msg = "AudioProcessor not initialized. setup() must be called."
            raise RuntimeError(msg)

        state_machine = AudioStitchingStateMachine(self.stitch_config)

        for chunk in elements_to_emit:
            try:
                # 1. Download audio!
                task_logger.info(
                    f"[Download] Downloading audio for {chunk.gcs_uri}"
                )
                chunk_data = self.audio_processor.download_audio_and_detect(
                    chunk.gcs_uri, chunk.timestamp_ms
                )
                task_logger.info(
                    f"[Download] Downloaded audio for {chunk.gcs_uri}"
                )

                time_range = TimeRange(
                    start_ms=chunk.timestamp_ms,
                    end_ms=chunk.timestamp_ms + chunk_data.duration_ms,
                )

                if self.stitch_config.bypass_stitching:
                    yield (
                        feed_id,
                        FlushRequest(
                            buffer=chunk_data.audio,
                            feed_id=feed_id,
                            session_id=curr_context.session_id or "unknown",
                            contributing_audio_uris=[chunk.gcs_uri],
                            time_range=time_range,
                            missing_prior_context=False,
                            missing_post_context=False,
                            start_audio_offset_ms=0,
                            end_audio_offset_ms=None,
                            transmission_id=generate_transmission_id(
                                curr_context.session_id,
                                time_range,
                            ),
                            feed_metadata=cast(
                                "FeedMetadata", curr_context.feed_metadata
                            ),
                        ),
                    )
                    continue

                payload = DownloadedChunkPayload(
                    chunk.gcs_uri,
                    chunk_data,
                    curr_context.session_id or "unknown",
                )

                # 2. Reconstruct StitcherContext!
                expected_ts = (
                    previous_expected_ts
                    if chunk == elements_to_emit[0]
                    else curr_context.expected_next_chunk_start_ms
                )
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
                    expected_next_chunk_start_ms=expected_ts,
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
                                last_start_ms_state,
                                timer_manager,
                                session_id=curr_context.session_id,
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
        transmission_buffer_state: BagRuntimeState = TRANSMISSION_BUFFER_STATE,  # type: ignore
        transmission_context_state: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        last_start_ms_state: ReadModifyWriteRuntimeState = LAST_START_MS_STATE,  # type: ignore
        stale_timer_event: RuntimeTimer = STALE_TIMER_EVENT_PARAM,  # type: ignore
        stale_timer_proc: RuntimeTimer = STALE_TIMER_PROC_PARAM,  # type: ignore
    ) -> Iterator[tuple[str, FlushRequest] | beam.pvalue.TaggedOutput]:
        """Handles the gap timeout by advancing the expected sequence."""
        curr_context = (
            transmission_context_state.read() or TransmissionContext()
        )
        curr_context = replace(curr_context, order_timer_active=False)
        transmission_context_state.write(curr_context)

        buffer_elements = curr_context.out_of_order_buffer
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
                curr_context,
                expected_next_chunk_start_ms=new_expected_next_ts,
                out_of_order_buffer=new_buffer_elements,
            )
            transmission_context_state.write(curr_context)

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
                    last_start_ms_state,
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
            if curr_context.session_id is None:
                msg = "Session ID cannot be None in _handle_stale_transmission"
                raise ValueError(msg)

            try:
                # Create a deterministic UUID
                time_range = TimeRange(
                    start_ms=start_time_ms, end_ms=end_time_ms
                )
                transmission_id = generate_transmission_id(
                    curr_context.session_id,
                    time_range,
                )

                yield (
                    key,
                    FlushRequest(
                        buffer=np.concatenate(audio_buffer),
                        feed_id=key,
                        session_id=curr_context.session_id,
                        contributing_audio_uris=processed_uris,
                        time_range=time_range,
                        missing_prior_context=curr_context.missing_prior_context,
                        missing_post_context=True,
                        start_audio_offset_ms=curr_context.start_audio_offset_ms,
                        end_audio_offset_ms=end_time_ms
                        - curr_context.buffer_start_time_ms,
                        transmission_id=transmission_id,
                        feed_metadata=cast(
                            "FeedMetadata", curr_context.feed_metadata
                        ),
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

        transmission_context.write(
            TransmissionContext(feed_metadata=curr_context.feed_metadata)
        )
        transmission_buffer.clear()
        timer_manager.clear()


@beam.typehints.with_input_types(tuple[str, ChunkMetadata])
@beam.typehints.with_output_types(tuple[str, FlushRequest])
class OrderedBypassFn(beam.DoFn):
    """A stateful Beam DoFn that handles chronological ordering and downloading,
    but bypasses the stitching state machine, yielding FlushRequests immediately.
    """

    TRANSMISSION_CONTEXT_SPEC = ReadModifyWriteStateSpec(
        "transmission_context", beam.coders.PickleCoder()
    )
    TRANSMISSION_CONTEXT_STATE = beam.DoFn.StateParam(TRANSMISSION_CONTEXT_SPEC)

    OUT_OF_ORDER_TIMER_SPEC = TimerSpec(
        "out_of_order_timer", beam.TimeDomain.WATERMARK
    )
    OUT_OF_ORDER_TIMER = beam.DoFn.TimerParam(OUT_OF_ORDER_TIMER_SPEC)

    def __init__(
        self,
        order_config: OrderRestorerConfig,
        stitch_config: StitchAudioConfig,
    ) -> None:
        self.order_config = order_config
        self.stitch_config = stitch_config
        self.audio_processor: AudioProcessor | None = None

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
        transmission_context_state: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        out_of_order_timer: RuntimeTimer = OUT_OF_ORDER_TIMER,  # type: ignore
    ) -> Iterator[tuple[str, FlushRequest] | beam.pvalue.TaggedOutput]:
        """Processes incoming chunks, orders them, and yields FlushRequests immediately."""
        feed_id, _metadata = element
        curr_context = (
            transmission_context_state.read() or TransmissionContext()
        )
        if curr_context.feed_metadata is None:
            curr_context = replace(
                curr_context, feed_metadata=_metadata.feed_metadata
            )

        # Handle session change and ordering via helper function
        elements_to_emit, curr_context, _session_changed = process_ordering(
            element,
            timestamp,
            curr_context,
            out_of_order_timer,
            self.order_config,
        )

        # Always write updated context!
        transmission_context_state.write(curr_context)

        if elements_to_emit:
            yield from self._download_and_yield(
                elements_to_emit, feed_id, curr_context
            )

    def _download_and_yield(
        self,
        elements_to_emit: list[BufferedChunk],
        feed_id: str,
        curr_context: TransmissionContext,
    ) -> Iterator[tuple[str, FlushRequest] | beam.pvalue.TaggedOutput]:
        if not self.audio_processor:
            msg = "AudioProcessor not initialized. setup() must be called."
            raise RuntimeError(msg)

        for chunk in elements_to_emit:
            try:
                chunk_data = self.audio_processor.download_audio_and_detect(
                    chunk.gcs_uri, chunk.timestamp_ms
                )

                time_range = TimeRange(
                    start_ms=chunk.timestamp_ms,
                    end_ms=chunk.timestamp_ms + chunk_data.duration_ms,
                )

                yield (
                    feed_id,
                    FlushRequest(
                        buffer=chunk_data.audio,
                        feed_id=feed_id,
                        session_id=curr_context.session_id or "unknown",
                        contributing_audio_uris=[chunk.gcs_uri],
                        time_range=time_range,
                        missing_prior_context=False,
                        missing_post_context=False,
                        start_audio_offset_ms=0,
                        end_audio_offset_ms=chunk_data.duration_ms,
                        transmission_id=generate_transmission_id(
                            curr_context.session_id or "unknown",
                            time_range,
                        ),
                        feed_metadata=cast(
                            "FeedMetadata", curr_context.feed_metadata
                        ),
                    ),
                )
            except Exception as e:
                logging.getLogger(__name__).exception(
                    f"Error processing chunk {chunk.gcs_uri} for feed {feed_id}"
                )
                yield beam.pvalue.TaggedOutput(
                    DEAD_LETTER_QUEUE_TAG,
                    {"error": str(e), "feed_id": feed_id},
                )

    @on_timer(OUT_OF_ORDER_TIMER_SPEC)
    def handle_gap_timeout(
        self,
        feed_id: str = beam.DoFn.KeyParam,  # type: ignore
        transmission_context_state: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        out_of_order_timer: RuntimeTimer = OUT_OF_ORDER_TIMER,  # type: ignore
    ) -> Iterator[tuple[str, FlushRequest] | beam.pvalue.TaggedOutput]:
        """Handles the gap timeout by advancing the expected sequence."""
        curr_context = (
            transmission_context_state.read() or TransmissionContext()
        )
        curr_context = replace(curr_context, order_timer_active=False)
        transmission_context_state.write(curr_context)

        buffer_elements = curr_context.out_of_order_buffer
        if buffer_elements:
            sorted_elements = sorted(buffer_elements)
            new_expected = sorted_elements[0].timestamp_ms

            logging.getLogger(__name__).warning(
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
                curr_context,
                expected_next_chunk_start_ms=new_expected_next_ts,
                out_of_order_buffer=new_buffer_elements,
            )
            transmission_context_state.write(curr_context)

            yield from self._download_and_yield(
                elements_to_emit, feed_id, curr_context
            )


SEQUENTIAL_BARRIER_SPEC = ReadModifyWriteStateSpec(
    "sequential_barrier", beam.coders.BooleanCoder()
)
# A dummy state parameter used exclusively to enforce chronological processing constraints on the Beam Runner per feed_id.
SEQUENTIAL_BARRIER_STATE = beam.DoFn.StateParam(SEQUENTIAL_BARRIER_SPEC)


@beam.typehints.with_input_types(tuple[str, FlushRequest])
@beam.typehints.with_output_types(TranscriptionResult)
class TranscribeAudioFn(beam.DoFn):
    """Submits the concatenated FLAC buffers generated by the upstream Stitcher to the
    transcription API and serializes the transcripts into a TranscriptionResult dataclass.
    """

    def __init__(
        self,
        config: TranscribeAudioConfig,
        transcriber_factory: Any | None = None,
    ) -> None:
        """Binds the TranscribeAudioConfig and initializes Beam metrics counters."""
        self.config = config
        self.transcriber_factory = transcriber_factory or get_transcriber

        self.audio_processor: AudioProcessor | None = None
        self.transcriber: Transcriber | None = None

        self.vad_speech_count = Metrics.counter(
            "TranscribeAudioFn", "vad_speech_count"
        )
        self.vad_silence_count = Metrics.counter(
            "TranscribeAudioFn", "vad_silence_count"
        )
        self.transcription_count = Metrics.counter(
            "TranscribeAudioFn", "transcription_count"
        )
        self.dlq_count = Metrics.counter("TranscribeAudioFn", "dlq_count")

        self.speech_duration_sec_dist = Metrics.distribution(
            "TranscribeAudioFn", "speech_duration_sec"
        )
        self.vad_eval_time_ms = Metrics.distribution(
            "TranscribeAudioFn", "vad_eval_time_ms"
        )
        self.transcription_time_ms = Metrics.distribution(
            "TranscribeAudioFn", "transcription_time_ms"
        )

    @override
    def setup(self) -> None:
        """Initializes internal clients once per worker."""
        # Grab the global pool singleton
        self.shared_resources = SHARED_RESOURCE_HANDLE.acquire(SharedResources)

        self.audio_processor = AudioProcessor(
            self.config.vad_type,
            self.config.vad_config,
            shared_resources=self.shared_resources,
        )
        self.audio_processor.setup()

        self.transcriber = self.shared_resources.get_transcriber(
            factory=self.transcriber_factory,
            transcriber_type=self.config.transcriber_type,
            project_id=self.config.project_id,
            config_json=self.config.transcriber_config,
        )

        if self.audio_processor.gcs_client is None:
            msg = "GCS client not found in AudioProcessor. must call setup() first."
            raise RuntimeError(msg)

        self.audio_uploader = GCSAudioUploader(
            gcs_client=self.audio_processor.gcs_client,
        )

    def _export_and_transcribe(
        self,
        request: FlushRequest,
    ) -> TranscriptionResult | None:
        """Concatenates buffered chunks, renders to an intermediate audio format, and dispatches to the inference client."""
        if self.audio_processor is None:
            msg = "AudioProcessor not initialized. setup() must be called."
            raise RuntimeError(msg)
        if self.transcriber is None:
            msg = "Transcriber not initialized. setup() must be called."
            raise RuntimeError(msg)

        if request.buffer is None or request.buffer.size == 0:
            return None

        success, flac_bytes, processed_audio = (
            self.audio_processor.process_buffer(request.buffer)
        )
        if not success or flac_bytes is None or processed_audio is None:
            self.vad_silence_count.inc()
            logger.info(
                "VAD detected no speech in buffer. Dropping transmission."
            )
            return None

        self.vad_speech_count.inc()
        duration_sec = len(processed_audio) / float(SAMPLE_RATE_HZ)
        self.speech_duration_sec_dist.update(int(duration_sec))

        if not self.config.canonical_audio_bucket:
            canonical_audio_uri, playback_audio_uri = None, None
        else:
            dt = datetime.fromtimestamp(
                request.time_range.start_ms / 1000.0, tz=UTC
            )

            flac_path = f"lossless/{request.feed_id}/{dt:%Y/%m/%d}/{request.transmission_id}.flac"
            m4a_path = f"playback/{request.feed_id}/{dt:%Y/%m/%d}/{request.transmission_id}.m4a"

            canonical_audio_uri, playback_audio_uri = (
                self.audio_uploader.upload_audio_derivatives(
                    bucket_name=self.config.canonical_audio_bucket,
                    flac_path=flac_path,
                    m4a_path=m4a_path,
                    flac_bytes=flac_bytes,
                    processed_audio=processed_audio,
                    export_m4a_fn=self.audio_processor.export_m4a,
                )
            )
        if not canonical_audio_uri and (
            request.contributing_audio_uris
            and len(request.contributing_audio_uris) == 1
        ):
            canonical_audio_uri = request.contributing_audio_uris[0]

        transcribe_start = time.time()

        transcript = self.transcriber.transcribe(
            audio_data=flac_bytes,
        )
        if transcript is None:
            logger.info("Transcription yielded no text. Dropping transmission.")
            return None
        duration_ms = int((time.time() - transcribe_start) * MS_PER_SECOND)
        self.transcription_time_ms.update(duration_ms)

        logger.info(f"TRANSCRIPT [{request.feed_id}]: {transcript}")

        if request.start_audio_offset_ms is None:
            msg = "Missing start_audio_offset_ms in FlushRequest"
            raise ValueError(msg)
        if request.end_audio_offset_ms is None:
            msg = "Missing end_audio_offset_ms in FlushRequest"
            raise ValueError(msg)
        if canonical_audio_uri is None:
            msg = "Missing canonical_audio_uri"
            raise ValueError(msg)
        if playback_audio_uri is None:
            msg = "Missing playback_audio_uri"
            raise ValueError(msg)

        return TranscriptionResult(
            feed_id=request.feed_id,
            session_id=request.session_id,
            contributing_audio_uris=request.contributing_audio_uris,
            transcript=transcript,
            time_range=request.time_range,
            transmission_id=request.transmission_id,
            missing_prior_context=request.missing_prior_context,
            missing_post_context=request.missing_post_context,
            start_audio_offset_ms=request.start_audio_offset_ms,
            end_audio_offset_ms=request.end_audio_offset_ms,
            canonical_audio_uri=canonical_audio_uri,
            playback_audio_uri=playback_audio_uri,
            feed_metadata=request.feed_metadata,
        )

    @override
    def process(  # type: ignore[override]
        self,
        element: tuple[str, FlushRequest],
        sequential_barrier: ReadModifyWriteRuntimeState = SEQUENTIAL_BARRIER_STATE,  # type: ignore
        *args: Any,
        **kwargs: Any,
    ) -> Iterator[TranscriptionResult | beam.pvalue.TaggedOutput]:
        """Submits the consolidated flushed buffer strictly sequentially to the external transcription API."""
        feed_id, request = element
        try:
            transcribed = self._export_and_transcribe(request)
            if transcribed:
                self.transcription_count.inc()
                yield transcribed
        except Exception as e:
            if not self.config.route_to_dlq:
                raise
            self.dlq_count.inc()
            logger.exception("Error transcribing buffer for feed %s", feed_id)
            msg = str(e)
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER_QUEUE_TAG, {"error": msg, "feed_id": feed_id}
            )

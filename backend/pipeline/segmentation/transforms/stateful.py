"""Stateful Apache Beam transforms for the radio transcription pipeline.

This module defines the core stateful and chronological restoration boundary
DoFns in our Apache Beam DAG. It houses transforms responsible for:
1. unmarshaling incoming Pub/Sub JSON elements.
2. restoration of out-of-order segment arrivals using SequenceBuffer.
3. stateful timer management for processing window timeout flushes.
4. execution of speech-to-text transcription API requests.

All high-level audio download/concatenation/VAD heuristics are cleanly
decoupled from Beam timer variables and delegated to StitcherEngine.


## Decoupled Stateful/Stateless Hybrid Architecture (Stage 2 & Stage 3)

To prevent Dataflow Windmill state locking and GIL contention bottlenecks, the audio
segmentation pipeline uses a decoupled, hybrid metadata/physical-retrieval flow:
1. **Stage 2 (Stateful - OrderedStitchAudioFn)**: Performs chronological sequencing,
   session FSM tracking, and VAD segment calculations. To keep persistent state sizes
   extremely small (<1 KB) and lock times under microseconds, **no raw audio bytes are stored
   in stateful cell persistent bag states**.
2. **Stage 3 (Stateless - UploadRawSegmentFn)**: Performs the heavy physical work of
   downloading contributing audio chunks from GCS, slicing them according to Stage 2's
   VAD segments, stitching them, and compressing the result to FLAC. Since this stage is
   completely stateless, Dataflow can distribute and execute these tasks in parallel across
   unlimited worker threads.


## Dataflow Windmill Execution Model

**This section is critical reading for anyone familiar with Beam on other runners
(Flink, Spark, Direct) but new to GCP Dataflow Streaming.**

### What is Windmill?

Windmill is Dataflow's internal streaming execution engine — the component that
manages work distribution, state persistence, and exactly-once semantics. It is
entirely invisible at the Beam API level: your DoFn code looks identical whether
it runs on Dataflow, Flink, or the Direct runner. However, Windmill has execution
constraints that don't exist on other runners and that materially affect how
stateful DoFns should be written.

### Bundle Leases

Windmill executes DoFn work in *bundles*. Each bundle is a unit of work assigned
to a worker thread with a **hard 300-second wall-clock lease**. If a bundle does
not complete and commit its outputs within that window, Windmill considers it
failed, rolls back all state changes from that bundle, and re-enqueues the work
for retry on another worker.

On other runners, bundles are either unbounded or their timeout behavior is
configurable and generally much more forgiving. On Dataflow Streaming, the 300s
limit is non-negotiable.

### Implications for Stateful DoFns

Stateful DoFns (those using `@StateSpec` and `@TimerSpec`) are particularly
affected because all elements sharing the same key are processed sequentially
within a single bundle. This means:

- If a feed's `out_of_order_buffer` contains N chunks and draining it takes
  longer than 300 seconds, the entire bundle is aborted and retried.
- Large backlogs can accumulate after pipeline restarts, upstream traffic spikes,
  or processing slowdowns (e.g., lock contention in worker threads). When the
  pipeline tries to drain a large backlog in a single bundle, it risks breaching
  the lease.
- Unlike element-level failures (which surface as exceptions and can be sent to
  a DLQ), a lease timeout produces no output at all — the work simply replays.

### The Self-Chaining Timer Pattern

To work around the lease limit without discarding data, we use a *self-chaining
timer* pattern:

1. `drain_ready_elements` is called with `max_emit=MAX_CHUNKS_PER_WINDMILL_BUNDLE`
   to cap the number of chunks processed in a single bundle.
2. If the buffer still has remaining chunks after the cap is hit (`clamped=True`),
   the DoFn immediately sets `gap_timer_event` to the current watermark
   timestamp (`gap_timer_event.set(timestamp)`). If running in live
   streaming, it also schedules `gap_timer_proc` in processing time.
3. Because the watermark timer fires at or before the current watermark, Windmill schedules
   a new bundle immediately, which calls the gap timeout handler and drains the next
   slice of up to `MAX_CHUNKS_PER_WINDMILL_BUNDLE` chunks.
4. This continues until the buffer is empty, at which point both timers are cleared and
   normal gap-timeout behavior resumes.

The constant `MAX_CHUNKS_PER_WINDMILL_BUNDLE` (defined in `constants.py`) is
sized so that `N_chunks x per_chunk_latency` stays well under 300 seconds. If
per-chunk processing or external I/O latency increases significantly in the
future, this value should be reduced accordingly.
"""

import heapq
import logging as std_logging
import time
from collections.abc import Iterable, Iterator
from dataclasses import replace
from typing import Any, override

import apache_beam as beam
from apache_beam.transforms.userstate import (
    ReadModifyWriteRuntimeState,
    ReadModifyWriteStateSpec,
    RuntimeTimer,
    TimerSpec,
    on_timer,
)
from apache_beam.utils.shared import Shared
from apache_beam.utils.timestamp import Timestamp

from backend.pipeline.common import constants as common_constants
from backend.pipeline.common import tracing_utils
from backend.pipeline.common.log_helper import get_logger, get_task_logger
from backend.pipeline.segmentation import coders as trans_coders
from backend.pipeline.segmentation import constants as trans_constants
from backend.pipeline.segmentation import datatypes
from backend.pipeline.segmentation.audio import vad
from backend.pipeline.segmentation.constants import (
    MAX_CHUNKS_PER_WINDMILL_BUNDLE,
    WINDMILL_TIMER_MIN_ADVANCE_SECS,
)
from backend.pipeline.segmentation.state import sequence_buffer
from backend.pipeline.segmentation.storage import (
    acquire_shared_gcs_client,
)
from backend.pipeline.segmentation.transforms import stitcher_engine

SHARED_RESOURCE_HANDLE = Shared()

logger = get_task_logger(
    __name__, {"system": "transcription", "component": "ordered-stitcher"}
)


def _get_task_logger(
    feed_id: str, session_id: str | None, component: str
) -> std_logging.LoggerAdapter:
    """Creates a contextual LoggerAdapter for tracing items through the pipeline."""
    return std_logging.LoggerAdapter(
        get_logger(__name__),
        {
            "system": "transcription",
            "component": component,
            "feed_id": feed_id,
            "session_id": session_id or "unknown",
        },
    )


def _write_transmission_context(
    state_cell: Any,
    context: datatypes.TransmissionContext,
) -> None:
    """Writes the context to transmission state or clears it completely if it is an empty IdleFeedState.

    Pruning completely empty IdleFeedStates from the database prevents session memory leaks and
    ensures that subsequent independent audio chunks start with a clean slate (initiating a new
    ActiveStitchingState and a fresh traceparent, effectively preventing Trace Context Hijacking).
    """
    if (
        isinstance(context, datatypes.IdleFeedState)
        and not context.out_of_order_buffer
        and not context.order_timer_active
    ):
        state_cell.clear()
    else:
        state_cell.write(context)


class StaleTimerManager:
    """Helper class to manage both event-time and processing-time stale timers."""

    def __init__(
        self,
        event_timer: RuntimeTimer,
        proc_timer: RuntimeTimer,
        config: datatypes.StitchAudioConfig,
    ) -> None:
        self.event_timer = event_timer
        self.proc_timer = proc_timer
        self.config = config

    def schedule(self, deadline_ms: int, *, is_backfill: bool) -> None:
        """Schedules either or both timers based on the backfill mode."""
        if is_backfill:
            # In backfill mode, use ONLY Event Time (Watermark) timer.
            self.proc_timer.clear()
            if deadline_ms > 0:
                deadline_s = deadline_ms / common_constants.MS_PER_SECOND
                self.event_timer.set(Timestamp(seconds=deadline_s))
            else:
                self.event_timer.clear()
        elif deadline_ms > 0:
            # Set event time timer based on data timeline
            deadline_s = deadline_ms / common_constants.MS_PER_SECOND
            self.event_timer.set(Timestamp(seconds=deadline_s))

            # Set processing time timer based on wall-clock time
            deadline_proc_s = (
                time.time()
                + self.config.stale_timeout_ms
                / float(common_constants.MS_PER_SECOND)
            )
            self.proc_timer.set(Timestamp(seconds=deadline_proc_s))
        else:
            self.event_timer.clear()
            self.proc_timer.clear()

    def clear(self) -> None:
        """Clears both timers."""
        self.event_timer.clear()
        self.proc_timer.clear()


def _handle_session_transition(
    curr_context: datatypes.TransmissionContext,
    metadata: datatypes.ChunkMetadata,
    gap_timer_event: RuntimeTimer,
    gap_timer_proc: RuntimeTimer,
    task_logger: Any,
) -> tuple[datatypes.ActiveStitchingState, bool]:
    """Handles transitions between active sessions or initialization from IdleFeedState."""
    if isinstance(curr_context, datatypes.IdleFeedState):
        new_context = datatypes.ActiveStitchingState(
            session_id=metadata.session_id,
            feed_metadata=metadata.feed_metadata,
            out_of_order_buffer=curr_context.out_of_order_buffer,
            order_timer_active=curr_context.order_timer_active,
            traceparent=metadata.traceparent
            or tracing_utils.get_current_traceparent(),
            baggage=metadata.baggage,
        )
        gap_timer_event.clear()
        gap_timer_proc.clear()
        return new_context, True

    if curr_context.session_id != metadata.session_id:
        task_logger.info(
            f"Session ID changed from {curr_context.session_id} to {metadata.session_id}. Resetting state."
        )
        gap_timer_event.clear()
        gap_timer_proc.clear()
        new_context = datatypes.ActiveStitchingState(
            session_id=metadata.session_id,
            feed_metadata=metadata.feed_metadata,
            traceparent=metadata.traceparent
            or tracing_utils.get_current_traceparent(),
            baggage=metadata.baggage,
        )
        return new_context, True

    if not isinstance(curr_context, datatypes.ActiveStitchingState):
        msg = "Invalid context type"
        raise TypeError(msg)
    return curr_context, False


def _manage_out_of_order_timers(
    gap_timer_event: RuntimeTimer,
    gap_timer_proc: RuntimeTimer,
    order_config: datatypes.OrderRestorerConfig,
    *,
    timestamp: Timestamp,
    clamped: bool,
    has_buffer_elements: bool,
    order_timer_active: bool,
    is_backfill: bool,
    old_expected_ts: int | None,
    new_expected_next_ts: int | None,
) -> bool:
    """Manages scheduling and clearing of out-of-order restoration timers.

    Timer management after a clamped or normal drain — three cases:

    1. CLAMPED (buffer still has chunks AND drain hit MAX_CHUNKS_PER_WINDMILL_BUNDLE):
       The previous drain stopped early to stay under Windmill's 300-second bundle
       lease. Setting the timer to `timestamp` (current watermark) causes Dataflow
       to fire handle_gap_timeout in the very next bundle, draining the next slice.
       Bundles chain this way until the buffer is empty. This prevents
       emitting oversized bundles that could exceed Windmill's bundle lease
       and get aborted/replayed (e.g., triggered by pipeline restarts,
       lock-induced slowdowns, or traffic spikes) by processing the queue in
       bounded bites and chaining to the next bundle.

    2. NORMAL GAP (buffer has chunks, drain was not clamped): schedule the
       standard gap-timeout deadline so we eventually flush even if some
       predecessors never arrive.

    3. DRAINED (buffer is now empty): clear the timer to avoid spurious firing.

    Returns the new state of order_timer_active.
    """
    if has_buffer_elements:
        if clamped:
            emitted_duration_ms = (
                (new_expected_next_ts - old_expected_ts)
                if (
                    old_expected_ts is not None
                    and new_expected_next_ts is not None
                )
                else 0
            )
            advance_sec = max(
                WINDMILL_TIMER_MIN_ADVANCE_SECS,
                float(emitted_duration_ms)
                / float(common_constants.MS_PER_SECOND),
            )
            gap_timer_event.set(timestamp + advance_sec)
            if is_backfill:
                gap_timer_proc.clear()
            else:
                gap_timer_proc.set(Timestamp(seconds=time.time() + advance_sec))
            return True

        if not order_timer_active:
            deadline_watermark = timestamp + (
                order_config.out_of_order_timeout_ms
                / float(common_constants.MS_PER_SECOND)
            )
            gap_timer_event.set(deadline_watermark)
            if is_backfill:
                gap_timer_proc.clear()
            else:
                deadline_proc = time.time() + (
                    order_config.out_of_order_timeout_ms
                    / float(common_constants.MS_PER_SECOND)
                )
                gap_timer_proc.set(Timestamp(seconds=deadline_proc))
            return True

        return order_timer_active

    if order_timer_active:
        gap_timer_event.clear()
        gap_timer_proc.clear()
        return False

    return order_timer_active


def process_ordering(
    element: tuple[str, datatypes.ChunkMetadata],
    timestamp: Timestamp,
    curr_context: datatypes.TransmissionContext,
    gap_timer_event: RuntimeTimer,
    gap_timer_proc: RuntimeTimer,
    order_config: datatypes.OrderRestorerConfig,
    *,
    is_backfill: bool,
    max_emit: int = MAX_CHUNKS_PER_WINDMILL_BUNDLE,
) -> tuple[list[datatypes.BufferedChunk], datatypes.ActiveStitchingState, bool]:
    """Handles session change detection and chronological ordering via SequenceBuffer."""
    feed_id, metadata = element
    task_logger = _get_task_logger(
        feed_id, metadata.session_id, "sequence-buffer"
    )

    curr_context, session_changed = _handle_session_transition(
        curr_context=curr_context,
        metadata=metadata,
        gap_timer_event=gap_timer_event,
        gap_timer_proc=gap_timer_proc,
        task_logger=task_logger,
    )

    seq_buf = sequence_buffer.SequenceBuffer(order_config)
    buffer_elements = curr_context.out_of_order_buffer
    current_ts_ms = (
        metadata.timestamp_ms
        if metadata.timestamp_ms is not None
        else int(float(timestamp) * common_constants.MS_PER_SECOND)
    )

    (
        new_expected_next_ts,
        new_buffer_elements,
        elements_to_emit,
        was_late,
        was_buffered,
    ) = seq_buf.process_chunk(
        current_ts_ms=current_ts_ms,
        gcs_uri=metadata.gcs_uri,
        expected_next_ts=curr_context.expected_next_chunk_start_ms,
        buffer_elements=buffer_elements,
        chunk_duration_ms=metadata.duration_ms,
        traceparent=metadata.traceparent,
        baggage=metadata.baggage,
        max_emit=max_emit,
    )

    if was_late:
        task_logger.debug(f"[Order] Late chunk: {metadata.gcs_uri}")
    if was_buffered:
        task_logger.debug(
            f"[Order] Buffered chunk from future: {metadata.gcs_uri}"
        )
    if elements_to_emit:
        task_logger.debug(f"[Order] Releasing {len(elements_to_emit)} chunks")

    # Update jitter buffer state
    old_expected_ts = curr_context.expected_next_chunk_start_ms
    curr_context = replace(
        curr_context,
        expected_next_chunk_start_ms=new_expected_next_ts,
        out_of_order_buffer=new_buffer_elements,
    )

    clamped = len(elements_to_emit) >= max_emit
    new_timer_active = _manage_out_of_order_timers(
        gap_timer_event=gap_timer_event,
        gap_timer_proc=gap_timer_proc,
        order_config=order_config,
        timestamp=timestamp,
        clamped=clamped,
        has_buffer_elements=bool(new_buffer_elements),
        order_timer_active=curr_context.order_timer_active,
        is_backfill=is_backfill,
        old_expected_ts=old_expected_ts,
        new_expected_next_ts=new_expected_next_ts,
    )
    curr_context = replace(curr_context, order_timer_active=new_timer_active)

    return elements_to_emit, curr_context, session_changed


def _evaluate_is_backfill(current_ts_ms: int, threshold_ms: int) -> bool:
    """Determines if a chunk is being processed in backfill/catch-up mode.

    In this context, "backfill" refers to catch-up processing when the pipeline
    comes back online after an outage, maintenance, or deployment. If a chunk's
    event time lags significantly behind current wall-clock processing time, it is
    flagged as backfill so downstream logic skips overlap validation and state updates.
    This prevents older catch-up timestamps from corrupting mainline sequence tracking.

    Args:
        current_ts_ms: The event timestamp of the incoming audio chunk in milliseconds.
        threshold_ms: Lateness threshold (in ms) beyond which a chunk is considered backfill.

    Returns:
        True if the chunk lateness meets or exceeds the backfill threshold.
    """
    lateness = time.time() * common_constants.MS_PER_SECOND - current_ts_ms
    return lateness >= threshold_ms


def _reschedule_gap_timeout(
    gap_timer_event: RuntimeTimer,
    gap_timer_proc: RuntimeTimer,
    order_config: datatypes.OrderRestorerConfig,
    *,
    timestamp: Timestamp,
    clamped: bool,
    is_backfill: bool,
    new_expected: int | None,
    new_expected_next_ts: int | None,
) -> bool:
    """Reschedules out-of-order restoration timers after a timeout drain."""
    if clamped:
        emitted_duration_ms = (
            (new_expected_next_ts - new_expected)
            if (new_expected is not None and new_expected_next_ts is not None)
            else 0
        )
        advance_sec = max(
            WINDMILL_TIMER_MIN_ADVANCE_SECS,
            float(emitted_duration_ms) / float(common_constants.MS_PER_SECOND),
        )
        deadline_watermark = timestamp + advance_sec
        deadline_proc = time.time() + advance_sec
    else:
        timeout_sec = order_config.out_of_order_timeout_ms / float(
            common_constants.MS_PER_SECOND
        )
        deadline_watermark = timestamp + timeout_sec
        deadline_proc = time.time() + timeout_sec

    gap_timer_event.set(deadline_watermark)
    if is_backfill:
        gap_timer_proc.clear()
    else:
        gap_timer_proc.set(Timestamp(seconds=deadline_proc))
    return True


@beam.typehints.with_input_types(tuple[str, datatypes.ChunkMetadata])
@beam.typehints.with_output_types(tuple[str, datatypes.FlushRequest])
class OrderedStitchAudioFn(beam.DoFn):
    """Stateful Apache Beam DoFn orchestrating out-of-order and stale windowing for continuous audio feeds.

    Enterprise Architectural Rationale: Why implement an explicit Jitter Buffer in Beam User State (`BagState`)
    rather than simply enabling native GCP Pub/Sub Subscription Ordering Keys?
    1. Total Autoscaler Head-of-Line Starvation: Pub/Sub subscription ordering strictly blocks delivering message #2
       until message #1 is fully computed and its official distributed network Acknowledgement (`Ack()`) is returned.
       In Beam, workers pull tens of thousands of messages in highly parallel, un-ordered work-stealing bundles.
       Enabling Pub/Sub ordering completely Head-of-Line starves the auto-scaled fleet (e.g., 99 worker pods sit
       100% completely idle waiting for a single pod to compute, complete Beam DAG traversal, and network `Ack()` chunk #1).
       Decoupling ordering into Beam State empowers our 100 worker machines to asynchronously ingest millions of chunks at maximum velocity.
    2. Exactly-Once ML FSM Protection: Pub/Sub fundamentally only guarantees `At-Least-Once Delivery`. Any transient
       network blip or VM preemption forces Pub/Sub to actively re-deliver un-acked duplicates. Our isolated Beam
       SequenceBuffer beautifully filters duplicate frames, entirely preventing false positive VAD speech boundaries.
    3. Bounded Micro-Batch Self-Chaining: When unrolling multi-day incident backlogs, emissions are clamped to
       `MAX_CHUNKS_PER_WINDMILL_BUNDLE` (10 chunks) and an Event-Time recursive timer is re-armed to open fresh worker
       bundles. This proves our worker heartbeats to central Windmill servers every ~3-4 seconds, entirely avoiding
       300-second bundle lease evictions ("poison pills") while ensuring perfectly smooth downstream side-input join checkpointing.
    """

    processed_in_bundle: int

    # --- State Specs ---

    TRANSMISSION_CONTEXT_SPEC = ReadModifyWriteStateSpec(
        "transmission_context", trans_coders.TransmissionContextCoder()
    )
    TRANSMISSION_CONTEXT_STATE = beam.DoFn.StateParam(TRANSMISSION_CONTEXT_SPEC)

    LAST_START_SPEC = ReadModifyWriteStateSpec(
        "last_start_ms", beam.coders.VarIntCoder()
    )
    LAST_START_MS_STATE = beam.DoFn.StateParam(LAST_START_SPEC)

    # --- Timers ---

    GAP_TIMER_EVENT_SPEC = TimerSpec(
        "gap_timer_event", beam.TimeDomain.WATERMARK
    )
    GAP_TIMER_EVENT = beam.DoFn.TimerParam(GAP_TIMER_EVENT_SPEC)

    GAP_TIMER_PROC_SPEC = TimerSpec("gap_timer_proc", beam.TimeDomain.REAL_TIME)
    GAP_TIMER_PROC = beam.DoFn.TimerParam(GAP_TIMER_PROC_SPEC)

    STALE_TIMER_EVENT_SPEC = TimerSpec(
        "stale_timer_event", beam.TimeDomain.WATERMARK
    )
    STALE_TIMER_EVENT_PARAM = beam.DoFn.TimerParam(STALE_TIMER_EVENT_SPEC)

    STALE_TIMER_PROC_SPEC = TimerSpec(
        "stale_timer_proc", beam.TimeDomain.REAL_TIME
    )
    STALE_TIMER_PROC_PARAM = beam.DoFn.TimerParam(STALE_TIMER_PROC_SPEC)

    DEFERRED_DRAIN_TIMER_SPEC = TimerSpec(
        "deferred_drain_timer", beam.TimeDomain.WATERMARK
    )
    DEFERRED_DRAIN_TIMER = beam.DoFn.TimerParam(DEFERRED_DRAIN_TIMER_SPEC)

    def __init__(
        self,
        order_config: datatypes.OrderRestorerConfig,
        stitch_config: datatypes.StitchAudioConfig,
    ) -> None:
        self.order_config = order_config
        self.stitch_config = stitch_config
        self.processed_in_bundle = 0

    @property
    def engine(self) -> Any:
        if not hasattr(self, "_engine_lazy"):
            self._engine_lazy = stitcher_engine.StitcherEngine(
                stitch_config=self.stitch_config,
                order_config=self.order_config,
                vad_config=self.stitch_config.vad_config,
            )
        return self._engine_lazy

    @engine.setter
    def engine(self, val: Any) -> None:
        self._engine_lazy = val

    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__.copy()
        if "_engine_lazy" in state:
            del state["_engine_lazy"]
        return state

    @override
    def setup(self) -> None:
        tracing_utils.setup_tracing(service_name="segmentation-pipeline")
        # Acquire process-level singletons natively via Beam's Shared handle
        shared_vad = SHARED_RESOURCE_HANDLE.acquire(
            lambda: vad.VoiceActivityDetector(models_dir=vad.MODELS_DIR),
            tag="vad",
        )

        shared_gcs = acquire_shared_gcs_client(
            project_id=self.stitch_config.project_id,
            shared_handle=SHARED_RESOURCE_HANDLE,
        )
        self.engine.processor.vad = shared_vad
        self.engine.processor.gcs_client = shared_gcs
        self.engine.setup()

    def start_bundle(self) -> None:
        """Initializes the bundle-level processed chunk counter to enforce lease limits."""
        self.processed_in_bundle = 0

    def _yield_tagged_outputs(
        self,
        results: Iterable[Any],
    ) -> Iterator[
        tuple[str, datatypes.FlushRequest] | beam.pvalue.TaggedOutput
    ]:
        """Yields results, tagging DLQ outputs appropriately."""
        for res in results:
            if (
                isinstance(res, tuple)
                and res[0] == trans_constants.DEAD_LETTER_QUEUE_TAG
            ):
                yield beam.pvalue.TaggedOutput(
                    trans_constants.DEAD_LETTER_QUEUE_TAG, res[1]
                )
            else:
                yield res

    @override
    def process(
        self,
        element: tuple[str, datatypes.ChunkMetadata],
        timestamp: Timestamp = beam.DoFn.TimestampParam,  # type: ignore
        transmission_context_state: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        last_start_ms_state: ReadModifyWriteRuntimeState = LAST_START_MS_STATE,  # type: ignore
        gap_timer_event: RuntimeTimer = GAP_TIMER_EVENT,  # type: ignore
        gap_timer_proc: RuntimeTimer = GAP_TIMER_PROC,  # type: ignore
        stale_timer_event: RuntimeTimer = STALE_TIMER_EVENT_PARAM,  # type: ignore
        stale_timer_proc: RuntimeTimer = STALE_TIMER_PROC_PARAM,  # type: ignore
        deferred_drain_timer: RuntimeTimer = DEFERRED_DRAIN_TIMER,  # type: ignore
    ) -> Iterator[
        tuple[str, datatypes.FlushRequest] | beam.pvalue.TaggedOutput
    ]:
        """Intercepts chunk arrival, resolves chronological ordering, and delegates to StitcherEngine."""
        feed_id, metadata = element
        trace_attrs = {}
        if metadata.traceparent:
            trace_attrs["traceparent"] = metadata.traceparent
        if metadata.baggage:
            trace_attrs["baggage"] = metadata.baggage

        # Windmill lease guard: if we've already processed MAX_CHUNKS_PER_WINDMILL_BUNDLE
        # in this bundle, defer this chunk to the next bundle by pushing it directly
        # to the out-of-order buffer and setting the self-chaining deferral timer.
        #
        # NOTE ON TRACEBACK MISDIRECTION (FUTURE DEBUGGING):
        # Under backlog, Dataflow bundles many elements for the same key. Because they
        # are processed sequentially on a single thread, the collective time of running
        # VAD on all elements can exceed Windmill's 300s lease limit. Because VAD ONNX
        # inference is CPU-heavy (occupying 95%+ of CPU time), any worker thread dump
        # captured during a timeout will show a traceback inside `vad.py` / `onnxruntime`.
        # This is a classic RED HERRING: the VAD itself is not deadlocked or slow. It is
        # simply being executed too many times sequentially. Capping the execution to 5
        # chunks per bundle here guarantees we stay well under the 300s limit and prevents
        # this timeout entirely.
        if (
            self.processed_in_bundle
            >= trans_constants.MAX_CHUNKS_PER_WINDMILL_BUNDLE
        ):
            curr_context = (
                transmission_context_state.read() or datatypes.IdleFeedState()
            )
            if isinstance(curr_context, datatypes.IdleFeedState):
                curr_context = datatypes.ActiveStitchingState(
                    session_id=metadata.session_id,
                    feed_metadata=metadata.feed_metadata,
                    out_of_order_buffer=curr_context.out_of_order_buffer,
                    order_timer_active=curr_context.order_timer_active,
                    traceparent=metadata.traceparent,
                    baggage=metadata.baggage,
                )

            buffer_elements = list(curr_context.out_of_order_buffer)
            heap = [sequence_buffer.ComparableChunk(c) for c in buffer_elements]
            heapq.heapify(heap)
            current_ts_ms = (
                metadata.timestamp_ms
                if metadata.timestamp_ms is not None
                else int(float(timestamp) * common_constants.MS_PER_SECOND)
            )
            heapq.heappush(
                heap,
                sequence_buffer.ComparableChunk(
                    datatypes.BufferedChunk(
                        current_ts_ms,
                        metadata.gcs_uri,
                        metadata.traceparent,
                        metadata.baggage,
                    )
                ),
            )
            new_buffer = [item.chunk for item in heap]

            curr_context = replace(
                curr_context,
                out_of_order_buffer=new_buffer,
            )
            _write_transmission_context(
                transmission_context_state, curr_context
            )
            deferred_drain_timer.set(timestamp)
            return

        results = []
        with tracing_utils.with_tracer_context(
            trace_attrs, "stitching_process", __name__
        ):
            current_ts_ms = int(
                float(timestamp) * common_constants.MS_PER_SECOND
            )
            is_backfill = _evaluate_is_backfill(
                current_ts_ms,
                self.stitch_config.backfill_lateness_threshold_ms,
            )
            curr_context = (
                transmission_context_state.read() or datatypes.IdleFeedState()
            )
            previous_expected_ts = (
                curr_context.expected_next_chunk_start_ms
                if isinstance(curr_context, datatypes.ActiveStitchingState)
                else None
            )

            # Handle chronological sequence buffering
            elements_to_emit, curr_context, session_changed = process_ordering(
                element,
                timestamp,
                curr_context,
                gap_timer_event,
                gap_timer_proc,
                self.order_config,
                is_backfill=is_backfill,
                max_emit=trans_constants.MAX_CHUNKS_PER_WINDMILL_BUNDLE
                - self.processed_in_bundle,
            )

            task_logger = _get_task_logger(
                feed_id, curr_context.session_id, "transcription-stitcher"
            )
            task_logger.debug(f"[Process] Processing chunk {metadata.gcs_uri}")

            if session_changed:
                stale_timer_event.clear()
                stale_timer_proc.clear()
                curr_context = replace(
                    curr_context, prior_audio_tail=None, sample_rate=None
                )

            # Commit initial sequence context updates
            _write_transmission_context(
                transmission_context_state, curr_context
            )

            # Delegate chunk elements to the execution engine
            if elements_to_emit:
                timer_manager = StaleTimerManager(
                    stale_timer_event, stale_timer_proc, self.stitch_config
                )

                is_backfill = _evaluate_is_backfill(
                    current_ts_ms,
                    self.stitch_config.backfill_lateness_threshold_ms,
                )

                for i, chunk in enumerate(elements_to_emit):
                    # Fetch current state context
                    curr_context = (
                        transmission_context_state.read()
                        or datatypes.IdleFeedState()
                    )
                    if isinstance(curr_context, datatypes.IdleFeedState):
                        curr_context = datatypes.ActiveStitchingState(
                            session_id=metadata.session_id,
                            feed_metadata=metadata.feed_metadata,
                            out_of_order_buffer=curr_context.out_of_order_buffer,
                            order_timer_active=curr_context.order_timer_active,
                            traceparent=metadata.traceparent,
                            baggage=metadata.baggage,
                        )
                    outputs, next_expected_ts = (
                        self.engine.process_ordering_chunk(
                            chunk=chunk,
                            feed_id=feed_id,
                            curr_context=curr_context,
                            transmission_context_state=transmission_context_state,
                            last_start_ms_state=last_start_ms_state,
                            timer_manager=timer_manager,
                            previous_expected_ts=previous_expected_ts,
                            is_backfill=is_backfill,
                            clear_buffer=(session_changed and i == 0),
                        )
                    )
                    results.extend(outputs)
                    previous_expected_ts = next_expected_ts
                    self.processed_in_bundle += 1

        yield from self._yield_tagged_outputs(results)

    @on_timer(GAP_TIMER_EVENT_SPEC)
    def handle_gap_timeout_event(
        self,
        feed_id: str = beam.DoFn.KeyParam,  # type: ignore
        transmission_context_state: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        last_start_ms_state: ReadModifyWriteRuntimeState = LAST_START_MS_STATE,  # type: ignore
        stale_timer_event: RuntimeTimer = STALE_TIMER_EVENT_PARAM,  # type: ignore
        stale_timer_proc: RuntimeTimer = STALE_TIMER_PROC_PARAM,  # type: ignore
        timestamp: Timestamp = beam.DoFn.TimestampParam,  # type: ignore
        gap_timer_event: RuntimeTimer = GAP_TIMER_EVENT,  # type: ignore
        gap_timer_proc: RuntimeTimer = GAP_TIMER_PROC,  # type: ignore
    ) -> Iterator[
        tuple[str, datatypes.FlushRequest] | beam.pvalue.TaggedOutput
    ]:
        """Handles the gap timeout triggered by the event-time watermark."""
        yield from self._handle_gap_timeout_common(
            feed_id=feed_id,
            transmission_context_state=transmission_context_state,
            last_start_ms_state=last_start_ms_state,
            stale_timer_event=stale_timer_event,
            stale_timer_proc=stale_timer_proc,
            timestamp=timestamp,
            gap_timer_event=gap_timer_event,
            gap_timer_proc=gap_timer_proc,
            timer_type="event",
        )

    @on_timer(GAP_TIMER_PROC_SPEC)
    def handle_gap_timeout_processing(
        self,
        feed_id: str = beam.DoFn.KeyParam,  # type: ignore
        transmission_context_state: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        last_start_ms_state: ReadModifyWriteRuntimeState = LAST_START_MS_STATE,  # type: ignore
        stale_timer_event: RuntimeTimer = STALE_TIMER_EVENT_PARAM,  # type: ignore
        stale_timer_proc: RuntimeTimer = STALE_TIMER_PROC_PARAM,  # type: ignore
        timestamp: Timestamp = beam.DoFn.TimestampParam,  # type: ignore
        gap_timer_event: RuntimeTimer = GAP_TIMER_EVENT,  # type: ignore
        gap_timer_proc: RuntimeTimer = GAP_TIMER_PROC,  # type: ignore
    ) -> Iterator[
        tuple[str, datatypes.FlushRequest] | beam.pvalue.TaggedOutput
    ]:
        """Handles the gap timeout triggered by the processing-time clock."""
        yield from self._handle_gap_timeout_common(
            feed_id=feed_id,
            transmission_context_state=transmission_context_state,
            last_start_ms_state=last_start_ms_state,
            stale_timer_event=stale_timer_event,
            stale_timer_proc=stale_timer_proc,
            timestamp=timestamp,
            gap_timer_event=gap_timer_event,
            gap_timer_proc=gap_timer_proc,
            timer_type="processing",
        )

    def _handle_gap_timeout_common(
        self,
        feed_id: str,
        transmission_context_state: ReadModifyWriteRuntimeState,
        last_start_ms_state: ReadModifyWriteRuntimeState,
        stale_timer_event: RuntimeTimer,
        stale_timer_proc: RuntimeTimer,
        timestamp: Timestamp,
        gap_timer_event: RuntimeTimer,
        gap_timer_proc: RuntimeTimer,
        *,
        timer_type: str,
    ) -> Iterator[
        tuple[str, datatypes.FlushRequest] | beam.pvalue.TaggedOutput
    ]:
        """Handles the gap timeout by advancing the expected sequence (common logic)."""
        curr_context = (
            transmission_context_state.read() or datatypes.IdleFeedState()
        )
        if isinstance(curr_context, datatypes.IdleFeedState):
            return
        trace_attrs: dict[str, str] = {}
        if curr_context.traceparent:
            trace_attrs["traceparent"] = curr_context.traceparent
        if curr_context.baggage:
            trace_attrs["baggage"] = curr_context.baggage
        active_session_id = curr_context.session_id
        active_feed_metadata = curr_context.feed_metadata
        active_traceparent = curr_context.traceparent
        active_baggage = curr_context.baggage

        results = []
        with tracing_utils.with_tracer_context(
            trace_attrs, "handle_audio_gap", __name__
        ):
            gap_timer_event.clear()
            gap_timer_proc.clear()
            curr_context = replace(curr_context, order_timer_active=False)
            _write_transmission_context(
                transmission_context_state, curr_context
            )

            buffer_elements = curr_context.out_of_order_buffer
            if buffer_elements:
                sorted_elements = sorted(
                    buffer_elements, key=lambda x: x.timestamp_ms
                )
                new_expected = sorted_elements[0].timestamp_ms

                logger.warning(
                    f"[{feed_id}] Gap timeout ({timer_type})! Advancing expected from {curr_context.expected_next_chunk_start_ms} to {new_expected}."
                )

                curr_context = replace(
                    curr_context,
                    expected_next_chunk_start_ms=new_expected,
                    missing_prior_context=True,
                )
                _write_transmission_context(
                    transmission_context_state, curr_context
                )

                seq_buf = sequence_buffer.SequenceBuffer(self.order_config)

                new_expected_next_ts, new_buffer_elements, elements_to_emit = (
                    seq_buf.drain_ready_elements(
                        expected_next_ts=new_expected,
                        buffer_elements=buffer_elements,
                        epsilon_ms=trans_constants.DEFAULT_FLOAT_TOLERANCE_MS,
                        max_emit=MAX_CHUNKS_PER_WINDMILL_BUNDLE,
                    )
                )

                first_chunk_ts = sorted_elements[0].timestamp_ms
                is_backfill = _evaluate_is_backfill(
                    first_chunk_ts,
                    self.stitch_config.backfill_lateness_threshold_ms,
                )

                clamped = (
                    len(elements_to_emit) >= MAX_CHUNKS_PER_WINDMILL_BUNDLE
                )
                if new_buffer_elements:
                    timer_active = _reschedule_gap_timeout(
                        gap_timer_event=gap_timer_event,
                        gap_timer_proc=gap_timer_proc,
                        order_config=self.order_config,
                        timestamp=timestamp,
                        clamped=clamped,
                        is_backfill=is_backfill,
                        new_expected=new_expected,
                        new_expected_next_ts=new_expected_next_ts,
                    )
                    curr_context = replace(
                        curr_context, order_timer_active=timer_active
                    )

                curr_context = replace(
                    curr_context,
                    expected_next_chunk_start_ms=new_expected_next_ts,
                    out_of_order_buffer=new_buffer_elements,
                )
                _write_transmission_context(
                    transmission_context_state, curr_context
                )

                # Handle ready elements
                if elements_to_emit:
                    timer_manager = StaleTimerManager(
                        stale_timer_event, stale_timer_proc, self.stitch_config
                    )

                    previous_expected_ts = new_expected

                    for chunk in elements_to_emit:
                        curr_context = (
                            transmission_context_state.read()
                            or datatypes.IdleFeedState()
                        )
                        if isinstance(curr_context, datatypes.IdleFeedState):
                            curr_context = datatypes.ActiveStitchingState(
                                session_id=active_session_id,
                                feed_metadata=active_feed_metadata,
                                out_of_order_buffer=curr_context.out_of_order_buffer,
                                order_timer_active=curr_context.order_timer_active,
                                traceparent=active_traceparent,
                                baggage=active_baggage,
                            )
                        outputs, next_expected_ts = (
                            self.engine.process_ordering_chunk(
                                chunk=chunk,
                                feed_id=feed_id,
                                curr_context=curr_context,
                                transmission_context_state=transmission_context_state,
                                last_start_ms_state=last_start_ms_state,
                                timer_manager=timer_manager,
                                previous_expected_ts=previous_expected_ts,
                                is_backfill=is_backfill,
                            )
                        )
                        results.extend(outputs)
                        previous_expected_ts = next_expected_ts

        yield from self._yield_tagged_outputs(results)

    @on_timer(DEFERRED_DRAIN_TIMER_SPEC)
    def handle_deferred_drain(
        self,
        feed_id: str = beam.DoFn.KeyParam,  # type: ignore
        transmission_context_state: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        last_start_ms_state: ReadModifyWriteRuntimeState = LAST_START_MS_STATE,  # type: ignore
        stale_timer_event: RuntimeTimer = STALE_TIMER_EVENT_PARAM,  # type: ignore
        stale_timer_proc: RuntimeTimer = STALE_TIMER_PROC_PARAM,  # type: ignore
        timestamp: Timestamp = beam.DoFn.TimestampParam,  # type: ignore
        deferred_drain_timer: RuntimeTimer = DEFERRED_DRAIN_TIMER,  # type: ignore
    ) -> Iterator[
        tuple[str, datatypes.FlushRequest] | beam.pvalue.TaggedOutput
    ]:
        """Drains deferred chunks from the sequence buffer in a fresh bundle."""
        curr_context = (
            transmission_context_state.read() or datatypes.IdleFeedState()
        )
        if isinstance(curr_context, datatypes.IdleFeedState):
            return
        trace_attrs: dict[str, str] = {}
        if curr_context.traceparent:
            trace_attrs["traceparent"] = curr_context.traceparent
        if curr_context.baggage:
            trace_attrs["baggage"] = curr_context.baggage
        active_session_id = curr_context.session_id
        active_feed_metadata = curr_context.feed_metadata
        active_traceparent = curr_context.traceparent
        active_baggage = curr_context.baggage

        results = []
        with tracing_utils.with_tracer_context(
            trace_attrs, "deferred_drain", __name__
        ):
            # We do NOT set missing_prior_context=True, keeping the continuous tail!
            seq_buf = sequence_buffer.SequenceBuffer(self.order_config)
            buffer_elements = curr_context.out_of_order_buffer

            # Cap the drain based on our remaining bundle capacity.
            # In a fresh timer-activated bundle, processed_in_bundle starts at 0, so
            # we can drain up to the full MAX_CHUNKS_PER_WINDMILL_BUNDLE (5 chunks).
            new_expected_next_ts, new_buffer_elements, elements_to_emit = (
                seq_buf.drain_ready_elements(
                    expected_next_ts=curr_context.expected_next_chunk_start_ms,
                    buffer_elements=buffer_elements,
                    epsilon_ms=trans_constants.DEFAULT_FLOAT_TOLERANCE_MS,
                    max_emit=trans_constants.MAX_CHUNKS_PER_WINDMILL_BUNDLE
                    - self.processed_in_bundle,
                )
            )

            clamped = len(elements_to_emit) >= (
                trans_constants.MAX_CHUNKS_PER_WINDMILL_BUNDLE
                - self.processed_in_bundle
            )
            if new_buffer_elements and clamped:
                # Still clamped, re-arm the deferral timer to self-chain into another bundle!
                deferred_drain_timer.set(timestamp)

            curr_context = replace(
                curr_context,
                expected_next_chunk_start_ms=new_expected_next_ts,
                out_of_order_buffer=new_buffer_elements,
            )
            _write_transmission_context(
                transmission_context_state, curr_context
            )

            if elements_to_emit:
                timer_manager = StaleTimerManager(
                    stale_timer_event, stale_timer_proc, self.stitch_config
                )

                # Assume backfill under backlog
                is_backfill = True
                previous_expected_ts = curr_context.expected_next_chunk_start_ms

                for chunk in elements_to_emit:
                    # Fetch current state context
                    curr_context = (
                        transmission_context_state.read()
                        or datatypes.IdleFeedState()
                    )
                    if isinstance(curr_context, datatypes.IdleFeedState):
                        curr_context = datatypes.ActiveStitchingState(
                            session_id=active_session_id,
                            feed_metadata=active_feed_metadata,
                            out_of_order_buffer=curr_context.out_of_order_buffer,
                            order_timer_active=curr_context.order_timer_active,
                            traceparent=active_traceparent,
                            baggage=active_baggage,
                        )
                    outputs, next_expected_ts = (
                        self.engine.process_ordering_chunk(
                            chunk=chunk,
                            feed_id=feed_id,
                            curr_context=curr_context,
                            transmission_context_state=transmission_context_state,
                            last_start_ms_state=last_start_ms_state,
                            timer_manager=timer_manager,
                            previous_expected_ts=previous_expected_ts,
                            is_backfill=is_backfill,
                        )
                    )
                    results.extend(outputs)
                    previous_expected_ts = next_expected_ts
                    self.processed_in_bundle += 1

        yield from self._yield_tagged_outputs(results)

    @on_timer(STALE_TIMER_EVENT_SPEC)
    def handle_stale_transmission_event(
        self,
        key: str = beam.DoFn.KeyParam,  # type: ignore
        transmission_context: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        last_start_ms_state: ReadModifyWriteRuntimeState = LAST_START_MS_STATE,  # type: ignore
        stale_timer_event: RuntimeTimer = STALE_TIMER_EVENT_PARAM,  # type: ignore
        stale_timer_proc: RuntimeTimer = STALE_TIMER_PROC_PARAM,  # type: ignore
    ) -> Iterator[
        tuple[str, datatypes.FlushRequest] | beam.pvalue.TaggedOutput
    ]:
        """Watermark crossed stale duration, delegate flush to StitcherEngine."""
        timer_manager = StaleTimerManager(
            stale_timer_event, stale_timer_proc, self.stitch_config
        )
        yield from self._yield_tagged_outputs(
            self.engine.handle_stale_transmission(
                key,
                transmission_context,
                last_start_ms_state,
                timer_manager,
            )
        )

    @on_timer(STALE_TIMER_PROC_SPEC)
    def handle_stale_transmission_proc(
        self,
        key: str = beam.DoFn.KeyParam,  # type: ignore
        transmission_context: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        last_start_ms_state: ReadModifyWriteRuntimeState = LAST_START_MS_STATE,  # type: ignore
        stale_timer_event: RuntimeTimer = STALE_TIMER_EVENT_PARAM,  # type: ignore
        stale_timer_proc: RuntimeTimer = STALE_TIMER_PROC_PARAM,  # type: ignore
    ) -> Iterator[
        tuple[str, datatypes.FlushRequest] | beam.pvalue.TaggedOutput
    ]:
        """Wall-clock crossed stale duration, delegate flush to StitcherEngine."""
        timer_manager = StaleTimerManager(
            stale_timer_event, stale_timer_proc, self.stitch_config
        )
        yield from self._yield_tagged_outputs(
            self.engine.handle_stale_transmission(
                key,
                transmission_context,
                last_start_ms_state,
                timer_manager,
            )
        )

    @property
    def audio_processor(self) -> Any:
        if hasattr(self, "engine"):
            return self.engine.processor
        return None

    @audio_processor.setter
    def audio_processor(self, val: Any) -> None:
        if hasattr(self, "engine"):
            self.engine.processor = val
